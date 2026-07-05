(ns event-sourcing.store
  "SQLite-backed event store using next.jdbc.

   Supports appending events with optimistic concurrency, loading event streams,
   managing snapshots, and retrieving all events for projection catch-up."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.data.json :as json]
            [event-sourcing.aggregate :as agg]))

;; ---------------------------------------------------------------------------
;; Schema initialization
;; ---------------------------------------------------------------------------

(def ^:private create-tables-sql
  ["CREATE TABLE IF NOT EXISTS events (
      sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
      aggregate_id    TEXT    NOT NULL,
      version         INTEGER NOT NULL,
      event_type      TEXT    NOT NULL,
      payload         TEXT    NOT NULL,
      timestamp       TEXT    NOT NULL,
      event_id        TEXT    NOT NULL,
      UNIQUE(aggregate_id, version)
    )"
   "CREATE INDEX IF NOT EXISTS ix_events_aggregate_id ON events(aggregate_id)"
   "CREATE TABLE IF NOT EXISTS snapshots (
      aggregate_id   TEXT    NOT NULL PRIMARY KEY,
      version        INTEGER NOT NULL,
      aggregate_type TEXT    NOT NULL,
      state          TEXT    NOT NULL,
      timestamp      TEXT    NOT NULL
    )"])

(defn create-datasource
  "Creates a next.jdbc datasource for the given SQLite database path."
  [db-path]
  (jdbc/get-datasource {:dbtype "sqlite" :dbname db-path}))

(defn initialize!
  "Creates the required database tables if they do not already exist."
  [ds]
  (jdbc/with-transaction [tx ds]
    (doseq [sql create-tables-sql]
      (jdbc/execute! tx [sql]))))

;; ---------------------------------------------------------------------------
;; Event persistence
;; ---------------------------------------------------------------------------

(defn append-events!
  "Appends a batch of events for a given aggregate with optimistic concurrency.
   Throws an exception if the current version does not match expected-version."
  [ds aggregate-id events expected-version]
  (jdbc/with-transaction [tx ds]
    (let [result (jdbc/execute-one! tx
                   ["SELECT MAX(version) AS max_version FROM events WHERE aggregate_id = ?"
                    aggregate-id]
                   {:builder-fn rs/as-unqualified-lower-maps})
          current-version (or (:max_version result) 0)]
      (when (not= current-version expected-version)
        (throw (ex-info "Concurrency conflict"
                        {:aggregate-id     aggregate-id
                         :expected-version expected-version
                         :actual-version   current-version})))
      (doseq [event events]
        (jdbc/execute! tx
          ["INSERT INTO events (aggregate_id, version, event_type, payload, timestamp, event_id)
            VALUES (?, ?, ?, ?, ?, ?)"
           aggregate-id
           (:version event)
           (name (:event-type event))
           (json/write-str (dissoc event :event-type :version))
           (str (or (:timestamp event) (java.time.Instant/now)))
           (str (or (:event-id event) (java.util.UUID/randomUUID)))])))))

(defn- row->event
  "Converts a database row map to a domain event map."
  [row]
  (let [payload (json/read-str (:payload row) :key-fn keyword)]
    (merge payload
           {:event-type (keyword (:event_type row))
            :version    (:version row)
            :event-id   (:event_id row)
            :timestamp  (:timestamp row)
            :aggregate-id (:aggregate_id row)
            :sequence-number (:sequence_number row)})))

(defn get-events
  "Retrieves all events for an aggregate, optionally starting after a given version."
  ([ds aggregate-id] (get-events ds aggregate-id 0))
  ([ds aggregate-id after-version]
   (let [rows (jdbc/execute! ds
                ["SELECT sequence_number, aggregate_id, version, event_type, payload, timestamp, event_id
                  FROM events
                  WHERE aggregate_id = ? AND version > ?
                  ORDER BY version"
                 aggregate-id after-version]
                {:builder-fn rs/as-unqualified-lower-maps})]
     (mapv row->event rows))))

(defn get-all-events
  "Retrieves all events across all aggregates, optionally after a global sequence number."
  ([ds] (get-all-events ds 0))
  ([ds after-sequence]
   (let [rows (jdbc/execute! ds
                ["SELECT sequence_number, aggregate_id, version, event_type, payload, timestamp, event_id
                  FROM events
                  WHERE sequence_number > ?
                  ORDER BY sequence_number"
                 after-sequence]
                {:builder-fn rs/as-unqualified-lower-maps})]
     (mapv row->event rows))))

;; ---------------------------------------------------------------------------
;; Snapshots
;; ---------------------------------------------------------------------------

(defn save-snapshot!
  "Saves a snapshot of an aggregate's state, upserting by aggregate-id."
  [ds snapshot]
  (jdbc/execute! ds
    ["INSERT INTO snapshots (aggregate_id, version, aggregate_type, state, timestamp)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(aggregate_id) DO UPDATE SET
        version = excluded.version,
        aggregate_type = excluded.aggregate_type,
        state = excluded.state,
        timestamp = excluded.timestamp"
     (:aggregate-id snapshot)
     (:version snapshot)
     (:aggregate-type snapshot)
     (json/write-str (:state snapshot))
     (str (:timestamp snapshot))]))

(defn get-latest-snapshot
  "Loads the most recent snapshot for an aggregate, or nil if none exists."
  [ds aggregate-id]
  (when-let [row (jdbc/execute-one! ds
                   ["SELECT aggregate_id, version, aggregate_type, state, timestamp
                     FROM snapshots
                     WHERE aggregate_id = ?"
                    aggregate-id]
                   {:builder-fn rs/as-unqualified-lower-maps})]
    {:aggregate-id   (:aggregate_id row)
     :version        (:version row)
     :aggregate-type (:aggregate_type row)
     :state          (json/read-str (:state row) :key-fn keyword)
     :timestamp      (:timestamp row)}))

;; ---------------------------------------------------------------------------
;; Aggregate repository
;; ---------------------------------------------------------------------------

(defn load-aggregate
  "Loads an aggregate by replaying its event stream. If a snapshot exists,
   only events after the snapshot version are replayed.

   Parameters:
     ds        - datasource
     initial   - initial empty aggregate map (must implement AggregateRoot)
     agg-id    - aggregate identifier string"
  [ds initial agg-id]
  (let [snapshot (get-latest-snapshot ds agg-id)
        base (if snapshot
               (agg/restore-from-snapshot initial snapshot)
               (assoc initial :aggregate-id agg-id :version 0 :uncommitted []))
        after-version (:version base 0)
        events (get-events ds agg-id after-version)]
    (agg/replay-events base events)))

(defn save-aggregate!
  "Persists all uncommitted events from the aggregate to the event store.
   Creates a snapshot if the snapshot interval has been reached.
   Returns the aggregate with uncommitted events cleared.

   Parameters:
     ds                - datasource
     aggregate         - the aggregate map
     snapshot-interval - number of events between snapshots (0 to disable)"
  ([ds aggregate] (save-aggregate! ds aggregate 50))
  ([ds aggregate snapshot-interval]
   (let [uncommitted (agg/get-uncommitted-events aggregate)]
     (when (seq uncommitted)
       (let [agg-id (:aggregate-id aggregate)
             new-version (:version aggregate)
             previous-version (- new-version (count uncommitted))]
         (append-events! ds agg-id uncommitted previous-version)
         ;; Snapshot when the committed range (previous-version, new-version]
         ;; crosses at least one multiple of snapshot-interval. Using version
         ;; buckets ensures a multi-event batch that jumps over the boundary
         ;; (e.g. 3 -> 7 with interval 5) still snapshots exactly once, while
         ;; repeated single-event saves snapshot at each boundary only once.
         (when (and (pos? snapshot-interval)
                    (> (quot new-version snapshot-interval)
                       (quot previous-version snapshot-interval)))
           (save-snapshot! ds (agg/create-snapshot aggregate)))))
     (agg/clear-uncommitted-events aggregate))))
