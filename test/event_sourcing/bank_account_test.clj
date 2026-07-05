(ns event-sourcing.bank-account-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-sourcing.aggregate :as agg]
            [event-sourcing.store :as store]
            [event-sourcing.projection :as proj]
            [event-sourcing.sample.bank-account :as bank]))

(deftest open-creates-account-with-initial-deposit
  (let [account (bank/open-account "acc-1" "Alice" 100.0)]
    (is (= "acc-1" (:aggregate-id account)))
    (is (= "Alice" (:account-holder account)))
    (is (= 100.0 (:balance account)))
    (is (= 1 (:version account)))))

(deftest deposit-increases-balance
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0 :description "Paycheck"))]
    (is (= 150.0 (:balance account)))
    (is (= 2 (:version account)))))

(deftest withdraw-decreases-balance
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/withdraw 30.0 :description "Coffee"))]
    (is (= 70.0 (:balance account)))))

(deftest withdraw-insufficient-funds-throws
  (let [account (bank/open-account "acc-1" "Alice" 50.0)]
    (is (thrown? clojure.lang.ExceptionInfo
                 (bank/withdraw account 100.0)))))

(deftest transfer-decreases-source-balance
  (let [account (-> (bank/open-account "acc-1" "Alice" 200.0)
                    (bank/transfer-to "acc-2" 75.0 :description "Rent"))]
    (is (= 125.0 (:balance account)))))

(deftest receive-transfer-increases-balance
  (let [account (-> (bank/open-account "acc-2" "Bob" 50.0)
                    (bank/receive-transfer "acc-1" 75.0 :description "Rent"))]
    (is (= 125.0 (:balance account)))))

(deftest uncommitted-events-tracks-all-raised-events
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0)
                    (bank/withdraw 20.0))]
    (is (= 3 (count (agg/get-uncommitted-events account))))))

(deftest clear-uncommitted-events-empties-list
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0)
                    agg/clear-uncommitted-events)]
    (is (empty? (agg/get-uncommitted-events account)))))

(deftest snapshot-round-trip-restores-state
  (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                    (bank/deposit 50.0)
                    (bank/withdraw 20.0))
        snapshot (agg/create-snapshot account)
        restored (agg/restore-from-snapshot (bank/make-bank-account) snapshot)]
    (is (= "acc-1" (:aggregate-id restored)))
    (is (= "Alice" (:account-holder restored)))
    (is (= 130.0 (:balance restored)))
    (is (= 3 (:version restored)))))

(deftest event-store-round-trip
  (testing "Events can be stored and replayed to rebuild aggregate state"
    (let [db-path (str "test_bank_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (let [account (-> (bank/open-account "acc-1" "Alice" 100.0)
                          (bank/deposit 50.0)
                          (bank/withdraw 20.0))
              _ (store/save-aggregate! ds account 0)
              reloaded (store/load-aggregate ds (bank/make-bank-account) "acc-1")]
          (is (= "acc-1" (:aggregate-id reloaded)))
          (is (= "Alice" (:account-holder reloaded)))
          (is (= 130.0 (:balance reloaded)))
          (is (= 3 (:version reloaded)))
          (is (empty? (agg/get-uncommitted-events reloaded))))
        (finally
          (.delete (java.io.File. db-path)))))))

(deftest snapshot-created-when-batch-crosses-interval-boundary
  (testing "A multi-event save that jumps over an interval boundary still snapshots"
    (let [db-path (str "test_snap_batch_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        ;; Interval 5. First save: versions 1..3 (no boundary crossed).
        (let [acc (-> (bank/open-account "acc-1" "Alice" 100.0)
                      (bank/deposit 10.0)
                      (bank/deposit 10.0))]
          (store/save-aggregate! ds acc 5)
          (is (nil? (store/get-latest-snapshot ds "acc-1"))
              "No snapshot before crossing the boundary"))
        ;; Second save: versions 4..7 in one batch, crossing version 5.
        (let [acc (-> (store/load-aggregate ds (bank/make-bank-account) "acc-1")
                      (bank/deposit 10.0)
                      (bank/deposit 10.0)
                      (bank/deposit 10.0)
                      (bank/deposit 10.0))]
          (store/save-aggregate! ds acc 5)
          (let [snap (store/get-latest-snapshot ds "acc-1")]
            (is (some? snap) "Snapshot created when batch crosses the boundary")
            (is (= 7 (:version snap)))))
        (finally
          (.delete (java.io.File. db-path)))))))

(deftest projection-catches-up-on-events
  (testing "Projections process stored events correctly"
    (let [db-path (str "test_proj_" (System/nanoTime) ".db")
          ds (store/create-datasource db-path)]
      (try
        (store/initialize! ds)
        (let [alice (bank/open-account "acc-1" "Alice" 100.0)
              _ (store/save-aggregate! ds alice 0)
              summary (bank/create-account-summary-projection)
              engine (-> (proj/create-projection-engine)
                         (proj/register! summary))]
          (proj/catch-up! engine (fn [after-seq] (store/get-all-events ds after-seq)))
          (let [alice-summary (get @summary "acc-1")]
            (is (= "Alice" (:account-holder alice-summary)))
            (is (= 100.0 (:balance alice-summary)))
            (is (= 1 (:transaction-count alice-summary)))))
        (finally
          (.delete (java.io.File. db-path)))))))
