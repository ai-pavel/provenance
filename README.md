# Event Sourcing Engine

[![CI](https://github.com/pavel-genai/event-sourcing-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/pavel-genai/event-sourcing-engine/actions/workflows/ci.yml)

A Clojure event sourcing framework with SQLite-backed event store (via next.jdbc), snapshot support, and a projection engine.

## Features

- **AggregateRoot protocol** with event replay using `reduce` and snapshotting
- **SQLite EventStore** (next.jdbc) for persisting and loading event streams with optimistic concurrency
- **Projection Engine** for building read models from stored events using atoms
- **Sample Domain**: Bank account with deposit, withdraw, and transfer events

## Structure

- `src/event_sourcing/aggregate.clj` — AggregateRoot protocol and core functions
- `src/event_sourcing/store.clj` — SQLite-backed event store and aggregate repository
- `src/event_sourcing/projection.clj` — Projection protocol and engine
- `src/event_sourcing/sample/bank_account.clj` — Sample bank account domain and projections
- `src/event_sourcing/core.clj` — Main entry point with bank account demo
- `test/event_sourcing/bank_account_test.clj` — clojure.test tests

## Running

```bash
lein run
```

## Testing

```bash
lein test
```

## Dependencies

- Clojure 1.11.1
- [next.jdbc](https://github.com/seancorfield/next-jdbc) for database access
- [SQLite JDBC](https://github.com/xerial/sqlite-jdbc) driver
- [clojure.data.json](https://github.com/clojure/data.json) for JSON serialization
