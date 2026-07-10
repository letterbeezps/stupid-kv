# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tutorial project: an incremental Rust implementation of an MVCC key-value database, based on [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx). Each completed section gets a git tag.

- **Language:** Rust (2021 edition)
- **Storage:** In-memory only
- **Concurrency:** MVCC with Snapshot Isolation (SI) and Serializable Snapshot Isolation (SSI)

## Commands

```bash
cargo test                          # run all tests
cargo test <test_name>              # run a single test by name
cargo test -- --nocapture           # run tests with stdout
cargo run --example 001_basic       # basic MVCC example
cargo run --example 002_ssi         # SSI + write-skew example
cargo build                         # build library
```

## Architecture

### Core Data Flow

A transaction goes through three stages before data lands in the datastore:

```
Transaction (writeset) → commit queue (conflict check) → merge queue (pending write) → datastore
```

1. `auto_commit` inserts a `Commit` entry into `transaction_commit_queue` with an atomically assigned commit ID.
2. Conflict detection scans `transaction_commit_queue` entries from `(tx.commit + 1)..current_commit_id`.
3. `atomic_merge` inserts a `Merge` entry into `transaction_merge_queue` with an Oracle-assigned nanosecond timestamp as the MVCC version.
4. The writeset is applied key-by-key into `datastore`, each key holding a `RwLock<Versions>`.
5. The merge queue entry is removed; the commit queue entry persists for future conflict detection.

### Key Structures

- **`Inner`** (`src/db/inner.rs`) — shared state behind `Arc`, holds all queues and the datastore.
- **`TransactionInner`** (`src/tx/transaction_inner.rs`) — per-transaction state: `commit` (snapshot of commit ID at tx start), `version` (Oracle timestamp at tx start), `writeset`, `readset` (SSI only), `readset_bloom`.
- **`Versions`** (`src/versions/versions.rs`) — sorted `SmallVec<[Version; 4]>` of `(version: u64, value: Option<Bytes>)`. `None` is a tombstone. `fetch_version(v)` returns the newest value with version ≤ v.
- **`Commit`** (`src/queue/commit.rs`) — writeset snapshot stored in commit queue; carries a `BloomFilter` over write keys and `(min_key, max_key)` for fast range skipping during conflict detection.
- **`BloomFilter`** (`src/bloom/bloom.rs`) — fixed 512-byte (4096-bit) filter using FNV-1a + Kirsch-Mitzenmacher double hashing (k=3). Used to accelerate conflict detection on both writesets and readsets.

### Two IDs: `commit` vs `version`

- **`commit`** — monotonic counter (`transaction_commit_id`), incremented once per committed writeset. Used solely for write/read conflict detection range queries.
- **`version`** — nanosecond timestamp from Oracle, used as the MVCC snapshot point. Read path uses `version` to find the newest visible value ≤ tx's snapshot version.

### Isolation Levels (`src/tx/isolation.rs`)

- **`SnapshotIsolation`** — detects write-write conflicts by checking if any concurrent committed tx touched the same keys.
- **`SerializableSnapshotIsolation`** — additionally tracks the readset and detects write-read conflicts (prevents write skew / phantoms).

### Read Path

`get`/`exists` check the **merge queue** first (pending committed writes not yet in datastore, scanned in reverse order by version), then fall back to **datastore**. This avoids a race where a committed tx's data is visible in the merge queue but not yet flushed.

### Conflict Detection (commit path)

1. Build a `BloomFilter` over the writeset keys.
2. Range-scan `transaction_commit_queue` for commits after `self.commit`.
3. For each: first do a bloom-range-skip (`max_key`/`min_key`), then bloom `may_contain`, then exact sorted-merge scan (`is_disjoint_writeset`).
4. For SSI: also check `is_disjoint_readset_bloom` against the current tx's readset.

## Progress Convention

| Tag | Section | Status |
|-----|---------|--------|
| `0.0.1` | Basic MVCC transactions | ✅ done |
| `0.0.2` | SSI + Bloom filter accelerated conflict detection | ✅ done |
| `0.0.3` | Runtime hardening (write-path safety, adaptive backoff, Oracle anti-drift) | ✅ done |

Rough planning: GC (version history + commit queue cleanup), Persistence (WAL/Snapshot).

## Adding a New Section

1. Write `docs/00N_name.md` (design: structures, algorithms, tradeoffs, ASCII diagram with English labels, worked example)
2. Implement in relevant modules
3. Add `examples/` entry if it demonstrates usage
4. Update README and the table above
5. `git tag -a 0.0.N -m "Section 0.0.N: <name>"`

## Document Style

- Diagram labels inside code blocks: **English only**
- Surrounding prose: Chinese
- Design docs: `docs/001_basic_transaction.md`, `docs/002_ssi_bloom_filter.md`, `docs/003_runtime_hardening.md`, etc.
