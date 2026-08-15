# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tutorial project: an incremental Rust implementation of an MVCC key-value database, based on [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx). Each completed section gets a git tag.

- **Language:** Rust (2021 edition)
- **Storage:** In-memory with full-state snapshot persistence (bincode + optional LZ4 compression)
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
- **`Persistence`** (`src/persistence/persistence.rs`) — full-state snapshot facade; holds `Arc<Inner>`, snapshot path, mode, compression mode, background thread handles. Provides `snapshot()` manual trigger and `load()` startup recovery.
- **`CompressedWriter` / `CompressedReader`** (`src/compression/compression.rs`) — transparent compression wrappers; auto-detect LZ4 format via magic number `0x04224D18`; types erased through `Box<dyn Write>` / `Box<dyn Read>`.

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

### Snapshot Persistence (`src/persistence/`)

Full-state snapshot persistence with atomic write protocol:

- **`Persistence`** — facade holding `Arc<Inner>`, snapshot path, mode, compression mode, background thread handle
- **`SnapshotMode`** — `Never` (manual only) or `Interval(Duration)` (auto periodic)
- **`PersistenceOptions`** — config DTO: `base_path`, `snapshot_path`, `snapshot_mode`, `compression_mode`
- **Atomic write protocol** — `tmp → rename → sync_all` three-phase; any crash point leaves old snapshot intact
- **Streaming serialization** — bincode `encode_into_std_write` / `decode_from_std_read`, O(1) memory per entry
- **Startup recovery** — `load()` restores datastore from snapshot before spawning GC threads (prevents version GC from eating just-loaded data)
- **Shutdown order** — snapshot worker stops before GC workers (ensures final snapshot sees complete version chains)
- **Error taxonomy** — `PersistenceError::Io | Serialization | Deserialization`

### LZ4 Compression (`src/compression/`)

Transparent, optional compression layer for snapshot files:

- **`CompressionMode`** — `None` (default, zero-cost equivalent to raw path) or `Lz4` (level 7 compression)
- **`CompressedWriter`** — write-side facade; `None` → `BufWriter`, `Lz4` → `Lz4Encoder`; types erased via `Box<dyn Write>`
- **`CompressedReader`** — read-side facade; auto-detects format by probing LZ4 magic number `0x04224D18` (first 4 bytes of file); backward-compatible with 0.0.6 uncompressed snapshots
- **Symmetric interface** — `snapshot()` / `load()` code changes only one line each (`BufWriter` → `CompressedWriter`, `BufReader` → `CompressedReader`)
- **`finish()` method** — explicit flush before `rename`, ensures compressed data fully written to disk

### AOL Incremental Log (`src/persistence/`)

Append-Only Log for reducing crash data loss window from snapshot interval to millisecond level:

- **`AolMode`** — `Never` (disabled, default), `SynchronousOnCommit` (sync write per commit), `AsynchronousAfterCommit` (push to lock-free queue, batch async)
- **`FsyncMode`** — `Never` (counter only), `EveryAppend` (fsync each batch), `Interval(Duration)` (periodic fsync)
- **`AsyncAppendOperation`** — struct holding `version` (u64) + `writeset` (BTreeMap<Bytes, Option<Bytes>>) for async dispatch
- **`append()`** — core method; Sync path uses `Mutex<File>` for linear writes; Async path pushes to `crossbeam_deque::Injector` and wakes `append_worker`
- **`truncate()`** — called after `snapshot()` completes; copies `[cutoff..file_len)` to tmp, overwrites back; or `set_len(0)` if no new writes
- **Three workers** — `append_worker` (batch consume from Injector, BATCH_SIZE=100, TIMEOUT=10ms), `fsync_worker` (periodic sync_all when `pending_syncs > 0`), `snapshot_worker` (extended with AOL cutoff + truncate)
- **`pending_syncs: AtomicU64`** — counter of unsynced AOL data; checked by fsync worker, snapshot truncate, and Drop
- **`last_fsync: Arc<Mutex<Instant>>`** — shared timestamp between sync append path and async worker path for fsync interval logic
- **`PersistenceError::LockFailed`** — new variant + `PoisonError` conversion for robust `Mutex<File>` handling
- **`TxError::TxCommitNotPersisted`** — transaction-level error when AOL write fails, triggers rollback of merge queue and writeset

### Persistence Integration

- **`Database::new_with_persistence()`** — construction order: `Inner` → `Persistence::new_with_options` (which calls `load()` + `spawn_snapshot_worker()` + `spawn_appender_worker()` + `spawn_fsync_worker()`) → GC threads → returned
- **`Database::shutdown()`** — reverse order: snapshot worker → append worker → fsync worker → cleanup worker → GC worker
- **`Persistence::clone()`** — multiple Arc shares one background thread; guarded by `Arc<RwLock<Option<JoinHandle>>>`
- **`load()`** — two-phase restore: snapshot restore → AOL replay (CompressedReader + bincode `(key, version, value)` decode, `Versions::push` for each)

## Progress Convention

| Tag | Section | Status |
|-----|---------|--------|
| `0.0.1` | Basic MVCC transactions | ✅ done |
| `0.0.2` | SSI + Bloom filter accelerated conflict detection | ✅ done |
| `0.0.3` | Runtime hardening (write-path safety, adaptive backoff, Oracle anti-drift) | ✅ done |
| `0.0.4` | Commit queue GC (active-txn refcount + Dekker double-fence protocol) | ✅ done |
| `0.0.5` | Version history GC (datastore version-chain compaction, gc_floor + dirty queue + full-scan fallback) | ✅ done |
| `0.0.6` | Snapshot persistence (bincode full-state snapshot, atomic write protocol, background worker, startup recovery) | ✅ done |
| `0.0.7` | LZ4 snapshot compression (transparent CompressedWriter/Reader, magic number auto-detect, backward compatible) | ✅ done |
| `0.0.8` | AOL incremental log (Append-Only Log, AolMode × FsyncMode, crossbeam_deque batch async, snapshot + AOL truncate) | ✅ done |

Rough planning: WAL formalization (checkpoint markers), AOL data integrity (CRC32), log archiving, entry-level compression.

## Adding a New Section

1. Write `docs/00N_name.md` (design: structures, algorithms, tradeoffs, ASCII diagram with English labels, worked example)
2. Implement in relevant modules
3. Add `examples/` entry if it demonstrates usage
4. Update README and the table above
5. `git tag -a 0.0.N -m "Section 0.0.N: <name>"`

## Document Style

- Diagram labels inside code blocks: **English only**
- Surrounding prose: Chinese
- Design docs: `docs/001_basic_transaction.md`, `docs/002_ssi_bloom_filter.md`, `docs/003_runtime_hardening.md`, `docs/004_commit_queue_gc.md`, `docs/005_version_history_gc.md`, `docs/006_snapshot.md`, `docs/007_lz4_compression.md`, etc.
