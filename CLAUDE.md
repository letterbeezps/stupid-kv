# Project Context — stupid-kv

This is a tutorial project: a Rust implementation of an MVCC KV database, based on [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx).

## Project Identity

- **Language:** Rust (2021 edition)
- **Purpose:** Learning database kernel development by building incrementally
- **Storage:** In-memory only (for now)
- **Concurrency:** MVCC with Snapshot Isolation
- **Reference:** [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx)

## Progress Convention

Each completed section corresponds to a **git tag**. Tags are assigned at completion time — planning items below are rough ideas, not a strict order; optimization sections may be inserted mid-way.

| Tag | Section | Status |
|-----|---------|--------|
| `0.0.1` | Basic MVCC transactions | ✅ done |

**Rough planning (unordered, subject to insertion of optimization sections):**

- GC — version history cleanup, commit queue cleanup
- Persistence — WAL / Snapshot on-disk
- SSI — Serializable Snapshot Isolation
- ...and possibly more optimization sections

Checkout a specific version: `git checkout <tag>`

## Project Layout

```
stupid-kv/
├── src/
│   ├── db/           # Database + Inner (shared state)
│   ├── oracle/       # Global timestamp oracle
│   ├── tx/           # Transaction implementation
│   ├── versions/     # Multi-version data management
│   ├── queue/        # Commit queue + merge queue
│   ├── kv/           # Key/value type conversions (IntoBytes)
│   ├── error/        # Error types (Error enum)
│   └── lib.rs        # Public API
├── examples/         # Runnable examples (cargo run --example <name>)
├── docs/             # Design documents (001_*.md, 002_*.md, ...)
├── README.md         # Project overview & progress
├── .trae/rules/      # Trae IDE assistant rules
└── Cargo.toml
```

## Key Dependencies

| Crate | Usage |
|-------|-------|
| `crossbeam-skiplist` | Lock-free concurrent SkipMap (commit queue, datastore) |
| `parking_lot` | RwLock for per-key version access |
| `smallvec` | Stack-allocated version lists (SmallVec<[Version; 4]>) |
| `bytes` | Byte buffer type |
| `thiserror` | Error types |
| `arc-swap` | Arc reference handling |

## Document Style Rules

1. **Diagrams (ASCII architecture, flow charts, etc.) — All labels and descriptions inside code block diagrams **must use English**. Body text around them uses Chinese.
2. **Design docs live in `docs/`** — named `001_basic_transaction.md`, `002_gc.md`, etc.
3. **Each section's design doc should explain:
   - What's being built & why
   - Core data structures with code snippets
   - Key algorithms (read path, write path, conflict detection, etc.)
   - Design tradeoffs (pros / cons table)
   - Architecture diagram (ASCII, English labels)
   - Worked example with concrete values

## Workflow — Adding a New Section

When implementing a new section (e.g., Section 0.0.2 GC):

1. **Design first — write `docs/002_gc.md` explaining the design
2. **Implement** — write code in relevant modules
3. **Add examples** — add an `examples/` entry if it demonstrates usage
4. **Update README** — add a row to the completed table
5. **Tag** — `git tag -a 0.0.2 -m "Section 0.0.2: GC for version history and commit queue"`

## Core Concepts (for context)

- **Oracle** — monotonic global timestamp generator (u64, based on system time in nanoseconds)
- **`commit`** — sequence ID from `transaction_commit_id`, used for write conflict detection
- **`version`** — timestamp from Oracle, used for snapshot read visibility
- **writeset** — `BTreeMap<Bytes, Option<Bytes>>`, `None` means tombstone (delete)
- **Commit queue** — `SkipMap<u64, Arc<Commit>>`, keeps committed writesets for conflict detection
- **Merge queue** — `SkipMap<u64, Arc<Merge>>`, temporary queue during datastore write
- **First-Committer-Wins** — write conflict detection strategy
- **Snapshot Isolation** — current isolation level; reads see a consistent snapshot at transaction creation time

## Commands

```bash
cargo test              # run unit tests
cargo run --example basic  # run example
cargo build             # build library
```
