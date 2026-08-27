# stupid-kv Python examples

Runnable examples showing how to use the `stupid_kv` Python binding.

## Prerequisites

Build & install the extension into your current Python environment:

```bash
cd ../   # from this examples/ directory, go to stupid-kv-py/
pip install maturin
maturin develop --release
```

This installs `stupid_kv` as an editable package — re-run after Rust source changes.

## Running

```bash
python 001_basic.py
python 002_isolation.py
```

Both scripts are self-contained and print what they're doing as they go.

## What's here

| File | Demonstrates |
|------|-------------|
| `001_basic.py` | Create db, set / get / exists / put / delete; builder chain (`.with_snapshot_isolation()`); context manager (`with`) |
| `002_isolation.py` | SI blind-write conflict; SSI write-skew detection; `KeyAlreadyExists` from `put` |

## Mapping to Rust examples

| Python | Rust counterpart |
|--------|------------------|
| `001_basic.py` | `examples/001_basic.rs` |
| `002_isolation.py` | `examples/002_ssi.rs` |
