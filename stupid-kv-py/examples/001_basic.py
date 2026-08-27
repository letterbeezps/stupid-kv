"""001_basic.py — Basic CRUD operations with the stupid-kv Python binding.

Mirrors `examples/001_basic.rs` from the Rust side.

Run:
    # 1. From the repo root, build & install the extension:
    cd stupid-kv-py
    pip install maturin
    maturin develop --release

    # 2. Run this script (any working dir, as long as the venv is active):
    python examples/001_basic.py
"""

import stupid_kv


def main() -> None:
    db = stupid_kv.Database()

    # ----- Write transaction -----
    tx = db.transaction(write=True)
    tx.set(b"key1", b"value1")
    tx.set(b"key2", b"value2")
    tx.commit()
    print("first transaction: set key1, key2")

    # ----- Read transaction -----
    tx = db.transaction(write=False)
    print(f"  exists(key1) = {tx.exists(b'key1')}")
    print(f"  get(key1)    = {tx.get(b'key1')!r}")
    print(f"  get(key2)    = {tx.get(b'key2')!r}")

    # Note: calling set/put/delete on a read-only tx raises TxNotWritable.
    # You must create the tx with write=True to mutate.

    # ----- Update + delete -----
    tx = db.transaction(write=True)
    tx.put(b"key3", b"value3")  # 'put' = insert if absent
    tx.delete(b"key1")
    tx.commit()

    tx = db.transaction(write=False)
    print(f"after commit:")
    print(f"  get(key3) = {tx.get(b'key3')!r}")  # b'value3'
    print(f"  get(key1) = {tx.get(b'key1')!r}")  # None
    print(f"  get(key2) = {tx.get(b'key2')!r}")  # b'value2'

    # ----- Context manager (sqlite-style: auto-commit on exit) -----
    with db.transaction(write=True) as tx:
        tx.set(b"auto", b"close")
    print(f"  get(auto) = {db.transaction(write=False).get(b'auto')!r}")

    # ----- Builder chaining -----
    db2 = stupid_kv.Database()
    with db2.transaction(write=True).with_snapshot_isolation() as tx:
        tx.set(b"k", b"v")
    print("builder chain + with statement OK")


if __name__ == "__main__":
    main()
