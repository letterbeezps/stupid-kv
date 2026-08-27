"""002_isolation.py — Isolation level conflict detection.

Mirrors `examples/002_ssi.rs` from the Rust side. Demonstrates:
  - Snapshot Isolation (SI) detecting write-write conflicts
  - Serializable Snapshot Isolation (SSI) additionally detecting
    read-write conflicts (write-skew prevention)

Run:
    cd stupid-kv-py
    python examples/002_isolation.py
"""

import stupid_kv


def si_blind_write_conflict() -> None:
    """Two concurrent SI transactions writing the same key: first wins, second fails."""
    print("--- SI: blind-write conflict ---")
    db = stupid_kv.Database()

    tx1 = db.transaction(write=True).with_snapshot_isolation()
    tx2 = db.transaction(write=True).with_snapshot_isolation()

    tx1.set(b"k", b"v1")
    tx2.set(b"k", b"v2")

    tx1.commit()  # first commit wins
    print("  tx1 committed")

    try:
        tx2.commit()
    except stupid_kv.KeyWriteConflict:
        print("  tx2.commit() -> KeyWriteConflict (as expected)")


def ssi_write_skew() -> None:
    """SSI catches write-skew: two txns read each other's values then
    write to disjoint keys based on what they read."""
    print("--- SSI: write-skew detection ---")
    db = stupid_kv.Database()

    # Setup: x = 5, y = 5
    with db.transaction(write=True) as tx:
        tx.set(b"x", b"5")
        tx.set(b"y", b"5")

    # tx1 reads y, decides to set x = x - 1  (requires y >= 1)
    # tx2 reads x, decides to set y = y - 1  (requires x >= 1)
    # Under SI: both commits succeed (write skew anomaly)
    # Under SSI: one fails (read-write conflict detected)
    tx1 = db.transaction(write=True).with_serializable_snapshot_isolation()
    tx2 = db.transaction(write=True).with_serializable_snapshot_isolation()

    y_for_tx1 = tx1.get(b"y")  # tx1 reads y
    x_for_tx2 = tx2.get(b"x")  # tx2 reads x
    print(f"  tx1 sees y={y_for_tx1!r}, tx2 sees x={x_for_tx2!r}")

    # Each assumes the other didn't change things based on read.
    tx1.set(b"x", b"4")  # tx1 writes x based on y
    tx2.set(b"y", b"4")  # tx2 writes y based on x

    tx1.commit()
    print("  tx1 committed")

    try:
        tx2.commit()
    except (stupid_kv.KeyReadConflict, stupid_kv.KeyWriteConflict):
        print("  tx2.commit() -> conflict (SSI prevented write-skew)")


def key_already_exists() -> None:
    """`put` is conditional insert; raises KeyAlreadyExists if key present."""
    print("--- put: KeyAlreadyExists ---")
    db = stupid_kv.Database()

    with db.transaction(write=True) as tx:
        tx.set(b"k", b"existing")

    try:
        with db.transaction(write=True) as tx:
            tx.put(b"k", b"new")
    except stupid_kv.KeyAlreadyExists:
        print("  put on existing key -> KeyAlreadyExists (as expected)")


def main() -> None:
    si_blind_write_conflict()
    ssi_write_skew()
    key_already_exists()


if __name__ == "__main__":
    main()
