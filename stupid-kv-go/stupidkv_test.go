package stupidkv

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func mustGet(t *testing.T, tx *Transaction, key string) []byte {
	t.Helper()
	v, err := tx.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) unexpected error: %v", key, err)
	}
	return v
}

func TestBasicCrud(t *testing.T) {
	db := New()
	defer db.Close()

	tx := db.Transaction(true)
	if err := tx.Set([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Close()

	rtx := db.Transaction(false)
	defer rtx.Close()
	if got := string(mustGet(t, rtx, "hello")); got != "world" {
		t.Fatalf("Get(hello) = %q, want %q", got, "world")
	}
	// Missing key: (nil, nil), mirroring Python's None.
	if v := mustGet(t, rtx, "absent"); v != nil {
		t.Fatalf("Get(absent) = %v, want nil", v)
	}

	// Update + delete.
	utx := db.Transaction(true)
	if err := utx.Set([]byte("hello"), []byte("updated")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := utx.Delete([]byte("hello")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := utx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	utx.Close()

	vtx := db.Transaction(false)
	defer vtx.Close()
	exists, err := vtx.Exists([]byte("hello"))
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("deleted key still exists")
	}
}

func TestPutAlreadyExists(t *testing.T) {
	db := New()
	defer db.Close()

	tx := db.Transaction(true)
	if err := tx.Set([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()

	tx2 := db.Transaction(true)
	defer tx2.Close()
	err := tx2.Put([]byte("k"), []byte("v2"))
	if !errors.Is(err, ErrKeyAlreadyExists) {
		t.Fatalf("Put on existing key: got %v, want ErrKeyAlreadyExists", err)
	}
}

func TestWriteConflict(t *testing.T) {
	db := New()
	defer db.Close()

	t1 := db.Transaction(true)
	t2 := db.Transaction(true)
	if err := t1.Set([]byte("k"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := t2.Set([]byte("k"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatalf("first commit should win: %v", err)
	}
	err := t2.Commit()
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("second commit: got %v, want ErrWriteConflict", err)
	}
	t1.Close()
	t2.Close()

	rtx := db.Transaction(false)
	defer rtx.Close()
	if got := string(mustGet(t, rtx, "k")); got != "a" {
		t.Fatalf("winner value = %q, want %q", got, "a")
	}
}

// TestReadConflictSSI reproduces the write-skew scenario from the Python
// 002_isolation example: two transactions each read the other's key and
// write their own. Under SSI exactly one commit may succeed.
func TestReadConflictSSI(t *testing.T) {
	db := New()
	defer db.Close()

	seed := db.Transaction(true)
	if err := seed.Set([]byte("x"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Set([]byte("y"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}
	seed.Close()

	t1 := db.Transaction(true).WithSerializableSnapshotIsolation()
	t2 := db.Transaction(true).WithSerializableSnapshotIsolation()

	// t1 reads y, writes x; t2 reads x, writes y.
	if _, err := t1.Get([]byte("y")); err != nil {
		t.Fatal(err)
	}
	if _, err := t2.Get([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := t1.Set([]byte("x"), []byte("10")); err != nil {
		t.Fatal(err)
	}
	if err := t2.Set([]byte("y"), []byte("20")); err != nil {
		t.Fatal(err)
	}

	if err := t1.Commit(); err != nil {
		t.Fatalf("first SSI commit should win: %v", err)
	}
	err := t2.Commit()
	if !errors.Is(err, ErrReadConflict) {
		t.Fatalf("second SSI commit: got %v, want ErrReadConflict", err)
	}
	t1.Close()
	t2.Close()
}

func TestReadOnlyNotWritable(t *testing.T) {
	db := New()
	defer db.Close()

	tx := db.Transaction(false)
	defer tx.Close()
	err := tx.Set([]byte("k"), []byte("v"))
	if !errors.Is(err, ErrTxNotWritable) {
		t.Fatalf("Set on read-only tx: got %v, want ErrTxNotWritable", err)
	}
}

func TestClosedTransaction(t *testing.T) {
	db := New()
	defer db.Close()

	tx := db.Transaction(true)
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !tx.Closed() {
		t.Fatal("Closed() = false after commit")
	}
	err := tx.Set([]byte("k2"), []byte("v2"))
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Set on closed tx: got %v, want ErrTxClosed", err)
	}
}

func TestCancel(t *testing.T) {
	db := New()
	defer db.Close()

	tx := db.Transaction(true)
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Cancel(); err != nil {
		t.Fatal(err)
	}

	rtx := db.Transaction(false)
	defer rtx.Close()
	if v := mustGet(t, rtx, "k"); v != nil {
		t.Fatalf("cancelled write visible: %v", v)
	}
}

func TestIsolationSwitch(t *testing.T) {
	db := New()
	defer db.Close()

	tx := db.Transaction(true).WithSnapshotIsolation().WithSerializableSnapshotIsolation()
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()
}

func TestConcurrentCommits(t *testing.T) {
	db := New()
	defer db.Close()

	const goroutines = 8
	const perGoroutine = 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				tx := db.Transaction(true)
				key := []byte{byte('a' + byte(g)), byte('0' + byte(i%10))}
				if err := tx.Set(key, []byte("v")); err != nil {
					t.Errorf("Set: %v", err)
					tx.Close()
					continue
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("Commit: %v", err)
				}
				tx.Close()
			}
		}(g)
	}
	wg.Wait()

	rtx := db.Transaction(false)
	defer rtx.Close()
	for g := 0; g < goroutines; g++ {
		for i := 0; i < 10; i++ {
			key := []byte{byte('a' + byte(g)), byte('0' + byte(i))}
			if v := mustGet(t, rtx, string(key)); v == nil {
				t.Fatalf("missing key %q after concurrent commits", key)
			}
		}
	}
}

func TestWithOptions(t *testing.T) {
	opts := NewDatabaseOptions().
		WithGcInterval(200 * time.Millisecond).
		WithCleanupInterval(500 * time.Millisecond).
		WithGcFullScanFrequency(5).
		WithPoolSize(64).
		WithResetThreshold(10).
		WithCleanup(false).
		WithGc(false)
	db := NewWithOptions(opts)
	defer db.Close()

	tx := db.Transaction(true)
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.bin")

	popts := NewPersistenceOptions(dir).
		WithPureSnapshot(time.Hour).
		WithSnapshotPath(snapshotPath)
	db, err := NewWithPersistence(nil, popts)
	if err != nil {
		t.Fatalf("NewWithPersistence: %v", err)
	}

	tx := db.Transaction(true)
	if err := tx.Set([]byte("persist"), []byte("me")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()
	if err := db.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	db.Close()

	// Reopen from the snapshot file.
	popts2 := NewPersistenceOptions(dir).WithSnapshotPath(snapshotPath)
	db2, err := NewWithPersistence(nil, popts2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	rtx := db2.Transaction(false)
	defer rtx.Close()
	if got := string(mustGet(t, rtx, "persist")); got != "me" {
		t.Fatalf("persisted value = %q, want %q", got, "me")
	}
}

func TestPersistenceLz4(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.bin")

	popts := NewPersistenceOptions(dir).
		WithPureSnapshot(time.Hour).
		WithSnapshotPath(snapshotPath).
		WithLz4Compression()
	db, err := NewWithPersistence(nil, popts)
	if err != nil {
		t.Fatalf("NewWithPersistence: %v", err)
	}
	tx := db.Transaction(true)
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()
	if err := db.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	db.Close()

	popts2 := NewPersistenceOptions(dir).WithSnapshotPath(snapshotPath)
	db2, err := NewWithPersistence(nil, popts2)
	if err != nil {
		t.Fatalf("reopen (lz4 auto-detect): %v", err)
	}
	defer db2.Close()
	rtx := db2.Transaction(false)
	defer rtx.Close()
	if got := string(mustGet(t, rtx, "k")); got != "v" {
		t.Fatalf("persisted value = %q, want %q", got, "v")
	}
}
