package stupidkv

/*
#include <stdlib.h>
#include "stupid_kv.h"
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// Database is an MVCC key-value database with snapshot persistence and an
// AOL incremental log. It wraps a Rust `stupid_kv::Database` behind a C ABI
// and is safe for concurrent use by multiple goroutines.
//
// Mirrors the Python binding's Database: create one per process, share it.
type Database struct {
	ptr *C.sk_database
}

// New creates an in-memory database.
func New() *Database {
	return NewWithOptions(nil)
}

// NewWithOptions creates a database with runtime options.
// A nil opts uses the Rust defaults.
func NewWithOptions(opts *DatabaseOptions) *Database {
	ptr := C.sk_db_new_with_options(opts.toC())
	db := &Database{ptr: ptr}
	runtime.SetFinalizer(db, (*Database).Close)
	return db
}

// NewWithPersistence creates a database with snapshot + AOL persistence.
// The persistence directory is created if missing; an error is returned on
// I/O failure (permissions, disk full, corrupt existing snapshot, ...).
func NewWithPersistence(opts *DatabaseOptions, popts *PersistenceOptions) (*Database, error) {
	cp, cstrs := popts.toC()
	defer freeCStrings(cstrs)

	var errOut *C.char
	ptr := C.sk_db_new_with_persistence(opts.toC(), cp, &errOut)
	if ptr == nil {
		// The FFI reports the failure only through err_out (message);
		// classify it as an I/O error, which is the only failure mode
		// of Database::new_with_persistence in practice.
		msg := ""
		if errOut != nil {
			msg = C.GoString(errOut)
			C.sk_free_string(errOut)
		}
		return nil, mapError(skIO, msg)
	}
	db := &Database{ptr: ptr}
	runtime.SetFinalizer(db, (*Database).Close)
	return db, nil
}

// Snapshot triggers a full snapshot manually (same atomic tmp → rename →
// sync_all protocol as the background worker). No-op when persistence is
// disabled. Useful right before Close to guarantee durability.
func (db *Database) Snapshot() error {
	var errOut *C.char
	rc := C.sk_db_snapshot(db.ptr, &errOut)
	return takeError(rc, errOut)
}

// Transaction begins a new transaction. write selects read-write (true) or
// read-only (false), mirroring Python's db.transaction(write=...).
//
// Callers should eventually call Commit / Cancel and then Close. Dropping
// all references without Close still cancels the transaction via the
// finalizer (Rust Drop auto-cancels open transactions).
func (db *Database) Transaction(write bool) *Transaction {
	w := C.int32_t(0)
	if write {
		w = 1
	}
	tx := &Transaction{
		ptr: C.sk_db_tx_begin(db.ptr, w),
		db:  db,
	}
	runtime.SetFinalizer(tx, (*Transaction).Close)
	return tx
}

// Close releases the database handle. Idempotent.
//
// Any transactions created from this database should be closed first:
// they hold their own Arc to the shared state, but this handle is
// consumed here.
func (db *Database) Close() {
	if db.ptr != nil {
		C.sk_db_free(db.ptr)
		db.ptr = nil
	}
}

// Transaction is an MVCC transaction, mirroring the Python binding's
// Transaction. The underlying Rust handle is internally synchronized, so a
// Transaction may be used from multiple goroutines.
//
// Get / Exists / Set / Put / Delete operate against the transaction's
// snapshot; Commit publishes the writeset atomically and returns
// ErrWriteConflict / ErrReadConflict / ErrKeyAlreadyExists on failure.
type Transaction struct {
	ptr *C.sk_tx
	db  *Database // keep the database alive while the transaction lives
}

// WithSnapshotIsolation switches the transaction to Snapshot Isolation
// (default) and returns the same transaction for chaining, mirroring the
// Python builder methods.
func (tx *Transaction) WithSnapshotIsolation() *Transaction {
	C.sk_tx_with_snapshot_isolation(tx.ptr)
	return tx
}

// WithSerializableSnapshotIsolation switches the transaction to SSI and
// returns the same transaction for chaining. Under SSI the readset is
// tracked and Commit fails with ErrReadConflict on write-read conflicts
// (write-skew prevention).
func (tx *Transaction) WithSerializableSnapshotIsolation() *Transaction {
	C.sk_tx_with_serializable_snapshot_isolation(tx.ptr)
	return tx
}

// Version returns the Oracle timestamp (MVCC snapshot point) captured at
// transaction start. 0 for a closed transaction.
func (tx *Transaction) Version() uint64 {
	return uint64(C.sk_tx_version(tx.ptr))
}

// Closed reports whether the transaction has been committed or cancelled.
func (tx *Transaction) Closed() bool {
	return C.sk_tx_is_closed(tx.ptr) != 0
}

// Get reads the value for key at the transaction's snapshot.
// A missing key is (nil, nil), mirroring Python's None.
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	var valOut *C.uint8_t
	var valLen C.size_t
	var errOut *C.char
	rc := C.sk_tx_get(tx.ptr, (*C.uint8_t)(cBytes(key)), C.size_t(len(key)),
		&valOut, &valLen, &errOut)
	switch {
	case rc == C.SK_OK:
		out := C.GoBytes(unsafe.Pointer(valOut), C.int(valLen))
		C.sk_free_value(valOut, valLen)
		return out, nil
	case rc == C.SK_NOT_FOUND:
		return nil, nil
	default:
		return nil, takeError(rc, errOut)
	}
}

// Exists reports whether key exists at the transaction's snapshot.
func (tx *Transaction) Exists(key []byte) (bool, error) {
	var out C.int32_t
	var errOut *C.char
	rc := C.sk_tx_exists(tx.ptr, (*C.uint8_t)(cBytes(key)), C.size_t(len(key)),
		&out, &errOut)
	if rc != C.SK_OK {
		return false, takeError(rc, errOut)
	}
	return out != 0, nil
}

// Set inserts or updates key. The write is local until Commit.
func (tx *Transaction) Set(key, value []byte) error {
	var errOut *C.char
	rc := C.sk_tx_set(tx.ptr,
		(*C.uint8_t)(cBytes(key)), C.size_t(len(key)),
		(*C.uint8_t)(cBytes(value)), C.size_t(len(value)),
		&errOut)
	return takeError(rc, errOut)
}

// Put inserts key only if absent; returns ErrKeyAlreadyExists otherwise.
func (tx *Transaction) Put(key, value []byte) error {
	var errOut *C.char
	rc := C.sk_tx_put(tx.ptr,
		(*C.uint8_t)(cBytes(key)), C.size_t(len(key)),
		(*C.uint8_t)(cBytes(value)), C.size_t(len(value)),
		&errOut)
	return takeError(rc, errOut)
}

// Delete removes key.
func (tx *Transaction) Delete(key []byte) error {
	var errOut *C.char
	rc := C.sk_tx_del(tx.ptr, (*C.uint8_t)(cBytes(key)), C.size_t(len(key)), &errOut)
	return takeError(rc, errOut)
}

// Commit publishes the writeset atomically. On conflict the transaction is
// closed and must be retried from a fresh Transaction.
func (tx *Transaction) Commit() error {
	var errOut *C.char
	rc := C.sk_tx_commit(tx.ptr, &errOut)
	return takeError(rc, errOut)
}

// Cancel rolls the transaction back. Safe to call on an already-closed
// transaction (no-op error).
func (tx *Transaction) Cancel() error {
	var errOut *C.char
	rc := C.sk_tx_cancel(tx.ptr, &errOut)
	return takeError(rc, errOut)
}

// Close releases the transaction handle. An open transaction is
// automatically cancelled (Rust Drop semantics). Idempotent.
func (tx *Transaction) Close() {
	if tx.ptr != nil {
		C.sk_tx_free(tx.ptr)
		tx.ptr = nil
	}
}

func freeCStrings(strs []*C.char) {
	for _, s := range strs {
		C.free(unsafe.Pointer(s))
	}
}
