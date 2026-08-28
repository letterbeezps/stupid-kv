package stupidkv

import (
	"errors"
	"fmt"
)

// Sentinel errors mirroring the Python binding's exception hierarchy.
// Use errors.Is to classify a failure:
//
//	if errors.Is(err, stupidkv.ErrWriteConflict) {
//	    // retry the transaction
//	}
var (
	// ErrTxClosed is returned when operating on a committed/cancelled/closed
	// transaction.
	ErrTxClosed = errors.New("stupidkv: transaction is closed")

	// ErrTxNotWritable is returned when writing through a read-only
	// transaction.
	ErrTxNotWritable = errors.New("stupidkv: transaction is not writable")

	// ErrWriteConflict is returned by Commit when a concurrent committed
	// transaction touched the same keys (Snapshot Isolation / SSI).
	ErrWriteConflict = errors.New("stupidkv: write-write conflict, retry the transaction")

	// ErrReadConflict is returned by Commit under Serializable Snapshot
	// Isolation when a concurrent committed transaction modified a key this
	// transaction read (write-skew protection).
	ErrReadConflict = errors.New("stupidkv: read-write conflict, retry the transaction")

	// ErrKeyAlreadyExists is returned by Put when the key already exists.
	ErrKeyAlreadyExists = errors.New("stupidkv: key already exists")
)

// mapError converts an FFI code + message into a Go error. Known codes wrap
// the exported sentinel errors so callers can errors.Is on them; IO-class
// errors carry the Rust-side detail message verbatim.
func mapError(code int32, msg string) error {
	switch code {
	case skTxClosed:
		return fmt.Errorf("%w", ErrTxClosed)
	case skTxNotWritable:
		return fmt.Errorf("%w", ErrTxNotWritable)
	case skWriteConflict:
		return fmt.Errorf("%w", ErrWriteConflict)
	case skReadConflict:
		return fmt.Errorf("%w", ErrReadConflict)
	case skAlreadyExists:
		return fmt.Errorf("%w", ErrKeyAlreadyExists)
	case skCommitNotPersisted, skIO:
		return errors.New("stupidkv: " + msg)
	default:
		if msg == "" {
			return fmt.Errorf("stupidkv: unknown error (code %d)", code)
		}
		return fmt.Errorf("stupidkv: %s (code %d)", msg, code)
	}
}
