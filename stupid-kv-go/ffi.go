// Package stupidkv provides Go bindings for the stupid-kv MVCC key-value
// database, mirroring the PyO3 bindings in stupid-kv-py.
//
// The bindings talk to a Rust cdylib over a C ABI (crate: stupid-kv-c).
// Build the library first, then import this package:
//
//	make lib          # in stupid-kv-go/: cargo build + copy dylib
//	go test ./...
//
// Usage mirrors the Python bindings:
//
//	db := stupidkv.New()
//	defer db.Close()
//
//	tx := db.Transaction(true) // write transaction
//	tx.Set([]byte("hello"), []byte("world"))
//	if err := tx.Commit(); err != nil {
//	    log.Fatal(err)
//	}
//
//	rtx := db.Transaction(false)
//	v, err := rtx.Get([]byte("hello")) // v == "world"
package stupidkv

/*
#cgo LDFLAGS: -L${SRCDIR}/lib -lstupid_kv_c
#cgo CFLAGS: -I${SRCDIR}/../stupid-kv-c/include

#include <stdlib.h>
#include "stupid_kv.h"
*/
import "C"
import "unsafe"

// Go-side mirror of the return codes in include/stupid_kv.h.
const (
	skOK                 int32 = 0
	skNotFound           int32 = 1
	skNullArg            int32 = -1
	skTxClosed           int32 = -2
	skTxNotWritable      int32 = -3
	skWriteConflict      int32 = -4
	skReadConflict       int32 = -5
	skAlreadyExists      int32 = -6
	skCommitNotPersisted int32 = -7
	skIO                 int32 = -8
	skPanic              int32 = -99
)

// takeError converts an FFI (code, err_out) pair into a Go error.
// The message buffer, if present, is copied and released.
func takeError(rc C.int32_t, errOut *C.char) error {
	code := int32(rc)
	if code == skOK {
		return nil
	}
	msg := ""
	if errOut != nil {
		msg = C.GoString(errOut)
		C.sk_free_string(errOut)
	}
	return mapError(code, msg)
}

// cBytes returns a pointer usable as a C byte buffer for b.
// A nil pointer with length 0 is valid on the Rust side (empty slice).
func cBytes(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(&b[0])
}
