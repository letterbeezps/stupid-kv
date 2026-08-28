// SSI (Serializable Snapshot Isolation) + write-skew example.
//
// Two transactions each read the other's key and write their own; under SSI
// exactly one commit succeeds with ErrReadConflict on the other.
//
// Build the cdylib first: make lib (in stupid-kv-go/)
// Run: go run ./examples/002_ssi
package main

import (
	"errors"
	"fmt"
	"log"

	stupidkv "github.com/letterbeezps/stupid-kv/stupid-kv-go"
)

func main() {
	db := stupidkv.New()
	defer db.Close()

	seed := db.Transaction(true)
	if err := seed.Set([]byte("x"), []byte("1")); err != nil {
		log.Fatal(err)
	}
	if err := seed.Set([]byte("y"), []byte("2")); err != nil {
		log.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		log.Fatal(err)
	}
	seed.Close()

	t1 := db.Transaction(true).WithSerializableSnapshotIsolation()
	t2 := db.Transaction(true).WithSerializableSnapshotIsolation()

	// t1 reads y and writes x; t2 reads x and writes y — classic write skew.
	if _, err := t1.Get([]byte("y")); err != nil {
		log.Fatal(err)
	}
	if _, err := t2.Get([]byte("x")); err != nil {
		log.Fatal(err)
	}
	if err := t1.Set([]byte("x"), []byte("10")); err != nil {
		log.Fatal(err)
	}
	if err := t2.Set([]byte("y"), []byte("20")); err != nil {
		log.Fatal(err)
	}

	if err := t1.Commit(); err != nil {
		log.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		if errors.Is(err, stupidkv.ErrReadConflict) {
			fmt.Println("t2 rejected with ErrReadConflict (write skew prevented)")
		} else {
			log.Fatal(err)
		}
	}
	t1.Close()
	t2.Close()
}
