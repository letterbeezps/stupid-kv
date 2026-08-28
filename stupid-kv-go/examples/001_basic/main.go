// Basic MVCC usage of the stupid-kv Go bindings.
//
// Build the cdylib first: make lib (in stupid-kv-go/)
// Run: go run ./examples/001_basic
package main

import (
	"fmt"
	"log"

	stupidkv "github.com/letterbeezps/stupid-kv/stupid-kv-go"
)

func main() {
	db := stupidkv.New()
	defer db.Close()

	// Write.
	tx := db.Transaction(true)
	if err := tx.Set([]byte("key1"), []byte("value1")); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	tx.Close()

	// Read.
	rtx := db.Transaction(false)
	v, err := rtx.Get([]byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("key1 = %s\n", v)
	rtx.Close()

	// Put only if absent.
	ptx := db.Transaction(true)
	err = ptx.Put([]byte("key1"), []byte("ignored")) // already exists
	fmt.Printf("put existing key: %v\n", err)
	ptx.Close()

	// Delete.
	dtx := db.Transaction(true)
	if err := dtx.Delete([]byte("key1")); err != nil {
		log.Fatal(err)
	}
	if err := dtx.Commit(); err != nil {
		log.Fatal(err)
	}
	dtx.Close()
}
