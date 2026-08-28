# Build the Rust cdylib into ./lib so cgo can link against it.
.PHONY: lib test vet example clean

lib:
	cd .. && cargo build -p stupid-kv-c --release
	mkdir -p lib
	cp ../target/release/libstupid_kv_c.dylib lib/ 2>/dev/null || \
		cp ../target/release/libstupid_kv_c.so lib/ || \
		cp ../target/release/stupid_kv_c.dll lib/

test: lib
	go test ./...

vet: lib
	go vet ./...

example: lib
	go run ./examples/001_basic
	go run ./examples/002_ssi

clean:
	rm -rf lib
