package stupidkv

/*
#include <stdlib.h>
#include "stupid_kv.h"
*/
import "C"
import (
	"time"
)

// DatabaseOptions mirrors the Python binding's DatabaseOptions.
// Zero values mean "use the Rust default" for numeric fields; pointer
// fields mean "use the default" when nil.
type DatabaseOptions struct {
	// GcInterval is the datastore version-GC scan period. 0 = default (500ms).
	GcInterval time.Duration
	// CleanupInterval is the commit-queue GC scan period. 0 = default (1s).
	CleanupInterval time.Duration
	// GcFullScanFrequency: every N incremental GC rounds a full scan runs.
	// 0 = default (20).
	GcFullScanFrequency uint64
	// PoolSize is the transaction object-pool capacity. 0 = default (512).
	PoolSize int
	// ResetThreshold: writesets larger than this are replaced instead of
	// cleared when reusing pooled transaction objects. 0 = default (100).
	ResetThreshold int
	// EnableCleanup: background commit-queue GC thread. nil = default (true).
	EnableCleanup *bool
	// EnableGc: background datastore version-GC thread. nil = default (true).
	EnableGc *bool
}

// NewDatabaseOptions returns options with the Rust defaults.
func NewDatabaseOptions() *DatabaseOptions { return &DatabaseOptions{} }

// WithGcInterval sets the datastore version-GC scan period.
func (o *DatabaseOptions) WithGcInterval(d time.Duration) *DatabaseOptions {
	o.GcInterval = d
	return o
}

// WithCleanupInterval sets the commit-queue GC scan period.
func (o *DatabaseOptions) WithCleanupInterval(d time.Duration) *DatabaseOptions {
	o.CleanupInterval = d
	return o
}

// WithGcFullScanFrequency sets the full-scan cadence (every N rounds).
func (o *DatabaseOptions) WithGcFullScanFrequency(n uint64) *DatabaseOptions {
	o.GcFullScanFrequency = n
	return o
}

// WithPoolSize sets the transaction object-pool capacity.
func (o *DatabaseOptions) WithPoolSize(n int) *DatabaseOptions {
	o.PoolSize = n
	return o
}

// WithResetThreshold sets the writeset-reuse threshold of the object pool.
func (o *DatabaseOptions) WithResetThreshold(n int) *DatabaseOptions {
	o.ResetThreshold = n
	return o
}

// WithCleanup returns o configured with the cleanup thread on/off.
func (o *DatabaseOptions) WithCleanup(enabled bool) *DatabaseOptions {
	o.EnableCleanup = &enabled
	return o
}

// WithGc returns o configured with the version-GC thread on/off.
func (o *DatabaseOptions) WithGc(enabled bool) *DatabaseOptions {
	o.EnableGc = &enabled
	return o
}

func (o *DatabaseOptions) toC() *C.sk_db_options {
	if o == nil {
		return nil
	}
	c := &C.sk_db_options{}
	c.gc_interval_ms = C.uint64_t(o.GcInterval.Milliseconds())
	c.cleanup_interval_ms = C.uint64_t(o.CleanupInterval.Milliseconds())
	c.gc_full_scan_frequency = C.uint64_t(o.GcFullScanFrequency)
	c.pool_size = C.uint64_t(o.PoolSize)
	c.reset_threshold = C.uint64_t(o.ResetThreshold)
	if o.EnableCleanup != nil {
		c.enable_cleanup = triBool(*o.EnableCleanup)
	}
	if o.EnableGc != nil {
		c.enable_gc = triBool(*o.EnableGc)
	}
	return c
}

// triBool encodes a tri-state bool for the FFI: -1 default, 0 false, 1 true.
func triBool(v bool) C.int32_t {
	if v {
		return 1
	}
	return 0
}

// Modes for PersistenceOptions, mirroring the Rust enums.
const (
	SnapshotNever    = 0 // snapshots only via manual triggers
	SnapshotInterval = 1 // periodic background snapshot

	AolNever = 0 // no AOL writes
	AolSync  = 1 // write + fsync per commit
	AolAsync = 2 // background batch writer
)

// PersistenceOptions mirrors the Python binding's PersistenceOptions.
// Construct with NewPersistenceOptions and configure via the builder
// methods (WithPureSnapshot / WithAsyncAol / WithSyncAol), matching the
// Rust presets.
type PersistenceOptions struct {
	// BasePath is the persistence root directory (created if missing).
	BasePath string

	// SnapshotPath optionally overrides the snapshot file path
	// (relative paths are resolved against BasePath).
	SnapshotPath string
	// AolPath optionally overrides the AOL file path
	// (relative paths are resolved against BasePath).
	AolPath string

	snapshotMode     int32
	snapshotInterval time.Duration
	aolMode          int32
	fsyncMode        int32
	fsyncInterval    time.Duration
	compression      int32
}

// NewPersistenceOptions returns options with the Rust defaults:
// no automatic snapshot, no AOL, no compression.
func NewPersistenceOptions(basePath string) *PersistenceOptions {
	return &PersistenceOptions{
		BasePath:     basePath,
		snapshotMode: SnapshotNever,
		aolMode:      AolNever,
		fsyncMode:    0, // FsyncMode::Never
		compression:  0, // CompressionMode::None
	}
}

// WithPureSnapshot enables periodic full snapshots only (no AOL).
// Crash-loss window equals the snapshot interval.
func (o *PersistenceOptions) WithPureSnapshot(interval time.Duration) *PersistenceOptions {
	o.snapshotMode = SnapshotInterval
	o.snapshotInterval = interval
	o.aolMode = AolNever
	o.fsyncMode = 0
	return o
}

// WithAsyncAol enables periodic snapshots plus the asynchronous batched
// AOL writer with periodic fsync (recommended default).
func (o *PersistenceOptions) WithAsyncAol(interval time.Duration) *PersistenceOptions {
	o.snapshotMode = SnapshotInterval
	o.snapshotInterval = interval
	o.aolMode = AolAsync
	o.fsyncMode = 2 // FsyncMode::Interval(100ms)
	o.fsyncInterval = 100 * time.Millisecond
	return o
}

// WithSyncAol enables periodic snapshots plus synchronous AOL writes with
// fsync on every commit (strongest durability, lowest throughput).
func (o *PersistenceOptions) WithSyncAol(interval time.Duration) *PersistenceOptions {
	o.snapshotMode = SnapshotInterval
	o.snapshotInterval = interval
	o.aolMode = AolSync
	o.fsyncMode = 1 // FsyncMode::EveryAppend
	return o
}

// WithLz4Compression enables transparent LZ4 compression of snapshot files.
func (o *PersistenceOptions) WithLz4Compression() *PersistenceOptions {
	o.compression = 1
	return o
}

// WithSnapshotPath overrides the snapshot file path (relative paths are
// resolved against BasePath).
func (o *PersistenceOptions) WithSnapshotPath(p string) *PersistenceOptions {
	o.SnapshotPath = p
	return o
}

// WithAolPath overrides the AOL file path (relative paths are resolved
// against BasePath).
func (o *PersistenceOptions) WithAolPath(p string) *PersistenceOptions {
	o.AolPath = p
	return o
}

// toC marshals the options into the C struct. All returned CStrings must be
// released by the caller via free(3).
func (o *PersistenceOptions) toC() (*C.sk_persist_options, []*C.char) {
	var keep []*C.char
	add := func(s string) *C.char {
		cs := C.CString(s)
		keep = append(keep, cs)
		return cs
	}
	c := &C.sk_persist_options{}
	c.base_path = add(o.BasePath)
	if o.SnapshotPath != "" {
		c.snapshot_path = add(o.SnapshotPath)
	}
	if o.AolPath != "" {
		c.aol_path = add(o.AolPath)
	}
	c.snapshot_mode = C.int32_t(o.snapshotMode)
	c.snapshot_interval_ms = C.uint64_t(o.snapshotInterval.Milliseconds())
	c.aol_mode = C.int32_t(o.aolMode)
	c.fsync_mode = C.int32_t(o.fsyncMode)
	c.fsync_interval_ms = C.uint64_t(o.fsyncInterval.Milliseconds())
	c.compression = C.int32_t(o.compression)
	return c, keep
}
