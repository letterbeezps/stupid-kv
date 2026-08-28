//! C ABI bindings for `stupid-kv`.
//!
//! Consumed by the Go bindings (`stupid-kv-go`) via cgo; the header lives
//! at `include/stupid_kv.h`.
//!
//! Design notes:
//! - Handles are opaque pointers (`Box::into_raw`); every constructor has a
//!   matching `_free` that runs the Rust `Drop` (auto-cancel for transactions).
//! - All entry points are `catch_unwind`-wrapped: panics never cross the FFI
//!   boundary (unwinding into C is UB).
//! - Errors are `(code, message)` pairs: the code is the return value, the
//!   message goes through the trailing `char **err_out` out-parameter
//!   (allocated by Rust, released via `sk_free_string`). A thread-local
//!   "last error" would be unreliable under cgo because consecutive calls
//!   may land on different OS threads.
//! - The transaction handle wraps `Mutex<Option<Transaction>>`: `Option`
//!   supports in-place isolation-level switching (the Rust builders consume
//!   `self`), `Mutex` makes the handle safe to share across goroutines.

#![allow(clippy::missing_safety_doc)]

use std::ffi::CString;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::slice;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

extern crate stupid_kv as kv;

use kv::error::Error as KvError;
use kv::{AolMode, CompressionMode, FsyncMode, SnapshotMode};

// =====================================================================
// Return codes (keep in sync with include/stupid_kv.h)
// =====================================================================

pub const SK_OK: i32 = 0;
pub const SK_NOT_FOUND: i32 = 1;

pub const SK_NULL_ARG: i32 = -1;
pub const SK_TX_CLOSED: i32 = -2;
pub const SK_TX_NOT_WRITABLE: i32 = -3;
pub const SK_WRITE_CONFLICT: i32 = -4;
pub const SK_READ_CONFLICT: i32 = -5;
pub const SK_ALREADY_EXISTS: i32 = -6;
pub const SK_COMMIT_NOT_PERSISTED: i32 = -7;
pub const SK_IO: i32 = -8;
pub const SK_PANIC: i32 = -99;

// =====================================================================
// Error plumbing
// =====================================================================

/// Store a message into `err_out` (if non-null). Caller releases via
/// `sk_free_string`.
fn set_err(err_out: *mut *mut c_char, msg: String) {
    if err_out.is_null() {
        return;
    }
    let c = CString::new(msg).unwrap_or_else(|_| CString::new("invalid error message").unwrap());
    unsafe { *err_out = c.into_raw() };
}

fn map_err(e: KvError) -> (i32, String) {
    match e {
        KvError::TxClosed => (SK_TX_CLOSED, "transaction is closed".into()),
        KvError::KeyWriteConflict => {
            (SK_WRITE_CONFLICT, "write conflict, retry the transaction".into())
        }
        KvError::KeyReadConflict => {
            (SK_READ_CONFLICT, "read conflict, retry the transaction".into())
        }
        KvError::TxNotWritable => (SK_TX_NOT_WRITABLE, "transaction is not writable".into()),
        KvError::KeyAlreadyExists => (SK_ALREADY_EXISTS, "key already exists".into()),
        KvError::TxCommitNotPersisted(p) => {
            (SK_COMMIT_NOT_PERSISTED, format!("commit not persisted: {p}"))
        }
    }
}

/// Panic barrier + error out-param plumbing shared by all fallible entry
/// points. `f` returns `Ok(code)` or `Err((code, message))`.
fn guard<F>(err_out: *mut *mut c_char, f: F) -> i32
where
    F: FnOnce() -> Result<i32, (i32, String)>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(code)) => code,
        Ok(Err((code, msg))) => {
            set_err(err_out, msg);
            code
        }
        Err(_) => {
            set_err(err_out, "rust panic escaped the FFI boundary".into());
            SK_PANIC
        }
    }
}

/// Borrow a raw `(ptr, len)` byte pair as a Rust slice.
/// `null` with `len == 0` is an empty slice; `null` with `len > 0` is
/// rejected so we never build a slice from a dangling pointer.
///
/// # Safety
/// `ptr` must point to `len` valid bytes (or be null when len is 0).
unsafe fn bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], i32> {
    if ptr.is_null() {
        if len != 0 {
            return Err(SK_NULL_ARG);
        }
        return Ok(&[]);
    }
    Ok(slice::from_raw_parts(ptr, len))
}

// =====================================================================
// Options (repr(C), keep in sync with include/stupid_kv.h)
// =====================================================================

#[repr(C)]
pub struct SkDbOptions {
    pub gc_interval_ms: u64,
    pub cleanup_interval_ms: u64,
    pub gc_full_scan_frequency: u64,
    pub pool_size: u64,
    pub reset_threshold: u64,
    pub enable_cleanup: i32,
    pub enable_gc: i32,
}

#[repr(C)]
pub struct SkPersistOptions {
    pub base_path: *const c_char,
    pub snapshot_path: *const c_char,
    pub aol_path: *const c_char,
    pub snapshot_mode: i32,
    pub snapshot_interval_ms: u64,
    pub aol_mode: i32,
    pub fsync_mode: i32,
    pub fsync_interval_ms: u64,
    pub compression: i32,
}

impl SkDbOptions {
    /// `0` / `-1` fields mean "keep the Rust default".
    unsafe fn to_kv(&self) -> kv::DatabaseOptions {
        let mut o = kv::DatabaseOptions::new();
        if self.gc_interval_ms != 0 {
            o.gc_interval = Duration::from_millis(self.gc_interval_ms);
        }
        if self.cleanup_interval_ms != 0 {
            o.cleanup_interval = Duration::from_millis(self.cleanup_interval_ms);
        }
        if self.gc_full_scan_frequency != 0 {
            o.gc_full_scan_frequency = self.gc_full_scan_frequency;
        }
        if self.pool_size != 0 {
            o.pool_size = self.pool_size as usize;
        }
        if self.reset_threshold != 0 {
            o.reset_threshold = self.reset_threshold as usize;
        }
        if self.enable_cleanup >= 0 {
            o.enable_cleanup = self.enable_cleanup != 0;
        }
        if self.enable_gc >= 0 {
            o.enable_gc = self.enable_gc != 0;
        }
        o
    }
}

impl SkPersistOptions {
    /// # Safety
    /// `base_path` must be a valid NUL-terminated C string (or null, which
    /// is rejected).
    unsafe fn to_kv(&self) -> Result<kv::PersistenceOptions, String> {
        if self.base_path.is_null() {
            return Err("base_path is NULL".into());
        }
        let base = std::ffi::CStr::from_ptr(self.base_path).to_string_lossy().into_owned();

        let snapshot_mode = match self.snapshot_mode {
            0 => SnapshotMode::Never,
            1 => SnapshotMode::Interval(Duration::from_millis(self.snapshot_interval_ms)),
            other => return Err(format!("invalid snapshot_mode: {other}")),
        };
        let aol_mode = match self.aol_mode {
            0 => AolMode::Never,
            1 => AolMode::SynchronousOnCommit,
            2 => AolMode::AsynchronousAfterCommit,
            other => return Err(format!("invalid aol_mode: {other}")),
        };
        let fsync_mode = match self.fsync_mode {
            0 => FsyncMode::Never,
            1 => FsyncMode::EveryAppend,
            2 => FsyncMode::Interval(Duration::from_millis(self.fsync_interval_ms.max(1))),
            other => return Err(format!("invalid fsync_mode: {other}")),
        };
        let compression_mode = match self.compression {
            0 => CompressionMode::None,
            1 => CompressionMode::Lz4,
            other => return Err(format!("invalid compression: {other}")),
        };

        let mut o = kv::PersistenceOptions::new(PathBuf::from(base));
        if !self.snapshot_path.is_null() {
            let p = std::ffi::CStr::from_ptr(self.snapshot_path).to_string_lossy().into_owned();
            o.snapshot_path = Some(PathBuf::from(p));
        }
        if !self.aol_path.is_null() {
            let p = std::ffi::CStr::from_ptr(self.aol_path).to_string_lossy().into_owned();
            o.aol_path = Some(PathBuf::from(p));
        }
        o.snapshot_mode = snapshot_mode;
        o.aol_mode = aol_mode;
        o.fsync_mode = fsync_mode;
        o.compression_mode = compression_mode;
        Ok(o)
    }
}

// =====================================================================
// Transaction handle
// =====================================================================

/// `Mutex<Option<Transaction>>`:
/// - `Mutex` → the handle is safe to call from any goroutine;
/// - `Option` → lets us run the consuming `with_*_isolation()` builders
///   in place (`take()` → rebuild → `Some()` back).
pub struct TxHandle(Mutex<Option<kv::Transaction>>);

fn lock_tx(tx: &TxHandle) -> MutexGuard<'_, Option<kv::Transaction>> {
    // A poisoned lock means a thread panicked while holding the guard;
    // the panic barrier already reported SK_PANIC for that call, so the
    // inner transaction state itself is still consistent. Unlock the
    // poison and carry on.
    tx.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// =====================================================================
// Database lifecycle
// =====================================================================

#[no_mangle]
pub extern "C" fn sk_db_new() -> *mut kv::Database {
    let db = catch_unwind(|| kv::Database::new());
    db.map(|db| Box::into_raw(Box::new(db))).unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `opts` may be null; otherwise it must point to a valid `sk_db_options`.
#[no_mangle]
pub unsafe extern "C" fn sk_db_new_with_options(opts: *const SkDbOptions) -> *mut kv::Database {
    let db = catch_unwind(|| {
        let kv_opts = if opts.is_null() {
            kv::DatabaseOptions::new()
        } else {
            (*opts).to_kv()
        };
        kv::Database::new_with_options(kv_opts)
    });
    db.map(|db| Box::into_raw(Box::new(db))).unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `opts` may be null; `popts` must be a valid `sk_persist_options` with a
/// valid NUL-terminated `base_path`; `err_out` may be null.
#[no_mangle]
pub unsafe extern "C" fn sk_db_new_with_persistence(
    opts: *const SkDbOptions,
    popts: *const SkPersistOptions,
    err_out: *mut *mut c_char,
) -> *mut kv::Database {
    let result = catch_unwind(|| {
        if popts.is_null() {
            return Err((SK_NULL_ARG, "popts is NULL".into()));
        }
        let kv_popts = match (*popts).to_kv() {
            Ok(o) => o,
            Err(msg) => return Err((SK_NULL_ARG, msg)),
        };
        let kv_opts = if opts.is_null() {
            kv::DatabaseOptions::new()
        } else {
            (*opts).to_kv()
        };
        kv::Database::new_with_persistence(kv_opts, kv_popts).map_err(|e| (SK_IO, e.to_string()))
    });
    match result {
        Ok(Ok(db)) => Box::into_raw(Box::new(db)),
        Ok(Err((_code, msg))) => {
            set_err(err_out, msg);
            std::ptr::null_mut()
        }
        Err(_) => {
            set_err(err_out, "rust panic escaped the FFI boundary".into());
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// `db` must be a handle returned by a `sk_db_new*` function and not yet
/// freed. All transactions opened from it should be freed first.
#[no_mangle]
pub unsafe extern "C" fn sk_db_free(db: *mut kv::Database) {
    if db.is_null() {
        return;
    }
    drop(Box::from_raw(db));
}

/// # Safety
/// `db` must be a live database handle.
#[no_mangle]
pub unsafe extern "C" fn sk_db_tx_begin(db: *mut kv::Database, write: i32) -> *mut TxHandle {
    let handle = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: caller guarantees `db` is a live handle.
        let tx = (*db).transaction(write != 0);
        TxHandle(Mutex::new(Some(tx)))
    }));
    handle.map(|h| Box::into_raw(Box::new(h))).unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `db` must be a live database handle; `err_out` may be null.
#[no_mangle]
pub unsafe extern "C" fn sk_db_snapshot(db: *mut kv::Database, err_out: *mut *mut c_char) -> i32 {
    if db.is_null() {
        return SK_NULL_ARG;
    }
    guard(err_out, || {
        (*db).snapshot().map_err(|e| (SK_IO, e.to_string()))?;
        Ok(SK_OK)
    })
}

// =====================================================================
// Transaction
// =====================================================================

/// # Safety
/// `tx` must be a handle returned by `sk_db_tx_begin` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_free(tx: *mut TxHandle) {
    if tx.is_null() {
        return;
    }
    // Dropping the inner `Transaction` auto-cancels an open transaction
    // (Rust Drop semantics), mirroring the Python bindings.
    drop(Box::from_raw(tx));
}

/// # Safety
/// `tx` must be a live transaction handle.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_version(tx: *mut TxHandle) -> u64 {
    if tx.is_null() {
        return 0;
    }
    catch_unwind(|| {
        let guard = lock_tx(&*tx);
        guard.as_ref().map(|t| t.version()).unwrap_or(0)
    })
    .unwrap_or(0)
}

/// # Safety
/// `tx` must be a live transaction handle.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_is_closed(tx: *mut TxHandle) -> i32 {
    if tx.is_null() {
        return 1;
    }
    catch_unwind(|| {
        let guard = lock_tx(&*tx);
        match guard.as_ref() {
            Some(t) => i32::from(t.closed()),
            None => 1,
        }
    })
    .unwrap_or(1)
}

/// # Safety
/// `tx` must be a live transaction handle.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_with_snapshot_isolation(tx: *mut TxHandle) -> i32 {
    switch_isolation(tx, false)
}

/// # Safety
/// `tx` must be a live transaction handle.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_with_serializable_snapshot_isolation(tx: *mut TxHandle) -> i32 {
    switch_isolation(tx, true)
}

unsafe fn switch_isolation(tx: *mut TxHandle, ssi: bool) -> i32 {
    if tx.is_null() {
        return SK_NULL_ARG;
    }
    guard(std::ptr::null_mut(), || {
        let guard = lock_tx(&*tx);
        let mut guard = guard;
        match guard.take() {
            Some(t) => {
                *guard = Some(if ssi {
                    t.with_serializable_snapshot_isolation()
                } else {
                    t.with_snapshot_isolation()
                });
                Ok(SK_OK)
            }
            None => Err((SK_TX_CLOSED, "transaction is closed".into())),
        }
    })
}

/// # Safety
/// `tx` must be a live handle; `key` must point to `key_len` bytes (or be
/// null with `key_len == 0`); `val_out` / `val_len` / `err_out` must be
/// valid out-parameters (or null, in which case results are dropped).
#[no_mangle]
pub unsafe extern "C" fn sk_tx_get(
    tx: *mut TxHandle,
    key: *const u8,
    key_len: usize,
    val_out: *mut *mut u8,
    val_len: *mut usize,
    err_out: *mut *mut c_char,
) -> i32 {
    if tx.is_null() {
        return SK_NULL_ARG;
    }
    guard(err_out, || {
        let key = bytes(key, key_len).map_err(|c| (c, "invalid key buffer".into()))?;
        let guard = lock_tx(&*tx);
        let t = guard.as_ref().ok_or((SK_TX_CLOSED, "transaction is closed".to_string()))?;
        match t.get(key) {
            Ok(Some(v)) => {
                // Copy out of the `Bytes` ref-counted buffer into an
                // owned `Box<[u8]>` handed to the caller.
                let mut boxed: Box<[u8]> = v.to_vec().into_boxed_slice();
                let len = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                if !val_out.is_null() {
                    *val_out = ptr;
                }
                if !val_len.is_null() {
                    *val_len = len;
                }
                if val_out.is_null() || val_len.is_null() {
                    // Caller is not interested in the value; reclaim it.
                    sk_free_value(ptr, len);
                }
                Ok(SK_OK)
            }
            Ok(None) => Ok(SK_NOT_FOUND),
            Err(e) => Err(map_err(e)),
        }
    })
}

/// # Safety
/// `tx` must be a live handle; `out` / `err_out` must be valid out-parameters.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_exists(
    tx: *mut TxHandle,
    key: *const u8,
    key_len: usize,
    out: *mut i32,
    err_out: *mut *mut c_char,
) -> i32 {
    if tx.is_null() {
        return SK_NULL_ARG;
    }
    guard(err_out, || {
        let key = bytes(key, key_len).map_err(|c| (c, "invalid key buffer".into()))?;
        let guard = lock_tx(&*tx);
        let t = guard.as_ref().ok_or((SK_TX_CLOSED, "transaction is closed".to_string()))?;
        match t.exists(key) {
            Ok(found) => {
                if !out.is_null() {
                    *out = i32::from(found);
                }
                Ok(SK_OK)
            }
            Err(e) => Err(map_err(e)),
        }
    })
}

/// # Safety
/// `tx` must be a live handle; key/value buffers must be valid.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_set(
    tx: *mut TxHandle,
    key: *const u8,
    key_len: usize,
    val: *const u8,
    val_len: usize,
    err_out: *mut *mut c_char,
) -> i32 {
    write_op(tx, key, key_len, Some((val, val_len)), WriteOp::Set, err_out)
}

/// # Safety
/// Same as `sk_tx_set`.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_put(
    tx: *mut TxHandle,
    key: *const u8,
    key_len: usize,
    val: *const u8,
    val_len: usize,
    err_out: *mut *mut c_char,
) -> i32 {
    write_op(tx, key, key_len, Some((val, val_len)), WriteOp::Put, err_out)
}

/// # Safety
/// Same as `sk_tx_set`, without a value buffer.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_del(
    tx: *mut TxHandle,
    key: *const u8,
    key_len: usize,
    err_out: *mut *mut c_char,
) -> i32 {
    write_op(tx, key, key_len, None, WriteOp::Del, err_out)
}

enum WriteOp {
    Set,
    Put,
    Del,
}

/// Shared body of `set` / `put` / `del`.
///
/// # Safety
/// Caller must validate `tx`; buffers are validated by `bytes()`.
unsafe fn write_op(
    tx: *mut TxHandle,
    key: *const u8,
    key_len: usize,
    val: Option<(*const u8, usize)>,
    op: WriteOp,
    err_out: *mut *mut c_char,
) -> i32 {
    if tx.is_null() {
        return SK_NULL_ARG;
    }
    guard(err_out, || {
        let key = bytes(key, key_len).map_err(|c| (c, "invalid key buffer".into()))?;
        let value = match val {
            Some((p, len)) => {
                Some(bytes(p, len).map_err(|c| (c, "invalid value buffer".into()))?)
            }
            None => None,
        };
        let mut guard = lock_tx(&*tx);
        let t = guard
            .as_mut()
            .ok_or((SK_TX_CLOSED, "transaction is closed".to_string()))?;
        let result = match op {
            WriteOp::Set => match value {
                Some(v) => t.set(key, v),
                None => Err(KvError::TxNotWritable),
            },
            WriteOp::Put => match value {
                Some(v) => t.put(key, v),
                None => Err(KvError::TxNotWritable),
            },
            WriteOp::Del => t.del(key),
        };
        result.map_err(map_err)?;
        Ok(SK_OK)
    })
}

/// # Safety
/// `tx` must be a live handle; `err_out` may be null.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_commit(tx: *mut TxHandle, err_out: *mut *mut c_char) -> i32 {
    if tx.is_null() {
        return SK_NULL_ARG;
    }
    guard(err_out, || {
        let mut guard = lock_tx(&*tx);
        let t = guard
            .as_mut()
            .ok_or((SK_TX_CLOSED, "transaction is closed".to_string()))?;
        t.commit().map_err(map_err)?;
        Ok(SK_OK)
    })
}

/// # Safety
/// `tx` must be a live handle; `err_out` may be null.
#[no_mangle]
pub unsafe extern "C" fn sk_tx_cancel(tx: *mut TxHandle, err_out: *mut *mut c_char) -> i32 {
    if tx.is_null() {
        return SK_NULL_ARG;
    }
    guard(err_out, || {
        let mut guard = lock_tx(&*tx);
        let t = guard
            .as_mut()
            .ok_or((SK_TX_CLOSED, "transaction is closed".to_string()))?;
        t.cancel().map_err(map_err)?;
        Ok(SK_OK)
    })
}

// =====================================================================
// Memory management
// =====================================================================

/// # Safety
/// `ptr` must be a pointer returned through `sk_tx_get`'s `val_out` with
/// the matching `len`, not yet freed. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn sk_free_value(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    let boxed = Box::from_raw(slice::from_raw_parts_mut(ptr, len) as *mut [u8]);
    drop(boxed);
}

/// # Safety
/// `ptr` must be a pointer returned through an `err_out` out-parameter
/// (allocated by `CString::into_raw`), not yet freed. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn sk_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    drop(CString::from_raw(ptr));
}
