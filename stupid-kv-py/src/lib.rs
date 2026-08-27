//! Python bindings for `stupid-kv` via PyO3.
//!
//! Build & install:
//! ```bash
//! pip install maturin
//! maturin develop --release
//! ```
//!
//! Usage:
//! ```python
//! import stupid_kv
//!
//! db = stupid_kv.Database()
//! tx = db.transaction(write=True)
//! tx.set(b"hello", b"world")
//! tx.commit()
//!
//! tx = db.transaction(write=False)
//! assert tx.get(b"hello") == b"world"
//! ```

use std::path::PathBuf;
use std::time::Duration;

// The `pyo3::create_exception!` macro below generates a module named `stupid_kv`,
// which collides with the external crate of the same name. Rename the crate at
// import time so we can refer to both unambiguously.
extern crate stupid_kv as kv;

use pyo3::exceptions::{PyException, PyOSError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

// =====================================================================
// Exceptions
// =====================================================================

pyo3::create_exception!(stupid_kv, StupidKvError, PyException);
pyo3::create_exception!(stupid_kv, KeyWriteConflict, StupidKvError);
pyo3::create_exception!(stupid_kv, KeyReadConflict, StupidKvError);
pyo3::create_exception!(stupid_kv, KeyAlreadyExists, StupidKvError);
pyo3::create_exception!(stupid_kv, TxClosed, StupidKvError);
pyo3::create_exception!(stupid_kv, TxNotWritable, StupidKvError);

fn tx_err_to_py(e: kv::error::Error) -> PyErr {
    use kv::error::Error;
    match e {
        Error::KeyWriteConflict => {
            KeyWriteConflict::new_err("write-write conflict detected")
        }
        Error::KeyReadConflict => KeyReadConflict::new_err(
            "read-write conflict detected (Serializable Snapshot Isolation)",
        ),
        Error::KeyAlreadyExists => {
            KeyAlreadyExists::new_err("key already exists")
        }
        Error::TxClosed => TxClosed::new_err("transaction is closed"),
        Error::TxNotWritable => {
            TxNotWritable::new_err("transaction is read-only")
        }
        Error::TxCommitNotPersisted(p) => PyOSError::new_err(p.to_string()),
    }
}

// =====================================================================
// Database
// =====================================================================

/// An MVCC key-value database with snapshot persistence and AOL incremental log.
///
/// Mirrors `sqlite3.Connection`: create one per process, share across threads.
///
/// Example:
///     db = stupid_kv.Database()
///     tx = db.transaction(write=True)
///     tx.set(b"k", b"v")
///     tx.commit()
#[pyclass(name = "Database")]
pub struct PyDatabase {
    inner: kv::Database,
}

#[pymethods]
impl PyDatabase {
    /// Create a new in-memory database.
    #[new]
    fn new() -> Self {
        Self {
            inner: kv::Database::new(),
        }
    }

    /// Create a database with custom options.
    #[staticmethod]
    fn with_options(opts: &PyDatabaseOptions) -> Self {
        Self {
            inner: kv::Database::new_with_options(opts.inner.clone()),
        }
    }

    /// Create a database with snapshot + AOL persistence enabled.
    ///
    /// Raises `OSError` on I/O failure.
    #[staticmethod]
    fn with_persistence(
        opts: &PyDatabaseOptions,
        persist: &PyPersistenceOptions,
    ) -> PyResult<Self> {
        kv::Database::new_with_persistence(opts.inner.clone(), persist.inner.clone())
            .map(|inner| Self { inner })
            .map_err(|e| PyOSError::new_err(e.to_string()))
    }

    /// Begin a new transaction. `write=True` for read-write, `write=False` for read-only.
    fn transaction(&self, write: bool) -> PyTransaction {
        PyTransaction {
            inner: Some(self.inner.transaction(write)),
        }
    }

    fn __repr__(&self) -> &'static str {
        "<stupid_kv.Database>"
    }

    /// Context manager support: `with stupid_kv.Database() as db: ...`
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Returns False (does not suppress exceptions).
    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        false
    }
}

// =====================================================================
// Transaction
// =====================================================================

/// An MVCC transaction.
///
/// Created via `Database.transaction()`. Must be either committed or cancelled
/// (drop auto-cancels). Use as a context manager for auto-rollback on exception.
#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    /// `None` once the transaction has been taken (moved into a builder or otherwise
    /// invalidated). Reading/writing a None transaction raises `TxClosed`.
    inner: Option<kv::Transaction>,
}

#[pymethods]
impl PyTransaction {
    /// Switch to Snapshot Isolation (SI). Consumes self, returns a new wrapper.
    fn with_snapshot_isolation(mut slf: PyRefMut<'_, Self>) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.map(|t| t.with_snapshot_isolation());
        slf.into()
    }

    /// Switch to Serializable Snapshot Isolation (SSI). Consumes self.
    fn with_serializable_snapshot_isolation(mut slf: PyRefMut<'_, Self>) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.map(|t| t.with_serializable_snapshot_isolation());
        slf.into()
    }

    /// Oracle timestamp (MVCC version) captured at transaction start.
    fn version(&self) -> u64 {
        self.inner.as_ref().map(|t| t.version()).unwrap_or(0)
    }

    /// True if commit/cancel has been called.
    fn closed(&self) -> bool {
        self.inner.as_ref().map(|t| t.closed()).unwrap_or(true)
    }

    /// Commit the transaction. Raises `KeyWriteConflict`, `KeyReadConflict`,
    /// `KeyAlreadyExists`, or `OSError` on failure.
    fn commit(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        py.allow_threads(|| inner.commit()).map_err(tx_err_to_py)
    }

    /// Cancel (rollback) the transaction. Idempotent if already closed.
    fn cancel(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        py.allow_threads(|| inner.cancel()).map_err(tx_err_to_py)
    }

    /// True if the key exists at this transaction's snapshot.
    fn exists(&self, py: Python<'_>, key: &[u8]) -> PyResult<bool> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        py.allow_threads(|| inner.exists(key)).map_err(tx_err_to_py)
    }

    /// Read the value for `key`, or None if absent.
    fn get(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        let result = py.allow_threads(|| inner.get(key)).map_err(tx_err_to_py)?;
        Ok(result.map(|b| PyBytes::new_bound(py, b.as_ref()).into()))
    }

    /// Insert or update a key. Raises `KeyWriteConflict` on conflict.
    fn set(&mut self, py: Python<'_>, key: &[u8], value: &[u8]) -> PyResult<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        py.allow_threads(|| inner.set(key, value)).map_err(tx_err_to_py)
    }

    /// Insert only if absent. Raises `KeyAlreadyExists` if key already exists.
    fn put(&mut self, py: Python<'_>, key: &[u8], value: &[u8]) -> PyResult<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        py.allow_threads(|| inner.put(key, value)).map_err(tx_err_to_py)
    }

    /// Delete a key. Raises `KeyWriteConflict` on conflict.
    #[pyo3(signature = (key))]
    fn delete(&mut self, py: Python<'_>, key: &[u8]) -> PyResult<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| TxClosed::new_err("transaction is closed"))?;
        py.allow_threads(|| inner.del(key)).map_err(tx_err_to_py)
    }

    /// Context manager: `with db.transaction(write=True) as tx: ...`
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager semantics (sqlite-style):
    /// - Normal exit (`exc_type is None`): auto-commit
    /// - Exception in progress: auto-rollback
    /// - Already committed/cancelled: no-op
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if self.closed() {
            return Ok(false);
        }
        if exc_type.is_none() {
            // Normal exit: commit (best-effort; ignore errors so the user sees
            // them on explicit commit() instead of double-reporting).
            let _ = py.allow_threads(|| self.inner.as_mut().unwrap().commit());
        } else {
            // Exception in flight: rollback.
            let _ = py.allow_threads(|| self.inner.as_mut().unwrap().cancel());
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "<stupid_kv.Transaction version={} closed={}>",
            self.version(),
            self.closed()
        )
    }
}

// =====================================================================
// DatabaseOptions
// =====================================================================

/// Tunable runtime options for a `Database`.
#[pyclass(name = "DatabaseOptions")]
#[derive(Clone)]
pub struct PyDatabaseOptions {
    inner: kv::DatabaseOptions,
}

#[pymethods]
impl PyDatabaseOptions {
    #[new]
    fn new() -> Self {
        Self {
            inner: kv::DatabaseOptions::new(),
        }
    }

    fn with_gc_interval(mut slf: PyRefMut<'_, Self>, ms: u64) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_gc_interval(Duration::from_millis(ms));
        slf.into()
    }

    fn with_cleanup_interval(mut slf: PyRefMut<'_, Self>, ms: u64) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_cleanup_interval(Duration::from_millis(ms));
        slf.into()
    }

    #[getter]
    fn pool_size(&self) -> usize {
        self.inner.pool_size
    }
    #[setter]
    fn set_pool_size(&mut self, value: usize) {
        self.inner.pool_size = value;
    }

    #[getter]
    fn enable_cleanup(&self) -> bool {
        self.inner.enable_cleanup
    }
    #[setter]
    fn set_enable_cleanup(&mut self, value: bool) {
        self.inner.enable_cleanup = value;
    }

    #[getter]
    fn enable_gc(&self) -> bool {
        self.inner.enable_gc
    }
    #[setter]
    fn set_enable_gc(&mut self, value: bool) {
        self.inner.enable_gc = value;
    }

    #[getter]
    fn gc_full_scan_frequency(&self) -> u64 {
        self.inner.gc_full_scan_frequency
    }
    #[setter]
    fn set_gc_full_scan_frequency(&mut self, value: u64) {
        self.inner.gc_full_scan_frequency = value;
    }

    #[getter]
    fn reset_threshold(&self) -> usize {
        self.inner.reset_threshold
    }
    #[setter]
    fn set_reset_threshold(&mut self, value: usize) {
        self.inner.reset_threshold = value;
    }

    fn __repr__(&self) -> String {
        format!(
            "<stupid_kv.DatabaseOptions pool_size={} cleanup={} gc={}>",
            self.inner.pool_size, self.inner.enable_cleanup, self.inner.enable_gc
        )
    }
}

// =====================================================================
// PersistenceOptions
// =====================================================================

/// Snapshot + AOL persistence configuration. Pass to `Database.with_persistence()`.
#[pyclass(name = "PersistenceOptions")]
#[derive(Clone)]
pub struct PyPersistenceOptions {
    inner: kv::PersistenceOptions,
}

#[pymethods]
impl PyPersistenceOptions {
    #[new]
    fn new(base_path: &str) -> Self {
        Self {
            inner: kv::PersistenceOptions::new(PathBuf::from(base_path)),
        }
    }

    /// Enable snapshot persistence with the given interval (seconds).
    fn with_pure_snapshot(mut slf: PyRefMut<'_, Self>, interval_sec: u64) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_pure_snapshot(Duration::from_secs(interval_sec));
        slf.into()
    }

    /// Enable asynchronous AOL with the given fsync interval (seconds).
    fn with_async_aol(mut slf: PyRefMut<'_, Self>, interval_sec: u64) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_async_aol(Duration::from_secs(interval_sec));
        slf.into()
    }

    /// Enable synchronous (per-commit) AOL with the given fsync interval (seconds).
    fn with_sync_aol(mut slf: PyRefMut<'_, Self>, interval_sec: u64) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_sync_aol(Duration::from_secs(interval_sec));
        slf.into()
    }

    fn with_snapshot_path(mut slf: PyRefMut<'_, Self>, path: &str) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_snapshot_path(Some(PathBuf::from(path)));
        slf.into()
    }

    fn with_aol_path(mut slf: PyRefMut<'_, Self>, path: &str) -> Py<Self> {
        let taken = std::mem::take(&mut slf.inner);
        slf.inner = taken.with_aol_path(Some(PathBuf::from(path)));
        slf.into()
    }

    #[getter]
    fn base_path(&self) -> String {
        self.inner.base_path.to_string_lossy().into_owned()
    }
    #[setter]
    fn set_base_path(&mut self, value: &str) {
        self.inner.base_path = PathBuf::from(value);
    }

    fn __repr__(&self) -> String {
        format!(
            "<stupid_kv.PersistenceOptions base_path='{}'>",
            self.inner.base_path.display()
        )
    }
}

// =====================================================================
// Module
// =====================================================================

#[pymodule]
fn stupid_kv(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyTransaction>()?;
    m.add_class::<PyDatabaseOptions>()?;
    m.add_class::<PyPersistenceOptions>()?;

    m.add("StupidKvError", m.py().get_type_bound::<StupidKvError>())?;
    m.add("KeyWriteConflict", m.py().get_type_bound::<KeyWriteConflict>())?;
    m.add("KeyReadConflict", m.py().get_type_bound::<KeyReadConflict>())?;
    m.add("KeyAlreadyExists", m.py().get_type_bound::<KeyAlreadyExists>())?;
    m.add("TxClosed", m.py().get_type_bound::<TxClosed>())?;
    m.add("TxNotWritable", m.py().get_type_bound::<TxNotWritable>())?;

    Ok(())
}
