use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{ops::Deref, sync::Arc};

use crate::db::inner::Inner;
use crate::options::{DEFAULT_CLEANUP_INTERVAL, DEFAULT_GC_FULL_SCAN_FREQUENCY, DEFAULT_GC_INTERVAL, DatabaseOptions};
use crate::tx::{Transaction, TransactionInner};



pub struct Database {
    inner: Arc<Inner>,

	/// commit queue GC 后台线程扫描周期。
	cleanup_interval: Duration,

	/// 版本 GC 后台线程扫描周期。
	gc_interval: Duration,

	/// 每 N 轮增量 GC 触发一次全量 GC；启动时会 clamp 到 `.max(1)` 防 `% 0` 除零。
	gc_full_scan_frequency: u64
}

impl Default for Database {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),

			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,

			gc_interval: DEFAULT_GC_INTERVAL,

			gc_full_scan_frequency: DEFAULT_GC_FULL_SCAN_FREQUENCY,
        }
    }
}

impl Drop for Database {
	/// Database 析构时必须停止后台清理线程并 join，
	/// 否则后台线程持有的 `Arc<Inner>` 会延长 Inner 生命周期，或在进程退出时留下悬挂线程。
	fn drop(&mut self) {
		self.shutdown();
	}
}

impl Deref for Database {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Database {
    pub fn new() -> Self {
        Self::new_with_options(DatabaseOptions::default())
    }

	/// 使用自定义选项构造 Database。按 `enable_cleanup` / `enable_gc` 启动两条后台 GC 线程。
	pub fn new_with_options(opts: DatabaseOptions) -> Self {
		let inner = Arc::new(Inner::new(&opts));
		let db = Database {
			inner,
			cleanup_interval: opts.cleanup_interval,
			gc_interval: opts.gc_interval,
			gc_full_scan_frequency: opts.gc_full_scan_frequency,
		};

		if opts.enable_cleanup {
			db.intialise_cleanup_worker();
		}

		if opts.enable_gc {
			db.initialise_garbage_worker();
		}

		db
	}

    pub fn transaction(&self, write: bool) -> Transaction {
        let inner = TransactionInner::new(self.inner.clone(), write);
        Transaction { inner: Some(inner) }
    }

	/// 手动触发一次 commit queue GC，与后台线程共用同一入口。
	pub fn run_cleanup(&self) {
		self.inner.run_cleanup_inner();
	}

	/// 关停两条后台 GC 线程：置开关 → `unpark` → `join`。
	/// 未启动的线程（handle 为 None）会被安全跳过。
	fn shutdown(&self) {
		self.background_threads_enabled.store(false, Ordering::Relaxed);
		if let Some(handle) = self.transaction_cleanup_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
		if let Some(handle) = self.garbage_collection_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
	}

	/// 启动 commit queue GC 后台线程。
	/// `park_timeout` 而非 `sleep`：shutdown 可通过 `unpark` 立即唤醒。
	fn intialise_cleanup_worker(&self) {
		let inner = self.inner.clone();
		if inner.transaction_cleanup_handle.read().is_none() {
			let interval = self.cleanup_interval;

			let handle = std::thread::spawn(move || {
				while inner.background_threads_enabled.load(Ordering::Relaxed) {
					std::thread::park_timeout(interval);

					if !inner.background_threads_enabled.load(Ordering::Relaxed) {
						break;
					}

					inner.run_cleanup_inner();
				}
			});
			*self.inner.transaction_cleanup_handle.write() = Some(handle);
		}
	}

	/// 启动 datastore 版本 GC 后台线程。
	/// 每轮：`compute_cleanup_ts` 算水位 → 增量 GC，每 N 轮再叠加一次全量 GC。
	/// 增量与全量共享同轮水位，避免读路径看到不一致的中间态。
	fn initialise_garbage_worker(&self) {
		let inner = self.inner.clone();
		if inner.garbage_collection_handle.read().is_none() {
			let interval = self.gc_interval;
			let full_scan_frequency = self.gc_full_scan_frequency.max(1);
			let handle = std::thread::spawn(move || {
				let mut cycle: u64 = 0;
				while inner.background_threads_enabled.load(Ordering::Relaxed) {
					std::thread::park_timeout(interval);

					if !inner.background_threads_enabled.load(Ordering::Relaxed) {
						break;
					}

					let cleanup_ts = inner.compute_cleanup_ts();
					inner.run_gc_dirty_inner(cleanup_ts);
					cycle += 1;
					if cycle.is_multiple_of(full_scan_frequency) {
						inner.run_gc_full(cleanup_ts);
					}
				}
			});
			*self.inner.garbage_collection_handle.write() = Some(handle);
		}
	}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_tx() {
		let db = Database::new();
		db.transaction(false);
	}

    #[test]
	fn finished_tx_not_writeable() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		let res = tx.cancel();
		assert!(res.is_ok());
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_err());
	}


    #[test]
	fn cancelled_tx_is_cancelled() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(!res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

    #[test]
	fn committed_tx_is_committed() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

    
}