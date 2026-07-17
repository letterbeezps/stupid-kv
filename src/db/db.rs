use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{ops::Deref, sync::Arc};

use crate::db::inner::Inner;
use crate::options::{DEFAULT_CLEANUP_INTERVAL, DatabaseOptions};
use crate::tx::{Transaction, TransactionInner};



pub struct Database {
    inner: Arc<Inner>,

	/// 后台 GC 清理线程的扫描周期，取自 `DatabaseOptions::cleanup_interval`。
	cleanup_interval: Duration,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),

			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
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

	/// 使用自定义选项构造 Database。
	/// 当 `opts.enable_cleanup` 为 true 时，会启动一个后台线程周期性执行 commit queue GC。
	pub fn new_with_options(opts: DatabaseOptions) -> Self {
		let inner = Arc::new(Inner::new(&opts));
		let db = Database {
			inner,
			cleanup_interval: opts.cleanup_interval,
		};

		if opts.enable_cleanup {
			db.intialise_cleanup_worker();
		}

		db
	}

    pub fn transaction(&self, write: bool) -> Transaction {
        let inner = TransactionInner::new(self.inner.clone(), write);
        Transaction { inner: Some(inner) }
    }

	/// 手动触发一次 commit queue GC。
	/// 与后台线程调用的是同一入口，便于测试及在关闭后台线程时按需回收。
	pub fn run_cleanup(&self) {
		self.inner.run_cleanup_inner();
	}

	/// 关停后台清理线程：
	/// 1. 将 `background_threads_enabled` 置为 false，通知线程退出循环；
	/// 2. `unpark` 唤醒可能正在 `park_timeout` 中沉睡的线程，避免等到超时才退出；
	/// 3. `join` 等待线程真正结束，保证 Inner 内部结构在无并发访问后再被释放。
	fn shutdown(&self) {
		self.background_threads_enabled.store(false, Ordering::Relaxed);
		{
			if let Some(handle) = self.transaction_cleanup_handle.write().take() {
				handle.thread().unpark();
				let _ = handle.join();
			}
		}
	}

	/// 启动后台 GC 清理线程。
	///
	/// 线程主循环：
	/// - `park_timeout(interval)` 挂起等待，既能被 `unpark` 提前唤醒（用于快速关停），
	///   又能在超时后自然醒来执行一次 `run_cleanup_inner`；
	/// - 醒来后再次检查 `background_threads_enabled`，避免关停后仍多跑一次清理。
	///
	/// 线程通过 clone 的 `Arc<Inner>` 独立持有 Inner 的所有权，
	/// 与 Database 的生命周期通过 `shutdown` 显式同步。
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