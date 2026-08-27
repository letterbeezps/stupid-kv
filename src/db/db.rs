use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{ops::Deref, sync::Arc};

use crate::PersistenceOptions;
use crate::db::inner::Inner;
use crate::options::{DEFAULT_CLEANUP_INTERVAL, DEFAULT_GC_FULL_SCAN_FREQUENCY, DEFAULT_GC_INTERVAL, DatabaseOptions};
use crate::persistence::Persistence;
use crate::pool::{DEFAULT_POOL_SIZE, Pool};
use crate::tx::Transaction;



pub struct Database {
    inner: Arc<Inner>,

    /// 事务对象池。复用 TransactionInner，降低频繁创建/销毁开销。
    pool: Arc<Pool>,

	/// commit queue GC 后台线程扫描周期。
	cleanup_interval: Duration,

	/// 版本 GC 后台线程扫描周期。
	gc_interval: Duration,

	/// 每 N 轮增量 GC 触发一次全量 GC；启动时会 clamp 到 `.max(1)` 防 `% 0` 除零。
	gc_full_scan_frequency: u64,

	/// 持久化实例。`None` 表示纯内存运行，不产生任何磁盘 IO。
	///
	/// 与 `Inner.persistence` 的双持有：
	/// - Database 侧持有 `Option<Persistence>`（值语义），Database 的构造 / shutdown 以此为准；
	/// - Inner 侧持有 `RwLock<Option<Arc<Persistence>>>`（引用语义），供以后其他模块从 Inner
	///   反向获取 Persistence 引用（如 WAL 模块写日志需要拿 Persistence 路径）。
	persistence: Option<Persistence>,
}

impl Default for Database {
    fn default() -> Self {
		let inner = Arc::new(Inner::default());
		let pool = Pool::new(inner.clone(), DEFAULT_POOL_SIZE);
        Self {
            inner,
			pool,

			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,

			gc_interval: DEFAULT_GC_INTERVAL,

			gc_full_scan_frequency: DEFAULT_GC_FULL_SCAN_FREQUENCY,

			persistence: None,
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
		let pool = Pool::new(inner.clone(), DEFAULT_POOL_SIZE);
		
		let db = Database {
			inner,
			pool,
			cleanup_interval: opts.cleanup_interval,
			gc_interval: opts.gc_interval,
			gc_full_scan_frequency: opts.gc_full_scan_frequency,
			persistence: None,
		};

		if opts.enable_cleanup {
			db.intialise_cleanup_worker();
		}

		if opts.enable_gc {
			db.initialise_garbage_worker();
		}

		db
	}

	/// 带持久化能力构造 Database：先建 Inner → 建 Persistence（内部 load 恢复快照） →
	/// 回填 `Inner.persistence` → 最后启动两条 GC 线程。
	///
	/// # 顺序约束
	///
	/// 两条 GC 线程必须在 `Persistence::new_with_options`（内部会 `load()` 恢复数据）之后再启动，
	/// 原因：
	/// - 版本 GC 计算 `cleanup_ts = min(now, earliest_active_version, oracle_now)`；
	/// - 刚 load 完还没有任何活跃事务注册 counter，`earliest_active_version = now`；
	/// - 如果此时先启动了 GC 线程，它会把 load 进来的历史版本直接当作"没人引用"一口气清掉，
	///   导致后续开启的老版本快照事务读不到应有的数据。
	///
	/// # 对外签名
	///
	/// 返回 `std::io::Result<Self>` 而非 `Result<Self, PersistenceError>`：
	/// 给调用方一个稳定、不依赖本库内部错误类型的纯 IO 结果（`PersistenceError` 三种变体
	/// 都通过 `std::io::Error::other` 包一层）。
	pub fn new_with_persistence(
		opts: DatabaseOptions,
		persistence_opts: PersistenceOptions,
	) -> std::io::Result<Self> {
		let inner = Arc::new(Inner::new(&opts));

		// Persistence::new_with_options 内部顺序：
		//   1) create_dir_all(base_path / snapshot_path)
		//   2) load() 从快照文件恢复 datastore
		//   3) spwan_snapshot_worker() 起后台周期快照线程（Interval 模式）
		let persist = Persistence::new_with_options(persistence_opts, inner.clone())
			.map_err(std::io::Error::other)?;

		// 双持有：Inner 也保存一份 Arc<Persistence> 引用，便于以后 WAL 等模块从 Inner 侧拿到。
		// 使用 Arc 而非值：Persistence 实现了 Clone（内部所有字段都是 Arc/RwLock 包装），
		// clone 出来的实例共享同一份 snapshot_handle / background_threads_enabled。
		inner.persistence.write().replace(Arc::new(persist.clone()));

		let pool = Pool::new(inner.clone(), DEFAULT_POOL_SIZE);
		let db = Database {
			inner,
			pool,
			cleanup_interval: opts.cleanup_interval,
			gc_interval: opts.gc_interval,
			gc_full_scan_frequency: opts.gc_full_scan_frequency,
			persistence: Some(persist),
		};

		// ★ 关键顺序：load 完、对外构造快完成时才启动 GC 线程，避免 GC 误收 load 进来的历史版本
		if opts.enable_cleanup {
			db.intialise_cleanup_worker();
		}

		if opts.enable_gc {
			db.initialise_garbage_worker();
		}

		Ok(db)
	}

    /// 开启一个事务。优先从对象池复用已结束的 TransactionInner，
    /// 池空时再新建；事务 Drop 时自动回收到池。
    pub fn transaction(&self, write: bool) -> Transaction {
        self.pool.get(write)
    }

	/// 手动触发一次 commit queue GC，与后台线程共用同一入口。
	pub fn run_cleanup(&self) {
		self.inner.run_cleanup_inner();
	}

	/// 手动触发一次 datastore 版本 GC，与后台线程共用同一入口。
	/// 先算 `cleanup_ts` 水位（所有活跃事务快照的下界），再执行
	/// 增量 GC（dirty queue）+ 全量 GC 兜底，两步都以该水位为回收上界。
	pub fn run_gc(&self) {
		let cleanup_ts = self.compute_cleanup_ts();
		self.run_gc_dirty_inner(cleanup_ts);
		self.run_gc_full(cleanup_ts);
	}

	/// 关停所有后台线程：
	///
	/// # 顺序（先关读线程再关写线程）
	/// 1. snapshot worker（读 datastore versions 链做序列化）—— 先关，保证最后一次 snapshot
	///    看到的版本链是完整的（GC 线程还没进场裁版本）；
	/// 2. commit queue GC（删 commit_queue 历史 entry）；
	/// 3. datastore 版本 GC（裁 versions 链 + 摘除空 entry）。
	///
	/// 每条线程同一套 shutdown 协议：`store(false)` 置开关 → `unpark` 唤醒 park → `join` 等待退出。
	/// 未启动的线程（handle 为 None）被 `take()` 拿到 None 后安全跳过。
	fn shutdown(&self) {

		{
			// 阶段 1：先关 snapshot worker（读线程）
			if let Some(ref persistence) = self.persistence {
				persistence.background_threads_enabled.store(false, Ordering::Release);

				// 等待快照线程完成。同样的操作在 persistence 回收的时候也会执行，作为兜底。
				if let Some(handle) = persistence.snapshot_handle.write().take() {
					handle.thread().unpark();
					let _ = handle.join();
				}
			}
		}

		// 阶段 2 & 3：关两条 GC 线程（写 datastore / commit_queue）
		self.background_threads_enabled.store(false, Ordering::Relaxed);
		{
			if let Some(handle) = self.transaction_cleanup_handle.write().take() {
				handle.thread().unpark();
				let _ = handle.join();
			}
			if let Some(handle) = self.garbage_collection_handle.write().take() {
				handle.thread().unpark();
				let _ = handle.join();
			}
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