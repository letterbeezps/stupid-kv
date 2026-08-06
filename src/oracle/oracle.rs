use std::{sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::Duration};



use crate::oracle::Inner;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use web_time::{SystemTime, Instant, UNIX_EPOCH};

/// 版本号发号器（Timestamp Oracle）。
///
/// 为 MVCC 提供**单调递增**的 `u64` 版本号，服务于两条路径：
/// - **快照点**：事务开启时通过 [`Oracle::current_timestamp`] 读一次，
///   作为该事务可见性视图的上界（见 `TransactionInner::new`）。
/// - **提交版本**：事务进入 merge queue 时，通过 [`Oracle::current_time_ns`]
///   派生候选版本号，并通过 `inner.timestamp` 上的 `fetch_max` 推进高水位
///   （见 `TransactionInner::atomic_merge`）。
///
/// 内部状态见 [`Inner`]：一个 `AtomicU64` 记录当前发出去的最大版本号（高水位），
/// 一个 `ArcSwap<(u64, Instant)>` 保存"墙钟 ns + 单调 Instant"锚点，
/// 由锚点派生的时间对 NTP 回拨免疫，且不必每次都走 `SystemTime::now()` 系统调用。
pub(crate) struct Oracle {
    pub(crate) inner: Arc<Inner>,
}

impl Drop for Oracle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Oracle {
    /// 构造一个新的 `Oracle`。
    ///
    /// 抓取一次当前墙钟 unix 纳秒作为锚点初值，同时记录对应的 `Instant`。
    /// 之后 [`Oracle::current_time_ns`] 都基于此锚点派生，保证单调性。
    /// 高水位 `timestamp` 初始化为锚点 unix ns。
    pub fn new(resync_interval: Duration) -> Arc<Self> {
        let reference_unix = Self::current_unix_ns();
        let reference_time = Instant::now();
        let oracle = Self{
            inner: Arc::new(
                Inner{
                    timestamp: AtomicU64::new(reference_unix),
                    reference: ArcSwap::new(Arc::new((reference_unix, reference_time))),
                    resync_enable: AtomicBool::new(true),
                    resync_handle: Mutex::new(None),
                    resync_interval,
                }
            )
        };
        oracle.background_resync();
        Arc::new(oracle)
    }

    /// 获取当前**系统墙钟**的 unix 纳秒时间戳。
    ///
    /// 仅用于 [`Oracle::new`] 中初始化锚点；运行期不要用它作为版本号，
    /// 因为墙钟可能被 NTP 回拨，无法保证单调。
    #[inline]
    pub(crate) fn current_unix_ns() -> u64 {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH); 
        timestamp.unwrap().as_nanos() as u64
    }

    /// 基于启动时锚定的 `(reference_unix, reference_instant)` 派生**当前 ns 时间**。
    ///
    /// 公式：`reference_unix + reference_instant.elapsed()`。
    /// - `Instant` 是操作系统提供的单调时钟，不会因 NTP 调整而回退，
    ///   故派生结果对墙钟回拨免疫。
    /// - 相比每次都调 `SystemTime::now()`，只做一次 `elapsed()`（用户态计算），
    ///   开销更低。
    ///
    /// 该值在 `atomic_merge` 中作为候选提交版本号；若小于等于当前高水位，
    /// 调用方会用 `last_ts + 1` 兜底，最终通过 `fetch_max` 推进高水位。
    ///
    /// # ⚠️ 单调性契约
    ///
    /// **本方法不保证跨 `background_resync` 严格单调**。resync 时会用当下的
    /// `SystemTime::now()`（墙钟）覆盖锚点，而两次 resync 之间派生用的是
    /// `Instant`（单调时钟）。当墙钟被 NTP slew 减速、被管理员向后调、
    /// 或 VM 挂起恢复导致墙钟落后于 Instant 派生值时，切换锚点的那一刻
    /// 返回值可能**小于**上一次调用的返回值。
    ///
    /// 这是刻意的取舍：无条件 resync 能让 Oracle 长期跟随真实墙钟、保留时间语义
    /// （否则会退化成一个纯计数器），代价就是跨 resync 的单调性由**调用方**兜底。
    /// MVCC 版本号的单调由 `atomic_merge` 里的 `fetch_max` 保证；其他场景若需要
    /// 严格单调的时间戳，也请自行叠加 `fetch_max` 语义，不要直接依赖本方法。
    #[inline]
    pub(crate) fn current_time_ns(&self) -> u64 {
        let reference = self.inner.reference.load();
        reference.0 + reference.1.elapsed().as_nanos() as u64
    }

    /// 优雅地停止 [`Oracle::background_resync`] 后台线程。
    ///
    /// 流程：
    /// 1. 将 `resync_enable` 置为 `false`，通知后台循环退出（下一次醒来后不再进入下一轮）。
    /// 2. 取出线程句柄，`unpark` 唤醒可能正在 `park_timeout` 中沉睡的线程，
    ///    避免最长要等一个完整 `resync_interval` 才退出。
    /// 3. `join` 等待线程真正结束，保证 `Drop` 返回后不再有对 `inner` 的后台访问。
    ///
    /// 使用 `Release` 与后台线程 loop 首部的 `Acquire` 配对：一旦后台看到
    /// `resync_enable == false`，本次 shutdown 之前对共享状态的所有写入都对它可见。
    ///
    /// 由 [`Oracle`] 的 `Drop` 实现调用，因此外部无需手动停机。
    fn shutdown(&self) {
        self.inner.resync_enable.store(false, Ordering::Release);

        if let Some(handle) = self.inner.resync_handle
        .lock()
        .take() {
            handle.thread().unpark();
            handle.join().unwrap();
        }
    }

    /// 启动后台线程，周期性地**重置时间锚点**，用来抑制 `Instant` 派生时间与真实墙钟之间的漂移。
    ///
    /// # 为什么要 resync：锚点漂移问题
    ///
    /// [`Oracle::current_time_ns`] 的实现是：
    ///
    /// ```text
    /// current_time_ns = reference_unix + reference_instant.elapsed()
    /// ```
    ///
    /// 其中 `reference_unix` 是构造时抓取一次的墙钟 unix 纳秒，`reference_instant`
    /// 是同一时刻的 `Instant`。`Instant` 是单调时钟，短期内非常稳定，但它与系统
    /// 墙钟并不共享同一个时间源：
    /// - 墙钟会被 NTP **slew**（缓慢调速）或 **step**（一次性跳变）修正；
    /// - 单调时钟不会随之调整；
    /// - 于是随着时间推移，`reference_unix + elapsed()` 会与"此刻真正的 unix ns"
    ///   越拉越远——这就是**锚点漂移**。
    ///
    /// 如果不 resync，Oracle 长时间运行后派生的时间会退化成"启动时刻的墙钟 +
    /// 一个越来越不准的单调偏移量"，虽然仍然单调，但已经脱离墙钟语义。
    ///
    /// # 做法
    ///
    /// 后台线程每隔 `resync_interval`：
    /// 1. `park_timeout(interval)` 睡眠；使用 `park` 而非 `sleep` 是为了能被
    ///    [`Oracle::shutdown`] 通过 `unpark` 提前唤醒，避免关停时最坏要等
    ///    一整个 interval。
    /// 2. 重新抓取 `SystemTime::now()` 和 `Instant::now()`，用 [`ArcSwap`] 原子替换
    ///    锚点对。因为是 `ArcSwap`，读侧 [`Oracle::current_time_ns`] 完全无锁。
    /// 3. 循环入口 `resync_enable.load(Acquire)` 与 `shutdown` 的 `Release` 存储配对，
    ///    保证 shutdown 一旦置位，本轮结束后线程一定退出。
    ///
    /// # 取舍
    ///
    /// resync 恢复了长期跟随墙钟的能力，代价是**跨 resync 边界不再严格单调**：
    /// 若墙钟被向后调（NTP slew 减速、管理员回拨、VM 挂起恢复），换锚点的那一刻
    /// 派生值可能比上一次调用小。这个语义在 [`Oracle::current_time_ns`] 的
    /// "单调性契约"里有专门说明——MVCC 版本号的严格单调由 `atomic_merge`
    /// 里的 `fetch_max` 兜底，而不是依赖本方法。
    ///
    /// 线程句柄写入 `inner.resync_handle`，供 `shutdown` 取出 `join`。
    fn background_resync(&self) {
      let oracle = self.inner.clone();
      let interval = oracle.resync_interval;
      let handle = std::thread::spawn(move || {
          while oracle.resync_enable.load(Ordering::Acquire) {
              std::thread::park_timeout(interval);
              let reference_unix = Self::current_unix_ns();
              let rederence_time = Instant::now();
              oracle.reference.store(Arc::new((reference_unix, rederence_time)));
          }
      });
      let mut guard = self.inner.resync_handle.lock();
      *guard = Some(handle);
  }
}

