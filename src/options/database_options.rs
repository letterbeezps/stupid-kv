use std::time::Duration;

use crate::pool::DEFAULT_POOL_SIZE;

/// commit queue GC 默认扫描周期：1s。
pub(crate) const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);

/// datastore 版本 GC 默认扫描周期：500ms。
/// 比 commit queue GC 更频繁——版本链增长与写入 QPS 直接相关。
pub(crate) const DEFAULT_GC_INTERVAL: Duration = Duration::from_millis(500);

/// 每 N 轮增量 GC 触发一次全量 GC 兜底冷 key。默认 20（即每 ~10s 全量一次）。
pub(crate) const DEFAULT_GC_FULL_SCAN_FREQUENCY: u64 = 20;

/// 事务对象池默认复用阈值（writeset 长度）。
/// 超过此值 reset 时用整块替换代替 clear，避免大块内存残留。
pub(crate) const DEFAULT_RESET_THRESHOLD: usize = 100;

#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    /// 事务对象池容量。满时 push 静默丢弃，退化为新建事务。
    pub pool_size: usize,

    /// timestampe oracle 定时重置时钟。
    pub resync_interval: Duration,

    /// 是否开启后台 commit queue GC 线程。关闭后可通过 `Database::run_cleanup()` 手动触发。
    pub enable_cleanup: bool,
    /// commit queue GC 扫描周期。
    pub cleanup_interval: Duration,

    /// 是否开启后台 datastore 版本 GC 线程。与 `enable_cleanup` 独立。
    pub enable_gc: bool,
    /// datastore 版本 GC 扫描周期。
    pub gc_interval: Duration,
    /// 每 N 轮增量 GC 触发一次全量 GC；折中 O(变更 key 数) 与 O(所有 key 数) 的开销。
    pub gc_full_scan_frequency: u64,

    /// 事务对象池复用阈值，见 DEFAULT_RESET_THRESHOLD。
    pub reset_threshold: usize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            pool_size: DEFAULT_POOL_SIZE,
            resync_interval:  DEFAULT_RESYNC_INTERVAL,
            enable_cleanup: true,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
            enable_gc: true,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_full_scan_frequency: DEFAULT_GC_FULL_SCAN_FREQUENCY,
            reset_threshold: DEFAULT_RESET_THRESHOLD,
        }
    }
}

impl DatabaseOptions {
    
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder：覆盖 datastore 版本 GC 扫描周期。
    pub fn with_gc_interval(mut self, interval: Duration) -> Self {
        self.gc_interval = interval;
        self
    }

    /// Builder：覆盖 commit queue GC 扫描周期。
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }
}