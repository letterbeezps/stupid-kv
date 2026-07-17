use std::time::Duration;

/// GC 后台清理线程默认扫描周期：每 1s 触发一次 `run_cleanup_inner`。
pub(crate) const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct DatabaseOptions {

    pub resync_interval: Duration,

    /// 是否开启后台 GC 清理线程。
    /// 关闭后仍可通过 `Database::run_cleanup()` 手动触发一次清理，
    /// 用于测试或对 GC 时机有严格控制诉求的场景。
    pub enable_cleanup: bool,
    /// 后台 GC 清理线程的扫描周期。
    pub cleanup_interval: Duration
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            resync_interval:  DEFAULT_RESYNC_INTERVAL,
            enable_cleanup: true,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
        }
    }
}

impl DatabaseOptions {
    
    pub fn new() -> Self {
        Self::default()
    }
}