use std::{path::PathBuf, time::Duration};

use crate::compression::CompressionMode;


/// 快照触发模式。决定快照文件由谁、在何时写入磁盘。
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// 不自动创建快照。
    ///
    /// 后台线程不启动；调用方必须通过 `Persistence::snapshot()` 手动触发全量快照。
    /// 适用于：写入极低、快照时机由上层业务精确控制（如每完成一批离线写入后手动落盘）。
    #[default]
    Never,

    /// 每 N 秒自动创建一次快照。
    ///
    /// 由 `spwan_snapshot_worker` 启动独立后台线程，通过 `park_timeout(interval)` 周期唤醒；
    /// 线程内部使用与手动 `snapshot()` 相同的原子落盘协议（tmp → rename → sync_all）。
    /// 适用于：常规在线 KV 工作负载，丢数据容忍窗口 = `interval`（两次快照之间崩溃最多丢 interval 内的写入）。
    Interval(Duration),
}

/// 持久化子系统配置项。由 `Database::new_with_persistence` 消费。
///
/// # 示例
///
/// ```rust,ignore
/// use std::time::Duration;
/// use stupid_kv::{PersistenceOptions, SnapshotMode};
///
/// // 基础路径 ./data，每 30 秒自动快照
/// let opts = PersistenceOptions::new("./data")
///     .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(30)));
///
/// // 或手动指定快照文件路径（相对 base_path）
/// let opts = PersistenceOptions::new("./data")
///     .with_snapshot_mode(SnapshotMode::Never)  // 纯手动
///     .with_snapshot_path(Some("snapshots/v1.bin".into()));
/// ```
pub struct PersistenceOptions {
    /// 数据持久化基础路径。`snapshot_path` 为相对路径 / None 时会拼接在它下面。
    ///
    /// 同时也是 `fs::create_dir_all` 的递归起点：不存在时会自动创建。
    pub base_path: PathBuf,

    /// 快照模式：Never（手动）或 Interval（后台周期）。
    pub snapshot_mode: SnapshotMode,

    /// 快照文件路径覆盖。
    ///
    /// - `None`：默认路径 = `{base_path}/snapshot.bin`
    /// - `Some(相对路径)`：实际路径 = `{base_path}/{相对路径}`，并保证其父目录被创建
    /// - `Some(绝对路径)`：原样使用，不拼接 base_path，但仍保证父目录存在
    pub snapshot_path: Option<PathBuf>,

    pub compression_mode: CompressionMode,
}

impl Default for PersistenceOptions {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./data"),
            snapshot_mode: SnapshotMode::default(),
            snapshot_path: None,
            compression_mode: CompressionMode::default(),
        }
    }
}

impl PersistenceOptions {
    /// 从基础路径创建配置；其余字段使用默认值。
    ///
    /// 默认：`SnapshotMode::Never` + 默认快照路径 `{base_path}/snapshot.bin`。
    pub fn new<P: Into<PathBuf>>(base_path: P) -> Self {
        Self {
            base_path: base_path.into(),
            ..Self::default()
        }
    }

    /// Builder：覆盖基础路径。
    pub fn with_base_path<P: Into<PathBuf>>(mut self, base_path: P) -> Self {
        self.base_path = base_path.into();
        self
    }

    /// Builder：覆盖快照触发模式。
    pub fn with_snapshot_mode(mut self, snapshot_mode: SnapshotMode) -> Self {
        self.snapshot_mode = snapshot_mode;
        self
    }

    /// Builder：覆盖压缩模式。
    pub fn with_compression_mode(mut self, compression_mode: CompressionMode) -> Self {
        self.compression_mode = compression_mode;
        self
    }
}
