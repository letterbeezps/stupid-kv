use std::{path::PathBuf, time::Duration};

use crate::compression::CompressionMode;

/// AOL（Append-Only Log）写入模式。决定事务提交时，写集如何被追加到 AOL 日志文件中。
///
/// AOL 是 WAL（Write-Ahead Log）的一种简化形式：在数据写入 datastore 之前，
/// 先将写集追加到 AOL 文件。当快照（全量 snapshot）完成后，AOL 中已被快照覆盖的部分
/// 会被截断（truncate），从而控制日志文件大小。
///
/// 三种模式对比：
///
/// | 模式 | 提交路径行为 | 性能影响 | 适用场景 |
/// |------|-------------|---------|---------|
/// | `Never` | 不写 AOL，纯内存 + 快照 | 最快，无额外 IO | 可容忍崩溃丢数据的场景（如缓存层） |
/// | `SynchronousOnCommit` | 提交线程同步写 AOL 文件 | 每次提交多一次 syscall | 强持久化要求，写入吞吐较低 |
/// | `AsynchronousAfterCommit` | 提交线程推入队列，后台线程批量写 | 提交延迟低，批量 IO 高效 | **推荐**：兼顾持久化与吞吐 |
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub enum AolMode {
    /// 不启用 AOL。事务提交不产生任何日志写入，崩溃后只能依赖最近一次快照恢复。
    ///
    /// 恢复窗口 = 快照间隔（`SnapshotMode::Interval` 的 duration）。
    #[default]
    Never,

    /// 同步 AOL：事务提交时**立即**将写集追加到 AOL 文件并可选 fsync。
    ///
    /// 每次 `commit()` 都会触发一次 `write()` syscall + 可选 `fsync()`，
    /// 提交延迟直接受磁盘吞吐限制。好处是提交返回后数据已在 OS PageCache（或磁盘）中，
    /// 崩溃恢复窗口趋近于 0。
    SynchronousOnCommit,

    /// 异步 AOL：事务提交时将写集推入无锁队列（`crossbeam_deque::Injector`），
    /// 由独立后台线程批量消费并写入 AOL 文件。
    ///
    /// 提交线程无磁盘 IO 阻塞，批量写还可合并 `fsync`，
    /// 整体吞吐远高于同步模式。但崩溃时队列中尚未消费的写集会丢失。
    AsynchronousAfterCommit,
}

/// fsync 策略：控制 AOL 追加后何时将 OS PageCache 刷到磁盘介质。
///
/// fsync 是 AOL 持久化的核心保证：没有 fsync，数据可能还在 PageCache 中，
/// 断电后 AOL 文件会出现截断，恢复时 bincode 解码会把半截条目当作 `UnexpectedEof` 跳过。
///
/// 三种策略的 trade-off：
///
/// | 策略 | fsync 频率 | 性能 | 数据安全 |
/// |------|-----------|------|---------|
/// | `Never` | 从不主动 fsync（仅依赖 OS flush） | 最快 | 依赖 OS，不保证 |
/// | `EveryAppend` | 每次 append 后立即 fsync | 最慢，每次两次 syscall | 最安全 |
/// | `Interval(Duration)` | 每 N 毫秒 fsync 一次，其余仅记 pending | 折中 | 窗口 = Duration |
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// 不主动 fsync。AOL 数据留在 OS PageCache 中，由操作系统自行决定何时落盘。
    ///
    /// 适合：与 `AolMode::Never` 搭配使用，或对数据安全无强要求的场景。
    #[default]
    Never,

    /// 每次 AOL append（同步或异步批量）完成后立即 `fsync()`。
    ///
    /// 提供最强持久化保证，但也是性能最差的模式。在异步模式下，
    /// 每次批量写入都会触发一次 fsync（而非每条操作一次），因此仍然比同步模式好。
    EveryAppend,

    /// 周期性 fsync。每隔指定 `Duration` 时间将累计的 AOL 数据刷盘一次。
    ///
    /// 两次 fsync 之间的写入只记在 `pending_syncs` 计数器中，
    /// 由独立的 fsync worker 线程或下次 fsync 触发时统一落盘。
    /// 提供可预测的持久化窗口，是生产环境推荐的折中方案。
    Interval(Duration),
}


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

    /// AOL 写入模式。控制事务提交时写集是否以及如何追加到 AOL 日志文件。
    ///
    /// 默认 `AolMode::Never`（不启用 AOL）。
    pub aol_mode: AolMode,

    /// AOL 日志文件路径覆盖。
    ///
    /// - `None`：默认路径 = `{base_path}/aol.bin`
    /// - `Some(相对路径)`：实际路径 = `{base_path}/{相对路径}`
    /// - `Some(绝对路径)`：原样使用，不拼接 base_path
    ///
    /// 仅当 `aol_mode != Never` 时生效。
    pub aol_path: Option<PathBuf>,

    /// fsync 策略。控制 AOL 追加后何时将数据从 OS PageCache 刷到磁盘。
    ///
    /// 默认 `FsyncMode::Never`（不主动 fsync）。
    pub fsync_mode: FsyncMode,

    pub compression_mode: CompressionMode,
}

impl Default for PersistenceOptions {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./data"),
            snapshot_mode: SnapshotMode::default(),
            aol_mode: AolMode::default(),
            fsync_mode: FsyncMode::default(),
            snapshot_path: None,
            aol_path: None,
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

    /// Builder：覆盖快照文件路径。
    pub fn with_snapshot_path(mut self, snapshot_path: Option<PathBuf>) -> Self {
        self.snapshot_path = snapshot_path;
        self
    }

    /// Builder：覆盖 AOL 模式。
    pub fn with_aol_mode(mut self, aol_mode: AolMode) -> Self {
        self.aol_mode = aol_mode;
        self
    }

    /// Builder：覆盖 fsync 模式。
    pub fn with_fsync_mode(mut self, fsync_mode: FsyncMode) -> Self {
        self.fsync_mode = fsync_mode;
        self
    }

    /// Builder：覆盖 AOL 文件路径。
    pub fn with_aol_path(mut self, aol_path: Option<PathBuf>) -> Self {
        self.aol_path = aol_path;
        self
    }

    /// Builder：覆盖压缩模式。
    pub fn with_compression_mode(mut self, compression_mode: CompressionMode) -> Self {
        self.compression_mode = compression_mode;
        self
    }

    /// 预设：纯快照模式（最简，零 AOL 开销）。
    ///
    /// 仅启用周期性全量快照，不启用 AOL 增量日志。崩溃丢数据窗口 = 快照间隔。
    /// 适用于可容忍丢数据的缓存层、测试/开发环境。
    ///
    /// - `snapshot_interval`：快照间隔（默认 60 秒）
    /// - AOL：`Never`（不启用）
    /// - fsync：`Never`（不主动刷盘）
    pub fn with_pure_snapshot(mut self, snapshot_interval: Duration) -> Self {
        self.snapshot_mode = SnapshotMode::Interval(snapshot_interval);
        self.aol_mode = AolMode::Never;
        self.fsync_mode = FsyncMode::Never;
        self
    }

    /// 预设：快照 + AOL 异步批量 + 周期 fsync（性价比最佳，推荐生产默认）。
    ///
    /// AOL 以异步批量方式写入，提交线程无磁盘 IO 阻塞；周期性 fsync 兜底，
    /// 平衡了持久化保证与写入吞吐。
    ///
    /// - `snapshot_interval`：快照间隔（默认 30 秒）
    /// - AOL：`AsynchronousAfterCommit`（后台线程批量消费写入）
    /// - fsync：`Interval(100ms)`（每 100ms 刷盘一次）
    pub fn with_async_aol(mut self, snapshot_interval: Duration) -> Self {
        self.snapshot_mode = SnapshotMode::Interval(snapshot_interval);
        self.aol_mode = AolMode::AsynchronousAfterCommit;
        self.fsync_mode = FsyncMode::Interval(Duration::from_millis(100));
        self
    }

    /// 预设：快照 + AOL 同步写入 + 每次 fsync（安全性最高）。
    ///
    /// 每次事务提交同步写入 AOL 文件并立即 fsync，提交返回后数据已在磁盘。
    /// 持久化保证最强，但写入吞吐最低。
    ///
    /// - `snapshot_interval`：快照间隔（默认 30 秒）
    /// - AOL：`SynchronousOnCommit`（提交线程同步写入）
    /// - fsync：`EveryAppend`（每次追加后立即 fsync）
    pub fn with_sync_aol(mut self, snapshot_interval: Duration) -> Self {
        self.snapshot_mode = SnapshotMode::Interval(snapshot_interval);
        self.aol_mode = AolMode::SynchronousOnCommit;
        self.fsync_mode = FsyncMode::EveryAppend;
        self
    }
}
