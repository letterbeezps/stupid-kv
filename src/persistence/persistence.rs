use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::{path::PathBuf};
use std::{fs::{self, File}};
use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}};

use bincode::config;
use bytes::Bytes;
use crossbeam_deque::{Injector, Steal};
use parking_lot::RwLock;

use crate::{AolMode, FsyncMode};
use crate::compression::{CompressedReader, CompressedWriter, CompressionMode};
use crate::versions::{Version, Versions};
use crate::{PersistenceOptions, SnapshotMode, db::inner::Inner, error::PersistenceError};


/// 异步追加操作：描述一次事务提交产生的、需要写入 AOL 文件的增量数据。
///
/// 在 `AolMode::AsynchronousAfterCommit` 模式下，事务提交线程不直接写磁盘，
/// 而是将 `AsyncAppendOperation` 推入 `crossbeam_deque::Injector` 无锁队列，
/// 由独立的 `append_worker` 线程批量消费。
///
/// # 字段
/// - `version`：Oracle 分配的 MVCC 版本号（纳秒时间戳），作为 AOL 记录的时间标识
/// - `writeset`：本次事务的写集快照，key 为 Bytes，value 为 `Option<Bytes>`（None 表示 tombstone 删除标记）
#[derive(Debug, Clone)]
pub(crate) struct AsyncAppendOperation {
    /// Oracle 分配的 MVCC 版本号（纳秒时间戳），标识写集的可见性版本。
    pub version: u64,
    /// 本次事务的写集：key → value，`None` 表示 tombstone（删除标记）。
    /// 与 `TransactionInner.writeset` 保持同样的 `BTreeMap<Bytes, Option<Bytes>>` 结构。
    pub writeset: BTreeMap<Bytes, Option<Bytes>>,
}

/// 持久化门面：封装快照写入（snapshot）、启动恢复（load）、AOL 增量日志与后台周期性 worker 线程。
///
/// ## 持久化模型
///
/// 系统采用「全量快照 + 增量 AOL」的混合持久化模型：
///
/// ### 全量快照（Snapshot）
/// - **写入**：`snapshot()` 把 datastore 每个 key 的完整版本链用 bincode 流式编码到临时文件，
///   完成后通过 `fs::rename` 原子替换旧快照，最后 `sync_all` 确认真正落到磁盘。
/// - **恢复**：`load()` 在构造函数里被调用，先从快照文件恢复全量数据，
///   再回放 AOL 增量日志中的提交记录，补齐最后一次快照之后的增量。
/// - **后台线程**：`SnapshotMode::Interval(dur)` 时启动独立线程周期生成快照。
///
/// ### 增量 AOL（Append-Only Log）
/// - **写入**：每次事务提交后，写集被追加到 AOL 文件（同步或异步批量）。
/// - **截断**：快照完成后，AOL 中已被快照覆盖的早期部分会被截断，控制日志文件大小。
/// - **恢复**：启动时 load 完快照后，继续读取 AOL 文件中剩余的增量记录。
///
/// ### 两阶段落盘协议
///
/// 1. 事务提交 → 写集进入 AOL（内存 buffer 或磁盘文件）
/// 2. 周期性 snapshot → 全量快照 + AOL truncate
///
/// 任何时刻崩溃，恢复流程 = snapshot load → AOL replay，保证数据不丢。
#[derive(Clone)]
pub struct Persistence {
    /// 内部数据库共享状态。snapshot 遍历 datastore，load 回填 datastore 都通过它。
    pub(crate) inner: Arc<Inner>,

    /// AOL 日志文件句柄。`Mutex` 保护并发写（追加操作），
    /// `Option` 控制是否启用：`AolMode::Never` 时为 `None`。
    ///
    /// 所有 AOL 写入操作都通过此 Mutex 串行化，保证文件内记录的线性一致性。
    pub(crate) aol: Option<Arc<Mutex<File>>>,

    /// AOL 日志文件在磁盘上的路径。推导逻辑与 `snapshot_path` 对称：
    /// `options.aol_path` 绝对路径直用、相对路径拼 base_path、None 则默认 `{base_path}/aol.bin`。
    pub(crate) aol_path: PathBuf,

    /// AOL 写入模式。决定事务提交时 AOL 的行为：不写 / 同步写 / 异步批量写。
    pub(crate) aol_mode: AolMode,

    /// fsync 策略。控制 AOL 追加后何时将数据刷到磁盘介质。
    pub(crate) fsync_mode: FsyncMode,

    /// 快照文件最终落地路径。
    ///
    /// 默认为 `{base_path}/snapshot.bin`；若 `PersistenceOptions.snapshot_path` 为绝对路径则直接使用，
    /// 相对路径则拼接在 `base_path` 下。
    pub(crate) snapshot_path: PathBuf,

    /// 快照触发模式：Never（纯手动调用 `snapshot()`）或 Interval（后台周期线程）。
    pub(crate) snapshot_mode: SnapshotMode,

    /// 后台线程运行开关，独立于 `Inner.background_threads_enabled`。
    ///
    /// 使用独立 Arc 是因为 Persistence 可以脱离 Database 单独构造使用，
    /// 此时 Inner 可能尚未存在或生命周期不同步。
    pub(crate) background_threads_enabled: Arc<AtomicBool>,

    /// 后台快照线程句柄。`Arc<RwLock<Option<...>>>` 双重包装：
    /// - `Arc`：Persistence 实现 Clone（多个引用共享同一份后台线程实例），
    ///   避免 Clone 后每个实例都再 spawn 一条线程；由第一个 Persistence 实例负责 spawn。
    /// - `RwLock<Option>`：`spawn_snapshot_worker` 的 `read().is_none()` 判空后
    ///   `write().replace(handle)` 两步插入，保证并发构造 Persistence 时只起一条线程。
    pub(crate) snapshot_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// 后台 AOL 追加线程句柄。仅在 `AolMode::AsynchronousAfterCommit` 下启动，
    /// 负责从 `async_append_injector` 批量消费写集并写入 AOL 文件。
    pub(crate) append_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// 后台 fsync worker 线程句柄。仅在 `FsyncMode::Interval` 下启动，
    /// 定期将 `pending_syncs` 计数器对应的 AOL 数据刷到磁盘。
    pub(crate) fsync_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// 快照文件压缩模式。决定 CompressedWriter/Reader 的行为（None 或 Lz4）。
    pub(crate) compression_mode: CompressionMode,

    /// 上次 fsync 的时间戳。`FsyncMode::Interval` 模式下由 append worker 和 fsync worker 共享，
    /// 用于判断是否到达下次 fsync 的时间窗口。
    pub(crate) last_fsync: Arc<Mutex<Instant>>,

    /// 待 fsync 的 AOL 追加操作计数。`FsyncMode::Never` 时不使用，
    /// `FsyncMode::Interval` 下由 append worker 递增、fsync worker 周期性清零。
    /// 用于在 truncate 或 drop 时判断是否需要补一次 sync_all。
    pub(crate) pending_syncs: Arc<AtomicU64>,

    /// 异步追加操作的无锁多生产者队列（`crossbeam_deque::Injector`）。
    /// 事务提交线程为生产者（`push`），append worker 线程为消费者（`steal`）。
    /// Injector 支持多生产者安全入队，Stealer 支持多消费者窃取，
    /// 适合 M:N 的生产者-消费者模型。
    pub(crate) async_append_injector: Arc<Injector<AsyncAppendOperation>>,
}

impl Persistence {

    /// 带配置构造 Persistence：创建目录 → 推导快照文件路径 → `load()` 恢复 → 启动所有后台 worker 线程。
    ///
    /// # 初始化顺序（关键）
    ///
    /// 1. `fs::create_dir_all(base_path)` — 保证基础目录存在
    /// 2. 推导 `aol_path` / `snapshot_path` — 解析绝对/相对/默认路径
    /// 3. 若 `aol_mode != Never`，创建并打开 AOL 文件（create + append + read）
    /// 4. `load()` — 先恢复快照全量数据，再回放 AOL 增量日志
    /// 5. 启动后台线程：snapshot worker → append worker → fsync worker
    ///
    /// 先恢复后启线程是因为后台线程可能立即触发 snapshot/truncate，
    /// 而这些操作需要一份完整的 datastore 视图。
    ///
    /// # 失败
    ///
    /// - 目录创建（`fs::create_dir_all`）失败 → `PersistenceError::Io`
    /// - AOL / 快照文件打开失败 → `PersistenceError::Io`
    /// - 快照 + AOL 解码失败 → `PersistenceError::Deserialization`
    pub(crate) fn new_with_options(
        options: PersistenceOptions,
        inner: Arc<Inner>,
    ) -> Result<Self, PersistenceError> {

        let base_path = &options.base_path;

        // 确保基础路径存在；不存在则递归创建（同 `mkdir -p` 语义）
        fs::create_dir_all(base_path)?;

        // 推导 aol_path 路径：
        //   1) options.aol_path 为 Some(绝对路径) → 直接使用
        //   2) options.aol_path 为 Some(相对路径) → base_path / path
        //   3) None → base_path / "aol.bin"
        let aol_path = if let Some(path) = options.aol_path {
            if path.is_absolute() {
                path
            } else {
                base_path.join(path)
            }
        } else {
            base_path.join("aol.bin")
        };

        // 推导快照落地路径：
        //   1) options.snapshot_path 为 Some(绝对路径) → 直接使用
        //   2) options.snapshot_path 为 Some(相对路径) → base_path / path
        //   3) None → base_path / "snapshot.bin"
        let snapshot_path = if let Some(path) = options.snapshot_path {
            if path.is_absolute() {
                path
            } else {
                base_path.join(path)
            }
        } else {
            base_path.join("snapshot.bin")
        };

        // 根据 AOL 模式决定是否创建 AOL 文件：
        // - `Never` → 不创建，aol 为 None
        // - 其他模式 → 创建或打开文件，以追加模式（append）+ 读模式（read）打开
        //   append 保证新记录追加到文件末尾；read 用于 load() 回放
        let aol = match options.aol_mode {
            AolMode::Never => None,
            _ => {
                if let Some(parent) = aol_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                // create=true: 文件不存在则创建；append=true: 每次写入追加到末尾；read=true: 支持 load() 回放
                let file = OpenOptions::new()
                     .create(true)
                     .append(true)
                     .read(true)
                     .open(&aol_path)?;
                Some(Arc::new(Mutex::new(file)))
            }
        };

        // 若快照文件所在的父目录还不存在（上面只保证了 base_path），再建一层。
        // 例：snapshot_path = base_path / "snapshots/1.bin" 时需要创建 snapshots/ 子目录。
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let this = Self {
            inner,
            aol,
            snapshot_path,
            aol_path,
            aol_mode: options.aol_mode,
            snapshot_mode: options.snapshot_mode,
            fsync_mode: options.fsync_mode,
            compression_mode: options.compression_mode,
            background_threads_enabled: Arc::new(AtomicBool::new(true)),
            snapshot_handle: Arc::new(RwLock::new(None)),
            append_handle: Arc::new(RwLock::new(None)),
            fsync_handle: Arc::new(RwLock::new(None)),
            last_fsync: Arc::new(Mutex::new(Instant::now())),
            pending_syncs: Arc::new(AtomicU64::new(0)),
            async_append_injector: Arc::new(Injector::new()),
        };

        // 先恢复数据再启后台线程：后台线程的 GC / 快照都需要一份完整的 datastore。
        this.load()?;

        this.spawn_snapshot_worker();

        this.spawn_appender_worker();

        this.spawn_fsync_worker();

        Ok(this)
    }

    /// 手动触发一次全量快照。后台线程内部使用的也是同一套闭包逻辑。
    ///
    /// 落盘协议（保证任何崩溃点都不会污染已有快照）：
    /// 1. `File::create(.tmp)` 在临时文件上编码，不触碰正式快照；
    /// 2. `BufWriter::flush()` 把用户态 buffer 刷进 OS PageCache；
    /// 3. `fs::rename(tmp, final)` **原子替换**（POSIX 同 FS 下保证原子）；
    /// 4. `File::open(final) + sync_all()` 把 PageCache 刷进磁盘介质，
    ///    封死「rename 成功但 data 还在 PageCache，断电后文件变半截」的窗口。
    /// 5. AOL truncate：以快照前 AOL 文件大小为基准，截断已被快照覆盖的早期日志。
    ///
    /// 任何一步失败都会 `fs::remove_file(.tmp)` 清理半成品；正式快照不会被改到。
    ///
    /// # AOL 截断逻辑
    ///
    /// 快照前记录 AOL 文件的当前位置 `aol_cutoff_position`，
    /// 快照完成后调用 `truncate()` 将 AOL 文件截断到该位置。
    /// 这保证了 AOL 中只保留本次快照之后产生的增量写集，
    /// 避免重复回放已在快照中存在的数据。
    pub fn snapshot(&self) -> Result<(), PersistenceError> {
        // 临时文件：同目录下扩展名 .tmp，保证 rename 在同一文件系统内（原子前提）
        let temp_path = self.snapshot_path.with_extension(".tmp");

        let result = || -> Result<(), PersistenceError> {
            // 1. 建临时文件（同名旧 tmp 会被 truncate，不影响正式快照）
            let file = File::create(&temp_path)?;

            // BufWriter 包装：8KB 块写入减少 syscall；bincode 的 encode_into_std_write
            // 逐条刷进 BufWriter 内部 buffer，不需要用户手动管理大 buffer。
            let mut  writer = CompressedWriter::new(file, self.compression_mode)?;

            // 在开始快照编码前，记录 AOL 文件的当前大小作为截断基准。
            // 快照过程中产生的 AOL 写入会追加到文件末尾，
            // 快照完成后 truncate 到此位置，即可清除已被快照覆盖的早期日志。
            let aol_cutoff_positon = if let Some(ref aol) = self.aol {
                aol.lock()?.metadata()?.len()
            } else {
                0
            };

            // 2. 遍历 datastore 逐条编码
            // - 读锁：entry.value().read()，不阻塞其他事务的读取 / 写入（写入拿写锁，
            //   但写入是对 Versions 的 push，当前读锁持有者看到的是 push 前的状态）
            // - all_versions() 克隆一份版本链：释放读锁后编码不依赖 RwLock，
            //   避免编码过程长时间占锁阻塞写路径。
            for entry in self.inner.datastore.iter() {
                let versions = entry.value().read().all_versions();
                if !versions.is_empty() {
                    bincode::serde::encode_into_std_write(
                        &(entry.key().clone(), versions),
                        &mut writer,
                        config::standard(),
                    )?;
                }
            }

            // 3. 刷 BufWriter → OS PageCache。不 flush 直接 rename 会有数据还在用户态。
            writer.flush()?;
            writer.finish()?;

            // 4. 原子 rename：tmp → final。POSIX 对同文件系统目标的 rename 保证：
            //    要么 final 仍指向旧 inode（旧快照完好），要么指向新 inode（写完整的 tmp），
            //    没有「半截 inode」的中间态。
            fs::rename(&temp_path, &self.snapshot_path)?;

            // 5. sync_all：把刚 rename 过来的「新快照文件」真正 flush 到磁盘（data + metadata）。
            //    若不做这一步，rename 返回成功但 data 仍在 PageCache，断电后文件会半截。
            //    重新 open 而不是复用上面的 file：BufWriter 已被 flush 且所有权不在这，
            //    重新打开语义更清晰、不依赖 BufWriter 内部状态。
            {
                let final_file = File::open(&self.snapshot_path)?;
                final_file.sync_all()?;
            }

            // 快照完成后，截断 AOL 文件到快照前的位置。
            // 这样 AOL 中只保留本次快照之后产生的增量写集，避免重复回放。
            Self::truncate(&self.aol,  aol_cutoff_positon, &self.pending_syncs)?;

            Ok(())
        }();

        // 失败清理 tmp：防止 crash/restart 之后磁盘上残留一堆半截 .tmp 占空间
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }

        result
    }

    /// 启动时从快照文件流式反序列化回填 datastore；空文件 / 文件不存在视为空数据库，静默通过。
    ///
    /// 流式解码保证内存占用是「单条 key」量级而非「整份 datastore」量级：
    /// - decode_from_std_read 每次只从 BufReader 里解一个 (key, versions) 条目；
    /// - 解完立即 insert 进 datastore，本地局部变量随即 drop；
    /// - 正常文件结束由 `DecodeError::Io { inner: UnexpectedEof }` 触发 break；
    /// - 其他任何解码错误（文件损坏、格式不兼容）都向上抛出 `Deserialization`，
    ///   调用方可选择删除坏快照重来。
    fn load(&self) -> Result<(), PersistenceError> {
        if self.snapshot_path.exists() {
            let file = File::open(&self.snapshot_path)?;
            // 取 metadata 判空：0 字节是空快照（可能是第一次创建 snapshot 文件还没落任何数据），
            // 不进循环，避免「空文件 → decode 立刻 UnexpectedEof → 空循环 break」的冗余路径。
            let metadata = file.metadata()?;
            if metadata.len() > 0 {
                // BufReader 对称于写端的 BufWriter：8KB 预读减少 syscall。
                let mut reader = CompressedReader::new(file)?;
                let mut count = 0;
                loop {
                    count += 1;

                    tracing::trace!("load snapshot entry {}", count);
                    // 快照条目类型。**必须**与 snapshot() 中 encode 的元组严格对称：
                    //   encode: (entry.key().clone() → Bytes,  all_versions → Vec<(u64, Option<Bytes>)>)
                    //   decode: (Bytes, Vec<(u64, Option<Bytes>)>)
                    type Entry = (Bytes, Vec<(u64, Option<Bytes>)>);

                    // 标注类型显式告诉编译器 decode 的目标；
                    // decode_from_std_read 是泛型函数，不标类型它推不出 T。
                    let result: Result<Entry, _> = bincode::serde::decode_from_std_read(&mut reader, config::standard());

                    match result {
                        Ok((k, versions)) => {
                            // 空 versions 虽然理论上不会被写端编码进来，但这里防御性再判一次，
                            // 避免往 SkipMap 里塞一个空 Versions 占一个 slot + 一个 RwLock。
                            if !versions.is_empty() {
                                // 用 push 一条一条组装，而不是直接构造 SmallVec：
                                // 复用 push 的去重 / 合并逻辑，保证加载后的不变式与在线写入一致。
                                let mut entries = Versions::new();
                                for (version, value) in versions {
                                    entries.push(Version {
                                        version,
                                        value,
                                    });
                                }
                                self.inner.datastore.insert(k, RwLock::new(entries));
                            }
                        }
                        Err(e) => match e {
                            // 正常 EOF：全部条目读完，跳出循环。
                            // bincode 当底层 Read 返回 Ok(0) 时把它包装成这个变体。
                            bincode::error::DecodeError::Io {
                                inner,
                                ..
                            } if inner.kind() == std::io::ErrorKind::UnexpectedEof => {
                                break;
                            },
                            // 其他解码错误：文件损坏 / 跨平台 native 字节序 / bincode 版本升级 breaking
                            e => return Err(PersistenceError::Deserialization(e)),
                        }
                    }
                }
            }
        }

        // ---- 第二阶段：回放 AOL 增量日志 ----
        // 快照恢复完成后，继续从 AOL 文件中读取快照之后的增量写集。
        // AOL 记录格式为 (key, version, value)，比快照的 (key, versions[]) 更紧凑。
        // 每条 AOL 记录对应一次事务提交中某个 key 的写入。
        'aol_replay: loop {
            if !self.aol_path.exists() {
                break;
            }

            let file = File::open(&self.aol_path)?;

            let metadata = file.metadata()?;
            if metadata.len() == 0 {
                break;
            }

            // AOL 也使用 CompressedReader 读取（对称于快照的压缩策略）
            let mut reader = CompressedReader::new(file)?;
            let mut count = 0;

            loop {
                count += 1;
                tracing::trace!("load aol entry {}", count);

                // AOL 条目类型：(key, version, value)
                // - key: Bytes，写入的键
                // - version: u64，MVCC 版本号
                // - value: Option<Bytes>，写入的值（None = tombstone）
                type Entry = (Bytes, u64, Option<Bytes>);

                let result: Result<Entry, _> = bincode::serde::decode_from_std_read(&mut reader, config::standard());

                match result {
                    Ok((k, version, val)) => {
                        // 若 key 已在 datastore 中（从快照恢复的），
                        // 则追加新版本；否则创建新的 Versions 条目。
                        if let Some(entry) = self.inner.datastore.get(&k) {
                            entry
                                .value()
                                .write()
                                .push(Version { 
                                    version, 
                                    value: val 
                                });
                        } else {
                            self
                                .inner
                                .datastore
                                .insert(
                                    k.clone(),
                                    RwLock::new(Versions::from(
                                        Version {
                                            version,
                                            value: val,
                                        }
                                    ))
                                );
                        }
                    }

                    // 正常 EOF：AOL 文件读完，跳出内循环
                    Err(e) => match e {
                        bincode::error::DecodeError::Io {
                            inner,
                            ..
                        } if inner.kind() == std::io::ErrorKind::UnexpectedEof => {
                            break;
                        },
                        // 其他解码错误：AOL 文件损坏
                        e => return Err(PersistenceError::Deserialization(e)),
                    }
                }
            }

            break 'aol_replay;
        }

        Ok(())
    }

    /// 截断 AOL 文件到指定位置。在快照完成后调用，清除已被快照覆盖的早期日志。
    ///
    /// # 截断策略
    ///
    /// 根据 `position`（快照前的 AOL 文件大小）与当前文件大小的关系：
    ///
    /// - `file_len <= position`：文件未增长（快照期间无新 AOL 写入），直接 `set_len(0)` 清空文件。
    /// - `file_len > position`：有新的 AOL 写入，需要保留 `position` 之后的部分。
    ///   实现方式：将 `position` 到文件末尾的内容复制到临时文件，再覆写 AOL 文件。
    ///   这种「复制-覆写」方式比直接 `set_len` 更可靠，因为部分 OS 对 `set_len` 后的数据
    ///   只做逻辑截断而非物理截断。
    ///
    /// 当 `position == 0` 时，额外将 `pending_syncs` 归零，
    /// 因为此时文件已被完全清空，没有需要补 fsync 的数据。
    fn truncate(
        aol: &Option<Arc<Mutex<File>>>,
        position: u64,
        pending_syncs: &Arc<AtomicU64>,
    ) -> Result<(), PersistenceError> {

        let Some(ref aol) = aol else {
            return Ok(());
        };

        let mut file = aol.lock()?;

        let file_len = file.metadata()?.len();

        if file_len > position {
            // 有新的 AOL 写入追加到了 position 之后，需要保留这部分。
            // 为了避免 `set_len` 在某些文件系统上不可靠，
            // 采用「复制到临时文件 → 覆写」的双重写入协议。
            let name = format!("aol_truncate_{}.tmp", std::process::id());

            let path = std::env::temp_dir().join(&name);

            let result = || -> Result<(), PersistenceError> {

                // 1. seek 到 position，将剩余内容复制到临时文件
                {
                    file.seek(SeekFrom::Start(position))?;

                    let mut tmp = File::create(&path)?;

                    std::io::copy(&mut *file, &mut tmp)?;

                    tmp.sync_all()?;
                }

                // 2. 用临时文件的内容覆写 AOL 文件（从 position 开始重新写入）
                {
                    let mut temp = File::open(&path)?;
                    std::io::copy(&mut temp, &mut *file)?;
                }

                file.flush()?;

                Ok(())
            }();

            let _ = fs::remove_file(&path);

            result?;
        } else {
            // 无新写入，直接清空文件
            file.set_len(0)?;
            file.flush()?;
        }

        // 如果 position 为 0（快照时 AOL 为空或已清空），
        // 重置 pending_syncs 计数器，避免残留的 fsync 等待
        if position == 0 {
            pending_syncs.store(0, Ordering::Release);
        }
        Ok(())
    }

    /// 启动后台 fsync worker 线程。仅在 `AolMode != Never && FsyncMode::Interval` 下生效。
    ///
    /// 该线程定期检查 `pending_syncs` 计数器：若有累积的未 fsync 写入，
    /// 则触发一次 `fsync()` 将 AOL 文件从 OS PageCache 刷到磁盘介质。
    /// 与 `spawn_appender_worker` 中的 fsync 逻辑形成互补——后者负责在批量写入时
    /// 判断是否需要 fsync，本线程负责兜底确保不会长时间遗漏 fsync。
    ///
    /// # 线程结构
    ///
    /// - `park_timeout(duration)` 周期唤醒，同时支持 shutdown 时 `unpark` 立即退出
    /// - 醒来后二次检查 `background_threads_enabled` 防止 shutdown 竞态
    /// - 使用 `Arc<RwLock<Option<JoinHandle>>>` 保证多实例 Persistence 只启动一条线程
    fn spawn_fsync_worker(&self) {

        if self.aol_mode == AolMode::Never {
            return;
        }

        let FsyncMode::Interval(duration) = self.fsync_mode else {
            return;
        };

        let Some(ref aol) = self.aol else {
            return;
        };
    
        if self.fsync_handle.read().is_none() {
            let aol = aol.clone();
            let pending_syncs = self.pending_syncs.clone();
            let enable = self.background_threads_enabled.clone();

            let handle = thread::spawn(move || {

                while enable.load(Ordering::Acquire) {
                    thread::park_timeout(duration);
                    if !enable.load(Ordering::Acquire) {
                        break;
                    }

                    // 仅当有未 fsync 的写入时才执行 sync_all
                    if pending_syncs.load(Ordering::Acquire) > 0 {
                        if let Ok(file) = aol.lock() {
                            if let Err(e) = file.sync_all() {
                                tracing::error!("fsync aol file failed: {:?}", e);
                            } else {
                                // fsync 成功，清零待同步计数
                                pending_syncs.store(0, Ordering::Release);
                            }
                        }
                    }
                }
            });
            *self.fsync_handle.write() = Some(handle);
        }
    }

    /// 根据 `snapshot_mode` 启动后台周期性快照线程；Never 模式直接 return。
    ///
    /// 线程结构与 04 节 cleanup worker / 05 节 gc worker 完全同构：
    /// - `park_timeout(interval)` 而非 `sleep`：shutdown 时可被 `unpark` 立即唤醒；
    /// - 醒来后二次检查 `background_threads_enabled`：防止「醒来瞬间正好被 shutdown」
    ///   导致多白跑一次 snapshot；
    /// - 内部 snapshot 失败不 panic，用 `tracing::error!` 打日志并清理 .tmp 文件。
    ///
    /// 并发安全：`snapshot_handle.read().is_none()` → `write().replace(handle)`
    /// 之间虽理论上有 TOCTOU 竞态，但本项目 Persistence 构造在单线程初始化路径，
    /// 多个 Clone 实例同时到达这里的概率极低；即使重复 spawn（handle 没被及时写入 write guard）
    /// 也只会多起一条线程，最终 shutdown 会 join 所有句柄，不产生 UB。
    fn spawn_snapshot_worker(&self) {
        // 快速路径：Never 模式不起线程
        if self.snapshot_mode == SnapshotMode::Never {
            return;
        }

        // let-else 解构 Interval：只对带 Duration 的变体起线程；
        // 未来新增其他模式（如 OnCommit）会自然落入 else return。
        let SnapshotMode::Interval(interval) = self.snapshot_mode else {
            return;
        };

        if self.snapshot_handle.read().is_none() {
            let inner = self.inner.clone();
            let aol = self.aol.clone();
            let snapshot_path = self.snapshot_path.clone();

            let pending_syncs = self.pending_syncs.clone();
            // clone Arc<AtomicBool>：线程持有其独立引用，即便 Persistence 被 drop
            // 也能通过该 Arc 读到 false 并退出。
            let enable = self.background_threads_enabled.clone();
            let compression_mode = self.compression_mode;
            let handle = thread::spawn(move || {
                while enable.load(Ordering::Acquire) {
                    // park_timeout 同时承担「睡眠 interval」和「可被 unpark 立即唤醒」两个职责。
                    thread::park_timeout(interval);

                    // 醒来后二次检查：避免 shutdown 的 unpark 恰好和 timeout 同时发生，
                    // 或者 enable 在 park 期间被置 false。
                    if !enable.load(Ordering::Acquire) {
                        break;
                    }

                    // 与公共 snapshot() 完全相同的闭包逻辑。单独复制一份而非封装成私有辅助函数
                    // 是因为 closure 捕获的变量不同（self 在线程闭包里不能直接用）。
                    let temp_path = snapshot_path.with_extension(".tmp");

                    let result = || -> Result<(), PersistenceError> {
                        let file = File::create(&temp_path)?;
                        let mut  writer = CompressedWriter::new(file, compression_mode)?;

                        let aol_cutoff_positon = if let Some(ref aol) = aol {
                            aol.lock()?.metadata()?.len()
                        } else {
                            0
                        };

                        for entry in inner.datastore.iter() {
                            let versions = entry.value().read().all_versions();
                            if !versions.is_empty() {
                                bincode::serde::encode_into_std_write(
                                    &(entry.key().clone(), versions),
                                    &mut writer,
                                    config::standard(),
                                )?;
                            }
                        }

                        writer.flush()?;
                        writer.finish()?;
                        fs::rename(&temp_path, &snapshot_path)?;
                        {
                            let final_file = File::open(&snapshot_path)?;
                            final_file.sync_all()?;
                        }

                        Self::truncate(&aol, aol_cutoff_positon, &pending_syncs)?;

                        Ok(())
                    }();

                    // worker 线程不能 panic（会把整个进程打崩），所以只打 error 日志并清 tmp。
                    if let Err(e) = result {
                        tracing::error!("snapshot worker error: {:?}", e);
                        let _ = fs::remove_file(&temp_path);
                    }
                }
            });

            *self.snapshot_handle.write() = Some(handle);
        }
    }


    /// 启动后台 AOL 追加 worker 线程。仅在 `AolMode::AsynchronousAfterCommit` 下生效。
    ///
    /// 该线程从 `async_append_injector` 无锁队列中批量消费 `AsyncAppendOperation`，
    /// 并将它们编码写入 AOL 文件。采用批量写（batch）的方式减少 syscall 次数。
    ///
    /// # 批量策略
    ///
    /// - 最多聚合 `BATCH_SIZE`（100）条操作后一次性写入
    /// - 队列为空时 `park_timeout(TIMEOUT_MS)` 短暂等待新数据，避免空转
    /// - 队列为空且本批有数据时立即刷写，不等待满批次
    ///
    /// # fsync 处理
    ///
    /// 批量写入完成后，根据 `fsync_mode` 决定是否以及何时 `sync_all`：
    /// - `Never`：仅递增 `pending_syncs`，不主动 fsync
    /// - `EveryAppend`：立即 fsync
    /// - `Interval`：检查是否到达 fsync 时间窗口，到了就 fsync，否则只递增计数
    ///
    /// # 线程结构
    ///
    /// - `park_timeout` + `background_threads_enabled` 双重开关，与其他 worker 同构
    /// - 错误只打日志不 panic，避免 worker 线程崩溃导致整个进程退出
    fn spawn_appender_worker(&self) {
        if self.aol_mode != AolMode::AsynchronousAfterCommit {
            return;
        }

        if let Some(ref aol) = self.aol {
            if self.append_handle.read().is_none() {
                let injector = self.async_append_injector.clone();
                let aol = aol.clone();
                let fsync_mode = self.fsync_mode;
                let enable = self.background_threads_enabled.clone();
                let pending_syncs = self.pending_syncs.clone();
                let last_fsync = self.last_fsync.clone();

                let handle = thread::spawn(move || {
                    const BATCH_SIZE: usize = 100;
                    const TIMEOUT_MS: u64 = 10;

                    let mut batch = Vec::with_capacity(BATCH_SIZE);

                    while enable.load(Ordering::Acquire) {
                        if !enable.load(Ordering::Acquire) {
                            break;
                        }

                        batch.clear();

                        // 内层循环：持续从 injector 窃取操作，直到满足批量条件或队列为空
                        loop {
                            if !enable.load(Ordering::Acquire) {
                                break;
                            }

                            match injector.steal() {
                                Steal::Retry => {
                                    // 临时竞争，yield 后重试
                                    std::thread::yield_now();
                                    continue;
                                }
                                Steal::Success(op) => {
                                    batch.push(op);
                                    if batch.len() >= BATCH_SIZE {
                                        break;
                                    }
                                }
                                Steal::Empty => {
                                    // 队列为空：如果已攒了一批则立即刷写
                                    if !batch.is_empty() {
                                        break;
                                    }
                                    // 否则 park 等待新数据
                                    thread::park_timeout(Duration::from_millis(TIMEOUT_MS));
                                }
                            }
                        }

                        if !batch.is_empty() {
                            let result = || -> Result<(), PersistenceError> {

                                if let Ok(mut file) = aol.lock() {
                                    // 批量编码写入：每条 (key, version, value) 记录独立 bincode 编码
                                    let mut writer = BufWriter::new(&mut *file);
                                    for op in &batch {
                                        for (k, v) in &op.writeset {
                                            bincode::serde::encode_into_std_write(
                                                (k, op.version, v),
                                                &mut writer,
                                                config::standard(),
                                            )?;
                                        }
                                    }
                                    writer.flush()?;
                                    drop(writer);

                                    // 根据 fsync_mode 决定刷盘策略
                                    match fsync_mode {
                                        FsyncMode::Never => {
                                            // 不主动 fsync，仅记录有未同步数据
                                            pending_syncs.fetch_add(1, Ordering::Release);
                                        }

                                        FsyncMode::EveryAppend => {
                                            // 每次批量写入后立即 fsync
                                            file.sync_all()?;
                                        }

                                        FsyncMode::Interval(duration) => {
                                            let now = Instant::now();

                                            let should_sync = {
                                                let mut last_fsync = last_fsync.lock()?;

                                                if now.duration_since(*last_fsync) >= duration {
                                                    *last_fsync = now;
                                                    true
                                                } else {
                                                    false
                                                }
                                            };

                                            if should_sync {
                                                file.sync_all()?;
                                                pending_syncs.store(0, Ordering::Release);
                                            } else {
                                                pending_syncs.fetch_add(1, Ordering::Release);
                                            }
                                        }
                                    }
                                }

                                Ok(())
                            }();

                            if let Err(e) = result {
                                tracing::error!("append worker error: {:?}", e);
                            }
                        }
                    }
                });

                *self.append_handle.write() = Some(handle);
            }
        }
    }

    /// 将一次事务提交的写集追加到 AOL 文件。
    ///
    /// 根据 `aol_mode` 选择不同的写入路径：
    ///
    /// - `Never`：直接返回 Ok(())，不写 AOL
    /// - `SynchronousOnCommit`：**同步写入**——在当前调用线程中加锁 AOL 文件，
    ///   逐 key 编码写入后根据 `fsync_mode` 决定是否立即 `sync_all`。
    ///   提交延迟 = 一次 bincode 编码 + 一次 write syscall + 可选 fsync。
    /// - `AsynchronousAfterCommit`：**异步写入**——将 `AsyncAppendOperation` 推入
    ///   `async_append_injector` 无锁队列，然后唤醒 append worker 线程。
    ///   提交线程无磁盘 IO 阻塞，写入由后台线程批量完成。
    ///
    /// # fsync 策略
    ///
    /// 在同步模式下，每次 append 后根据 `fsync_mode`：
    /// - `Never`：递增 `pending_syncs`，由 fsync worker 或 snapshot/drop 时兜底
    /// - `EveryAppend`：立即 `sync_all`
    /// - `Interval`：若距上次 fsync 已过 duration，立即 fsync；否则仅递增 pending_syncs
    ///
    /// # 错误处理
    ///
    /// 写入失败会向上返回 `PersistenceError`，由调用方（`TransactionInner::auto_commit`）
    /// 处理回滚逻辑（从合并队列移除、清空写集等）。
    pub(crate) fn append(
        &self,
        version: u64,
        writeset: &BTreeMap<Bytes, Option<Bytes>>,
    ) -> Result<(), PersistenceError> {
        if self.aol_mode == AolMode::Never || self.aol.is_none() {
            return Ok(());
        }

        match self.aol_mode {
            // 异步模式：提交记录推入无锁队列，由 append worker 批量写盘
            AolMode::AsynchronousAfterCommit => {
                self.async_append_injector.push(AsyncAppendOperation {
                    version,
                    writeset: writeset.clone(),
                });

                // 唤醒 append worker 线程（如果它在 park_timeout 中等待）
                if let Some(handle) = self.append_handle.read().as_ref() {
                    handle.thread().unpark();
                }
            },
            
            // 同步模式：提交记录在当前线程中直接写入 AOL 文件
            AolMode::SynchronousOnCommit => {
                // 锁住 AOL 文件 Mutex，确保写入的原子性（不会与其他线程的写入交错）
                let aol = self.aol.as_ref().unwrap();
                let mut file = aol.lock()?;
                let mut writer = BufWriter::new(&mut *file);

                // 逐 key 将写集编码为 (key, version, value) 元组写入 AOL
                for (k, v) in writeset {
                    bincode::serde::encode_into_std_write(
                        (k, version, v),
                        &mut writer,
                        config::standard(),
                    )?;
                }

                writer.flush()?;

                drop(writer);

                // 根据 fsync_mode 决定是否以及何时将数据刷到磁盘
                match self.fsync_mode {
                    FsyncMode::Never => {
                        // 不主动 fsync，仅记录待同步计数
                        self.pending_syncs.fetch_add(1, Ordering::Release);
                    }

                    FsyncMode::EveryAppend => {
                        // 每次同步追加后立即 fsync
                        file.sync_all()?;
                    }

                    FsyncMode::Interval(duration) => {
                        let now = Instant::now();

                        let should_sync = {
                            let mut last_fsync = self.last_fsync.lock()?;

                            if now.duration_since(*last_fsync) >= duration {
                                *last_fsync = now;
                                true
                            } else {
                                false
                            }
                        };

                        if should_sync {
                            file.sync_all()?;
                            self.pending_syncs.store(0, Ordering::Release);
                        } else {
                            self.pending_syncs.fetch_add(1, Ordering::Release);
                        }
                    }
                }
            },

            AolMode::Never => {}
        }
        

        Ok(())
    }
}

/// Persistence 析构兜底：关闭开关 + 清理线程句柄。
///
/// 注意：通常情况下 `Database::shutdown` 会**抢先**把 snapshot_handle 的 JoinHandle
/// 通过 `write().take()` 取出并 join，此时 Drop 里的 take 只能拿到 None。
/// 本 Drop 的真正作用是：防止调用方绕过 Database 直接 `Persistence::new_with_options(..)`
/// 使用、或者忘记调用 `Database::shutdown`，导致后台线程悬挂到进程退出。
///
/// 两条关闭路径谁先执行都安全：先执行的一方把 handle take 走并 join，
/// 后执行的一方拿到 None 直接 return。
impl Drop for Persistence {
    fn drop(&mut self) {
        // Release 序：对线程里的 Acquire load，保证本线程之前的所有写入（如 join 后的清理）
        // 都对对方可见。这里 store false 主要是给「worker 还在 park 循环里」的情况看。
        self.background_threads_enabled.store(false, Ordering::Release);

        // 关闭快照线程。理论上不会执行，因为当 database 关闭时，
        // database::shutdown 会抢先把 handle 设为 None 并 join；
        // 这里这么做是为了防止有人直接创建 persistence 实例导致快照线程未关闭。
        if let Some(handle) = self.snapshot_handle.write().take() {
            handle.thread().unpark();
            let _ = handle.join();
        }

        // 关闭 AOL append worker 线程
        if let Some(handle) = self.append_handle.write().take() {
            handle.thread().unpark();
            let _ = handle.join();
        }

        // 关闭 fsync worker 线程
        if let Some(handle) = self.fsync_handle.write().take() {
            handle.thread().unpark();
            let _ = handle.join();
        }

        // 最终兜底：若 AOL 启用且存在未 fsync 的数据，
        // 在 drop 前执行最后一次 sync_all，确保数据真正落到磁盘。
        // 这是最后的持久化防线——防止异步模式下 pending_syncs 非零时进程崩溃丢数据。
        if self.aol_mode != AolMode::Never && self.pending_syncs.load(Ordering::Acquire) > 0 {
            if let Some(ref aol) = self.aol {
                if let Ok(file) = aol.lock() {
                    let _ = file.sync_all();
                }
            }
        }
    }
}
