use std::io::{BufReader, BufWriter, Write};
use std::thread;
use std::{thread::JoinHandle};
use std::{path::PathBuf};
use std::{fs::{self, File}};
use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}};

use bincode::config;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::versions::{Version, Versions};
use crate::{PersistenceOptions, SnapshotMode, db::inner::Inner, error::PersistenceError};

/// 持久化门面：封装快照写入（snapshot）、启动恢复（load）与后台周期性快照线程。
///
/// 持久化模型采用「全量快照」而非 WAL：
/// - 写入：`snapshot()` 把 datastore 每个 key 的完整版本链用 bincode 流式编码到临时文件，
///   完成后通过 `fs::rename` 原子替换旧快照，最后 `sync_all` 确认真正落到磁盘 platter。
/// - 恢复：`load()` 在构造函数里被调用，从快照文件逐条 bincode 解码回 `Versions`，
///   用 `Versions::push` 保证加载后的版本链与在线写入后满足同一不变式。
/// - 后台线程：`SnapshotMode::Interval(dur)` 时启动一条独立线程，
///   使用与 04/05 节 GC 线程同构的 `park_timeout + 双重开关 + unpark/join` 模式。
#[derive(Clone)]
pub struct Persistence {
    /// 内部数据库共享状态。snapshot 遍历 datastore，load 回填 datastore 都通过它。
    pub(crate) inner: Arc<Inner>,

    /// 快照文件最终落地路径。
    ///
    /// 默认为 `{base_path}/snapshot.bin`；若 `PersistenceOptions.snapshot_path` 为绝对路径则直接使用，
    /// 相对路径则拼接在 `base_path` 下。
    pub(crate) snapshot_path: PathBuf,

    /// 快照触发模式：Never（纯手动调用 `snapshot()`）或 Interval（后台周期线程）。
    pub(crate) snapshot_mode: SnapshotMode,

    /// 后台快照线程运行开关，独立于 `Inner.background_threads_enabled`。
    ///
    /// 使用独立 Arc 是因为 Persistence 可以脱离 Database 单独构造使用，
    /// 此时 Inner 可能尚未存在或生命周期不同步。
    pub(crate) background_threads_enabled: Arc<AtomicBool>,

    /// 后台快照线程句柄。`Arc<RwLock<Option<...>>>` 双重包装是为了：
    /// - `Arc`：Persistence 实现了 Clone（多个引用共享同一份后台线程实例），
    ///   避免 Clone 后每个实例都再 spawn 一条线程；由第一个 Persistence 实例负责 spawn。
    /// - `RwLock<Option>`：`spwan_snapshot_worker` 的 `read().is_none()` 判空后
    ///   `write().replace(handle)` 两步插入，保证并发构造 Persistence 时只起一条线程。
    pub(crate) snapshot_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl Persistence {

    /// 带配置构造 Persistence：创建目录 → 推导快照文件路径 → `load()` 恢复 → `spwan_snapshot_worker()` 启动线程。
    ///
    /// # 失败
    ///
    /// - 目录创建（`fs::create_dir_all`）失败 → `PersistenceError::Io`
    /// - 快照文件 `load()` 解码失败 → `PersistenceError::Deserialization`
    pub(crate) fn new_with_options(
        options: PersistenceOptions,
        inner: Arc<Inner>,
    ) -> Result<Self, PersistenceError> {

        let base_path = &options.base_path;

        // 确保基础路径存在；不存在则递归创建（同 `mkdir -p` 语义）
        fs::create_dir_all(base_path)?;

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

        // 若快照文件所在的父目录还不存在（上面只保证了 base_path），再建一层。
        // 例：snapshot_path = base_path / "snapshots/1.bin" 时需要创建 snapshots/ 子目录。
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let this = Self {
            inner,
            snapshot_path,
            snapshot_mode: options.snapshot_mode,
            background_threads_enabled: Arc::new(AtomicBool::new(true)),
            snapshot_handle: Arc::new(RwLock::new(None)),
        };

        // 先恢复数据再启后台线程：后台线程的 GC / 快照都需要一份完整的 datastore。
        this.load()?;

        this.spwan_snapshot_worker();

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
    ///
    /// 任何一步失败都会 `fs::remove_file(.tmp)` 清理半成品；正式快照不会被改到。
    pub fn snapshot(&self) -> Result<(), PersistenceError> {
        // 临时文件：同目录下扩展名 .tmp，保证 rename 在同一文件系统内（原子前提）
        let temp_path = self.snapshot_path.with_extension(".tmp");

        let result = || -> Result<(), PersistenceError> {
            // 1. 建临时文件（同名旧 tmp 会被 truncate，不影响正式快照）
            let file = File::create(&temp_path)?;

            // BufWriter 包装：8KB 块写入减少 syscall；bincode 的 encode_into_std_write
            // 逐条刷进 BufWriter 内部 buffer，不需要用户手动管理大 buffer。
            let mut  writer = BufWriter::new(file);

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
                let mut reader = BufReader::new(file);
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

        Ok(())
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
    fn spwan_snapshot_worker(&self) {
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
            let snapshot_path = self.snapshot_path.clone();
            // clone Arc<AtomicBool>：线程持有其独立引用，即便 Persistence 被 drop
            // 也能通过该 Arc 读到 false 并退出。
            let enable = self.background_threads_enabled.clone();
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
                        let mut  writer = BufWriter::new(file);

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
                        fs::rename(&temp_path, &snapshot_path)?;
                        {
                            let final_file = File::open(&snapshot_path)?;
                            final_file.sync_all()?;
                        }

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
    }
}
