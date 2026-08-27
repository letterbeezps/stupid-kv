use std::collections::HashSet;
use std::{sync::Arc, thread::JoinHandle};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};

use bytes::Bytes;
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::persistence::Persistence;
use crate::{options::DatabaseOptions, oracle::Oracle, queue::{Commit, Merge}, versions::Versions};

/// counter 归零后的墓碑值，`register_counter` 见到墓碑必须放弃该 slot、
/// 换一个新 counter；防止"归零 → 从 map 摘除"窗口内被复活。
pub(crate) const COUNTER_TOMBSTONE: u64 = u64::MAX;

/// Stupid-KV 核心存储引擎内部结构体
/// 
/// # 事务提交流程
/// ```markdown
/// ┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
/// │ 事务发起请求      │────▶│ transaction_commit_queue │────▶│ MVCC隔离性检查 │
/// └─────────────────┘     └──────────────────┘     └─────────────┘
///                            │                         │
///                            │ 通过检查                 │ 检查失败
///                            ▼                         ▼
/// ┌──────────────────┐     ┌──────────────────┐     ┌─────────────┐
/// │ transaction_merge_queue│◀─────│ 事务合并准备     │     │ 事务回滚     │
/// └──────────────────┘     └──────────────────┘     └─────────────┘
///                            │
///                            │ 写入底层存储
///                            ▼
/// ┌──────────────────┐     ┌──────────────────┐
/// │   datastore      │◀────│ 数据持久化       │
/// └──────────────────┘     └──────────────────┘
///                            │
///                            │ 清理临时数据
///                            ▼
/// ┌──────────────────┐
/// │ 从transaction_merge_queue删除记录 │
/// └──────────────────┘
/// ```
/// 
/// # 核心组件说明
/// - **transaction_queue_id**: 事务队列全局唯一标识
/// - **transaction_commit_id**: 事务提交记录全局唯一标识
/// - **transaction_commit_queue**: 事务提交队列，用于MVCC隔离性检查
/// - **transaction_merge_id**: 事务合并记录全局唯一标识
/// - **transaction_merge_queue**: 事务合并队列，存储待持久化的事务数据
/// - **datastore**: 底层数据存储，最终持久化的键值对数据
pub struct Inner {
    /// 时间戳生成器，用于生成事务提交记录的版本号
    pub(crate) oracle: Arc<Oracle>,
    

    /// 事务提交ID, 用于唯一标识每个事务提交记录，全局递增，用于提交事务时的隔离性检查
    pub(crate) transaction_commit_id: AtomicU64,
    /// 事务队列ID, 标识该事务在事务队列中的唯一标识
    pub(crate) transaction_queue_id: AtomicU64,
    /// 事务提交队列, 用于存储事务提交记录，键为事务提交ID，值为事务提交记录
    /// 提交事务时首先写入此队列，在此阶段会执行MVCC隔离性检查，确保事务之间的隔离性
    /// 事务提交记录包含事务队列ID、事务操作键值对等信息
    pub(crate) transaction_commit_queue: SkipMap<u64, Arc<Commit>>,

    /// 事务合并ID, 用于唯一标识每个事务合并记录，全局递增
    pub(crate) transaction_merge_id: AtomicU64,
    /// 事务合并队列, 用于存储已经通过隔离性检查、等待写入底层数据存储的事务合并记录
    /// 键为版本号，值为事务合并记录，事务合并记录包含事务合并队列ID、事务操作键值对等信息
    /// 当数据成功写入底层存储后，会从此队列中删除对应的记录
    pub(crate) transaction_merge_queue: SkipMap<u64, Arc<Merge>>,

    /// 活跃事务快照 commit 引用计数表。key = 事务快照起点 `transaction_commit_id`，
    /// value = 当前持有该快照的活跃事务数（共享原子计数器）。
    /// commit queue GC 遍历取最早活跃 key 作水位线，`< 水位线` 的 commit queue
    /// entry 可安全清理。详见 `docs/004_commit_queue_gc.md`。
    pub(crate) counter_by_commit: SkipMap<u64, Arc<AtomicU64>>,
    /// commit queue GC 后台线程句柄，Database 析构时 join。
    pub(crate) transaction_cleanup_handle: RwLock<Option<JoinHandle<()>>>,

    /// 活跃事务快照 version 引用计数表。与 `counter_by_commit` 结构对称，
    /// 但 key 是 Oracle 时间戳，服务于 datastore 版本 GC。
    pub(crate) counter_by_oracle: SkipMap<u64, Arc<AtomicU64>>,

    /// datastore 版本 GC 后台线程句柄，Database 析构时 join。
    pub(crate) garbage_collection_handle: RwLock<Option<JoinHandle<()>>>,

    /// GC 已发布的水位线上界。`compute_cleanup_ts` 在动手回收前 `fetch_max`
    /// 到此处，`register_counter` 事后检查 `gc_floor <= v`——阻止"新事务恰好
    /// 拿到已被 GC 判死的快照"这种 counter 事后登记救不回来的情况。
    /// 详见 `docs/005_version_history_gc.md`。
    pub(crate) gc_floor: AtomicU64,

    /// 版本 GC 的增量脏 key 队列。事务提交把写入 key 推入本队列，
    /// GC 线程消费队列只处理最近有写入的 key，避免每轮扫全表；
    /// `SegQueue` 无锁 MPMC，消费端用 HashSet 去重。
    pub(crate) gc_dirty_keys: SegQueue<Bytes>,

    /// 底层数据存储，存储最终的键值对数据
    /// 每个键对应一个RwLock保护的Versions结构，用于实现MVCC版本管理
    pub(crate) datastore: SkipMap<Bytes, RwLock<Versions>>,

    /// 后台任务运行开关。
    /// Database drop 时被置为 false，后台清理线程读到该标志后退出循环。
    pub(crate) background_threads_enabled: AtomicBool,

    /// 持久化实例引用。与 `Database.persistence` 的双持有：
    /// - Database 持有 `Option<Persistence>`（值语义），生命周期由 Database 构造 / Drop 管理；
    /// - Inner 持有 `RwLock<Option<Arc<Persistence>>>`（引用语义），供以后其他模块（如 WAL）
    ///   从 Inner 侧反向访问 Persistence 的路径 / 配置信息，不需要同时持有 Database 引用。
    ///
    /// 包 `RwLock<Option<Arc<...>>>` 三层：
    /// - `RwLock`：写只发生在 `Database::new_with_persistence` 的构造阶段，读无竞争；
    /// - `Option`：纯内存模式下为 None，不分配 Persistence；
    /// - `Arc`：与 Database.persistence 里的值共享同一实例（clone 的是 Arc 引用，不是实例）。
    pub(crate) persistence: RwLock<Option<Arc<Persistence>>>,

    /// 复用阈值：writeset 超过此长度时 reset 整块替换，否则只 clear。
    /// 用于事务对象池，避免大小事务交替产生 allocator 抖动。
    pub(crate) reset_threshold: usize,
}

impl Inner {
    pub fn new(opts: &DatabaseOptions) -> Self {
        Self {
            oracle:Oracle::new(opts.resync_interval),
            transaction_queue_id: AtomicU64::new(0),
            transaction_commit_id: AtomicU64::new(0),
            transaction_commit_queue: SkipMap::new(),
            transaction_merge_id: AtomicU64::new(0),
            transaction_merge_queue: SkipMap::new(),
            counter_by_commit: SkipMap::new(),
            transaction_cleanup_handle: RwLock::new(None),
            counter_by_oracle: SkipMap::new(),
            garbage_collection_handle: RwLock::new(None),
            gc_floor: AtomicU64::new(0),
            gc_dirty_keys: SegQueue::new(),
            datastore: SkipMap::new(),
            background_threads_enabled: AtomicBool::new(true),
            persistence: RwLock::new(None),
            reset_threshold: opts.reset_threshold,
        }
    }
}

impl Inner {

    /// 扫描 `counter_by_oracle` 取最早活跃快照 version（datastore 版本 GC 水位线）。
    /// 空 map 时返回 `fallback`。底层复用 `earliest_active`。
    #[inline]
    pub(crate) fn earlist_active_version(&self, fallback: u64) -> u64 {
        earliest_active(&self.counter_by_oracle, fallback)
    }

    /// 扫描 `counter_by_commit` 取最早活跃快照 commit（commit queue GC 水位线）。
    /// 空 map 时返回 `fallback`。底层复用 `earliest_active`。
    #[inline]
	pub(crate) fn earliest_active_commit(&self, fallback: u64) -> u64 {
		earliest_active(&self.counter_by_commit, fallback)
	}

    /// commit queue GC：删除 `< earliest_active_commit` 的历史 commit entry。
    ///
    /// 先读 `transaction_commit_id` 作 fallback、再扫 counter map：确保漏扫的
    /// 并发注册事务其快照 `>= fallback`，冲突窗口 `(snapshot, ..)` 不会被误删。
    /// 用 `< oldest` 而非 `<= oldest`：oldest 本身是某活跃事务快照起点，
    /// 该事务仍需扫描 `> oldest` 做冲突检测，oldest 自己不能删。
    ///
    /// 详见 `docs/004_commit_queue_gc.md`。
    pub(crate) fn run_cleanup_inner(&self) {
        let fallback = self.transaction_commit_id.load(Ordering::SeqCst);
        let oldest = self.earliest_active_commit(fallback);
        self.transaction_commit_queue.range(..oldest).for_each(|e| {
            e.remove();
        });
    }

    /// 计算 datastore 版本 GC 的安全水位线：**任何活跃事务能看到的版本都 > 返回值**。
    ///
    /// 算法（两次扫描 + 中间发布 gc_floor + F_gc）：
    /// ```text
    /// proposed      = min(now, first_scan, oracle_now)
    /// gc_floor.fetch_max(proposed, SeqCst)   // 发布水位线，供 register_counter 事前检查
    /// fence(SeqCst)                           // F_gc，闭合 Dekker 协议
    /// cleanup_ts    = min(proposed, second_scan)
    /// ```
    ///
    /// 与 `register_counter` 一起维护不变式：本轮回收后仍能注册成功的事务，
    /// 其 version 必然 > cleanup_ts。两次扫描 + gc_floor 保证任何并发注册的
    /// 事务要么被第二次扫描看见（水位被压低），要么在 `register_counter` 里
    /// 读到 `gc_floor > v` 主动 rollback。
    ///
    /// `proposed` 必须 cap 到 `oracle_now`：idle 数据库下 wall clock 会推进但
    /// Oracle 时间戳不动，不 cap 会让 `gc_floor` 永远超过任何新事务能拿到的
    /// version，注册陷入死循环。
    ///
    /// 详见 `docs/005_version_history_gc.md`。
    pub(crate) fn compute_cleanup_ts(&self) -> u64 {
        let now = self.oracle.current_time_ns();
        let earliest_tx = self.earlist_active_version(now);
        let oracle_now = self.oracle.inner.timestamp.load(Ordering::SeqCst);

        let proposed = now.min(earliest_tx).min(oracle_now);
        self.gc_floor.fetch_max(proposed, Ordering::SeqCst);
        fence(Ordering::SeqCst);   // F_gc

        let earliest_after = self.earlist_active_version(now);
        proposed.min(earliest_after)
    }

    /// 全量版本 GC：遍历 datastore 每个 key，就地压缩版本链，空版本链摘除 entry。
    /// 增量路径 (`run_gc_dirty_inner`) 的兜底，覆盖冷 key 与纯 tombstone 僵尸 entry。
    ///
    /// **entry.remove() 与写路径的握手**：`entry.remove()` 必须在持有 `versions` 写锁时调用，
    /// 与 `TransactionInner::commit` 中"拿到写锁 → 检查 `entry.is_removed()` → 重试"的
    /// 循环相呼应：写路径可能已通过 `get_or_insert_with` 拿到同一个 entry 但尚未取锁，
    /// GC 在写锁保护下摘除后，写路径拿到锁时会看到 `is_removed()` 为 true 并重新走
    /// `get_or_insert_with` 拿到新 entry，避免把数据写入即将被回收的节点。
    pub(crate) fn run_gc_full(&self, cleanup_ts: u64) {
        for entry in self.datastore.iter() {
            let mut versions = entry.value().write();
            if versions.gc_older_versions(cleanup_ts) == 0 {
                entry.remove();
            }
        }
    }

    /// 增量版本 GC：只处理 `gc_dirty_keys` 队列中最近有写入的 key。
    /// HashSet 去重：同一 key 在一轮内可能被多次提交、多次入队，只 GC 一次。
    pub(crate) fn run_gc_dirty_inner(&self, cleanup_ts: u64) {
        let mut seen = HashSet::new();
        while let Some(key) = self.gc_dirty_keys.pop() {
            if !seen.insert(key.clone()) {
                continue;
            }
            self.gc_key(&key, cleanup_ts);
        }
    }

    /// 对单个 key 压缩版本链；空链则从 datastore 摘除 entry。
    /// key 已不在 datastore 时（前一轮已摘除等），静默返回。
    /// entry.remove() 必须在持有 `versions` 写锁时调用，参见 `run_gc_full` 的说明。
    fn gc_key(&self, key: &Bytes, cleanup_ts: u64) {
        if let Some(entry) = self.datastore.get(key) {
            let mut versions = entry.value().write();
            if versions.gc_older_versions(cleanup_ts) == 0 {
                entry.remove();
            }
        }
    }
}

/// GC 水位线扫描原语：遍历 counter map 返回首个活跃 key（即最早活跃 commit/version），
/// map 为空则返回 `fallback`。被 `earliest_active_commit` / `earlist_active_version` 共用。
///
/// 入口的 `fence(SeqCst)` 是 Dekker 双 fence 协议的 GC 半边（`F_gc`），
/// 与 `register_counter` 中的 `F_tx` 一起保证：任何注册成功的活跃事务 TX，
/// 只要 GC 端读到 `fallback > TX.snapshot`，就必然能读到 `counter[snapshot] >= 1`。
/// 缺 fence 会在弱内存序架构上读到过期的 counter=0，误删活跃事务需要的数据。
///
/// 完整证明见 `docs/004_commit_queue_gc.md`。
#[inline]
fn earliest_active(map: &SkipMap<u64, Arc<AtomicU64>>, fallback: u64) -> u64 {
    fence(Ordering::SeqCst);   // F_gc
    for entry in map.iter() {
        let c = entry.value().load(Ordering::Acquire);
        // 跳过归零 / 墓碑 slot（正在退场，不代表活跃事务）
        if c != 0 && c != COUNTER_TOMBSTONE {
            return *entry.key();
        }
    }
    fallback
}

impl Default for Inner {
    fn default() -> Self {
        Self::new(&&DatabaseOptions::default())
    }
}