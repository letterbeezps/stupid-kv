use std::{sync::Arc, thread::JoinHandle};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::{options::DatabaseOptions, oracle::Oracle, queue::{Commit, Merge}, versions::Versions};

/// `counter_by_commit` 中 counter 的墓碑值：表示该 commit 对应的
/// 引用计数已经归零，正在等待被从 map 中摘除。之后再想 +1 需要
/// 换到新的 counter，而不是复活这个已经被判死的 slot。
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

    /// 活跃事务引用计数表，用于 commit queue GC 计算水位线。
    ///
    /// - key：事务开始时读取的 `transaction_commit_id`，即该事务的快照起点 commit
    /// - value：当前仍持有该快照起点的活跃事务个数（共享的原子计数器）
    ///
    /// 事务开始时在对应 counter 上 +1，事务销毁时 -1，减到 0 则从 map 中摘除。
    /// GC 时通过遍历该 map 取最早的活跃 commit 作为水位线，`< 水位线` 的
    /// commit queue entry 可以被安全清理。
    pub(crate) counter_by_commit: SkipMap<u64, Arc<AtomicU64>>,
    /// 后台 GC 清理线程句柄。
    /// Database 析构时通过它 join 后台线程，确保清理线程与 Inner 生命周期同步。
    pub(crate) transaction_cleanup_handle: RwLock<Option<JoinHandle<()>>>,

    /// 底层数据存储，存储最终的键值对数据
    /// 每个键对应一个RwLock保护的Versions结构，用于实现MVCC版本管理
    pub(crate) datastore: SkipMap<Bytes, RwLock<Versions>>,

    /// 后台任务运行开关。
    /// Database drop 时被置为 false，后台清理线程读到该标志后退出循环。
    pub(crate) background_threads_enabled: AtomicBool,
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
            datastore: SkipMap::new(),
            background_threads_enabled: AtomicBool::new(true),
        }
    }
}

impl Inner {

    /// 计算当前活跃事务中最早的快照起点 commit。
    /// 若 `counter_by_commit` 中没有活跃 counter，则返回 `fallback` 作为兜底水位线。
    #[inline]
	pub(crate) fn earliest_active_commit(&self, fallback: u64) -> u64 {
		earliest_active(&self.counter_by_commit, fallback)
	}

    /// commit queue GC 核心逻辑：删除所有活跃事务不再需要的旧提交记录。
    ///
    /// 算法：
    /// 1. 先读 `transaction_commit_id` 作为 fallback 水位线；
    /// 2. 再扫描 `counter_by_commit` 取最早活跃 commit 作为 oldest；
    /// 3. 删除 `transaction_commit_queue` 中所有 key `< oldest` 的 entry。
    ///
    /// 顺序关键：先读 fallback 再扫描 counter map，能保证任何"注册中被漏扫"
    /// 的并发事务，其快照 `>= fallback`，其冲突窗口 `(snapshot, ..)` 不会被误删。
    /// 用 `< oldest` 而非 `<= oldest`：oldest 本身是某个活跃事务的快照起点，
    /// 该事务仍需扫描 `> oldest` 的 commit queue 做冲突检测，oldest 自己不能删。
    pub(crate) fn run_cleanup_inner(&self) {
        let fallback = self.transaction_commit_id.load(Ordering::SeqCst);
        let oldest = self.earliest_active_commit(fallback);
        self.transaction_commit_queue.range(..oldest).for_each(|e| {
            e.remove();
        });
    }
}

/// 扫描 `counter_by_commit`，返回最早的活跃 commit id。
///
/// # 并发正确性（Dekker 风格双 fence 协议）
///
/// GC 必须维护的安全不变式：对任何"注册已完成、快照起点为 v"的活跃事务 TX，
///
/// ```text
///     若本次扫描读到 fallback > v ⟹ 必然读到 counter[v] >= 1
/// ```
///
/// 违反该不变式意味着 GC 会用 fallback 当水位线，把 `commit_queue` 中
/// `(v, fallback)` 区间的记录当作过期数据删掉，而这些正是 TX 提交时
/// 做冲突检测要扫描的记录——一旦被并发误删，会导致写写/写读冲突静默漏检，
/// 出现 lost update / write skew 等正确性 bug。
///
/// TX 侧（见 `register_counter`）通过 "CAS → F_tx → reload commit_id" 承诺：
/// **若 TX 注册成功，则它的 CAS 在 SC 全局序上早于任何后续把 commit_id 抬高的写。**
///
/// 但这只是 TX 端对自己行为的承诺，GC 想利用它必须把自己的两次 load 也
/// 串成 SC 有序——这就是本函数入口 `F_gc` 的职责：
///
/// ```text
///     TX:    CAS counter[v]=1 ── F_tx ── reload commit_id (== v)
///                                                 ↑ SC 全局序
///     Cmt:   commit_id: v → v+1                   │
///                                                 ↓
///     GC:    load fallback ── F_gc ── load counter[v]
/// ```
///
/// 当 GC 读到 `fallback > v` 时，Committer 的写必然排在 GC 的 fallback load 之前，
/// 也必然排在 TX 的 reload 之后；`F_tx` 与 `F_gc` 一起把 TX 的 CAS 钉在
/// GC 的 counter load 之前，Acquire load 因此一定能观察到 `counter[v] >= 1`。
///
/// # 场景 1：注册完成的事务，GC 一定看见
///
/// ```text
///     TX-A: CAS counter[5]=1 (Release) ── F_tx ── reload commit_id=5 ✓ 返回
///     Cmt:                                        commit_id: 5 → 6
///     GC:                                                     load fallback=6 (SC)
///                                                             F_gc
///                                                             load counter[5] → 1 (Acquire)
/// ```
///
/// fallback=6 > TX-A 的快照 5；双 fence 把 TX-A 的 CAS 钉在 GC 的 counter load 之前，
/// GC 判定 5 号活跃，`oldest=5`，只删 `commit_queue` 中 `< 5` 的记录，
/// TX-A 提交时扫描 `> 5` 的区间不受影响。
///
/// # 场景 2：注册未完成的事务，GC 看不到也安全
///
/// ```text
///     TX-B: load commit_id=5
///           CAS counter[5]=1  ← 尚未执行
///     GC:                     load fallback=5 (SC)
///                             F_gc
///                             load counter[5] → 0（此 slot 甚至可能不存在）
///                             ⇒ oldest = fallback = 5，删 < 5 的记录
///     TX-B:                   继续执行 CAS、F_tx、reload=5 ✓ 返回
/// ```
///
/// TX-B 后续注册成功时，其快照仍为 5，需要扫描的区间是 `> 5`；
/// GC 删的是 `< 5` 的记录，与 TX-B 的扫描区间无交集。
/// 只要 TX-B 观察到"commit_id 稳定为 5"这个事实存在，`fallback` 就不可能小于 5，
/// 所以 GC 走 fallback 兜底始终安全。
///
/// # 场景 3：缺失 `F_gc` 时的乱序（弱内存序架构上）
///
/// ```text
///     TX-A: CAS counter[5]=1 (Release) ── F_tx ── reload commit_id=5 ✓ 返回
///     Cmt:                                        commit_id: 5 → 6
///     GC:                                                     load fallback=6
///                                                             (无 F_gc)
///                                                             load counter[5] → 0 (stale)
/// ```
///
/// GC 的 counter Acquire load 与 fallback SC load 之间没有强制顺序，
/// 可以从本地缓存读到"初始 0"而未观察到 TX-A 的 Release。GC 判定"5 号无人"，
/// 用 fallback=6 兜底，删除 `commit_queue` 中 key `< 6` 的记录——
/// **TX-A 提交时 `range(6..)` 扫不到冲突源，冲突检测静默失效**。
///
/// # 其它细节
///
/// - 跳过 `counter == 0` 或 `COUNTER_TOMBSTONE`：这些 slot 正在退场
///   （已归零或已打墓碑等待被摘除），不代表当前活跃事务。
#[inline]
fn earliest_active(map: &SkipMap<u64, Arc<AtomicU64>>, fallback: u64) -> u64 {
    fence(Ordering::SeqCst);
    for entry in map.iter() {
        let c = entry.value().load(Ordering::Acquire);
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