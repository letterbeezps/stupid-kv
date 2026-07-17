use std::{ collections::BTreeMap, sync::{Arc, atomic::{AtomicU64, Ordering, fence}}};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use papaya::HashSet;
use parking_lot::{Mutex, lock_api::RwLock};

use crate::{bloom::BloomFilter, inner::COUNTER_TOMBSTONE};
use crate::db::inner::Inner;
use crate::error::Error;
use crate::kv::IntoBytes;
use crate::queue::{Commit, Merge};
use crate::tx::IsolationLevel;
use crate::versions::{Version, Versions};


pub(crate) struct TransactionInner {
    /// 事务隔离级别
    pub(crate) mode: IsolationLevel,
    /// 事务是否已完成
    pub(crate) done: bool,
    /// 事务是否是写事务
    pub(crate) write: bool,
    /// 该事务创建时db的commit ID，由db::Inner::transaction_commit_id
    pub(crate) commit: u64,
    /// 本事务在 `counter_by_commit[commit]` 上共享的引用计数。
    ///
    /// - 多个并发事务若读到同一 `transaction_commit_id`，会共享同一个 counter；
    /// - 事务创建时由 `register_counter` 完成 +1；
    /// - 事务销毁时由 `release_counter` 完成 -1，减到 0 则打上墓碑并由 `Transaction::drop`
    ///   将对应 entry 从 `counter_by_commit` 中摘除。
    /// GC 通过该 counter 判定：只要它非零，就说明还有事务把 `commit` 当作快照起点，
    /// `<= commit` 的 commit queue entry 都不可以被回收。
    pub(crate) counter_commit: Arc<AtomicU64>,
    /// 该事务创建时db的数据的当前版本号，由db::Inner::Oracle分配
    pub(crate) version: u64,
    /// 该事务读取的键值对的键集合
    pub(crate) readset: HashSet<Bytes>,
    /// 该事务读取的键值对的键集合的布隆过滤器
    pub (crate) readeset_bloom: Mutex<BloomFilter>,
    /// 该事务的写操作键值对，键为键，值为值
    pub(crate) writeset: BTreeMap<Bytes, Option<Bytes>>,
    /// 该事务的数据库实例
    pub(crate) database: Arc<Inner>,
}

/// 为新事务在 `counter_by_commit` 上注册一次引用，返回读到的 commit id 以及共享的 counter。
///
/// # 目标
///
/// 该函数需要在 `transaction_commit_id` 存在并发写入（`atomic_commit` 会 `fetch_add`）
/// 的前提下，保证：
/// 1. 返回的 `(commit, counter)` 满足：`counter` 就是 `counter_by_commit[commit]` 当前活跃的那一份；
/// 2. 不会误"复活"一个已经被打上墓碑、正等待被 `Transaction::drop` 从 map 中摘除的 counter；
/// 3. 与 GC 侧的 `earliest_active` 扫描配对，共同维护 GC 的安全不变式：
///    ```text
///        GC 读到 fallback > v ⟹ GC 一定读到 counter[v] >= 1
///    ```
///    从而 GC 不会把本事务提交时还要扫描的 `commit_queue` 记录当作过期数据回收。
///
/// # 与 GC 端的 Dekker 协议
///
/// 本函数是这个跨线程协议的"写侧半边"，GC 端在 `earliest_active` 中提供"读侧半边"：
///
/// ```text
///     TX (register_counter)                GC (earliest_active)
///     ─────────────────────                ────────────────────
///     A. load commit_id → v                X. load commit_id → fallback  (SC)
///     B. CAS counter[v]: 0 → 1  (Release)  Y. fence(SeqCst)  [F_gc]
///     C. fence(SeqCst)  [F_tx]             Z. load counter[v] (Acquire)
///     D. reload commit_id (必须仍 = v)
/// ```
///
/// 关键论证：若 GC 读到 `fallback > v`，则在 SC 全局序中：
/// 1. Committer 的 `commit_id: v → v+_` 排在 X 之前；
/// 2. 由 D 的稳定性检查（reload 仍见 v），Committer 的写又必然排在 D 之后，
///    因此也排在 `F_tx` 之后；
/// 3. `F_tx` 与 `F_gc` 一起把 B 的 CAS 钉在 Z 的 Acquire load 之前；
/// 4. Acquire load 因此一定看到 `counter[v] >= 1`。
///
/// `F_tx` 让本线程的 CAS 在 SC 全局序上排在 reload 之前，具体是否被 GC 观察到还依赖
/// GC 侧的 `F_gc` 把它的两次 load 也钉进 SC 全局序。两个 fence 缺一不可。
///
/// # 场景 1：注册在 fallback 推进之前完成
///
/// ```text
///     TX-A: load commit_id=5
///           CAS counter[5]=1
///           F_tx
///           reload commit_id=5 ✓ 返回
///     Cmt:                       commit_id: 5 → 6
///     GC:                                       load fallback=6
///                                               F_gc
///                                               load counter[5] → 1
/// ```
///
/// fallback=6 > TX-A 的快照 5；双 fence 把 TX-A 的 CAS 钉在 GC 的 counter load 之前，
/// GC 判定 5 号活跃，`oldest=5`，TX-A 需要扫描的 `> 5` 区间被完整保留。
///
/// # 场景 2：注册途中 commit_id 被抬高，reload 失败重试
///
/// ```text
///     TX-A: load commit_id=5
///           CAS counter[5]=1
///           F_tx
///     Cmt:  commit_id: 5 → 6
///     TX-A: reload commit_id=6 ≠ 5 ✗
///           → release_counter(counter[5])  // 撤销 +1
///           → 若归零，摘除 map[5]
///           → 回到 loop 头
///           load commit_id=6
///           CAS counter[6]=1
///           F_tx
///           reload commit_id=6 ✓ 返回
/// ```
///
/// 撤销时用 `Arc::ptr_eq` 校验 map 中 key=5 上仍是同一个 counter，
/// 防止误删已被后来者替换成新 counter 的同 key entry。
///
/// # 场景 3：GC 与注册并发交错，未完成的注册被兜底覆盖
///
/// ```text
///     TX-B: load commit_id=5
///           CAS counter[5]=1  ← 尚未执行
///     GC:                     load fallback=5
///                             F_gc
///                             load counter[5] → 0（slot 甚至可能不存在）
///                             ⇒ oldest=5，删 commit_queue 中 < 5 的记录
///     TX-B:                   继续 CAS、F_tx、reload=5 ✓ 返回
/// ```
///
/// TX-B 后续注册成功时快照仍为 5，其冲突扫描区间是 `> 5`；GC 删的是 `< 5` 的记录，
/// 与 TX-B 扫描区间无交集。只要 TX-B 最终能观察到"commit_id 在 5 稳定"，
/// GC 之前读到的 fallback 就不可能小于 5，兜底始终安全。
#[inline]
fn register_counter(
    map: &SkipMap<u64, Arc<AtomicU64>>,
    atomic: &AtomicU64,
) -> (u64, Arc<AtomicU64>) {
    loop {
        let v = atomic.load(Ordering::SeqCst);
        let counter = map.get_or_insert_with(v, || Arc::new(AtomicU64::new(0))).value().clone();
        // 在 counter 上 CAS +1；墓碑 slot 直接放弃，进入下一轮以拿到新的 counter
        let acquired = loop {
            let current = counter.load(Ordering::SeqCst);
            if current == COUNTER_TOMBSTONE {
                break false;
            }
            if counter
                .compare_exchange_weak(current, current+1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break true;
            }
        };
        if !acquired {
            continue;
        }
        // F_tx：与 `earliest_active` 里的 F_gc 配对，构成 Dekker 双 fence 协议。
        // 让 SC 全局序中形成 "CAS < reload" 的边，再配合 GC 侧的
        // "fallback load < counter load" 推出 "CAS < GC 的 counter load"，
        // Acquire load 因此一定看到 counter >= 1。
        fence(Ordering::SeqCst);
        let atomic_stable = atomic.load(Ordering::SeqCst) == v;
        if atomic_stable {
            return (v, counter);
        }
        // 期间 commit id 已经推进：撤销本次 +1；若因此归零则把墓碑 slot 从 map 摘除，
        // 用 `Arc::ptr_eq` 防止误删已经被替换成新 counter 的同 key entry。
        if release_counter(&counter) {
            if let Some(e) = map.get(&v) {
                if Arc::ptr_eq(e.value(), &counter) {
                    e.remove();
                }
            }
        }

    }
}

/// 事务退场时对共享 counter 执行 -1；返回 true 表示 counter 已被打上墓碑，
/// 由调用方负责把对应 entry 从 `counter_by_commit` 中摘除。
///
/// - `current > 1`：普通递减，counter 仍有其他活跃事务持有；
/// - `current == 1`：这是最后一个持有者，直接 CAS 到 `COUNTER_TOMBSTONE`，
///   墓碑保证后续再看到该 slot 的 `register_counter` 不会将其复活；
/// - CAS 失败则重试，保证 -1/墓碑化操作最终成功。
#[inline]
pub(crate) fn release_counter(counter: &AtomicU64) -> bool {
    loop {
        let current = counter.load(Ordering::SeqCst);
        if current > 1 {
            if counter
                .compare_exchange_weak(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return false;
            }
            continue;
        }
        debug_assert_eq!(current, 1);
        if counter
            .compare_exchange_weak(1, COUNTER_TOMBSTONE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
    }
}

impl TransactionInner {
    pub(crate) fn new(db: Arc<Inner>, write: bool) -> Self {
        let version = db.oracle.current_timestamp();
        // 通过 register_counter 同时读取快照起点 commit 并在 counter_by_commit 上 +1，
        // 保证本事务对 GC 立即可见——GC 不会在事务生命周期内把 `<= commit` 的
        // commit queue entry 误当作过期数据回收掉。
        let (commit, counter_commit) = register_counter(&db.counter_by_commit, &db.transaction_commit_id);

        Self {
            mode: IsolationLevel::SnapshotIsolation,
            done: false,
            write,
            commit,
            counter_commit,
            version,
            readset: HashSet::new(),
            readeset_bloom: Mutex::new(BloomFilter::new()),
            writeset: BTreeMap::new(),
            database: db,
        }
    }

    /// 获取该事务创建时的数据版本号
    pub fn version(&self) -> u64 {
        self.version
    }

    /// 获取该事务是否已关闭
    /// 已关闭的事务不能再进行写操作
    pub fn closed(&self) -> bool {
        self.done
    }

    pub fn cancel(&mut self) -> Result<(), Error> {
        if self.done {
            return Err(Error::TxClosed);
        }
        self.done = true;
        if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
            self.readset.pin().clear();
            self.readeset_bloom.lock().clear();
        }
        self.writeset.clear();
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        if self.done {
            return Err(Error::TxClosed);
        }
        self.done = true;
        if self.writeset.is_empty() {
            return Ok(());
        }
        let writeset = Arc::new(std::mem::take(&mut self.writeset));
        let mut  writeset_bloom = BloomFilter::new();
        for key in writeset.keys() {
            writeset_bloom.insert(key);
        }

        // 提取writeset中的最大key和最小key
        let max_key = writeset.keys().next_back().cloned().unwrap_or_default();
        let min_key = writeset.keys().next().cloned().unwrap_or_default();

        // 尝试将数据写入提交队列, 并返回事务提交ID和提交的结果
        let (version, entry) = self.auto_commit(Commit { 
            writeset: writeset.clone(),
            id: self.database.transaction_queue_id.fetch_add(1, Ordering::AcqRel) + 1, 
            writeset_bloom,
            max_key,
            min_key,
        });
        // 事务要求时快照隔离级别，也就是可重复读取
        if self.mode >= IsolationLevel::SnapshotIsolation {
            // 要确保不能有其他事务和当前事务修改了相同的键值对
            // 扫描从该事务创建时的提交ID + 1开始，到当前事务的提交ID结束，扫描所有其他事务修改的键值对，左闭右开，
            // 如果有其他事务修改了相同的键值对，就返回错误
            for tx in self.database.transaction_commit_queue.range(self.commit + 1 .. version) {
                if !tx.value().is_disjoint_writeset_bloom(&entry) {
                    self.database.transaction_commit_queue.remove(&version);
                    if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
                        self.readset.pin().clear();
                        self.readeset_bloom.lock().clear();
                    }
                    self.writeset.clear();
                    return Err(Error::KeyWriteConflict);
                }
                if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
                if !tx
                    .value()
                    .is_disjoint_readset_bloom(&self.readset, &self.readeset_bloom.lock())
                {
                    self.database.transaction_commit_queue.remove(&version);
                    
                    self.readset.pin().clear();
                    self.readeset_bloom.lock().clear();
                    self.writeset.clear();
                    return Err(Error::KeyReadConflict);
                }
            }
            }
        }
        // 将数据写入合并队列，返回合并数据的版本号和数据本身
        let (version, entry) = self.atomic_merge(Merge {
            writeset: writeset,
            id: self.database.transaction_merge_id.fetch_add(1, Ordering::AcqRel) + 1, 
        });
        // 将数据写入数据存储
        for (key, value) in entry.writeset.iter() {
            // 并发安全设计
            // 1: 两个commit 并发写入同一个不存在的key
            // 2: 单个commit 写入一个已经存在的key，但是中途遇到
            let value = value.clone();
            loop {
                let entry = 
                    self
                    .database
                    .datastore
                    .get_or_insert_with(key.clone(), || {
                        RwLock::new(Versions::from(Version {
                            version,
                            value: value.clone(),
                        }))
                    });
                let mut versions = 
                    entry
                    .value()
                    .write();
                if entry.is_removed() {
                    continue;
                }
                versions.push(Version { version, value });
                break;
            }
        }
        // 从合并队列中移除数据，此时数据已经写入数据存储，可以安全地移除，合并队列只存储待提交的数据
        self.database.transaction_merge_queue.remove(&version);
        if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
            self.readset.pin().clear();
            self.readeset_bloom.lock().clear();
        }
        // 清空事务的写操作键值对
        self.writeset.clear();
        Ok(())
    }

    /// 检查键是否存在
    /// 如果键存在，就返回true，否则返回false
    pub fn exists<K>(&self, key: K) -> Result<bool, Error>
    where 
        K: IntoBytes,
    {
        let lookup = key.as_slice();
        if self.done == true {
            return Err(Error::TxClosed);
        }

        let res = match self.write {
            // 如果事务是写事务，就从写操作键值对中检查键是否存在
            true => match self.writeset.get(lookup) {
                Some(_) => true,
                None => {
                    let res = self.exists_in_datastore(lookup, self.version);
                    if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
                        let guard = self.readset.pin();
                        if !guard.contains(lookup) {
                            guard.insert(lookup.into_bytes());
                            self.readeset_bloom.lock().insert(lookup);
                        }
                    }
                    res
                }
            },
            // 如果事务是读事务，就从数据存储中检查键是否存在
            false => self.exists_in_datastore(lookup, self.version),
        };
        Ok(res)
    }

    /// 获取键对应的值
    /// 如果键存在，就返回键对应的值，否则返回None
    pub fn get<K>(&self, key: K) -> Result<Option<Bytes>, Error>
    where 
        K: IntoBytes,
    {
        let lookup = key.as_slice();
        if self.done == true {
            return Err(Error::TxClosed);
        }
        let res = match self.write {
            // 如果事务是写事务，就从写操作键值对中获取数据
            true => match self.writeset.get(lookup) {
                Some(v) => v.clone(),
                None => {
                    let res = self.fetch_in_datastore(lookup, self.version);
                    if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
                        let guard = self.readset.pin();
                        if !guard.contains(lookup) {
                            guard.insert(lookup.into_bytes());
                            self.readeset_bloom.lock().insert(lookup);
                        }
                    }
                    res
                },
            },
            // 如果事务是读事务，就从数据存储中获取数据
            false => self.fetch_in_datastore(lookup, self.version),
        };
        Ok(res)
    }

    /// 设置键对应的值
    /// 如果键存在，就更新键对应的值，否则创建一个新的键值对
    pub fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
    where 
        K: IntoBytes,
        V: IntoBytes,
    {
        if self.done == true {
            return Err(Error::TxClosed);
        }
        if self.write == false {
            return Err(Error::TxNotWritable);
        }
        self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
        Ok(())
    }

    /// 插入键值对
    /// 如果键存在，就返回错误，否则创建一个新的键值对
    pub fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
    where 
        K: IntoBytes,
        V: IntoBytes,
    {
        let lookup = key.as_slice();
        if self.done == true {
            return Err(Error::TxClosed);
        }
        if self.write == false {
            return Err(Error::TxNotWritable);
        }
        match self.writeset.get(lookup) {
            Some(_) => return Err(Error::KeyAlreadyExists),
            None => match self.exists_in_datastore(lookup, self.version) {
                true => return Err(Error::KeyAlreadyExists),
                false => {
                    self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
                }
            },
        }
        Ok(())
    }

    /// 删除键值对
    /// 不检查键是否存在，直接删除
    pub fn del<K>(&mut self, key: K) -> Result<(), Error>
    where 
        K: IntoBytes,
    {
        if self.done == true {
            return Err(Error::TxClosed);
        }
        if self.write == false {
            return Err(Error::TxNotWritable);
        }
        self.writeset.insert(key.into_bytes(), None);
        Ok(())
    }

    /// ------------------------------------------------------ 辅助函数 ------------------------------------------------------
    /// 将数据写入提交队列，并返回事务提交ID和提交的结果
    /// 这里要考虑并发写入的情况，需要确保数据的原子性，确保每个事务的提交ID是唯一的
    #[inline(always)]
    pub fn auto_commit(&self, updates: Commit) -> (u64, Arc<Commit>) {
        let mut spins = 0;
        // 数据在事务队列中的队列ID
        let id = updates.id;
        let updates = Arc::new(updates);
        let queue = &self.database.transaction_commit_queue;
        loop {
            // 尝试获取一个提交版本号
            let version = self.database.transaction_commit_id.load(Ordering::Acquire) + 1;
            // 尝试将数据写入提交队列
            let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
            // 确认数据写入成功后，返回提交版本号和提交的结果
            if id == entry.value().id {
                // 更新当前db的提交版本号
                self.database.transaction_commit_id.fetch_add(1, Ordering::Release);
                return (version, Arc::clone(&updates));
            }
            // 如果数据写入失败，继续尝试
            backoff(spins);
            spins += 1;
        }
    }

    /// 将数据写入合并队列，并返回合并数据的版本号和数据本身
    /// 这里要考虑并发写入的情况，需要确保数据的原子性，确保每个事务的版本号是唯一的
    #[inline(always)]
    fn atomic_merge(&self, updates: Merge) -> (u64, Arc<Merge>) {
        let mut spins = 0;
        // 数据在合并队列中的队列ID
        let id = updates.id;
        let updates = Arc::new(updates);
        let oracle = self.database.oracle.clone();
        let queue = &self.database.transaction_merge_queue;
        loop {
            // 通过时间戳来获取数据的版本号，版本号是单调递增的
            let mut version = oracle.current_time_ns();
            // 拿到当前已经存在的版本号，确保新的版本号大于当前存在的版本号
            let last_ts = oracle.inner.timestamp.load(Ordering::Acquire);
            if version <= last_ts {
                version = last_ts + 1;
            }
            // 尝试将数据写入合并队列
            let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
            // 确认数据写入成功后，返回合并ID和合并的结果
            if id == entry.value().id {
                // 更新当前的时间戳
                oracle.inner.timestamp.fetch_max(version, Ordering::Release);
                return (version, Arc::clone(&updates));
            }
            // 如果数据写入失败，继续尝试
            backoff(spins);
            spins += 1;
        }
    }

    /// 检查键是否存在
    /// 如果键存在，就返回true，否则返回false
    #[inline(always)]
    fn exists_in_datastore<K>(&self, key: K, version: u64) -> bool
    where 
        K: IntoBytes,
    {
        let key = key.as_slice();
        // 遍历合并队列，检查键是否存在
        // 如果键存在，就返回true，否则返回false
        let iter = self.database.transaction_merge_queue.range(..=version);
        for entry in iter.rev() {
            if !entry.is_removed() {
                if let Some(v) = entry
                .value()
                .writeset
                .get(key)
                {
                    return v.is_some();
                }
            }
        }
        self
        .database
        .datastore
        .get(key)
        .map(|e| 
            match e.value().try_read() {
                Some(guard) => guard.exists_version(version),
                None => e
                        .value()
                        .read()
                        .exists_version(version),
        })
        .is_some_and(|v| v)
    }

    #[inline(always)]
    fn fetch_in_datastore<K>(&self, key: K, version: u64) -> Option<Bytes>
    where
        K: IntoBytes
    {
        let key = key.as_slice();
        let iter = self.database.transaction_merge_queue.range(..=version);
        for entry in iter.rev() {
            if !entry.is_removed() {
                if let Some(v) = entry
                .value()
                .writeset
                .get(key)
                {
                    return v.clone();
                }
            }
        }
        self
        .database
        .datastore
        .get(key)
        .and_then(|e| 
            match e.value().try_read() {
                Some(guard) => guard.fetch_version(version),
                None => e
                        .value()
                        .read()
                        .fetch_version(version),
        })
    }

}

/// 自适应退避：随竞争次数升级策略，避免无效 CPU 空转（乐观先快，悲观后让）
/// - spins < 10:   spin_loop hint — 发出 x86 PAUSE / ARM YIELD 指令，线程仍占用 CPU，但降低
///                 流水线投机执行压力，减少内存总线流量，让同核超线程（HyperThread）有机会推进
/// - spins < 100:  yield_now     — 主动让出当前时间片，OS 调度器可将 CPU 分配给持 slot 的线程，
///                 CPU 利用率不变但有效工作增加，避免纯自旋浪费整个核心
/// - spins >= 100: park_timeout  — 将线程挂起 10µs，CPU 完全释放给其他线程，消除活锁风险，
///                 代价是唤醒延迟（需等 OS 调度），适合高并发下竞争持续无法快速解决的情况
#[inline(always)]
fn backoff(spins: usize) {
    if spins < 10 {
        std::hint::spin_loop();
    } else {
        if spins < 100 {
            std::thread::yield_now();
        } else {
            std::thread::park_timeout(std::time::Duration::from_micros(10));
        }
    }
}