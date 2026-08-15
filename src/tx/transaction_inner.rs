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
    /// 多个并发事务读到同一 commit id 时共享同一 counter；Drop 时 -1，归零则打墓碑并摘除 map entry。
    /// 只要它非零，commit queue GC 就不会回收 `< commit` 的 commit queue entry。
    pub(crate) counter_commit: Arc<AtomicU64>,
    /// 该事务创建时db的数据的当前版本号，由db::Inner::Oracle分配
    pub(crate) version: u64,
    /// 本事务在 `counter_by_oracle[version]` 上共享的引用计数。
    /// 结构与 `counter_commit` 对称，服务于 datastore 版本 GC：
    /// 只要它非零，`<= version` 的可见版本就必须保留，否则本事务的快照读会破损。
    pub(crate) counter_version: Arc<AtomicU64>,
    /// 该事务读取的键值对的键集合
    pub(crate) readset: HashSet<Bytes>,
    /// 该事务读取的键值对的键集合的布隆过滤器
    pub (crate) readeset_bloom: Mutex<BloomFilter>,
    /// 该事务的写操作键值对，键为键，值为值
    pub(crate) writeset: BTreeMap<Bytes, Option<Bytes>>,
    /// 该事务的数据库实例
    pub(crate) database: Arc<Inner>,
}

/// 事务侧向 GC 登记快照：读 atomic → 在 `map[v]` 上 CAS +1 → 返回 `(v, counter)`。
/// 被两条 GC 协议共用：
/// - `counter_by_commit` / `transaction_commit_id`：commit queue GC；
/// - `counter_by_oracle` / `oracle.timestamp`：datastore 版本 GC（需传 `gc_floor`）。
///
/// 与 GC 侧 `earliest_active` 一起维护不变式：
///
/// ```text
///     对任何注册成功、快照为 v 的活跃事务 TX，
///     若 GC 读到 fallback > v ⟹ GC 必然读到 counter[v] >= 1。
/// ```
///
/// 实现要点：
/// 1. **CAS 后插 `F_tx` 再 reload atomic**：与 GC 侧的 `F_gc` 配对形成 Dekker 双 fence 协议，
///    把 TX 的 CAS 钉在 GC 的 counter load 之前。
/// 2. **`gc_floor` 事前检查**（仅 version GC 场景）：若 GC 已把 v 判死，主动 rollback 重试。
///    commit queue GC 不存在"GC 决定回收某 commit 后新事务恰好拿到它"的问题，传 `None`。
/// 3. **墓碑处理**：见到 `COUNTER_TOMBSTONE` 的 slot 直接放弃、下一轮循环重取——
///    该 slot 正在退场，在其上 +1 会破坏不变式。
/// 4. **rollback 时的 remove 用 `Arc::ptr_eq` 校验**：防止误删已被替换成新 counter 的同 key entry。
///
/// 完整证明、并发场景演示、失败模式见 `docs/004_commit_queue_gc.md`
/// 和 `docs/005_version_history_gc.md`。
#[inline]
fn register_counter(
    map: &SkipMap<u64, Arc<AtomicU64>>,
    atomic: &AtomicU64,
    gc_floor: Option<&AtomicU64>
) -> (u64, Arc<AtomicU64>) {
    loop {
        let v = atomic.load(Ordering::SeqCst);
        let counter = map.get_or_insert_with(v, || Arc::new(AtomicU64::new(0))).value().clone();
        // 在 counter 上 CAS +1；见到墓碑 slot 直接放弃，下一轮拿新的 counter
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
        // F_tx：与 `earliest_active` 里的 F_gc 配对，构成 Dekker 双 fence 协议
        fence(Ordering::SeqCst);
        let atomic_stable = atomic.load(Ordering::SeqCst) == v;
        // gc_floor 检查仅在 version GC 场景启用（commit queue GC 传 None）
        let floor_ok = match gc_floor {
            Some(floor) => floor.load(Ordering::SeqCst) <= v,
            None => true,
        };
        if atomic_stable && floor_ok {
            return (v, counter);
        }

        // atomic 已推进 或 v 已被 GC 判死：撤销 +1，归零则从 map 摘除。
        // Arc::ptr_eq 防止误删已被替换成新 counter 的同 key entry。
        if release_counter(&counter) {
            if let Some(e) = map.get(&v) {
                if Arc::ptr_eq(e.value(), &counter) {
                    e.remove();
                }
            }
        }
    }
}

/// 对共享 counter 执行 -1；返回 true 表示归零并已打墓碑，调用方负责从 map 摘除对应 entry。
/// - `> 1`：普通递减；
/// - `== 1`：最后一个持有者，CAS 到 `COUNTER_TOMBSTONE`，阻止 `register_counter` 复活；
/// - CAS 失败重试直至成功。
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
        // 在 counter_by_oracle 上 +1 登记本事务的快照 version，供 datastore 版本 GC 保护。
        // 传入 &db.gc_floor：若 GC 已把 v 判死，register_counter 会内部重试拿更新的时间戳。
        let (version, counter_version) = register_counter(&db.counter_by_oracle, &db.oracle.inner.timestamp, Some(&db.gc_floor));
        // 在 counter_by_commit 上 +1 登记本事务的快照 commit，供 commit queue GC 保护。
        // commit queue GC 不需要 gc_floor 事前检查，传 None。
        let (commit, counter_commit) = register_counter(&db.counter_by_commit, &db.transaction_commit_id, None);

        Self {
            mode: IsolationLevel::SnapshotIsolation,
            done: false,
            write,
            commit,
            counter_commit,
            version,
            counter_version,
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
            // 3: 与版本 GC 摘除同 key entry 的竞争
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
                // 与版本 GC 的握手协议：`Inner::run_gc_full` / `gc_key` 摘除空版本链
                // 的 entry 时必须持有此 `versions` 写锁，因此这里拿到锁之后再检查
                // `is_removed()` 就能可靠判断该 entry 是否已经被 GC 判死；若已判死，
                // 重试从 `get_or_insert_with` 开始，拿到新 entry 后再写入，
                // 避免把数据写入即将从 datastore 中被摘除的节点、丢失本次提交。
                if entry.is_removed() {
                    continue;
                }
                versions.push(Version { version, value });
                break;
            }
            // 把写入 key 推入版本 GC 的增量脏队列，供 `run_gc_dirty_inner` 消费
            self.database.gc_dirty_keys.push(key.clone());
        }

        // ---- AOL 持久化阶段 ----
        // 数据已成功写入 datastore + 合并队列。现在将写集追加到 AOL 文件。
        // 这是持久化的关键步骤：保证即使 datastore 在后续步骤中丢失（如进程崩溃），
        // 数据也能从 AOL 日志中恢复。
        if let Some(p) = self.database.persistence.read().clone() {
            if let Err(e) = p.append(version, &entry.writeset) {
                // AOL 写入失败——需要回滚已完成的内存状态：
                // 1. 从合并队列移除 version（数据已进入 datastore，但合并队列中的
                //    条目需要在 commit 时被正常清理，否则会残留脏数据）
                // 2. 清空 readset/readset_bloom（SSI 模式下的读集合）
                // 3. 清空 writeset（事务状态重置）
                self.database.transaction_merge_queue.remove(&version);
                if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
                    self.readset.pin().clear();
                    self.readeset_bloom.lock().clear();
                }
                self.writeset.clear();
                return Err(Error::TxCommitNotPersisted(e));
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