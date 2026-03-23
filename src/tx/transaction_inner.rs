use std::{ collections::BTreeMap, sync::{Arc, atomic::Ordering}};

use bytes::Bytes;
use parking_lot::lock_api::RwLock;

use crate::{db::inner::Inner, error::Error, kv::IntoBytes, queue::{Commit, Merge}, tx::IsolationLevel, versions::{Version, Versions}};


pub(crate) struct TransactionInner {
    /// 事务隔离级别
    pub(crate) mode: IsolationLevel,
    /// 事务是否已完成
    pub(crate) done: bool,
    /// 事务是否是写事务
    pub(crate) write: bool,
    /// 该事务创建时db的commit ID，由db::Inner::transaction_commit_id
    pub(crate) commit: u64,
    /// 该事务创建时db的数据的当前版本号，由db::Inner::Oracle分配
    pub(crate) version: u64,
    /// 该事务的写操作键值对，键为键，值为值
    pub(crate) writeset: BTreeMap<Bytes, Option<Bytes>>,
    /// 该事务的数据库实例
    pub(crate) database: Arc<Inner>,
}

impl TransactionInner {
    pub(crate) fn new(db: Arc<Inner>, write: bool) -> Self {
        let version = db.oracle.current_timestamp();
        let commit = db.transaction_commit_id.load(Ordering::Relaxed);
        Self {
            mode: IsolationLevel::SnapshotIsolation,
            done: false,
            write,
            commit,
            version,
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
        // 尝试将数据写入提交队列, 并返回事务提交ID和提交的结果
        let (version, entry) = self.auto_commit(Commit { 
            writeset: writeset.clone(),
            id: self.database.transaction_queue_id.fetch_add(1, Ordering::AcqRel) + 1, 
        });
        // 事务要求时快照隔离级别，也就是可重复读取
        if self.mode >= IsolationLevel::SnapshotIsolation {
            // 要确保不能有其他事务和当前事务修改了相同的键值对
            // 扫描从该事务创建时的提交ID + 1开始，到当前事务的提交ID结束，扫描所有其他事务修改的键值对，左闭右开，
            // 如果有其他事务修改了相同的键值对，就返回错误
            for tx in self.database.transaction_commit_queue.range(self.commit + 1 .. version) {
                if !tx.value().is_disjoint_writeset(&entry) {
                    self.database.transaction_commit_queue.remove(&version);
                    self.writeset.clear();
                    return Err(Error::KeyWriteConflict);
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
            let value = value.clone();
            // 如果键存在，就将新的版本号和值写入版本列表
            if let Some(entry) = self.database.datastore.get(key) {
                let mut versions = entry.value().write();
                versions.push(Version {
                    version,
                    value,
                });
            } else {
            // 如果键不存在，就创建一个新的版本列表
                self.database.datastore.insert(
                    key.clone(), 
                    RwLock::new(Versions::from(Version {
                        version,
                        value,
                    })),
                );
            }
        }
        // 从合并队列中移除数据，此时数据已经写入数据存储，可以安全地移除，合并队列只存储待提交的数据
        self.database.transaction_merge_queue.remove(&version);
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
                None => self.exists_in_datastore(lookup, self.version),
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
        }
    }

    /// 将数据写入合并队列，并返回合并数据的版本号和数据本身
    /// 这里要考虑并发写入的情况，需要确保数据的原子性，确保每个事务的版本号是唯一的
    #[inline(always)]
    fn atomic_merge(&self, updates: Merge) -> (u64, Arc<Merge>) {
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