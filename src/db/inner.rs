use std::sync::{Arc, atomic::AtomicU64};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::{options::DatabaseOptions, oracle::Oracle, queue::{Commit, Merge}, versions::Versions};



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

    /// 底层数据存储，存储最终的键值对数据
    /// 每个键对应一个RwLock保护的Versions结构，用于实现MVCC版本管理
    pub(crate) datastore: SkipMap<Bytes, RwLock<Versions>>,
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
            datastore: SkipMap::new(),
        }
    }
}

impl Default for Inner {
    fn default() -> Self {
        Self::new(&&DatabaseOptions::default())
    }
}