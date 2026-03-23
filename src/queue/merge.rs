use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;


pub struct Merge {
    /// 对应事务的合并队列ID，由 transaction_merge_id 来分配
    pub(crate) id: u64,
    /// 本次事务的写操作集
    pub(crate) writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
}