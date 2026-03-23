use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use tracing::debug;

use crate::LOG_TARGET_CONFLICTS;


pub struct Commit {
    /// 对应事务的提交队列ID，由 transaction_queue_id 来分配
    pub(crate) id: u64,
    /// 本次事务的写操作集
    pub(crate) writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
}

impl Commit {
    /// 如果两个事务的写操作集没有交集，就返回 true
    pub fn is_disjoint_writeset(&self, other: &Commit) -> bool {
        // self.writeset.keys().all(|k| !other.writeset.contains_key(k))
        let mut a = self.writeset.keys();
        let mut b = other.writeset.keys();
        let mut next_a = a.next();
        let mut next_b = b.next();
        while let (Some(ka), Some(kb)) = (next_a, next_b) {
            match ka.cmp(kb) {
                Ordering::Less => next_a = a.next(),
                Ordering::Greater => next_b = b.next(),
                Ordering::Equal => {
                    #[cfg(debug_assertions)]
                    debug!(target: LOG_TARGET_CONFLICTS, "KeyWriteConflict involving {:?}", ka);
                    return false;
                }
            }
        }
        // 如果遍历完两个事务的写操作集，都没有发现交集，就返回 true
        true
    }
}