use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use papaya::HashSet;
use tracing::debug;

use crate::{LOG_TARGET_CONFLICTS, bloom::BloomFilter};


pub struct Commit {
    /// 对应事务的提交队列ID，由 transaction_queue_id 来分配
    pub(crate) id: u64,
    /// 本次事务的写操作集
    pub(crate) writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
    /// 本次事务的写操作集的 Bloom 过滤器
    pub(crate) writeset_bloom: BloomFilter,
    /// 本次事务的写操作集的最大key
    pub(crate) max_key: Bytes,
    /// 本次事务的写操作集的最小key
    pub(crate) min_key: Bytes,
}

impl Commit {

    /// 通过 writeset_bloom 检查两个事务的写操作集是否有交集
    pub fn is_disjoint_writeset_bloom(&self, other: &Arc<Commit>) -> bool {
        if self.max_key < other.min_key || self.min_key > other.max_key {
            return true;
        }
        let mut maybe = false;
        for key in self.writeset.keys() {
            if other.writeset_bloom.may_contain(key) {
                maybe = true;
                break;
            }
        }
        // 如果 bloom 过滤器没有返回 true，就返回 true, 表示两个事务的写操作集没有交集
        if !maybe {
            return true;
        }
        self.is_disjoint_writeset(other)
    }

    /// 如果两个事务的写操作集没有交集，就返回 true
    pub fn is_disjoint_writeset(&self, other: &Arc<Commit>) -> bool {
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

    pub fn is_disjoint_readset_bloom(&self, other: &HashSet<Bytes>, bloom: &BloomFilter) -> bool {
        if bloom.is_empty() {
            return true;
        }
        let mut maybe = false;
        for key in self.writeset.keys() {
            if bloom.may_contain(key) {
                maybe = true;
                break;
            }
        }
        // 如果 bloom 过滤器没有返回 true，就返回 true, 表示两个事务的读操作集没有交集
        if !maybe {
            return true;
        }
        // 如果 bloom 过滤器返回 true，就继续检查两个事务的读操作集是否有交集
        self.is_disjoint_readset(other)
    }

    /// 检查其他事务的读操作集与当前事务的写操作集是否有交集，就返回 true
    pub fn is_disjoint_readset(&self, other: &HashSet<Bytes>) -> bool {
        let other = other.pin();
        if !other.is_empty() {
            // 最小化遍历次数来检查是否有交集，如果 other 长度更小，就遍历 other，否则遍历 self.writeset
            if other.len() < self.writeset.len() {
                for key in other.iter() {
                    if self.writeset.contains_key(key) {
                        #[cfg(debug_assertions)]
                        debug!(target: LOG_TARGET_CONFLICTS, "KeyReadConflict involving {:?}", key);
                        return false;
                    }
                }
            } else {
                for key in self.writeset.keys() {
                    if other.contains(key) {
                        #[cfg(debug_assertions)]
                        debug!(target: LOG_TARGET_CONFLICTS, "KeyReadConflict involving {:?}", key);
                        return false;
                    }
                }
            }
        }
        true
    }
}