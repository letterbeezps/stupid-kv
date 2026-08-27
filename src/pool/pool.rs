//! 事务对象池：复用已结束的 TransactionInner，降低频繁创建/销毁事务的开销。
//!
//! 工作流程：
//! 1. `Database::transaction()` 调用 `Pool::get()` 取出或新建一个 TransactionInner；
//! 2. 事务执行完毕（commit / cancel / drop）时，`Transaction::Drop` 将 inner 回收到池中；
//! 3. 下次 `Pool::get()` 优先从池中 pop，拿到后调用 `reset()` 重置状态，比从零构造更便宜。
//!
//! 池使用 crossbeam ArrayQueue（有界 MPSC），满了直接丢弃 push（`let _ =`），
//! 不会阻塞调用者。因此池是"软上限"——高并发下可能有部分事务不被复用，退化为新建。

use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::{inner::Inner, tx::{Transaction, TransactionInner}};

/// 池默认容量；高并发下 transaction 创建速率可匹配，低并发下几乎全命中。
pub(crate) const DEFAULT_POOL_SIZE: usize = 512;

/// 事务对象池。整个 Database 共享一个实例（Arc 持有），
/// 因此池的 get / put 必须线程安全——ArrayQueue 提供无锁 MPSC。
pub(crate) struct Pool {
    /// 共享数据库状态；新建 TransactionInner 时需要 Arc<Inner>。
    inner: Arc<Inner>,

    /// 空闲事务队列。有界，满时 push 静默丢弃。
    pool: ArrayQueue<TransactionInner>,
}

impl Pool {
    /// 构造一个新池，Database 持有 Arc 以便传给每个 Transaction。
    pub(crate) fn new(inner: Arc<Inner>, size: usize) -> Arc<Self> {
        Arc::new(Self {
            inner,
            pool: ArrayQueue::new(size),
        })
    }

    /// 将用完的 TransactionInner 放回池。`self` 必须是 &Arc<Self>——
    /// Transaction::Drop 里持有的就是 Arc<Pool>，直接传进来。
    /// ArrayQueue 满时 push 返回 Err，这里忽略，表示"不再复用、让它 drop"。
    pub(crate) fn put(self: &Arc<Self>, inner: TransactionInner) {
        let _ = self.pool.push(inner);
    }

    /// 从池中取一个事务：有则 reset 后返回，无则新建。
    ///
    /// `self: &Arc<Self>` 而非 `&self` 的原因：方法体最后要把 `Arc<Pool>`
    /// 本身 clone 到 Transaction.pool 字段里，让 Transaction::Drop 能把
    /// inner 回收到同一个池。如果签名只是 `&self`，`self` 的类型是 `&Pool`，
    /// 根本拿不到外部那个 Arc，也就没法 clone。
    pub(crate) fn get(self: &Arc<Self>, write: bool) -> Transaction {
        let inner = if let Some(mut tx) = self.pool.pop() {
            // 命中池：reset 重置所有字段（version / commit / counter / writeset / readset）
            tx.reset(write);
            tx
        } else {
            // 未命中：正常新建
            TransactionInner::new(self.inner.clone(), write)
        };
        Transaction {
            pool: self.clone(),
            inner: Some(inner),
        }
    }
}
