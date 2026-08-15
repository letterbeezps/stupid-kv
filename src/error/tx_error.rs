use thiserror::Error;

use crate::error::PersistenceError;

#[derive(Debug, Error)]
pub enum Error {
    /// 事务已经关闭
    #[error("transaction is closed")]
    TxClosed,

    #[error("Write conflict, retry the transaction")]
    KeyWriteConflict,

    #[error("Read conflict, retry the transaction")]
    KeyReadConflict,

    #[error("Transaction is not writable")]
    TxNotWritable,

    #[error("Key already exists, cannot be overwritten")]
    KeyAlreadyExists,

    /// 事务提交的 AOL 持久化失败。
    ///
    /// 当 `TransactionInner::auto_commit` 在完成数据写入 datastore 后，
    /// 尝试将写集追加到 AOL 文件时发生错误（如 Mutex 获取失败、IO 错误等），
    /// 此时事务会回滚已完成的内存操作并返回此错误。
    /// 内部包含具体的 `PersistenceError`，便于上层定位根因。
    #[error("Transaction commit AOL persistence failed: {0}")]
	TxCommitNotPersisted(PersistenceError),
}