use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    /// 事务已经关闭
    #[error("transaction is closed")]
    TxClosed,

    #[error("Write conflict, retry the transaction")]
    KeyWriteConflict,

    #[error("Transaction is not writable")]
    TxNotWritable,

    #[error("Key already exists, cannot be overwritten")]
    KeyAlreadyExists,
}