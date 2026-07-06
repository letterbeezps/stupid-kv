
#[derive(PartialEq, PartialOrd)]
pub enum IsolationLevel {
    // 快照隔离级别
    SnapshotIsolation,
    // 可序列化快照隔离级别
    SerializableSnapshotIsolation,
}