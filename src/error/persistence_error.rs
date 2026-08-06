use thiserror::Error;
use std::io::Error as IoError;
use bincode::error::EncodeError as BincodeEncodeError;
use bincode::error::DecodeError as BincodeDecodeError;

/// 持久化子系统错误分类。三分类分别对应快照生命周期的三个阶段：
///
/// | 变体 | 阶段 | 典型根因 |
/// |------|------|---------|
/// | `Io` | 文件系统操作（create/open/rename/remove/sync_all/metadata） | 磁盘满、权限不足、跨文件系统 EXDEV、父目录不存在 |
/// | `Serialization` | 编码阶段（`encode_into_std_write` 内部） | bincode 序列化 limit（当前配置无 limit 不触发）、底层 BufWriter 刷盘时的 IO 故障 |
/// | `Deserialization` | 解码阶段（`decode_from_std_read`，**非 UnexpectedEof**） | 快照文件损坏 / 截断、跨架构 native 字节序不兼容、bincode 主版本升级 breaking change |
#[derive(Debug, Error)]
pub enum PersistenceError {

    /// 文件系统级 IO 错误。通过 `#[from]` 自动把 `std::io::Error` 包进来，
    /// `snapshot()` / `load()` 内部直接 `?` 就能完成错误转换。
    #[error("IO error: {0}")]
    Io(#[from] IoError),

    /// bincode 编码失败。发生在 `snapshot()` 将 (key, versions) 写入 BufWriter 期间。
    #[error("Serialization error: {0}")]
    Serialization(#[from] BincodeEncodeError),

    /// bincode 解码失败（**不包含**正常 EOF 的 `UnexpectedEof` 包装——那种被 `load()` 当作文件结束判 break）。
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] BincodeDecodeError),
}
