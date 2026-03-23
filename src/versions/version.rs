use std::cmp::Ordering;

use bytes::Bytes;

#[derive(Clone, Eq, PartialEq)]
pub struct Version {
    /// 唯一版本号，由全局时钟生成
    pub(crate) version: u64,
    /// 实际存储的数据
    pub(crate) value: Option<Bytes>
}

impl Ord for Version {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.version.cmp(&other.version)
    }
}

impl PartialOrd for Version {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}