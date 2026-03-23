use bytes::Bytes;



pub trait IntoBytes {
    /// 将当前值转换为字节切片
    fn as_slice(&self) -> &[u8];
    /// 将当前值转换为字节序列
    fn into_bytes(self) -> Bytes;
}

impl IntoBytes for &[u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }

    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl IntoBytes for &str {
    fn as_slice(&self) -> &[u8] {
        self.as_bytes()
    }

    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}