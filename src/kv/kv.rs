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

/// `String` 拥有所有权，可直接消费其底层 `Vec<u8>` 转成 `Bytes`，避免额外拷贝。
impl IntoBytes for String {
    fn as_slice(&self) -> &[u8] {
        self.as_bytes()
    }

    fn into_bytes(self) -> Bytes {
        // `String::into_bytes` 直接归还底层 buffer，`Bytes::from` 零拷贝接管。
        Bytes::from(self.into_bytes())
    }
}

/// 借用引用无法夺走所有权，只能拷贝底层字节。
impl IntoBytes for &String {
    fn as_slice(&self) -> &[u8] {
        self.as_bytes()
    }

    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

/// 已经是字节序列的 `Vec<u8>`，同样可零拷贝转成 `Bytes`；
/// 覆盖此实现主要是为了让写入超大 value（如测试中的 1MB 数据）时避免多余的 memcpy。
impl IntoBytes for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        // 注意：这里的 `self.as_slice()` 调用的是 `Vec<u8>` 的 inherent 方法，
        // 而非 trait 的同名方法，因此不会递归。
        self.as_slice()
    }

    fn into_bytes(self) -> Bytes {
        Bytes::from(self)
    }
}