//! 通用 Bloom 过滤器实现
//!
//! # 概述
//!
//! Bloom 过滤器是一种**概率型**集合数据结构，用于快速判断一个元素是否**可能**存在于集合中。
//! 它有两个关键性质：
//!
//! - **不会漏判**（no false negative）：如果 `may_contain` 返回 `false`，则该 key 一定没插入过。
//! - **可能误判**（false positive）：如果 `may_contain` 返回 `true`，该 key 只是**有可能**插入过，
//!   并不保证一定存在。误判率随元素数量上升而上升。
//!
//! 代价与收益：
//!
//! - 插入 / 查询都是 O(k) 时间，k 为哈希函数个数（本实现 k = 3）。
//! - 空间占用极小（这里固定 512 字节），远小于一个 HashSet。
//!
//! # 应用场景
//!
//! 在本项目中，Bloom 过滤器常用于加速读路径：例如在某些历史版本或
//! 写集（writeset）上判断某个 key 是否**完全不可能**出现，从而避免
//! 对每条记录进行昂贵的版本链遍历。
//!
//! # 关键算法：Kirsch-Mitzenmacher 技巧
//!
//! 传统 Bloom 过滤器需要 k 个**真正独立**的哈希函数。本实现只计算 2 个哈希 (h1, h2)，
//! 然后用线性组合生成 k 个"伪独立"的哈希位置：
//!
//! ```text
//!     g_i(x) = h1(x) + i * h2(x)   (i = 0, 1, ..., k-1)
//! ```
//!
//! Kirsch & Mitzenmacher 在 2006 年的论文 *Less Hashing, Same Performance: Building a Better Bloom Filter*
//! 中证明：这种方式生成的哈希值，在误判率的渐近分析上与使用 k 个真正独立哈希的过滤器等价。
//! 优点：少计算 k − 2 次哈希，性能更优。
//!
//! 论文引用：
//! > Kirsch, A., & Mitzenmacher, M. (2006). *Less Hashing, Same Performance: Building a Better
//! > Bloom Filter*. IEEE/ACM Transactions on Networking.
//!
//! # 双哈希来源
//!
//! - **h1**：64 位 FNV-1a 非加密哈希（见 `hash` 函数注释）。
//! - **h2**：对 h1 做 mix finalizer 派生，来源于 Murmur/xxHash 系列的黄金比例混合技巧。
//!
//! h2 **不是独立计算**的哈希，而是从 h1 经高质量混合（黄金比例乘 + 循环左移）后得到。
//! 实践中这种组合足以保证 Kirsch-Mitzenmacher 公式的输出具有足够的独立性。

/// Bloom 过滤器的位图大小（比特数）。
/// 即 `bits` 数组中实际承载的 bit 数量。
const BLOOM_BITS: usize = 4096;

/// 实际分配的字节数。`4096 / 8 = 512`，即 `bits: [u8; 512]`。
const BLOOM_BYTES: usize = BLOOM_BITS / 8;

/// 每个 key 使用多少个哈希函数 / 落点位点（即 Kirsch-Mitzenmacher 公式中的 k）。
const BLOOM_HASHE_NUMS: u32 = 3;

/// Bloom 过滤器主体。
///
/// 内部用一个固定大小的字节数组作为位图，把这些字节视作 4096 个 bit
/// 每个 bit 都可以视作一个"bucket"。代码实现上：第 `bucket` 个字节里的第 `bit` 位
/// 通过 `self.bits[bucket / 8] |= 1 << (bucket % 8)` 来访问。
pub(crate) struct BloomFilter {
    /// 位图：512 字节 = 4096 bit。
    /// 初始全为 0；插入 key 时把对应位置的 bit 置 1；clear 时重新置 0。
    /// 注意：位图不做删除（不支持 `delete` 操作）。
    bits: [u8; BLOOM_BYTES],
    /// 已经插入过的 key 数量。仅用于 `is_empty` 判断；
    /// Bloom 过滤器本身不依赖此值做任何正确性保证。
    count: usize,
}

impl BloomFilter {
    /// 构造一个全新的、空的 Bloom 过滤器。
    pub fn new() -> Self {
        Self {
            bits: [0; BLOOM_BYTES],
            count: 0,
        }
    }

    /// 插入一个 key。
    ///
    /// 步骤：
    /// 1. 计算 key 的两个基础哈希 (h1, h2)，见 [`Self::hash`]。
    /// 2. 用 Kirsch-Mitzenmacher 公式派生 k=3 个落点位点 `g_i = h1 + i * h2`。
    /// 3. 把 3 个对应 bit 全部置 1（用 OR，不影响其它位）。
    /// 4. 计数 +1。
    ///
    /// 注意：Bloom 过滤器的插入是**幂等**的——重复插入同一个 key 不会出错。
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        let hashes = Self::hash(key);
        for i in 0..BLOOM_HASHE_NUMS {
            // 先把 64 bit 的派生哈希值限到 [0, BLOOM_BITS) 区间
            let hash = Self::nth_hash(hashes, i) % (BLOOM_BITS as u64);
            // 再把 0..BLOOM_BITS 的位置映射到位图里的字节和位
            let bucket = hash as usize / 8; // 哪个字节
            let bit = hash as usize % 8; // 该字节中的第几位
            self.bits[bucket] |= 1 << bit;
        }
        self.count += 1;
    }

    /// 判断 key 是否**可能**存在于过滤器中。
    ///
    /// - 返回 `false` ⇒ **一定**不在（无误判）。
    /// - 返回 `true` ⇒ **可能**在（存在小概率 false positive）。
    ///
    /// 实现策略：检查该 key 的所有 k 个落点位点，**任何一个位为 0 就立即返回 false**。
    /// 只有 k 个位**全部**为 1 时，才返回 true。
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let hashes = Self::hash(key);
        for i in 0..BLOOM_HASHE_NUMS {
            let hash = Self::nth_hash(hashes, i) % (BLOOM_BITS as u64);
            let bucket = hash as usize / 8;
            let bit = hash as usize % 8;
            if (self.bits[bucket] & (1 << bit)) == 0 {
                // 任何一个位为 0 ⇒ 该 key 不可能在集合中
                return false;
            }
        }
        true
    }

    /// 过滤器是否为空（没有插入过任何 key）。
    /// 仅基于插入计数 `count` 判断，简单可靠。
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// 清空过滤器，复位为初始状态。
    pub fn clear(&mut self) {
        self.bits = [0; BLOOM_BYTES];
        self.count = 0;
    }

    /// 对偶 FNV-1a 哈希 + Murmur 风格 mix finalizer
    ///
    /// 返回 `(h1, h2)`：
    ///
    /// ## h1：64 位 FNV-1a
    ///
    /// FNV（Fowler / Noll / Vo）是一种**非加密**的快速哈希。
    /// 论文 / 规范见 <http://www.isthe.com/chongo/tech/comp/fnv/>。
    ///
    /// 关键常数：
    /// - **offset basis**：0xcbf29ce484222325（= 14695981039346656037）
    /// - **prime**：0x100000001b3（= 1099511628211，FNV-1a 64 位素数）
    ///
    /// 算法对每个字节：先 XOR 后乘（注意："FNV-1a" 的 a 就是 alt / alternate 的意思，
    /// 表示 XOR 和 MUL 的顺序与 FNV-1 相反；FNV-1a 的扩散性更好）。
    ///
    /// 选择 FNV-1a 的理由：实现极简、常数固定、无状态、跨平台一致。
    /// 不适用于密码学场景，但作为 Bloom 过滤器 / 哈希表基础哈希足够好。
    ///
    /// ## h2：由 h1 派生的 mix finalizer
    ///
    /// ```text
    ///     h2 = (h1 * GOLDEN_RATIO).rotate_left(31)
    /// ```
    ///
    /// - GOLDEN_RATIO = 0x9e3779b97f4a7c15 ≈ 2^64 / φ（φ 为黄金比例）。
    ///   这是 **Murmur3** 与 **xxHash** 系列中广泛使用的 64 位混合常数，
    ///   理论基础：将 bit 与一个奇数相乘可以产生良好的 avalanche 效果。
    /// - `rotate_left(31)` 提供第二轮位扩散：
    ///   - 31 = 64 − 33 ≈ 大位移，让高低位彻底交叉。
    ///   - 哈希类的"finalizer"通常选 31 或 27 这种"大位移 + 小位移"的组合。
    ///
    /// h2 不是独立计算的哈希，而是从 h1 通过 mix 函数派生的。
    /// 配合 Kirsch-Mitzenmacher 公式使用时，输出位点的独立性与理论 k 个独立哈希相当。
    #[inline]
    fn hash(key: &[u8]) -> (u64, u64) {
        // FNV-1a 64-bit：从 offset basis 开始，对每个字节：xor 后乘 prime
        let mut h1: u64 = 0xcbf29ce484222325; // FNV-1a 64-bit offset basis
        for &byte in key {
            h1 ^= byte as u64;
            h1 = h1.wrapping_mul(0x100000001b3); // FNV-1a 64-bit prime
        }
        // Mix finalizer：用黄金比例乘 + rotate_left(31) 从 h1 派生 h2
        let h2 = h1.wrapping_mul(0x9e3779b97f4a7c15).rotate_left(31);
        (h1, h2)
    }

    /// Kirsch-Mitzenmacher 公式：派生第 `n` 个哈希值
    ///
    /// 论文给出的公式：
    ///
    /// ```text
    ///     g_n(x) = h1(x) + n * h2(x)
    /// ```
    ///
    /// 其中 `hashes.0` = h1(x)，`hashes.1` = h2(x)，`n` 就是第 i 个索引。
    /// 使用 `wrapping_*` 系列操作确保对任意 `n` 都不会溢出（u64 自然 wrap-around）。
    ///
    /// 这种线性组合构造出的 k 个哈希位的分布，与使用 k 个真正独立哈希函数的
    /// Bloom 过滤器在渐近意义下拥有相同的误判率。
    #[inline]
    fn nth_hash(hashes: (u64, u64), n: u32) -> u64 {
        hashes.0.wrapping_add((n as u64).wrapping_mul(hashes.1))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_contains_nothing() {
        let bf = BloomFilter::new();
        assert!(bf.is_empty());
        assert!(!bf.may_contain(b"hello"));
        assert!(!bf.may_contain(b"world"));
    }

    #[test]
    fn inserted_keys_are_found() {
        let mut bf = BloomFilter::new();
        bf.insert(b"hello");
        bf.insert(b"world");
        assert!(!bf.is_empty());
        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
    }

    #[test]
    fn missing_keys_usually_not_found() {
        let mut bf = BloomFilter::new();
        for i in 0..100u32 {
            bf.insert(&i.to_le_bytes());
        }
        let mut false_positives = 0;
        for i in 1000..2000u32 {
            if bf.may_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }
        assert!(false_positives < 100, "too many false positives: {}", false_positives);
    }

    #[test]
    fn clear_resets_filter() {
        let mut bf = BloomFilter::new();
        bf.insert(b"hello");
        assert!(bf.may_contain(b"hello"));
        bf.clear();
        assert!(bf.is_empty());
        assert!(!bf.may_contain(b"hello"));
    }
}
