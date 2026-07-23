//! 大事务 / 大 value 场景的集成测试。
//!
//! 这两个用例主要验证：
//! 1. `IntoBytes` 对 `String` / `Vec<u8>` 的实现能被正常调用（否则 `tx.set(String, ...)`
//!    与 `tx.set(_, Vec<u8>)` 都无法通过类型检查）；
//! 2. 单个事务写入上万键、或写入 1MB 级别的 value 时，commit / merge / datastore
//!    这条主链路不会因为体量放大而出现问题（例如 SmallVec 溢出、Bloom filter 误差等）。

use bytes::Bytes;
use stupid_kv::Database;



/// 单事务写入 10_000 个键，覆盖 writeset 排序、conflict-check bloom filter、
/// 以及 datastore 的批量插入。提交后随机抽样读取头/中/尾三个 key 验证正确性。
#[test]
fn transaction_with_thousands_of_keys() {
    let db = Database::new();

    let num_keys = 10_000;

    // 一次写事务批量插入所有 key。key 使用零填充字符串以保证字典序与数值序一致。
    let mut tx = db.transaction(true);
    for i in 0..num_keys {
        let key = format!("key_{:06}", i);
        let value = format!("value_{}", i);
        tx.set(key, value).unwrap();
    }
    tx.commit().unwrap();

    // 开启一个只读事务读取快照。抽样覆盖首、中、末三个位置。
    let tx = db.transaction(false);

    assert_eq!(tx.get("key_000000").unwrap(), Some(Bytes::from("value_0")));
	assert_eq!(tx.get("key_005000").unwrap(), Some(Bytes::from("value_5000")));
	assert_eq!(tx.get("key_009999").unwrap(), Some(Bytes::from("value_9999")));
}

/// 分别写入 1KB / 10KB / 100KB / 1MB 的 value，检查大 value 端到端可读、
/// 且长度与内容都不发生截断或损坏。
#[test]
fn large_value_handling() {
	let db = Database::new();

	// 递增的 value 尺寸：1KB / 10KB / 100KB / 1MB。
	let sizes = [1024, 10 * 1024, 100 * 1024, 1024 * 1024];

	let mut tx = db.transaction(true);
	for (i, size) in sizes.iter().enumerate() {
		let key = format!("large_key_{}", i);
		// 用 'x' 填充固定长度的 Vec<u8>，走 `IntoBytes for Vec<u8>` 的零拷贝路径。
		let value = vec![b'x'; *size];
		tx.set(key, value).unwrap();
	}
	tx.commit().unwrap();

	// 重新开启只读事务，验证每个 key 的 value 长度与内容都完全一致。
	let tx = db.transaction(false);
	for (i, expected_size) in sizes.iter().enumerate() {
		let key = format!("large_key_{}", i);
		let value = tx.get(&key).unwrap().expect("Value should exist");
		assert_eq!(value.len(), *expected_size, "Value {} should be {} bytes", i, expected_size);
		// 内容校验：所有字节都应为 'x'，任何位偏移都会被这个断言捕获。
		assert!(value.iter().all(|&b| b == b'x'), "All bytes should be 'x'");
	}
}

