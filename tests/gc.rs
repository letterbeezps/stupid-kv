//! GC 集成测试：覆盖 datastore 版本 GC（`run_gc`）与 commit queue GC（`run_cleanup`）
//! 的手动 / 后台两条路径。通过 `with_gc_interval` / `with_cleanup_interval` 把后台线程
//! 周期拉长到 1h（等效关闭），来精准验证手动入口；再用 50~100ms 的短周期验证
//! 后台线程自动回收行为。

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use stupid_kv::{Database, DatabaseOptions};


/// 手动 GC：同一 key 反复写 10 版后回收旧版本，读路径仍应返回最新 v9。
#[test]
fn manual_gc_reove_stale_versions() {
    let db = Database::new_with_options(
        DatabaseOptions::default()
                .with_gc_interval(Duration::from_secs(3600))
                .with_cleanup_interval(Duration::from_secs(3600))
    );

    for i in 0..10 {
        let mut tx = db.transaction(true);
        tx.set("key", format!("v{}", i)).unwrap();
        tx.commit().unwrap();
    }

    db.run_gc();

    let mut tx = db.transaction(false);
	let current = tx.get("key").unwrap();
	assert!(current.is_some(), "Current value should exist after GC");
	assert_eq!(current.unwrap().as_ref(), b"v9", "Should have latest value");
	tx.cancel().unwrap();
}

/// 活跃只读事务持有旧 snapshot 时，GC 不得回收其可见的版本。
/// 验证 `compute_cleanup_ts` 的水位受活跃事务下界约束。
#[test]
fn manual_gc_respects_active_transactions() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_secs(3600)),
	);

	// Create initial version
	let mut tx = db.transaction(true);
	tx.set("key", "v1").unwrap();
	tx.commit().unwrap();

	// Start long-lived read transaction
	let read_tx = db.transaction(false);
	let initial = read_tx.get("key").unwrap();
	assert_eq!(initial, Some(Bytes::from("v1")));

	// Update multiple times
	for i in 2..10 {
		let mut tx = db.transaction(true);
		tx.set("key", format!("v{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Run manual GC
	db.run_gc();

	// Active read transaction should still see original value
	let after_gc = read_tx.get("key").unwrap();
	assert_eq!(
		after_gc,
		Some(Bytes::from("v1")),
		"Active transaction should still see v1 after GC"
	);
}


/// 删除后 GC：tombstone 及其之前的所有版本都应被清空，`get` 返回 `None`。
#[test]
fn manual_gc_removes_deleted_keys() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_secs(3600)),
	);

	// Create and delete key
	let mut tx = db.transaction(true);
	tx.set("deleted_key", "value").unwrap();
	tx.commit().unwrap();

	let mut tx = db.transaction(true);
	tx.del("deleted_key").unwrap();
	tx.commit().unwrap();

	// Wait a tiny bit to ensure delete timestamp is old enough
	std::thread::sleep(Duration::from_millis(10));

	// Run manual GC
	db.run_gc();

	// Key should not exist
	let mut tx = db.transaction(false);
	assert!(tx.get("deleted_key").unwrap().is_none());
	tx.cancel().unwrap();
}

/// 后台 GC：100ms 周期下写 20 版，等一次 GC 触发后最新版本仍可读。
/// 仅冒烟验证后台线程真的被拉起并有回收动作。
#[test]
fn background_gc_runs_automatically() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(100))
			.with_cleanup_interval(Duration::from_millis(100)),
	);

	// Create many versions quickly
	for i in 0..20 {
		let mut tx = db.transaction(true);
		tx.set("gc_key", format!("value_{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Wait for background GC to run
	std::thread::sleep(Duration::from_millis(500));

	// Current value should still be available
	let mut tx = db.transaction(false);
	let val = tx.get("gc_key").unwrap();
	assert!(val.is_some());
	tx.cancel().unwrap();
}

/// 多 key × 多版本：10 个 key 各写 5 版，GC 后每个 key 都保留最新版。
/// 验证增量 GC 的 dirty queue 能正确覆盖多 key 场景。
#[test]
fn gc_handles_multiple_keys() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(100))
			.with_cleanup_interval(Duration::from_millis(100)),
	);

	// Create multiple keys with multiple versions
	for key_id in 0..10 {
		for version in 0..5 {
			let mut tx = db.transaction(true);
			tx.set(format!("key_{}", key_id), format!("v{}", version)).unwrap();
			tx.commit().unwrap();
		}
	}

	// Wait for GC
	std::thread::sleep(Duration::from_millis(300));

	// All keys should still have their latest values
	let mut tx = db.transaction(false);
	for key_id in 0..10 {
		let val = tx.get(format!("key_{}", key_id)).unwrap();
		assert_eq!(val, Some(Bytes::from("v4")), "Key {} should have latest value", key_id);
	}
	tx.cancel().unwrap();
}

/// 保留 + 删除混合：GC 后保留 key 仍在、删除 key 彻底消失。
#[test]
fn gc_with_mixed_deletes() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	);

	// Create some keys
	let mut tx = db.transaction(true);
	tx.set("keep1", "value1").unwrap();
	tx.set("keep2", "value2").unwrap();
	tx.set("delete1", "value").unwrap();
	tx.set("delete2", "value").unwrap();
	tx.commit().unwrap();

	// Delete some
	let mut tx = db.transaction(true);
	tx.del("delete1").unwrap();
	tx.del("delete2").unwrap();
	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Verify state
	let mut tx = db.transaction(false);
	assert_eq!(tx.get("keep1").unwrap(), Some(Bytes::from("value1")));
	assert_eq!(tx.get("keep2").unwrap(), Some(Bytes::from("value2")));
	assert!(tx.get("delete1").unwrap().is_none());
	assert!(tx.get("delete2").unwrap().is_none());
	tx.cancel().unwrap();
}


/// 后台 commit queue GC：短周期下 50 次提交后，`transaction_commit_queue`
/// 应被自动回收，但已提交的 key 仍能被新事务读到。
#[test]
fn transaction_cleanup_runs_automatically() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_millis(100)),
	);

	// Create and complete many transactions
	for i in 0..50 {
		let mut tx = db.transaction(true);
		tx.set(format!("key_{}", i), "value").unwrap();
		tx.commit().unwrap();
	}

	// Wait for cleanup to run
	std::thread::sleep(Duration::from_millis(300));

	// New transactions should work fine
	let mut tx = db.transaction(false);
	for i in 0..50 {
		assert!(tx.exists(format!("key_{}", i)).unwrap());
	}
	tx.cancel().unwrap();
}

/// 手动 `run_cleanup`：关闭后台线程后，主动触发 commit queue 回收。
#[test]
fn manual_cleanup() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_secs(3600)), // Disable auto cleanup
	);

	// Create transactions
	for i in 0..20 {
		let mut tx = db.transaction(true);
		tx.set(format!("key_{}", i), "value").unwrap();
		tx.commit().unwrap();
	}

	// Run manual cleanup
	db.run_cleanup();

	// Should still work
	let mut tx = db.transaction(false);
	for i in 0..20 {
		assert!(tx.exists(format!("key_{}", i)).unwrap());
	}
	tx.cancel().unwrap();
}

// =============================================================================
// Concurrent GC Tests
// =============================================================================

/// 并发压测：4 线程 × 50 op（读写混合）与后台 GC 同时跑，
/// 结束后所有 key 都应存在 —— GC 不能误删活跃写入。
#[test]
fn concurrent_gc_and_transactions() {
	let db = Arc::new(Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	));

	let num_threads = 4;
	let ops_per_thread = 50;

	let handles: Vec<_> = (0..num_threads)
		.map(|thread_id| {
			let db = Arc::clone(&db);

			std::thread::spawn(move || {
				for op in 0..ops_per_thread {
					let key = format!("thread_{}_key_{}", thread_id, op % 10);

					// Write
					let mut tx = db.transaction(true);
					tx.set(&key, format!("value_{}_{}", thread_id, op)).unwrap();
					let _ = tx.commit();

					// Read
					let tx = db.transaction(false);
					let _ = tx.get(&key);
				}
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}

	// Verify integrity
	let mut tx = db.transaction(false);
	for thread_id in 0..num_threads {
		for key_id in 0..10 {
			let key = format!("thread_{}_key_{}", thread_id, key_id);
			let val = tx.get(&key).unwrap();
			assert!(val.is_some(), "Key {} should exist", key);
		}
	}
	tx.cancel().unwrap();
}

/// 全删除场景：所有 key 都变成 tombstone，GC 后 datastore entry 应彻底消失。
/// 覆盖 `run_gc_full` 对纯 tombstone 僵尸 entry 的清理路径。
#[test]
fn gc_with_only_deleted_keys() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	);

	// Create and immediately delete
	let mut tx = db.transaction(true);
	for i in 0..10 {
		tx.set(format!("temp_{}", i), "value").unwrap();
	}
	tx.commit().unwrap();

	let mut tx = db.transaction(true);
	for i in 0..10 {
		tx.del(format!("temp_{}", i)).unwrap();
	}
	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// All should be gone
	let mut tx = db.transaction(false);
	for i in 0..10 {
		assert!(tx.get(format!("temp_{}", i)).unwrap().is_none());
	}
	tx.cancel().unwrap();
}

/// 空库 GC 边界：无任何数据时 `run_gc` / `run_cleanup` 不应 panic，
/// 之后仍可正常读写。
#[test]
fn gc_empty_database() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	);

	// Run GC on empty database
	db.run_gc();
	db.run_cleanup();

	// Should work fine
	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("value")));
	tx.cancel().unwrap();
}

/// tombstone 与活跃 reader：删除前建立的只读事务应看到旧值（受 cleanup_ts 保护），
/// 之后新起的事务看到已删除。验证 tombstone 在 snapshot 语义下的可见性隔离。
#[test]
fn gc_preserves_tombstones_for_active_readers() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	);

	// Create and then delete a key
	let mut tx = db.transaction(true);
	tx.set("key", "original").unwrap();
	tx.commit().unwrap();

	// Start a read that sees the key
	let read_tx = db.transaction(false);
	assert_eq!(read_tx.get("key").unwrap(), Some(Bytes::from("original")));

	// Delete the key
	let mut tx = db.transaction(true);
	tx.del("key").unwrap();
	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Old reader should still see original value
	assert_eq!(
		read_tx.get("key").unwrap(),
		Some(Bytes::from("original")),
		"Reader should see value from its snapshot"
	);

	// New reader should see deletion
	let new_tx = db.transaction(false);
	assert!(new_tx.get("key").unwrap().is_none());
}
