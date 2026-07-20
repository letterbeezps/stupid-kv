use bytes::Bytes;
use stupid_kv::Database;


// =============================================================================
// Snapshot Isolation Tests
// =============================================================================
#[test]
fn snapshot_isolation_read_sees_consistent_snapshot() {
    let db = Database::new();

    let mut tx = db.transaction(true);
    tx.set("key1", "value1").unwrap();
    tx.set("key2", "value2").unwrap();
    tx.commit().unwrap();


    let mut read_tx = db.transaction(false);

    let mut write_tx = db.transaction(true);
    write_tx.set("key1", "modify1").unwrap();
    write_tx.set("key2", "modify2").unwrap();
    write_tx.commit().unwrap();

    assert_eq!(
        read_tx.get("key1").unwrap(),
        Some(Bytes::from("value1")),
        "SI read shoudl see original value for key1"
    );
    assert_eq!(
		read_tx.get("key2").unwrap(),
		Some(Bytes::from("value2")),
		"SI read should see original value for key2"
	);

    read_tx.cancel().unwrap();

	// New transaction should see updated values
	let mut new_tx = db.transaction(false);
	assert_eq!(
		new_tx.get("key1").unwrap(),
		Some(Bytes::from("modify1")),
		"New transaction should see modified value"
	);
	new_tx.cancel().unwrap();
}

#[test]
fn snapshot_isolation_allows_concurrent_writes_to_different_keys() {
    let db = Database::new();

    let mut tx = db.transaction(true);
    tx.set("key1", "value1").unwrap();
    tx.set("key2", "value2").unwrap();
    tx.commit().unwrap();

    let mut tx1 = db.transaction(true);
    let mut tx2 = db.transaction(true);

	tx1.set("key1", "tx1_value").unwrap();

	tx2.set("key2", "tx2_value").unwrap();

	// Both should commit successfully with SI (no conflict on different keys)
	assert!(tx1.commit().is_ok(), "tx1 should commit successfully");
	assert!(tx2.commit().is_ok(), "tx2 should commit successfully (different key)");

    let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key1").unwrap(), Some(Bytes::from("tx1_value")));
	assert_eq!(verify_tx.get("key2").unwrap(), Some(Bytes::from("tx2_value")));
	verify_tx.cancel().unwrap();
}

// =============================================================================
// Serializable Snapshot Isolation (SSI) Tests
// =============================================================================

#[test]
fn ssi_detects_write_write_confict_on_same_key() {

    let db = Database::new();

    let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
    let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

    tx1.set("key", "value1").unwrap();
	tx2.set("key", "value2").unwrap();

	// First commit succeeds
	assert!(tx1.commit().is_ok(), "First committer should succeed");

	// Second commit should fail due to write-write conflict
	assert!(tx2.commit().is_err(), "Second committer should fail due to write conflict");

    let mut verify_tx = db.transaction(false);
	assert_eq!(
		verify_tx.get("key").unwrap(),
		Some(Bytes::from("value1")),
		"Value should be from first committed transaction"
	);
	verify_tx.cancel().unwrap();

}

#[test]
fn ssi_detects_read_write_conflict() {
    let db = Database::new();

    let mut tx = db.transaction(true);
    tx.set("key1", "value1").unwrap();
    tx.commit().unwrap();

    let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
    let a = tx1.get("key1").unwrap();
    assert_eq!(a,Some(Bytes::from("value1")));

    let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("key1", "modified").unwrap();
	assert!(tx2.commit().is_ok(), "tx2 should commit first");

    tx1.set("other_key", "value").unwrap();

	// 1. tx1 观察key1
    // 2. tx1 根据观察key1的结果决定修改other_key
    // 3. ssi 隔离级别组织该操作，因为key1 被tx2修改了
	assert!(tx1.commit().is_err(), "tx1 should fail due to read-write conflict");
}

#[test]
fn ssi_isoloation_concurrent_writes_to_disjoint_keys() {
    let db = Database::new();

    let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

    tx1.set("key_a", "value_a").unwrap();
	tx2.set("key_b", "value_b").unwrap();

    assert!(tx1.commit().is_ok(), "tx1 should succeed");
	assert!(tx2.commit().is_ok(), "tx2 should succeed");

    let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key_a").unwrap(), Some(Bytes::from("value_a")));
	assert_eq!(verify_tx.get("key_b").unwrap(), Some(Bytes::from("value_b")));
	verify_tx.cancel().unwrap();
}

#[test]
fn ssi_read_on_non_existent_key_then_concurrent_insert() {
    let db = Database::new();

    let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
    assert!(tx1.get("empty").unwrap().is_none(), "empty should not exist");

    let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
    tx2.set("empty", "value").unwrap();
	assert!(tx2.commit().is_ok(), "tx2 should commit");

    // 与 ssi_detects_read_write_conflict 同理
    tx1.set("other", "data").unwrap();
    assert!(tx1.commit().is_err(), "tx1 should fail due to read-write conflict on new_key");
}

#[test]
fn ssi_exists_check_creates_read_dependency() {
	let db = Database::new();

	// tx1 checks existence of a key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert!(!tx1.exists("key").unwrap(), "Key should not exist");

	// tx2 creates that key
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("key", "value").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes something else and tries to commit
	tx1.set("other", "data").unwrap();
	assert!(tx1.commit().is_err(), "tx1 should fail due to exists check conflict");
}

#[test]
fn ssi_delete_conflict() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();
	tx.commit().unwrap();

	// tx1 reads the key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert!(tx1.get("key").unwrap().is_some());

	// tx2 deletes the key
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.del("key").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 tries to modify based on what it read
	tx1.set("key", "new_value").unwrap();
	assert!(tx1.commit().is_err(), "tx1 should fail because key was deleted");
}

#[test]
fn ssi_multiple_readers_one_writer() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// Multiple readers start
	let mut reader1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut reader2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Both read the same key
	let _ = reader1.get("key").unwrap();
	let _ = reader2.get("key").unwrap();

	// One reader becomes a writer and commits
	reader1.set("key", "modified").unwrap();
	assert!(reader1.commit().is_ok(), "First writer should succeed");

	// Second reader tries to write something else
	reader2.set("other", "value").unwrap();
	assert!(reader2.commit().is_err(), "Second reader should fail due to read conflict");
}

// =============================================================================
// Mixed Isolation Level Tests
// =============================================================================

#[test]
fn ssi_detects_phantom_via_read_tracking() {
	let db = Database::new();

	// Both transactions run at SSI so readset tracking is enabled
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	// tx1 observes that `key` does not exist — this insertion into the
	// readset is what turns a phantom into a detectable conflict.
	assert!(tx1.get("key").unwrap().is_none());

	// tx2 creates the key and commits
	tx2.set("key", "value").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes something else based on its (now stale) observation
	tx1.set("other", "data").unwrap();

	// SSI must reject: tx1's readset {"key"} intersects tx2's writeset {"key"}
	assert!(tx1.commit().is_err(), "SSI should detect the phantom via read tracking");
}

#[test]
fn si_mode_allows_read_write_anomaly() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI reads the key
	let mut tx1 = db.transaction(true);
	let _ = tx1.get("key").unwrap();

	// tx2 modifies the key
	let mut tx2 = db.transaction(true);
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes to a different key
	tx1.set("other", "value").unwrap();

	// SI should allow this (no read tracking for conflict detection)
	assert!(tx1.commit().is_ok(), "SI should not track read-write conflicts");
}

#[test]
fn concurrent_counter_increment_conflict() {
	let db = Database::new();

	// Create counter
	let mut tx = db.transaction(true);
	tx.set("counter", "0").unwrap();
	tx.commit().unwrap();

	// Two transactions try to increment
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Both read current value
	let val1 = tx1.get("counter").unwrap().unwrap();
	let val2 = tx2.get("counter").unwrap().unwrap();

	assert_eq!(val1.as_ref(), b"0");
	assert_eq!(val2.as_ref(), b"0");

	// Both try to increment
	tx1.set("counter", "1").unwrap();
	tx2.set("counter", "1").unwrap();

	// First commits
	assert!(tx1.commit().is_ok());

	// Second should fail (lost update prevention)
	assert!(tx2.commit().is_err(), "Second increment should fail to prevent lost update");

	// Verify counter is 1, not 2
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("counter").unwrap(), Some(Bytes::from("1")));
	verify.cancel().unwrap();
}