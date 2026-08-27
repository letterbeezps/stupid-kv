use std::sync::Arc;

use bytes::Bytes;

use crate::kv::IntoBytes;
use crate::pool::Pool;
use crate::tx::{TransactionInner, release_counter};
use crate::error::Error;
use crate::tx::isolation::IsolationLevel;


pub struct Transaction {
    /// 所属对象池。Transaction::Drop 负责把 inner 回收到这里，
    /// 而不是让它随 Transaction 一起 drop 掉。
	pub(crate) pool: Arc<Pool>,

    /// 事务内部状态。None 表示已被 Drop take 走并回收到池。
    pub(crate) inner: Option<TransactionInner>,
}

impl Drop for Transaction {
	/// 释放事务在 counter_by_commit 与 counter_by_oracle 上持有的引用。
	/// 覆盖 commit / cancel / 显式 drop / panic 所有退场路径；
	/// `release_counter` 返回 true 表示归零并已打墓碑，本事务负责摘除 map entry。
	/// 两个 counter 的释放彼此独立。
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			if release_counter(&inner.counter_commit) {
				inner.database.counter_by_commit.remove(&inner.commit);
			}
			if release_counter(&inner.counter_version) {
				inner.database.counter_by_oracle.remove(&inner.version);
			}
			// 回收 inner 到对象池，供下次 transaction() 复用。
			// 池满时 put 会静默丢弃，退化为普通 drop。
			self.pool.put(inner);
		}
	}
}

impl Transaction {

    pub fn with_snapshot_isolation(mut self) -> Self {
        self.inner.as_mut().unwrap().mode = IsolationLevel::SnapshotIsolation;
        self
    }
		
    pub fn with_serializable_snapshot_isolation(mut self) -> Self {
        self.inner.as_mut().unwrap().mode = IsolationLevel::SerializableSnapshotIsolation;
        self
    }
    
    pub fn version(&self) -> u64 {
        self.inner.as_ref().unwrap().version()
    }

    pub fn closed(&self) -> bool {
        self.inner.as_ref().unwrap().closed()
    }

    pub fn cancel(&mut self) -> Result<(), Error> {
        self.inner.as_mut().unwrap().cancel()
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        self.inner.as_mut().unwrap().commit()
    }

    pub fn exists<K>(&self, key: K) -> Result<bool, Error>
    where
        K: IntoBytes,
    {
        self.inner.as_ref().unwrap().exists(key)
    }

    pub fn get<K>(&self, key: K) -> Result<Option<Bytes>, Error>
    where
        K: IntoBytes,
    {
        self.inner.as_ref().unwrap().get(key)
    }

    pub fn set<K, V>(&mut self, key: K, value: V) -> Result<(), Error>
    where
        K: IntoBytes,
        V: IntoBytes,
    {
        self.inner.as_mut().unwrap().set(key, value)
    }

    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<(), Error>
    where
        K: IntoBytes,
        V: IntoBytes,
    {
        self.inner.as_mut().unwrap().put(key, value)
    }

    pub fn del<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: IntoBytes,
    {
        self.inner.as_mut().unwrap().del(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, error::Error};


    #[test]
    fn mvcc_non_conflicting_keys_should_succeed() {
        let db = Database::new();
        let mut tx1 = db.transaction(true);
        let mut tx2 = db.transaction(true);

        // ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key2").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
    }

    #[test]
	fn mvcc_conflicting_blind_writes_should_error() {
		let db = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);

        // 两个新事务，commit id 都是 0，都是读取db.transaction_commit_id
        // 所以这两个事务是存在写冲突的
        assert_eq!(tx1.inner.as_ref().unwrap().commit, 0);
        assert_eq!(tx2.inner.as_ref().unwrap().commit, 0);

        // 两个事务的version 是相同的
        assert_eq!(
            tx1.inner.as_ref().unwrap().version(), 
            tx2.inner.as_ref().unwrap().version());
		// ----------
        // 因为没有写入key1，所以读取不到key1的值
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		// ----------
        // 因为没有写入key1，所以读取不到key1的值
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key1", "value2").unwrap();
		// ----------
		assert!(tx1.commit().is_ok());
        // tx2 在 tx1 之后，会发现key1已经被修改，所以会报错
		let res = tx2.commit();
		assert!(res.is_err());
        
		assert!(matches!(res.unwrap_err(), Error::KeyWriteConflict));

        let mut tx3 = db.transaction(true);
        // tx3 是新的事务，有一个更高的 version, 所以会读取到 tx1 写入的值
        assert_eq!(tx3.get("key1").unwrap(), Some("value1".into()));
        // tx3 有一个新的commit起点，不会因为 tx1已经写入key1 而报错
        tx3.set("key1", "value3").unwrap();
        assert!(tx3.commit().is_ok());
	}

    #[test]
	fn mvcc_si_conflicting_read_keys_should_succeed() {
		let db = Database::new();
		// ----------
		let mut tx1 = db.transaction(true).with_snapshot_isolation();
		let mut tx2 = db.transaction(true).with_snapshot_isolation();

        // 两个新事务，commit id 都是 0，都是读取db.transaction_commit_id
        assert_eq!(tx1.inner.as_ref().unwrap().commit, 0);
        assert_eq!(tx2.inner.as_ref().unwrap().commit, 0);

        // 两个事务的version 是相同的
        assert_eq!(
            tx1.inner.as_ref().unwrap().version(), 
            tx2.inner.as_ref().unwrap().version());

		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
        let commit_id = db.transaction_commit_id.load(std::sync::atomic::Ordering::Acquire);    
        assert_eq!(commit_id, 2);
	}

    #[test]
	fn mvcc_conflicting_read_deleted_keys_should_error() {
		let db = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		assert!(tx2.get("key").unwrap().is_some());
		tx2.del("key").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
        // tx3 和 tx2 有相同的 version 起点，所以tx3在读取数据的时候看不到tx2修改的值，依然读取的是tx1 写入的值
		assert_eq!(tx3.get("key").unwrap(), Some("value1".into()));
        // tx3 和 tx2 有相同的commit id 起点，所以tx3在写入数据的时候会检测到tx2已经删除了key，所以会报错
		tx3.set("key", "value2").unwrap();
		let res = tx3.commit();
		assert!(res.is_err());
        
		assert!(matches!(res.unwrap_err(), Error::KeyWriteConflict));
	}

    #[test]
	fn mvcc_transaction_queue_correctness() {
		let db = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		std::mem::drop(tx1);
		// ----------
		let mut tx2 = db.transaction(true);
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
		std::mem::drop(tx2);
		// ----------
		let mut tx3 = db.transaction(true);
		tx3.set("key", "value").unwrap();
		// ----------
		let mut tx4 = db.transaction(true);
		tx4.set("key", "value").unwrap();
		// ----------
		assert!(tx3.commit().is_ok());
		assert!(tx4.commit().is_err());
		std::mem::drop(tx3);
		std::mem::drop(tx4);
		// ----------
		let mut tx5 = db.transaction(true);
		tx5.set("key", "other").unwrap();
		// ----------
		let mut tx6 = db.transaction(true);
		tx6.set("key", "other").unwrap();
		// ----------
		assert!(tx5.commit().is_ok());
		assert!(tx6.commit().is_err());
		std::mem::drop(tx5);
		std::mem::drop(tx6);
		// ----------
		let mut tx7 = db.transaction(true);
		tx7.set("key", "change").unwrap();
		// ----------
		let mut tx8 = db.transaction(true);
		tx8.set("key", "change").unwrap();
		// ----------
		assert!(tx7.commit().is_ok());
		assert!(tx7.commit().is_err());
		std::mem::drop(tx7);
		std::mem::drop(tx8);
	}

    #[test]
	fn test_snapshot_isolation() {
		let db = Database::new();

		let key1 = "key1";
		let key2 = "key2";
		let value1 = "baz";
		let value2 = "bar";

		// no conflict
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			assert!(txn1.commit().is_ok());

			assert!(txn2.get(key2).unwrap().is_none());
			txn2.set(key2, value2).unwrap();
			assert!(txn2.commit().is_ok());
		}

		// conflict when the write key was updated by another transaction
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			assert!(txn1.commit().is_ok());

			assert!(txn2.get(key1).is_ok());
			txn2.set(key1, value2).unwrap();
			assert!(txn2.commit().is_err());
		}

		// blind writes should not succeed
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn2.set(key1, value2).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}

		// conflict when the read key was updated by another transaction
		{
			let key = "key3";

			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key).unwrap().is_none());
			txn2.set(key, value1).unwrap();
			assert!(txn2.commit().is_err());
		}

	}

	#[test]
	fn test_serializable_snapshot_isolation() {
		let db = Database::new();

		let key1 = "key1";
		let key2 = "key2";
		let value1 = "baz";
		let value2 = "bar";

		// no conflict
		{
			let mut txn1 = db.transaction(true).with_serializable_snapshot_isolation();
			let mut txn2 = db.transaction(true).with_serializable_snapshot_isolation();

			txn1.set(key1, value1).unwrap();
			assert!(txn1.commit().is_ok());

			assert!(txn2.get(key2).unwrap().is_none());
			txn2.set(key2, value2).unwrap();
			assert!(txn2.commit().is_ok());
		}

		// conflict when the write key was updated by another transaction
		{
			let mut txn1 = db.transaction(true).with_serializable_snapshot_isolation();
			let mut txn2 = db.transaction(true).with_serializable_snapshot_isolation();

			txn1.set(key1, value1).unwrap();
			assert!(txn1.commit().is_ok());

			assert!(txn2.get(key1).is_ok());
			txn2.set(key1, value2).unwrap();
			assert!(txn2.commit().is_err());
		}

		// blind writes should not succeed
		{
			let mut txn1 = db.transaction(true).with_serializable_snapshot_isolation();
			let mut txn2 = db.transaction(true).with_serializable_snapshot_isolation();

			txn1.set(key1, value1).unwrap();
			txn2.set(key1, value2).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}

		// conflict when the read key was updated by another transaction
		{
			let key = "key3";

			let mut txn1 = db.transaction(true).with_serializable_snapshot_isolation();
			let mut txn2 = db.transaction(true).with_serializable_snapshot_isolation();

			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key).unwrap().is_none());
			txn2.set(key, value1).unwrap();
			assert!(txn2.commit().is_err());
		}

		// SSI 通过 读冲突检测 （Read Conflict Detection）来防止 Write-Skew：
		// 当事务提交时，检查它读取过的所有 key 是否在事务生命周期内被其他事务修改过，如果有则拒绝提交。
		{
			let key = "key4";

			let mut txn1 = db.transaction(true).with_serializable_snapshot_isolation();
			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true).with_serializable_snapshot_isolation();
			let mut txn3 = db.transaction(true).with_serializable_snapshot_isolation();

			txn2.del(key).unwrap();
			assert!(txn2.commit().is_ok());

			assert!(txn3.get(key).is_ok());
			txn3.set(key1, value2).unwrap();
            // SSI隔离级别：tx3 和 tx2 有相同的commit id 起点，
			// 尽管tx3没有修改key，只是读取了key，但是tx3在提交时发现tx2已经修改了key，所以会报错
			assert!(txn3.commit().is_err());
		}
	}

    fn new_db() -> Database {
        let db = Database::new();
        let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		// Start a new read-write transaction (txn)
		let mut txn = db.transaction(true);
		txn.set(key1, value1).unwrap();
		txn.set(key2, value2).unwrap();
		txn.commit().unwrap();
        db
    }

    // G0: Write Cycles (dirty writes)
	#[test]
	fn test_anomaly_g0() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value3 = "v3";
		let value4 = "v4";
		let value5 = "v5";
		let value6 = "v6";

		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			assert_eq!(txn1.get(key1).unwrap(), Some("v1".into()));
			assert_eq!(txn1.get(key2).unwrap(), Some("v2".into()));
			assert!(txn2.get(key1).is_ok());
			assert!(txn2.get(key2).is_ok());

			txn1.set(key1, value3).unwrap();
			txn2.set(key1, value4).unwrap();

			txn1.set(key2, value5).unwrap();

			txn1.commit().unwrap();

			txn2.set(key2, value6).unwrap();
			assert!(txn2.commit().is_err());
		}

		{
			let txn3 = db.transaction(true);
			let val1 = txn3.get(key1).unwrap().unwrap();
			assert_eq!(val1, value3);
			let val2 = txn3.get(key2).unwrap().unwrap();
			assert_eq!(val2, value5);
		}
	}

    

}