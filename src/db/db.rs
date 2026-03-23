use std::{ops::Deref, sync::Arc};

use crate::db::inner::Inner;
use crate::tx::{Transaction, TransactionInner};



pub struct Database {
    inner: Arc<Inner>
}

impl Default for Database {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

impl Deref for Database {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn transaction(&self, write: bool) -> Transaction {
        let inner = TransactionInner::new(self.inner.clone(), write);
        Transaction { inner: Some(inner) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_tx() {
		let db = Database::new();
		db.transaction(false);
	}

    #[test]
	fn finished_tx_not_writeable() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		let res = tx.cancel();
		assert!(res.is_ok());
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_err());
	}


    #[test]
	fn cancelled_tx_is_cancelled() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(!res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

    #[test]
	fn committed_tx_is_committed() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

    
}