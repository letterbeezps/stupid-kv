use std::sync::{Arc, atomic::{AtomicU64, Ordering}};



use crate::oracle::Inner;
use arc_swap::ArcSwap;
use web_time::{SystemTime, Instant, UNIX_EPOCH};

pub(crate) struct Oracle {
    pub(crate) inner: Arc<Inner>,
}

impl Oracle {
    pub fn new() -> Arc<Self> {
        let reference_unix = Self::current_unix_ns();
        let reference_time = Instant::now();
        let oracle = Self{
            inner: Arc::new(
                Inner{
                    timestamp: AtomicU64::new(reference_unix),
                    reference: ArcSwap::new(Arc::new((reference_unix, reference_time))),
                }
            )
        };
        Arc::new(oracle)    
    }

    #[inline]
    pub fn current_timestamp(&self) -> u64 {
        self.inner.timestamp.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn current_unix_ns() -> u64 {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH); 
        timestamp.unwrap().as_nanos() as u64
    }

    #[inline]
    pub(crate) fn current_time_ns(&self) -> u64 {
        let reference = self.inner.reference.load();
        reference.0 + reference.1.elapsed().as_nanos() as u64
    }
}

