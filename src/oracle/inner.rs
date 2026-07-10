use std::{sync::atomic::{AtomicBool, AtomicU64}, thread::JoinHandle, time::Duration};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use web_time::Instant;

pub(crate) struct Inner {
    pub(crate) timestamp: AtomicU64,

    pub(crate) reference: ArcSwap<(u64, Instant)>,

    pub(crate) resync_enable: AtomicBool,

    pub(crate) resync_handle: Mutex<Option<JoinHandle<()>>>,

    pub(crate) resync_interval: Duration,
}