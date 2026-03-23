use std::{sync::atomic::AtomicU64};

use arc_swap::ArcSwap;
use web_time::Instant;

pub(crate) struct Inner {
    pub(crate) timestamp: AtomicU64,

    pub(crate) reference: ArcSwap<(u64, Instant)>,
}