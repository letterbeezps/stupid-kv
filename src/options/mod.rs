use std::time::Duration;

pub(crate) const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct DatabaseOptions {

    pub resync_interval: Duration
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self { 
            resync_interval:  DEFAULT_RESYNC_INTERVAL,
        }
    }
}

impl DatabaseOptions {
    
    pub fn new() -> Self {
        Self::default()
    }
}