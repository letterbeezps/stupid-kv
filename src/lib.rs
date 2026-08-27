pub const LOG_TARGET_CONFLICTS: &str = "stupid-kv::conflicts";

mod db;
mod tx;
mod versions;
mod queue;
mod oracle;
mod kv;
pub mod error;
mod bloom;
mod options;
mod persistence;
mod compression;
mod pool;



#[doc(inline)]
pub use self::db::*;

#[doc(inline)]
pub use self::options::*;