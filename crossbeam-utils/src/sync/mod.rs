//! Thread synchronization primitives.
//!
//! * [`CancelToken`], a cancellation token that wakes all waiters at once.
//! * [`Parker`], a thread parking primitive.
//! * [`ShardedLock`], a sharded reader-writer lock with fast concurrent reads.
//! * [`WaitGroup`], for synchronizing the beginning or end of some computation.

#[cfg(not(crossbeam_loom))]
mod cancel_token;
#[cfg(not(crossbeam_loom))]
mod once_lock;
mod parker;
#[cfg(not(crossbeam_loom))]
mod sharded_lock;
mod wait_group;

#[cfg(not(crossbeam_loom))]
pub use self::cancel_token::{CallbackFailure, CallbackReceipt, CancelToken, Registration, Token};
#[cfg(not(crossbeam_loom))]
pub use self::sharded_lock::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
pub use self::{
    parker::{Parker, UnparkReason, Unparker},
    wait_group::WaitGroup,
};
