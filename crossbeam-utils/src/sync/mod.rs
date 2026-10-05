//! Thread synchronization primitives.
//!
//! * [`CancellationToken`], a one-shot signal that wakes all waiting threads.
//! * [`Parker`], a thread parking primitive.
//! * [`ShardedLock`], a sharded reader-writer lock with fast concurrent reads.
//! * [`WaitGroup`], for synchronizing the beginning or end of some computation.

#[cfg(not(crossbeam_loom))]
mod cancellation_token;
#[cfg(not(crossbeam_loom))]
mod once_lock;
mod parker;
#[cfg(not(crossbeam_loom))]
mod sharded_lock;
mod wait_group;

#[cfg(not(crossbeam_loom))]
pub use self::{
    cancellation_token::{
        CallbackError, CallbackHandle, CancellationToken, CancellationTokenSource,
    },
    sharded_lock::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard},
};
pub use self::{
    parker::{Parker, UnparkReason, Unparker},
    wait_group::WaitGroup,
};
