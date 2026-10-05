//! Explicit batch channels.
//!
//! A batch channel sends whole `Vec<T>` batches instead of individual messages. A batch is
//! either enqueued in its entirety or returned to the caller unchanged - it is never partially
//! accepted. The channel capacity is counted in the total number of *messages*, not in the
//! number of batches.
//!
//! Sending works in two steps:
//!
//! 1. Quota for `batch.len()` messages is atomically reserved under a lock (blocked senders
//!    wait on a condition variable until enough quota is released).
//! 2. The batch is handed to the underlying channel. If that hand-off fails, the reserved
//!    quota is released again, so quota is never leaked by a failed send.
//!
//! Receiving a batch releases the corresponding quota and wakes up blocked senders. Dropping
//! the last receiver wakes all blocked senders with a `Disconnected` error that contains their
//! original `Vec`. Messages are moved through the channel and never cloned, so `T` does not
//! need to implement `Clone`.
//!
//! Empty batches and batches larger than the total capacity are rejected up front.
//!
//! # Examples
//!
//! ```
//! use crossbeam_channel::batch::batch_channel;
//!
//! let (s, r) = batch_channel(3);
//!
//! // Two batches of two messages do not fit into a capacity of 3: the second batch is
//! // rejected as a whole instead of being partially accepted.
//! s.send_batch(vec![1, 2]).unwrap();
//! assert!(s.try_send_batch(vec![3, 4]).is_err());
//!
//! // After the first batch is received, the second batch fits completely.
//! assert_eq!(r.recv_batch(), Ok(vec![1, 2]));
//! s.send_batch(vec![3, 4]).unwrap();
//! assert_eq!(r.recv_batch(), Ok(vec![3, 4]));
//! ```

use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

use std::error;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::channel::{Receiver, Sender, unbounded};
use crate::err::{RecvError, RecvTimeoutError, SendError, TryRecvError};

/// Creates a batch channel of bounded message capacity.
///
/// The channel can hold at most `capacity` messages in total, spread over any number of
/// batches. A batch is delivered atomically: either the whole `Vec` is enqueued or it is
/// returned to the sender unchanged.
///
/// # Examples
///
/// ```
/// use crossbeam_channel::batch::batch_channel;
///
/// let (s, r) = batch_channel(3);
/// s.send_batch(vec![1, 2, 3]).unwrap();
/// assert_eq!(r.recv_batch(), Ok(vec![1, 2, 3]));
/// ```
pub fn batch_channel<T>(capacity: usize) -> (BatchSender<T>, BatchReceiver<T>) {
    let (s, r) = unbounded();
    let shared = Arc::new(Shared {
        capacity,
        state: Mutex::new(State {
            used: 0,
            receivers: 1,
        }),
        space_available: Condvar::new(),
    });
    (
        BatchSender {
            inner: s,
            shared: shared.clone(),
        },
        BatchReceiver {
            inner: r,
            shared,
        },
    )
}

/// Shared state between all senders and receivers of a batch channel.
struct Shared {
    /// Total message capacity of the channel.
    capacity: usize,
    /// Quota bookkeeping, guarded by a mutex so reservation is atomic.
    state: Mutex<State>,
    /// Signalled whenever quota is released or the last receiver is dropped.
    space_available: Condvar,
}

struct State {
    /// Number of messages currently occupying the channel.
    used: usize,
    /// Number of live receivers. When it drops to zero, all blocked senders are woken.
    receivers: usize,
}

impl Shared {
    /// Releases `n` units of quota and wakes blocked senders.
    fn release(&self, n: usize) {
        let mut state = self.state.lock().unwrap();
        debug_assert!(state.used >= n, "batch channel quota underflow");
        state.used -= n;
        drop(state);
        self.space_available.notify_all();
    }

    /// Current number of messages occupying the channel.
    fn used(&self) -> usize {
        self.state.lock().unwrap().used
    }
}

/// The sending side of a [`batch_channel`].
///
/// Clone it to create additional senders. Messages are moved, never cloned, so `T` does not
/// need to be `Clone`.
pub struct BatchSender<T> {
    inner: Sender<Vec<T>>,
    shared: Arc<Shared>,
}

/// Result of the up-front batch validation.
enum PreCheck {
    Empty,
    TooLarge,
}

impl<T> BatchSender<T> {
    /// Validates a batch that is about to be sent and returns its length.
    fn check(&self, batch: &Vec<T>) -> Result<usize, PreCheck> {
        if batch.is_empty() {
            Err(PreCheck::Empty)
        } else if batch.len() > self.shared.capacity {
            Err(PreCheck::TooLarge)
        } else {
            Ok(batch.len())
        }
    }

    /// Hands a reserved batch over to the underlying channel.
    ///
    /// If the hand-off fails, the reserved quota is released so it is not leaked.
    fn deliver(&self, batch: Vec<T>, n: usize) -> Result<(), Vec<T>> {
        match self.inner.send(batch) {
            Ok(()) => Ok(()),
            Err(SendError(batch)) => {
                self.shared.release(n);
                Err(batch)
            }
        }
    }

    /// Blocks until the whole batch can be enqueued, then sends it.
    ///
    /// Returns the batch unchanged if it is empty, larger than the total capacity, or all
    /// receivers have been dropped.
    pub fn send_batch(&self, batch: Vec<T>) -> Result<(), SendBatchError<T>> {
        let n = match self.check(&batch) {
            Ok(n) => n,
            Err(PreCheck::Empty) => return Err(SendBatchError::Empty(batch)),
            Err(PreCheck::TooLarge) => return Err(SendBatchError::TooLarge(batch)),
        };
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if state.receivers == 0 {
                return Err(SendBatchError::Disconnected(batch));
            }
            if state.used + n <= self.shared.capacity {
                // Atomically reserve quota for the whole batch.
                state.used += n;
                break;
            }
            state = self.shared.space_available.wait(state).unwrap();
        }
        drop(state);
        match self.deliver(batch, n) {
            Ok(()) => Ok(()),
            Err(batch) => Err(SendBatchError::Disconnected(batch)),
        }
    }

    /// Attempts to send the whole batch without blocking.
    ///
    /// Returns the batch unchanged if it is empty, larger than the total capacity, does not
    /// fit completely right now, or all receivers have been dropped.
    pub fn try_send_batch(&self, batch: Vec<T>) -> Result<(), TrySendBatchError<T>> {
        let n = match self.check(&batch) {
            Ok(n) => n,
            Err(PreCheck::Empty) => return Err(TrySendBatchError::Empty(batch)),
            Err(PreCheck::TooLarge) => return Err(TrySendBatchError::TooLarge(batch)),
        };
        let mut state = self.shared.state.lock().unwrap();
        if state.receivers == 0 {
            return Err(TrySendBatchError::Disconnected(batch));
        }
        if state.used + n > self.shared.capacity {
            return Err(TrySendBatchError::Full(batch));
        }
        state.used += n;
        drop(state);
        match self.deliver(batch, n) {
            Ok(()) => Ok(()),
            Err(batch) => Err(TrySendBatchError::Disconnected(batch)),
        }
    }

    /// Blocks until the whole batch can be enqueued or the timeout expires.
    ///
    /// Returns the batch unchanged if it is empty, larger than the total capacity, the
    /// timeout expired before enough quota was released, or all receivers have been dropped.
    /// A timed-out send never reserved quota, so nothing leaks.
    pub fn send_batch_timeout(
        &self,
        batch: Vec<T>,
        timeout: Duration,
    ) -> Result<(), SendBatchTimeoutError<T>> {
        let n = match self.check(&batch) {
            Ok(n) => n,
            Err(PreCheck::Empty) => return Err(SendBatchTimeoutError::Empty(batch)),
            Err(PreCheck::TooLarge) => return Err(SendBatchTimeoutError::TooLarge(batch)),
        };
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if state.receivers == 0 {
                return Err(SendBatchTimeoutError::Disconnected(batch));
            }
            if state.used + n <= self.shared.capacity {
                state.used += n;
                break;
            }
            let remaining = match deadline.and_then(|d| d.checked_duration_since(Instant::now()))
            {
                Some(remaining) if !remaining.is_zero() => remaining,
                _ => return Err(SendBatchTimeoutError::Timeout(batch)),
            };
            state = self
                .shared
                .space_available
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
        drop(state);
        match self.deliver(batch, n) {
            Ok(()) => Ok(()),
            Err(batch) => Err(SendBatchTimeoutError::Disconnected(batch)),
        }
    }

    /// Returns the total message capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    /// Returns the number of messages currently occupying the channel.
    pub fn len(&self) -> usize {
        self.shared.used()
    }

    /// Returns `true` if no message is currently occupying the channel.
    pub fn is_empty(&self) -> bool {
        self.shared.used() == 0
    }
}

impl<T> Clone for BatchSender<T> {
    fn clone(&self) -> Self {
        BatchSender {
            inner: self.inner.clone(),
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for BatchSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchSender")
            .field("capacity", &self.shared.capacity)
            .field("len", &self.shared.used())
            .finish()
    }
}

/// The receiving side of a [`batch_channel`].
///
/// Receiving a batch releases its quota and wakes senders blocked on a full channel. Dropping
/// the last receiver wakes all blocked senders with a `Disconnected` error containing their
/// original batch.
pub struct BatchReceiver<T> {
    inner: Receiver<Vec<T>>,
    shared: Arc<Shared>,
}

impl<T> BatchReceiver<T> {
    /// Blocks until a batch is available, then returns it and releases its quota.
    pub fn recv_batch(&self) -> Result<Vec<T>, RecvError> {
        let batch = self.inner.recv()?;
        self.shared.release(batch.len());
        Ok(batch)
    }

    /// Returns a pending batch without blocking, releasing its quota.
    pub fn try_recv_batch(&self) -> Result<Vec<T>, TryRecvError> {
        let batch = self.inner.try_recv()?;
        self.shared.release(batch.len());
        Ok(batch)
    }

    /// Blocks until a batch is available or the timeout expires, releasing its quota.
    pub fn recv_batch_timeout(&self, timeout: Duration) -> Result<Vec<T>, RecvTimeoutError> {
        let batch = self.inner.recv_timeout(timeout)?;
        self.shared.release(batch.len());
        Ok(batch)
    }

    /// Returns the total message capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    /// Returns the number of messages currently occupying the channel.
    pub fn len(&self) -> usize {
        self.shared.used()
    }

    /// Returns `true` if no message is currently occupying the channel.
    pub fn is_empty(&self) -> bool {
        self.shared.used() == 0
    }
}

impl<T> Clone for BatchReceiver<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock().unwrap();
        state.receivers += 1;
        drop(state);
        BatchReceiver {
            inner: self.inner.clone(),
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for BatchReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchReceiver")
            .field("capacity", &self.shared.capacity)
            .field("len", &self.shared.used())
            .finish()
    }
}

impl<T> Drop for BatchReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.receivers -= 1;
        let last = state.receivers == 0;
        drop(state);
        if last {
            // Wake all blocked senders so they can reclaim their original batches.
            self.shared.space_available.notify_all();
        }
    }
}

/// An error returned from [`BatchSender::send_batch`].
///
/// The whole batch is returned unchanged so it can be recovered.
pub enum SendBatchError<T> {
    /// The batch was empty. Empty batches are rejected up front.
    Empty(Vec<T>),
    /// The batch was larger than the total channel capacity and can never be sent.
    TooLarge(Vec<T>),
    /// All receivers have been dropped.
    Disconnected(Vec<T>),
}

/// An error returned from [`BatchSender::try_send_batch`].
///
/// The whole batch is returned unchanged so it can be recovered.
pub enum TrySendBatchError<T> {
    /// The batch was empty. Empty batches are rejected up front.
    Empty(Vec<T>),
    /// The batch was larger than the total channel capacity and can never be sent.
    TooLarge(Vec<T>),
    /// The batch did not fit completely into the remaining capacity. It was not partially
    /// accepted.
    Full(Vec<T>),
    /// All receivers have been dropped.
    Disconnected(Vec<T>),
}

/// An error returned from [`BatchSender::send_batch_timeout`].
///
/// The whole batch is returned unchanged so it can be recovered.
pub enum SendBatchTimeoutError<T> {
    /// The batch was empty. Empty batches are rejected up front.
    Empty(Vec<T>),
    /// The batch was larger than the total channel capacity and can never be sent.
    TooLarge(Vec<T>),
    /// The timeout expired before enough quota was released for the whole batch.
    Timeout(Vec<T>),
    /// All receivers have been dropped.
    Disconnected(Vec<T>),
}

macro_rules! impl_batch_error {
    ($name:ident, $(($variant:ident, $msg:expr)),* $(,)?) => {
        impl<T> $name<T> {
            /// Consumes the error and returns the original batch unchanged.
            pub fn into_inner(self) -> Vec<T> {
                match self {
                    $($name::$variant(batch) => batch,)*
                }
            }
        }

        impl<T> fmt::Debug for $name<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $($name::$variant(_) => {
                        f.debug_tuple(concat!(stringify!($name), "::", stringify!($variant)))
                            .field(&format_args!(".."))
                            .finish()
                    })*
                }
            }
        }

        impl<T> fmt::Display for $name<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $($name::$variant(_) => f.write_str($msg),)*
                }
            }
        }

        impl<T> error::Error for $name<T> {}
    };
}

impl_batch_error!(
    SendBatchError,
    (Empty, "cannot send an empty batch"),
    (TooLarge, "batch is larger than the total channel capacity"),
    (Disconnected, "sending on a disconnected batch channel"),
);
impl_batch_error!(
    TrySendBatchError,
    (Empty, "cannot send an empty batch"),
    (TooLarge, "batch is larger than the total channel capacity"),
    (Full, "batch does not fit into the remaining channel capacity"),
    (Disconnected, "sending on a disconnected batch channel"),
);
impl_batch_error!(
    SendBatchTimeoutError,
    (Empty, "cannot send an empty batch"),
    (TooLarge, "batch is larger than the total channel capacity"),
    (Timeout, "timed out waiting for enough batch channel capacity"),
    (Disconnected, "sending on a disconnected batch channel"),
);
