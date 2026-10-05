//! An explicit, bounded batch channel.
//!
//! A batch channel sends and receives whole `Vec<T>` batches. A batch is either
//! enqueued in full or returned to the sender as-is. Capacity is measured by the
//! total number of messages, not the number of batches.
//!
//! The implementation reserves quota atomically before handing a batch to the
//! underlying queue. If the hand-off fails (e.g. because the channel became
//! disconnected), the reserved quota is released. Receiving a batch releases the
//! corresponding quota. Senders waiting for quota are woken up when quota is
//! freed, when the deadline expires, or when all receivers are dropped.
//!
//! Messages are moved by ownership, so `T` does not need to implement [`Clone`].

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use std::error;
use std::sync::Condvar;
use std::time::Instant;

use crate::utils::Mutex;

/// The shared interior of a batch channel.
struct Inner<T> {
    /// Maximum number of messages the channel can hold.
    cap: usize,

    /// Number of messages currently enqueued plus messages reserved by in-flight
    /// send operations. This value is always at most `cap`.
    used: AtomicUsize,

    /// The enqueued batches.
    queue: Mutex<VecDeque<Vec<T>>>,

    /// Signalled when quota is freed or receivers disconnect.
    senders: Condvar,

    /// Signalled when a batch is enqueued or senders disconnect.
    receivers: Condvar,

    /// Number of live senders.
    sender_count: AtomicUsize,

    /// Number of live receivers.
    receiver_count: AtomicUsize,
}

impl<T> Inner<T> {
    /// Atomically reserves quota for a batch of `n` messages.
    ///
    /// Returns `true` if the quota was reserved, `false` if there is not enough
    /// capacity.
    #[inline]
    fn try_reserve(&self, n: usize) -> bool {
        loop {
            let used = self.used.load(Ordering::Acquire);
            if used + n > self.cap {
                return false;
            }
            match self.used.compare_exchange_weak(
                used,
                used + n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Releases `n` messages worth of quota.
    #[inline]
    fn release(&self, n: usize) {
        self.used.fetch_sub(n, Ordering::AcqRel);
    }

    /// Returns `true` if all senders have been dropped.
    #[inline]
    fn senders_disconnected(&self) -> bool {
        self.sender_count.load(Ordering::Acquire) == 0
    }

    /// Returns `true` if all receivers have been dropped.
    #[inline]
    fn receivers_disconnected(&self) -> bool {
        self.receiver_count.load(Ordering::Acquire) == 0
    }
}

/// A sender handle to a batch channel.
///
/// Senders can be cloned to obtain multiple handles to the same channel.
pub struct BatchSender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> BatchSender<T> {
    /// Attempts to send a batch into the channel without blocking.
    ///
    /// The batch is either enqueued in full or returned as-is.
    ///
    /// # Errors
    ///
    /// - [`TrySendBatchError::Full`] if there is not enough capacity.
    /// - [`TrySendBatchError::Disconnected`] if all receivers have been dropped.
    /// - [`TrySendBatchError::Empty`] if the batch is empty.
    /// - [`TrySendBatchError::TooLarge`] if the batch exceeds the total capacity.
    pub fn try_send_batch(&self, batch: Vec<T>) -> Result<(), TrySendBatchError<Vec<T>>> {
        if batch.is_empty() {
            return Err(TrySendBatchError::Empty(batch));
        }
        if batch.len() > self.inner.cap {
            return Err(TrySendBatchError::TooLarge(batch));
        }

        // Lock the queue first so the disconnected check and the enqueue are
        // atomic with respect to receiver disconnection.
        let mut queue = self.inner.queue.lock();
        if self.inner.receivers_disconnected() {
            return Err(TrySendBatchError::Disconnected(batch));
        }

        // Atomically reserve quota before handing the batch to the channel.
        if !self.inner.try_reserve(batch.len()) {
            return Err(TrySendBatchError::Full(batch));
        }

        queue.push_back(batch);
        self.inner.receivers.notify_one();
        Ok(())
    }

    /// Sends a batch into the channel, blocking until it is enqueued or the
    /// deadline expires.
    ///
    /// The batch is either enqueued in full or returned as-is.
    fn send_batch_until(
        &self,
        batch: Vec<T>,
        deadline: Option<Instant>,
    ) -> Result<(), SendBatchTimeoutError<Vec<T>>> {
        if batch.is_empty() {
            return Err(SendBatchTimeoutError::Empty(batch));
        }
        if batch.len() > self.inner.cap {
            return Err(SendBatchTimeoutError::TooLarge(batch));
        }

        let n = batch.len();
        loop {
            // Check the deadline before each attempt.
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    return Err(SendBatchTimeoutError::Timeout(batch));
                }
            }

            let mut queue = self.inner.queue.lock();

            // Try to reserve quota under the queue lock to avoid lost wakeups.
            if self.inner.try_reserve(n) {
                if self.inner.receivers_disconnected() {
                    self.inner.release(n);
                    return Err(SendBatchTimeoutError::Disconnected(batch));
                }
                queue.push_back(batch);
                self.inner.receivers.notify_one();
                return Ok(());
            }

            if self.inner.receivers_disconnected() {
                return Err(SendBatchTimeoutError::Disconnected(batch));
            }

            // Wait for quota, a timeout, or disconnection.
            let (new_queue, _) = match deadline {
                Some(d) => {
                    let timeout = d.saturating_duration_since(Instant::now());
                    let (guard, result) = self.inner.senders.wait_timeout(queue, timeout).unwrap();
                    (guard, result.timed_out())
                }
                None => {
                    let guard = self.inner.senders.wait(queue).unwrap();
                    (guard, false)
                }
            };
            queue = new_queue;
            // Loop again to re-check the deadline and retry.
        }
    }

    /// Sends a batch into the channel, blocking until it is enqueued.
    ///
    /// The batch is either enqueued in full or returned as-is.
    ///
    /// # Errors
    ///
    /// - [`SendBatchError::Disconnected`] if all receivers have been dropped.
    /// - [`SendBatchError::Empty`] if the batch is empty.
    /// - [`SendBatchError::TooLarge`] if the batch exceeds the total capacity.
    pub fn send_batch(&self, batch: Vec<T>) -> Result<(), SendBatchError<Vec<T>>> {
        self.send_batch_until(batch, None)
            .map_err(|err| match err {
                SendBatchTimeoutError::Disconnected(b) => SendBatchError::Disconnected(b),
                SendBatchTimeoutError::Empty(b) => SendBatchError::Empty(b),
                SendBatchTimeoutError::TooLarge(b) => SendBatchError::TooLarge(b),
                SendBatchTimeoutError::Timeout(_) => unreachable!(),
            })
    }

    /// Sends a batch into the channel, blocking until it is enqueued or the
    /// timeout expires.
    ///
    /// The batch is either enqueued in full or returned as-is.
    ///
    /// # Errors
    ///
    /// - [`SendBatchTimeoutError::Timeout`] if the timeout expires.
    /// - [`SendBatchTimeoutError::Disconnected`] if all receivers have been dropped.
    /// - [`SendBatchTimeoutError::Empty`] if the batch is empty.
    /// - [`SendBatchTimeoutError::TooLarge`] if the batch exceeds the total capacity.
    pub fn send_batch_timeout(
        &self,
        batch: Vec<T>,
        timeout: Duration,
    ) -> Result<(), SendBatchTimeoutError<Vec<T>>> {
        self.send_batch_until(batch, Some(Instant::now() + timeout))
    }

    /// Returns the total capacity of the channel in messages.
    pub fn capacity(&self) -> usize {
        self.inner.cap
    }

    /// Returns the number of messages currently enqueued.
    pub fn len(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Returns `true` if the channel contains no messages.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the channel is full.
    pub fn is_full(&self) -> bool {
        self.len() >= self.inner.cap
    }
}

impl<T> Clone for BatchSender<T> {
    fn clone(&self) -> Self {
        self.inner.sender_count.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for BatchSender<T> {
    fn drop(&mut self) {
        let queue = self.inner.queue.lock();
        if self.inner.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // This was the last sender. Wake up any blocked receivers.
            self.inner.receivers.notify_all();
        }
        drop(queue);
    }
}

impl<T> fmt::Debug for BatchSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchSender")
            .field("capacity", &self.inner.cap)
            .field("used", &self.inner.used.load(Ordering::Relaxed))
            .finish()
    }
}

/// A receiver handle to a batch channel.
///
/// Receivers can be cloned to obtain multiple handles to the same channel.
pub struct BatchReceiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> BatchReceiver<T> {
    /// Attempts to receive a batch from the channel without blocking.
    ///
    /// # Errors
    ///
    /// - [`TryRecvBatchError::Empty`] if the channel is empty.
    /// - [`TryRecvBatchError::Disconnected`] if the channel is empty and all
    ///   senders have been dropped.
    pub fn try_recv_batch(&self) -> Result<Vec<T>, TryRecvBatchError> {
        let mut queue = self.inner.queue.lock();
        if let Some(batch) = queue.pop_front() {
            self.inner.release(batch.len());
            self.inner.senders.notify_one();
            return Ok(batch);
        }

        if self.inner.senders_disconnected() {
            return Err(TryRecvBatchError::Disconnected);
        }
        Err(TryRecvBatchError::Empty)
    }

    /// Receives a batch from the channel, blocking until one is available or
    /// the deadline expires.
    fn recv_batch_until(
        &self,
        deadline: Option<Instant>,
    ) -> Result<Vec<T>, RecvBatchTimeoutError> {
        loop {
            // Check the deadline before each attempt.
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    return Err(RecvBatchTimeoutError::Timeout);
                }
            }

            let mut queue = self.inner.queue.lock();

            if let Some(batch) = queue.pop_front() {
                self.inner.release(batch.len());
                self.inner.senders.notify_one();
                return Ok(batch);
            }

            if self.inner.senders_disconnected() {
                return Err(RecvBatchTimeoutError::Disconnected);
            }

            // Wait for a batch, a timeout, or disconnection.
            let (new_queue, _) = match deadline {
                Some(d) => {
                    let timeout = d.saturating_duration_since(Instant::now());
                    let (guard, result) = self.inner.receivers.wait_timeout(queue, timeout).unwrap();
                    (guard, result.timed_out())
                }
                None => {
                    let guard = self.inner.receivers.wait(queue).unwrap();
                    (guard, false)
                }
            };
            queue = new_queue;
            // Loop again to re-check the deadline and retry.
        }
    }

    /// Receives a batch from the channel, blocking until one is available.
    ///
    /// # Errors
    ///
    /// Returns [`RecvBatchError`] if the channel is empty and all senders have
    /// been dropped.
    pub fn recv_batch(&self) -> Result<Vec<T>, RecvBatchError> {
        self.recv_batch_until(None).map_err(|err| match err {
            RecvBatchTimeoutError::Disconnected => RecvBatchError,
            RecvBatchTimeoutError::Timeout => unreachable!(),
        })
    }

    /// Receives a batch from the channel, blocking until one is available or
    /// the timeout expires.
    ///
    /// # Errors
    ///
    /// - [`RecvBatchTimeoutError::Timeout`] if the timeout expires.
    /// - [`RecvBatchTimeoutError::Disconnected`] if the channel is empty and all
    ///   senders have been dropped.
    pub fn recv_batch_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Vec<T>, RecvBatchTimeoutError> {
        self.recv_batch_until(Some(Instant::now() + timeout))
    }

    /// Returns the total capacity of the channel in messages.
    pub fn capacity(&self) -> usize {
        self.inner.cap
    }

    /// Returns the number of messages currently enqueued.
    pub fn len(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Returns `true` if the channel contains no messages.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the channel is full.
    pub fn is_full(&self) -> bool {
        self.len() >= self.inner.cap
    }
}

impl<T> Clone for BatchReceiver<T> {
    fn clone(&self) -> Self {
        self.inner.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for BatchReceiver<T> {
    fn drop(&mut self) {
        let queue = self.inner.queue.lock();
        if self.inner.receiver_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // This was the last receiver. Wake up any blocked senders.
            self.inner.senders.notify_all();
        }
        drop(queue);
    }
}

impl<T> fmt::Debug for BatchReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchReceiver")
            .field("capacity", &self.inner.cap)
            .field("used", &self.inner.used.load(Ordering::Relaxed))
            .finish()
    }
}

/// Creates a bounded batch channel with the given capacity.
///
/// The capacity is measured in the total number of messages, not the number of
/// batches.
///
/// # Panics
///
/// Panics if `cap` is zero.
pub fn batch_channel<T>(cap: usize) -> (BatchSender<T>, BatchReceiver<T>) {
    assert!(cap > 0, "batch channel capacity must be positive");

    let inner = Arc::new(Inner {
        cap,
        used: AtomicUsize::new(0),
        queue: Mutex::new(VecDeque::new()),
        senders: Condvar::new(),
        receivers: Condvar::new(),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    });

    (
        BatchSender {
            inner: Arc::clone(&inner),
        },
        BatchReceiver { inner },
    )
}

/// An error returned from the [`BatchSender::send_batch`] method.
///
/// The batch could not be sent because the channel is disconnected, the batch is
/// empty, or the batch exceeds the total capacity.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum SendBatchError<T> {
    /// The batch could not be sent because the channel is disconnected.
    Disconnected(T),

    /// The batch could not be sent because it is empty.
    Empty(T),

    /// The batch could not be sent because it exceeds the total capacity.
    TooLarge(T),
}

/// An error returned from the [`BatchSender::try_send_batch`] method.
///
/// The batch could not be sent because the channel is full or disconnected, the
/// batch is empty, or the batch exceeds the total capacity.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendBatchError<T> {
    /// The batch could not be sent because the channel is full.
    Full(T),

    /// The batch could not be sent because the channel is disconnected.
    Disconnected(T),

    /// The batch could not be sent because it is empty.
    Empty(T),

    /// The batch could not be sent because it exceeds the total capacity.
    TooLarge(T),
}

/// An error returned from the [`BatchSender::send_batch_timeout`] method.
///
/// The batch could not be sent because the operation timed out, the channel is
/// disconnected, the batch is empty, or the batch exceeds the total capacity.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum SendBatchTimeoutError<T> {
    /// The batch could not be sent because the operation timed out.
    Timeout(T),

    /// The batch could not be sent because the channel is disconnected.
    Disconnected(T),

    /// The batch could not be sent because it is empty.
    Empty(T),

    /// The batch could not be sent because it exceeds the total capacity.
    TooLarge(T),
}

/// An error returned from the [`BatchReceiver::recv_batch`] method.
///
/// A batch could not be received because the channel is empty and disconnected.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct RecvBatchError;

/// An error returned from the [`BatchReceiver::try_recv_batch`] method.
///
/// A batch could not be received because the channel is empty or disconnected.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvBatchError {
    /// A batch could not be received because the channel is empty.
    Empty,

    /// A batch could not be received because the channel is empty and
    /// disconnected.
    Disconnected,
}

/// An error returned from the [`BatchReceiver::recv_batch_timeout`] method.
///
/// A batch could not be received because the operation timed out or the channel
/// is empty and disconnected.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum RecvBatchTimeoutError {
    /// A batch could not be received because the operation timed out.
    Timeout,

    /// A batch could not be received because the channel is empty and
    /// disconnected.
    Disconnected,
}

impl<T> fmt::Debug for SendBatchError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected(_) => "Disconnected(..)".fmt(f),
            Self::Empty(_) => "Empty(..)".fmt(f),
            Self::TooLarge(_) => "TooLarge(..)".fmt(f),
        }
    }
}

impl<T> fmt::Display for SendBatchError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected(_) => "sending on a disconnected channel".fmt(f),
            Self::Empty(_) => "sending an empty batch".fmt(f),
            Self::TooLarge(_) => "sending a batch larger than the total capacity".fmt(f),
        }
    }
}

impl<T: Send> error::Error for SendBatchError<T> {}

impl<T> SendBatchError<T> {
    /// Unwraps the batch.
    pub fn into_inner(self) -> T {
        match self {
            Self::Disconnected(v) | Self::Empty(v) | Self::TooLarge(v) => v,
        }
    }

    /// Returns `true` if the send operation failed because the channel is
    /// disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }

    /// Returns `true` if the send operation failed because the batch is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }

    /// Returns `true` if the send operation failed because the batch exceeds the
    /// total capacity.
    pub fn is_too_large(&self) -> bool {
        matches!(self, Self::TooLarge(_))
    }
}

impl<T> fmt::Debug for TrySendBatchError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => "Full(..)".fmt(f),
            Self::Disconnected(_) => "Disconnected(..)".fmt(f),
            Self::Empty(_) => "Empty(..)".fmt(f),
            Self::TooLarge(_) => "TooLarge(..)".fmt(f),
        }
    }
}

impl<T> fmt::Display for TrySendBatchError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => "sending on a full channel".fmt(f),
            Self::Disconnected(_) => "sending on a disconnected channel".fmt(f),
            Self::Empty(_) => "sending an empty batch".fmt(f),
            Self::TooLarge(_) => "sending a batch larger than the total capacity".fmt(f),
        }
    }
}

impl<T: Send> error::Error for TrySendBatchError<T> {}

impl<T> From<SendBatchError<T>> for TrySendBatchError<T> {
    fn from(err: SendBatchError<T>) -> Self {
        match err {
            SendBatchError::Disconnected(v) => Self::Disconnected(v),
            SendBatchError::Empty(v) => Self::Empty(v),
            SendBatchError::TooLarge(v) => Self::TooLarge(v),
        }
    }
}

impl<T> TrySendBatchError<T> {
    /// Unwraps the batch.
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(v) | Self::Disconnected(v) | Self::Empty(v) | Self::TooLarge(v) => v,
        }
    }

    /// Returns `true` if the send operation failed because the channel is full.
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }

    /// Returns `true` if the send operation failed because the channel is
    /// disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }

    /// Returns `true` if the send operation failed because the batch is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }

    /// Returns `true` if the send operation failed because the batch exceeds the
    /// total capacity.
    pub fn is_too_large(&self) -> bool {
        matches!(self, Self::TooLarge(_))
    }
}

impl<T> fmt::Debug for SendBatchTimeoutError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(_) => "Timeout(..)".fmt(f),
            Self::Disconnected(_) => "Disconnected(..)".fmt(f),
            Self::Empty(_) => "Empty(..)".fmt(f),
            Self::TooLarge(_) => "TooLarge(..)".fmt(f),
        }
    }
}

impl<T> fmt::Display for SendBatchTimeoutError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(_) => "timed out waiting on send operation".fmt(f),
            Self::Disconnected(_) => "sending on a disconnected channel".fmt(f),
            Self::Empty(_) => "sending an empty batch".fmt(f),
            Self::TooLarge(_) => "sending a batch larger than the total capacity".fmt(f),
        }
    }
}

impl<T: Send> error::Error for SendBatchTimeoutError<T> {}

impl<T> From<SendBatchError<T>> for SendBatchTimeoutError<T> {
    fn from(err: SendBatchError<T>) -> Self {
        match err {
            SendBatchError::Disconnected(v) => Self::Disconnected(v),
            SendBatchError::Empty(v) => Self::Empty(v),
            SendBatchError::TooLarge(v) => Self::TooLarge(v),
        }
    }
}

impl<T> SendBatchTimeoutError<T> {
    /// Unwraps the batch.
    pub fn into_inner(self) -> T {
        match self {
            Self::Timeout(v) | Self::Disconnected(v) | Self::Empty(v) | Self::TooLarge(v) => v,
        }
    }

    /// Returns `true` if the send operation timed out.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// Returns `true` if the send operation failed because the channel is
    /// disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }

    /// Returns `true` if the send operation failed because the batch is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }

    /// Returns `true` if the send operation failed because the batch exceeds the
    /// total capacity.
    pub fn is_too_large(&self) -> bool {
        matches!(self, Self::TooLarge(_))
    }
}

impl fmt::Display for RecvBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "receiving on an empty and disconnected channel".fmt(f)
    }
}

impl error::Error for RecvBatchError {}

impl fmt::Display for TryRecvBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => "receiving on an empty channel".fmt(f),
            Self::Disconnected => "receiving on an empty and disconnected channel".fmt(f),
        }
    }
}

impl error::Error for TryRecvBatchError {}

impl From<RecvBatchError> for TryRecvBatchError {
    fn from(err: RecvBatchError) -> Self {
        match err {
            RecvBatchError => Self::Disconnected,
        }
    }
}

impl TryRecvBatchError {
    /// Returns `true` if the receive operation failed because the channel is
    /// empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Returns `true` if the receive operation failed because the channel is
    /// disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

impl fmt::Display for RecvBatchTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => "timed out waiting on receive operation".fmt(f),
            Self::Disconnected => "channel is empty and disconnected".fmt(f),
        }
    }
}

impl error::Error for RecvBatchTimeoutError {}

impl From<RecvBatchError> for RecvBatchTimeoutError {
    fn from(err: RecvBatchError) -> Self {
        match err {
            RecvBatchError => Self::Disconnected,
        }
    }
}

impl RecvBatchTimeoutError {
    /// Returns `true` if the receive operation timed out.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }

    /// Returns `true` if the receive operation failed because the channel is
    /// disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}
