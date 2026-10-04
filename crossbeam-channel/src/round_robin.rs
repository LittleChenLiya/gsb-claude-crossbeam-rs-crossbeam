//! Explicit round-robin receiver over multiple [`Receiver`]s.
//!
//! This module provides [`RoundRobinReceiver`], a receiver that polls a fixed list of
//! [`Receiver`]s in a fair round-robin order. Unlike [`Select`], which chooses a ready
//! operation at random, [`RoundRobinReceiver`] guarantees that already-ready channels are
//! visited in rotation so that no single channel can starve the others.
//!
//! # Two-phase receive
//!
//! Receiving proceeds in two phases:
//!
//! 1. **Pre-ready round-robin guarantee.** The receiver first scans every *enabled*,
//!    non-disconnected channel with [`try_recv`](Receiver::try_recv), starting immediately
//!    after the channel that last produced a message. The first channel that already holds a
//!    message wins, and the cursor is advanced past it. This phase never blocks and gives a
//!    strict fairness guarantee: over `N` receives, each channel is visited exactly once in
//!    order `0, 1, ..., N-1` (modulo channels that are disabled or have been fully drained and
//!    disconnected).
//!
//! 2. **Blocking wake-up selection.** If every enabled channel is empty, the receiver falls
//!    back to [`Select`] and blocks until at least one channel becomes ready. When several
//!    channels become ready at the same time, [`Select`]'s existing fairness rule applies (a
//!    random one among the ready operations is chosen). After [`Select`] returns, the cursor
//!    is updated to the selected channel before the message is consumed, so the next receive
//!    starts scanning from the channel after it.
//!
//! # Disconnection and draining
//!
//! A channel whose sender has been dropped is *not* removed immediately. The receiver keeps
//! polling it with `try_recv` so that any messages still queued in the channel are drained
//! first. Only once a channel is both empty and disconnected is it marked as disconnected and
//! skipped on subsequent scans. This guarantees that queue contents are never lost because of
//! a disconnection.
//!
//! # Close wake-up
//!
//! A dedicated internal close channel is registered with the blocking [`Select`]. Calling
//! [`close`](RoundRobinReceiver::close) sends a wake-up message that unblocks any parked
//! [`recv`](RoundRobinReceiver::recv) and makes it return [`RecvError`]. The closed state is
//! sticky: subsequent `recv` calls return [`RecvError`] immediately without blocking.

use crate::channel::{Receiver, Sender, bounded};
use crate::err::{RecvError, TryRecvError};
use crate::select::Select;
use alloc::vec::Vec;
use alloc::vec;
use core::fmt;

/// A handle that can [close](CloseHandle::close) a [`RoundRobinReceiver`] from another thread.
///
/// This is useful when the receiver is owned by one thread (which is parked in
/// [`recv`](RoundRobinReceiver::recv)) and another thread needs to wake it up by closing it.
///
/// # Examples
///
/// ```
/// use std::thread;
/// use crossbeam_channel::{unbounded, RoundRobinReceiver};
///
/// let (s, r) = unbounded::<i32>();
/// let mut rr = RoundRobinReceiver::new(vec![r]);
/// let handle = rr.close_handle();
///
/// thread::spawn(move || {
///     // `rr` is moved into this thread; `handle` can close it from the outside.
///     let _ = rr.recv();
/// });
///
/// handle.close();
/// ```
#[derive(Clone, Debug)]
pub struct CloseHandle {
    close_sender: Sender<()>,
}

impl CloseHandle {
    /// Closes the associated [`RoundRobinReceiver`].
    ///
    /// This has the same effect as [`RoundRobinReceiver::close`].
    pub fn close(&self) {
        let _ = self.close_sender.try_send(());
    }
}

/// An explicit round-robin receiver over a fixed list of [`Receiver`]s.
///
/// Channels are polled in rotation, starting after the channel that last produced a message,
/// so that already-ready channels are visited in fair round-robin order rather than being
/// starved by a fixed first channel. When all channels are empty, the receiver blocks using
/// [`Select`] and is woken up as soon as any channel becomes ready (or the receiver is
/// [closed](RoundRobinReceiver::close)).
///
/// See the [module-level documentation](self) for the two-phase receive semantics.
///
/// # Examples
///
/// ```
/// use crossbeam_channel::{unbounded, RoundRobinReceiver};
///
/// let (s0, r0) = unbounded();
/// let (s1, r1) = unbounded();
/// let (s2, r2) = unbounded();
///
/// s0.send(0).unwrap();
/// s1.send(1).unwrap();
/// s2.send(2).unwrap();
///
/// let mut rr = RoundRobinReceiver::new(vec![r0, r1, r2]);
///
/// assert_eq!(rr.recv(), Ok(0));
/// assert_eq!(rr.recv(), Ok(1));
/// assert_eq!(rr.recv(), Ok(2));
/// ```
pub struct RoundRobinReceiver<T> {
    /// The receivers to poll, in round-robin order.
    receivers: Vec<Receiver<T>>,

    /// Whether each receiver has been explicitly disabled.
    disabled: Vec<bool>,

    /// Whether each receiver is known to be empty and disconnected.
    disconnected: Vec<bool>,

    /// Index of the next channel to try.
    ///
    /// After a successful receive from channel `i`, this is set to `(i + 1) % n` so that the
    /// next scan starts immediately after it.
    cursor: usize,

    /// Sender half of the internal close wake-up channel.
    close_sender: Sender<()>,

    /// Receiver half of the internal close wake-up channel, registered with [`Select`].
    close_receiver: Receiver<()>,

    /// Whether the receiver has been closed.
    ///
    /// Once closed, every subsequent [`recv`](RoundRobinReceiver::recv) returns
    /// [`RecvError`] immediately.
    closed: bool,
}

impl<T> RoundRobinReceiver<T> {
    /// Creates a new round-robin receiver over the given list of receivers.
    ///
    /// The receivers are polled in the order they appear in the vector. The first
    /// [`recv`](RoundRobinReceiver::recv) starts scanning from index `0`.
    ///
    /// An empty vector is allowed: receiving from such a receiver returns [`RecvError`]
    /// immediately.
    pub fn new(receivers: Vec<Receiver<T>>) -> Self {
        let n = receivers.len();
        // A bounded channel with capacity 1 is enough: a single wake-up message is all we need.
        let (close_sender, close_receiver) = bounded(1);
        Self {
            receivers,
            disabled: vec![false; n],
            disconnected: vec![false; n],
            cursor: 0,
            close_sender,
            close_receiver,
            closed: false,
        }
    }

    /// Returns the number of receivers in the round-robin list.
    pub fn len(&self) -> usize {
        self.receivers.len()
    }

    /// Returns `true` if the round-robin list contains no receivers.
    pub fn is_empty(&self) -> bool {
        self.receivers.is_empty()
    }

    /// Disables the receiver at the given index.
    ///
    /// A disabled channel is skipped during round-robin scans and is not registered with the
    /// blocking [`Select`]. It can be re-enabled later with [`enable`](RoundRobinReceiver::enable).
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn disable(&mut self, index: usize) {
        assert!(index < self.receivers.len(), "index out of bounds");
        self.disabled[index] = true;
    }

    /// Enables the receiver at the given index.
    ///
    /// This undoes a previous [`disable`](RoundRobinReceiver::disable) call.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn enable(&mut self, index: usize) {
        assert!(index < self.receivers.len(), "index out of bounds");
        self.disabled[index] = false;
    }

    /// Returns `true` if the receiver at the given index is disabled.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn is_disabled(&self, index: usize) -> bool {
        assert!(index < self.receivers.len(), "index out of bounds");
        self.disabled[index]
    }

    /// Closes the receiver.
    ///
    /// This sends a wake-up message that unblocks any thread currently parked in
    /// [`recv`](RoundRobinReceiver::recv) and makes it return [`RecvError`]. The closed state is
    /// sticky: subsequent `recv` calls return [`RecvError`] immediately without blocking.
    ///
    /// Calling this method more than once has no additional effect.
    pub fn close(&self) {
        // `try_send` on a bounded(1) channel fails once the wake-up message is already queued;
        // that is fine because a single wake-up is sufficient.
        let _ = self.close_sender.try_send(());
    }

    /// Returns a [`CloseHandle`] that can close the receiver from another thread.
    ///
    /// This is useful when the receiver is owned by one thread (which is parked in
    /// [`recv`](RoundRobinReceiver::recv)) and another thread needs to wake it up.
    pub fn close_handle(&self) -> CloseHandle {
        CloseHandle {
            close_sender: self.close_sender.clone(),
        }
    }

    /// Receives a message from one of the receivers in round-robin order, blocking if necessary.
    ///
    /// See the [module-level documentation](self) for the two-phase receive semantics.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] if every receiver is empty and disconnected, or if the receiver
    /// has been [closed](RoundRobinReceiver::close).
    pub fn recv(&mut self) -> Result<T, RecvError> {
        if self.closed {
            return Err(RecvError);
        }

        loop {
            // Phase 1: non-blocking round-robin scan over already-ready channels.
            let n = self.receivers.len();
            for offset in 0..n {
                let i = (self.cursor + offset) % n;
                if self.disabled[i] || self.disconnected[i] {
                    continue;
                }
                match self.receivers[i].try_recv() {
                    Ok(msg) => {
                        self.cursor = (i + 1) % n;
                        return Ok(msg);
                    }
                    Err(TryRecvError::Disconnected) => {
                        // The channel is empty and disconnected. Mark it so it is skipped on
                        // subsequent scans. Note that any messages still queued in the channel
                        // were already drained by the successful `try_recv` calls above.
                        self.disconnected[i] = true;
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }

            // Check whether any channel is still connected (enabled and not disconnected).
            // This is computed *after* the scan so that channels which became disconnected
            // during the scan are correctly accounted for.
            let any_connected = (0..n).any(|i| !self.disabled[i] && !self.disconnected[i]);
            if !any_connected {
                // All receivers are disabled or disconnected (and empty).
                return Err(RecvError);
            }

            // Phase 2: block using `Select` until at least one channel becomes ready.
            let mut sel = Select::new();
            let mut receiver_indices = Vec::new();
            for i in 0..n {
                if self.disabled[i] || self.disconnected[i] {
                    continue;
                }
                sel.recv(&self.receivers[i]);
                receiver_indices.push(i);
            }
            let close_idx = sel.recv(&self.close_receiver);

            let oper = sel.select();
            let sel_idx = oper.index();

            if sel_idx == close_idx {
                // Consume the close message to complete the selected operation, then latch the
                // closed state so subsequent calls return immediately.
                let _ = oper.recv(&self.close_receiver);
                self.closed = true;
                return Err(RecvError);
            }

            let i = receiver_indices[sel_idx];
            match oper.recv(&self.receivers[i]) {
                Ok(msg) => {
                    // Update the cursor to the channel after the selected one, as required by
                    // the round-robin guarantee.
                    self.cursor = (i + 1) % n;
                    return Ok(msg);
                }
                Err(RecvError) => {
                    // The channel became disconnected (and is now empty). Mark it and retry.
                    self.disconnected[i] = true;
                }
            }
        }
    }

    /// Attempts to receive a message from one of the receivers in round-robin order without
    /// blocking.
    ///
    /// This performs only the non-blocking pre-ready scan (phase 1). If no channel is ready,
    /// it returns [`TryRecvError::Empty`] instead of blocking.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] if no channel has a message, or
    /// [`TryRecvError::Disconnected`] if every channel is empty and disconnected.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.closed {
            return Err(TryRecvError::Disconnected);
        }

        let n = self.receivers.len();
        let mut any_empty = false;
        for offset in 0..n {
            let i = (self.cursor + offset) % n;
            if self.disabled[i] {
                // A disabled channel may be re-enabled later, so treat it as potentially
                // having messages.
                any_empty = true;
                continue;
            }
            if self.disconnected[i] {
                continue;
            }
            match self.receivers[i].try_recv() {
                Ok(msg) => {
                    self.cursor = (i + 1) % n;
                    return Ok(msg);
                }
                Err(TryRecvError::Disconnected) => {
                    self.disconnected[i] = true;
                }
                Err(TryRecvError::Empty) => {
                    any_empty = true;
                }
            }
        }

        if any_empty {
            Err(TryRecvError::Empty)
        } else {
            Err(TryRecvError::Disconnected)
        }
    }
}

impl<T> fmt::Debug for RoundRobinReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoundRobinReceiver")
            .field("len", &self.receivers.len())
            .field("cursor", &self.cursor)
            .field("closed", &self.closed)
            .finish()
    }
}
