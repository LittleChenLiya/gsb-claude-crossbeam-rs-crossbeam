//! Explicit round-robin receiving across multiple channels.
//!
//! [`RoundRobin`] combines several [`Receiver`]s into one explicit rotation
//! receiver. Each successful receive moves a cursor to the channel right after
//! the one that produced the message, and the next receive starts scanning
//! from there, so a permanently-ready first channel cannot starve the others.
//!
//! # Pre-ready round-robin guarantee
//!
//! If one or more channels already hold queued messages when [`recv`] is
//! called, the next message is taken from the first ready channel in
//! round-robin order, starting just after the channel that produced the
//! previous message. This phase is fully deterministic: with three channels
//! that each hold two messages, the channels are consumed in the order
//! `0, 1, 2, 0, 1, 2`.
//!
//! # Blocking wakeup selection
//!
//! If every live channel is empty, [`recv`] parks the current thread in a
//! [`Select`] over all channels plus an internal shutdown channel. When
//! several channels become ready while [`recv`] is blocked, the winner is
//! chosen by the `Select` selection rules (its built-in pseudo-random
//! fairness), *not* by the round-robin cursor. After the wakeup, the cursor
//! is moved past the channel that won, so subsequent pre-ready receives
//! resume the strict rotation from there.
//!
//! # Disconnection
//!
//! A disconnected channel keeps delivering the messages that are already
//! queued: [`try_recv`](Receiver::try_recv) drains the queue before reporting
//! disconnection, and [`RoundRobin`] only removes a channel once it is both
//! disconnected and drained, so no queued message is ever lost because of a
//! disconnect. Once the last channel has been removed (or the set was empty
//! to begin with), [`recv`] returns [`RoundRobinError::Disconnected`]
//! immediately instead of blocking.
//!
//! # Shutdown
//!
//! [`shutdown`](RoundRobin::shutdown) (or a cloned [`ShutdownHandle`]) wakes
//! up a [`recv`] that is blocked in `Select`, so no receiver is ever left
//! blocked forever. Shutdown is latched: once observed, every subsequent
//! [`recv`] returns [`RoundRobinError::Shutdown`].
//!
//! [`recv`]: RoundRobin::recv
//!
//! # Examples
//!
//! ```
//! use crossbeam_channel::{unbounded, RoundRobin};
//!
//! let (s0, r0) = unbounded();
//! let (s1, r1) = unbounded();
//!
//! s0.send("a").unwrap();
//! s1.send("b").unwrap();
//! s0.send("c").unwrap();
//!
//! let mut rr = RoundRobin::new(vec![r0, r1]);
//! assert_eq!(rr.recv(), Ok((0, "a")));
//! assert_eq!(rr.recv(), Ok((1, "b")));
//! assert_eq!(rr.recv(), Ok((0, "c")));
//! ```

use alloc::vec::Vec;
use core::fmt;
use std::error;

use crate::channel::{bounded, Receiver, Sender};
use crate::err::{RecvError, TryRecvError};
use crate::select::Select;

/// An error returned from [`RoundRobin::recv`].
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum RoundRobinError {
    /// Every channel has been disconnected and fully drained (or the set of
    /// channels was empty to begin with).
    Disconnected,

    /// Shutdown was requested through [`RoundRobin::shutdown`] or a
    /// [`ShutdownHandle`].
    Shutdown,
}

impl fmt::Display for RoundRobinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoundRobinError::Disconnected => {
                write!(f, "all channels are disconnected and drained")
            }
            RoundRobinError::Shutdown => {
                write!(f, "the round-robin receiver has been shut down")
            }
        }
    }
}

impl error::Error for RoundRobinError {}

/// A clonable handle used to wake up a blocked [`RoundRobin::recv`].
///
/// Obtained from [`RoundRobin::shutdown_handle`]. Cloning the handle creates
/// another entry point to the same shutdown signal.
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    shutdown_tx: Sender<()>,
}

impl ShutdownHandle {
    /// Requests shutdown, waking up a blocked [`RoundRobin::recv`].
    ///
    /// This method never blocks: if a shutdown request is already pending,
    /// the call is a no-op.
    pub fn shutdown(&self) {
        // Ignore `Full` (a request is already pending) and `Disconnected`
        // (the `RoundRobin` was dropped).
        let _ = self.shutdown_tx.try_send(());
    }
}

/// An explicit round-robin receiver over multiple [`Receiver`]s.
///
/// See the [module-level documentation](self) for the exact receiving rules.
#[derive(Debug)]
pub struct RoundRobin<T> {
    /// Live channels, in their original relative order.
    receivers: Vec<Receiver<T>>,

    /// Index of the channel at which the next receive starts scanning.
    cursor: usize,

    /// Sending side of the internal shutdown signal.
    shutdown_tx: Sender<()>,

    /// Receiving side of the internal shutdown signal.
    shutdown_rx: Receiver<()>,

    /// Set once a shutdown request has been observed by `recv`.
    shutdown: bool,
}

impl<T> RoundRobin<T> {
    /// Creates a round-robin receiver over `receivers`.
    ///
    /// The position of a receiver in the vector defines its slot in the
    /// rotation. An empty vector is allowed: [`recv`] then immediately
    /// returns [`RoundRobinError::Disconnected`] without ever blocking.
    ///
    /// [`recv`]: RoundRobin::recv
    pub fn new(receivers: Vec<Receiver<T>>) -> Self {
        let (shutdown_tx, shutdown_rx) = bounded(1);
        RoundRobin {
            receivers,
            cursor: 0,
            shutdown_tx,
            shutdown_rx,
            shutdown: false,
        }
    }

    /// Returns the number of live (not yet removed) channels.
    pub fn len(&self) -> usize {
        self.receivers.len()
    }

    /// Returns `true` if there are no live channels left.
    pub fn is_empty(&self) -> bool {
        self.receivers.is_empty()
    }

    /// Returns `true` if shutdown has been requested and observed.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Returns a handle that can request shutdown from another thread.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    /// Requests shutdown, waking up a blocked [`recv`] if there is one.
    ///
    /// [`recv`]: RoundRobin::recv
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.try_send(());
    }

    /// Receives the next message in round-robin order.
    ///
    /// Returns the index of the channel the message came from (in the
    /// current, compacted channel list) along with the message.
    ///
    /// See the [module-level documentation](self) for the distinction
    /// between the deterministic pre-ready round-robin phase and the
    /// `Select`-driven blocking wakeup phase.
    pub fn recv(&mut self) -> Result<(usize, T), RoundRobinError> {
        // Latch a pending shutdown request before doing anything else, so a
        // shutdown that raced with a receive is never silently dropped.
        if !self.shutdown && self.shutdown_rx.try_recv().is_ok() {
            self.shutdown = true;
        }
        if self.shutdown {
            return Err(RoundRobinError::Shutdown);
        }

        loop {
            if self.receivers.is_empty() {
                return Err(RoundRobinError::Disconnected);
            }

            // Pre-ready phase: strict round-robin over the channels that
            // already hold a message, starting at the cursor.
            let n = self.receivers.len();
            let mut removed = false;
            for step in 0..n {
                let i = (self.cursor + step) % n;
                match self.receivers[i].try_recv() {
                    Ok(msg) => {
                        self.cursor = (i + 1) % n;
                        return Ok((i, msg));
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        // `try_recv` returns queued messages before reporting
                        // disconnection, so this channel is already drained
                        // and nothing is lost by removing it.
                        self.remove_at(i);
                        removed = true;
                        break;
                    }
                }
            }
            if removed {
                continue;
            }

            // Blocking phase: every live channel is currently empty, so park
            // in `Select` over all of them plus the shutdown signal. If
            // several channels become ready at once, the `Select` selection
            // rules pick the winner.
            let mut sel = Select::new();
            let mut indices = Vec::with_capacity(self.receivers.len() + 1);
            for r in &self.receivers {
                indices.push(sel.recv(r));
            }
            let shutdown_index = sel.recv(&self.shutdown_rx);

            let oper = sel.select();
            let index = oper.index();

            if index == shutdown_index {
                // Consume the token so the shutdown state is observed
                // exactly once here, then latch it.
                let _ = oper.recv(&self.shutdown_rx);
                self.shutdown = true;
                return Err(RoundRobinError::Shutdown);
            }

            let pos = indices
                .iter()
                .position(|&i| i == index)
                .expect("Select returned an unknown operation index");
            match oper.recv(&self.receivers[pos]) {
                Ok(msg) => {
                    // Update the cursor only after the wakeup has been
                    // resolved, so the next receive continues right after
                    // the channel that won the wakeup.
                    self.cursor = (pos + 1) % self.receivers.len();
                    return Ok((pos, msg));
                }
                Err(RecvError) => {
                    // The channel disconnected while blocked and its queue
                    // is drained; remove it and retry.
                    self.remove_at(pos);
                }
            }
        }
    }

    /// Removes the channel at `index`, keeping the cursor pointed at the
    /// same logical next channel.
    fn remove_at(&mut self, index: usize) {
        self.receivers.remove(index);
        let n = self.receivers.len();
        if n == 0 {
            self.cursor = 0;
        } else {
            if index < self.cursor {
                self.cursor -= 1;
            }
            self.cursor %= n;
        }
    }
}
