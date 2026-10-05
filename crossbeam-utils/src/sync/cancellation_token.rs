//! A cancellation token that wakes every waiting thread.
//!
//! A [`CancellationToken`] is a one-shot broadcast signal: once
//! [cancelled](CancellationToken::cancel), *all* threads waiting on the token are
//! woken up, and every registered callback is invoked. This is in contrast to a
//! condition variable notification that wakes only one waiter.
//!
//! The token is created from a [`CancellationTokenSource`], which represents
//! ownership of the token. The source is cloneable; when the last source is
//! dropped, the token is cancelled automatically. Cloning a token does not
//! extend the lifetime of any source: tokens only hold a shared reference to the
//! underlying state, so dropping the last owner cancels the token even if tokens
//! are still alive.
//!
//! # Lifecycle
//!
//! 1. Create a [`CancellationTokenSource`] with [`CancellationTokenSource::new`].
//! 2. Hand out [`CancellationToken`]s to workers with
//!    [`CancellationTokenSource::token`]. Tokens can be cloned freely.
//! 3. Workers wait on the token with [`CancellationToken::wait`] or register a
//!    callback with [`CancellationToken::on_cancel`].
//! 4. Cancellation is triggered either explicitly with
//!    [`CancellationToken::cancel`] / [`CancellationTokenSource::cancel`], or
//!    automatically when the last [`CancellationTokenSource`] is dropped.
//! 5. Cancellation is permanent and idempotent: waiting after cancellation
//!    returns immediately, and callbacks registered after cancellation are
//!    invoked immediately.
//!
//! # Callbacks
//!
//! Callbacks are registered with [`CancellationToken::on_cancel`] and return a
//! [`CallbackHandle`]. The handle can:
//!
//! * [`revoke`](CallbackHandle::revoke) the callback *before* it starts running,
//!   in which case the callback is guaranteed never to be called;
//! * [`join`](CallbackHandle::join) the callback, waiting for it to finish and
//!   observing whether it ran successfully, panicked, or was revoked.
//!
//! Callbacks run outside of the token's internal lock, at most once, and a
//! panicking callback does not prevent other callbacks or waiters from being
//! notified. The panic is captured and returned from [`CallbackHandle::join`] as
//! an independent receipt.
//!
//! # Examples
//!
//! ```
//! use crossbeam_utils::sync::CancellationTokenSource;
//! use std::thread;
//!
//! let source = CancellationTokenSource::new();
//! let token = source.token();
//!
//! let handle = thread::spawn(move || {
//!     token.wait();
//!     // ... clean up ...
//! });
//!
//! source.cancel(); // wakes the waiting thread
//! handle.join().unwrap();
//! ```
//!
//! Last owner drop cancels the token:
//!
//! ```
//! use crossbeam_utils::sync::CancellationTokenSource;
//!
//! let source = CancellationTokenSource::new();
//! let token = source.token();
//! drop(source); // the last owner exits ...
//! assert!(token.is_cancelled()); // ... and the token is cancelled
//! ```

use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    any::Any,
    fmt, mem,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicUsize, Ordering},
};
use std::{
    panic,
    sync::{Condvar, Mutex},
    thread,
};

/// A cancellation token.
///
/// Waiting threads and registered callbacks are all woken up once the token is
/// cancelled. See the [module-level documentation](self) for details.
pub struct CancellationToken {
    inner: Arc<Inner>,
}

/// An owned handle controlling a [`CancellationToken`].
///
/// The source is cloneable. When the last source is dropped, the token is
/// cancelled automatically. See the [module-level documentation](self) for
/// details.
pub struct CancellationTokenSource {
    inner: Arc<Inner>,
}

/// A handle to a callback registered with [`CancellationToken::on_cancel`].
///
/// The handle can be used to [`revoke`](CallbackHandle::revoke) the callback
/// before it runs or to [`join`](CallbackHandle::join) it after it ran.
///
/// Dropping the handle does *not* revoke the callback; use
/// [`revoke`](CallbackHandle::revoke) to cancel the registration.
pub struct CallbackHandle {
    entry: Arc<CallbackEntry>,
}

/// The outcome of a cancellation callback, as observed by
/// [`CallbackHandle::join`].
#[derive(Debug)]
pub enum CallbackError {
    /// The callback panicked. The payload is the value the callback panicked
    /// with.
    Panic(Box<dyn Any + Send + 'static>),
    /// The callback was revoked before it could run, so it was never called.
    Revoked,
}

impl CallbackError {
    /// Returns `true` if the callback panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self, CallbackError::Panic(_))
    }

    /// Returns `true` if the callback was revoked before it could run.
    pub fn is_revoked(&self) -> bool {
        matches!(self, CallbackError::Revoked)
    }

    /// Returns a reference to the panic payload if the callback panicked.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send + 'static)> {
        match self {
            CallbackError::Panic(payload) => Some(&**payload),
            CallbackError::Revoked => None,
        }
    }

    /// Consumes the error and returns the panic payload if the callback
    /// panicked.
    pub fn into_panic_payload(self) -> Option<Box<dyn Any + Send + 'static>> {
        match self {
            CallbackError::Panic(payload) => Some(payload),
            CallbackError::Revoked => None,
        }
    }
}

impl fmt::Display for CallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallbackError::Panic(_) => write!(f, "cancellation callback panicked"),
            CallbackError::Revoked => {
                write!(f, "cancellation callback was revoked before running")
            }
        }
    }
}

impl std::error::Error for CallbackError {}

/// Internal state shared between all tokens and sources.
struct Inner {
    state: Mutex<State>,
    condvar: Condvar,
    /// Number of outstanding [`CancellationTokenSource`]s.
    owners: AtomicUsize,
}

struct State {
    cancelled: bool,
    /// Callbacks waiting to be run on cancellation.
    callbacks: Vec<Arc<CallbackEntry>>,
}

/// A single registered callback.
struct CallbackEntry {
    state: Mutex<EntryState>,
    done: Condvar,
}

struct EntryState {
    /// The callback, until it is taken either by cancellation or by revocation.
    callback: Option<Box<dyn FnOnce() + Send + 'static>>,
    /// The outcome once the callback has run or been revoked.
    outcome: Option<Outcome>,
}

enum Outcome {
    /// The callback ran; the value is the result of catching its panic.
    Ran(thread::Result<()>),
    /// The callback was revoked before it could run.
    Revoked,
}

impl Inner {
    fn new() -> Self {
        Self {
            state: Mutex::new(State {
                cancelled: false,
                callbacks: Vec::new(),
            }),
            condvar: Condvar::new(),
            owners: AtomicUsize::new(1),
        }
    }

    /// Cancels the token, waking all waiters and running all pending callbacks.
    ///
    /// Cancellation is idempotent: only the first call has an effect.
    fn cancel(&self) {
        let entries = {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return;
            }
            state.cancelled = true;
            self.condvar.notify_all();
            mem::take(&mut state.callbacks)
        };

        // Callbacks run outside of the lock, one at a time. A panicking
        // callback does not stop the others from running.
        for entry in entries {
            entry.run();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.state.lock().unwrap().cancelled
    }

    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.cancelled {
            state = self.condvar.wait(state).unwrap();
        }
    }

    fn on_cancel<F>(&self, callback: F) -> CallbackHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let entry = Arc::new(CallbackEntry {
            state: Mutex::new(EntryState {
                callback: Some(Box::new(callback)),
                outcome: None,
            }),
            done: Condvar::new(),
        });

        let already_cancelled = {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                true
            } else {
                state.callbacks.push(Arc::clone(&entry));
                false
            }
        };

        if already_cancelled {
            // The token is already cancelled: run the callback immediately,
            // still outside of the lock.
            entry.run();
        }

        CallbackHandle { entry }
    }
}

impl CallbackEntry {
    /// Runs the callback if it has not been revoked yet, catching any panic.
    fn run(&self) {
        let callback = self.state.lock().unwrap().callback.take();

        if let Some(callback) = callback {
            let outcome = Outcome::Ran(panic::catch_unwind(AssertUnwindSafe(callback)));
            let mut state = self.state.lock().unwrap();
            state.outcome = Some(outcome);
            self.done.notify_all();
        }
    }

    /// Revokes the callback before it starts running.
    ///
    /// Returns `true` if the callback was revoked before it could run, in which
    /// case it is guaranteed never to be called. Returns `false` if the callback
    /// had already started (or finished) running.
    fn revoke(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.callback.take().is_some() {
            state.outcome = Some(Outcome::Revoked);
            self.done.notify_all();
            true
        } else {
            false
        }
    }

    /// Waits until the callback has finished running or has been revoked.
    fn join(&self) -> Result<(), CallbackError> {
        let mut state = self.state.lock().unwrap();
        while state.outcome.is_none() {
            state = self.done.wait(state).unwrap();
        }

        match state.outcome.take().unwrap() {
            Outcome::Ran(result) => result.map_err(CallbackError::Panic),
            Outcome::Revoked => Err(CallbackError::Revoked),
        }
    }
}

impl CancellationTokenSource {
    /// Creates a new cancellation source.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::new()),
        }
    }

    /// Returns a [`CancellationToken`] that is cancelled when this source (or
    /// any of its clones) is dropped, or when [`cancel`](CancellationToken::cancel)
    /// is called.
    pub fn token(&self) -> CancellationToken {
        CancellationToken {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Cancels the token, waking all waiters and running all callbacks.
    ///
    /// Cancellation is idempotent: calling this method more than once has no
    /// additional effect.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Returns `true` if the token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

impl Default for CancellationTokenSource {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CancellationTokenSource {
    fn clone(&self) -> Self {
        self.inner.owners.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for CancellationTokenSource {
    fn drop(&mut self) {
        // When the last owner exits, the token is cancelled automatically.
        if self.inner.owners.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.cancel();
        }
    }
}

impl CancellationToken {
    /// Returns `true` if the token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Cancels the token, waking all waiters and running all callbacks.
    ///
    /// Cancellation is idempotent: calling this method more than once has no
    /// additional effect.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Blocks the current thread until the token is cancelled.
    ///
    /// If the token is already cancelled, this method returns immediately.
    pub fn wait(&self) {
        self.inner.wait();
    }

    /// Registers a callback to be run when the token is cancelled.
    ///
    /// The callback runs at most once, outside of the token's internal lock. If
    /// the token is already cancelled, the callback is run immediately.
    ///
    /// The returned [`CallbackHandle`] can be used to revoke the callback before
    /// it runs or to join it after it ran.
    pub fn on_cancel<F>(&self, callback: F) -> CallbackHandle
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.on_cancel(callback)
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl CallbackHandle {
    /// Revokes the callback before it starts running.
    ///
    /// Returns `true` if the callback was revoked before it could run, in which
    /// case it is guaranteed never to be called. Returns `false` if the callback
    /// had already started (or finished) running.
    pub fn revoke(&self) -> bool {
        self.entry.revoke()
    }

    /// Waits until the callback has finished running or has been revoked, and
    /// returns its outcome.
    ///
    /// # Errors
    ///
    /// Returns [`CallbackError::Panic`] if the callback panicked, or
    /// [`CallbackError::Revoked`] if it was revoked before it could run.
    pub fn join(self) -> Result<(), CallbackError> {
        self.entry.join()
    }
}

impl fmt::Debug for CancellationTokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationTokenSource")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl fmt::Debug for CallbackHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackHandle").finish()
    }
}
