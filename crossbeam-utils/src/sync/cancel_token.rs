//! A cancellation token that broadcasts to *all* waiters.
//!
//! # Lifecycle
//!
//! * [`CancelToken`] is the **owner** of the cancellation state. Cloning a
//!   `CancelToken` creates another owner. Cancellation happens exactly once,
//!   either when [`CancelToken::cancel`] is called explicitly, or when the
//!   **last owner is dropped** (implicit cancel-on-exit).
//! * [`Token`] is a weak **observer**: it can [`wait`](Token::wait) for
//!   cancellation and [`register`](Token::register) callbacks, but it holds
//!   only a weak reference to the shared state, so tokens never extend the
//!   lifetime of the owner. Once every owner is gone, all tokens observe
//!   cancellation immediately.
//! * Cancellation wakes **every** waiter (`Condvar::notify_all`), not just
//!   one: there is no "one message wakes one thread" semantics here.
//!
//! # Callbacks
//!
//! * [`Token::register`] returns a [`Registration`] handle. Calling
//!   [`Registration::revoke`] before the callback starts executing guarantees
//!   the callback is never called (revoke either wins or loses the race
//!   against cancellation, never both).
//! * Registering on an already-cancelled token runs the callback immediately
//!   on the registering thread.
//! * Callbacks always run **outside** the internal lock and at most once, so
//!   a callback may safely call [`CancelToken::cancel`], [`Token::wait`] or
//!   [`Token::register`] again without deadlocking.
//! * A failing callback (returning an `Err` or panicking) never prevents the
//!   remaining callbacks from running. Every executed callback leaves an
//!   independent [`CallbackReceipt`] recording its own outcome, retrievable
//!   with [`CancelToken::take_receipts`] or [`Token::take_receipts`].

use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;
use core::panic::AssertUnwindSafe;
use core::sync::atomic::{AtomicUsize, Ordering, fence};

use std::panic::catch_unwind;
use std::sync::{Condvar, Mutex, MutexGuard};

/// A boxed cancellation callback.
type Callback = Box<dyn FnOnce() -> Result<(), CallbackFailure> + Send>;

/// Shared state behind a [`CancelToken`]/[`Token`] pair.
struct Inner {
    state: Mutex<State>,
    cvar: Condvar,
    /// Number of live [`CancelToken`] owners. Reaching zero triggers cancel.
    owners: AtomicUsize,
}

struct State {
    cancelled: bool,
    /// Pending callbacks, executed in registration order (FIFO).
    callbacks: Vec<(u64, Callback)>,
    /// Receipts of callbacks that have already executed.
    receipts: Vec<CallbackReceipt>,
    next_id: u64,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Callbacks never run while the lock is held, so the mutex can only
        // be poisoned by a panic in our own (infallible) critical sections.
        // Ignore poisoning rather than cascading a failure into waiters.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Runs `cancel` on the shared state. Idempotent and reentrant: the second
/// and later calls observe `cancelled == true` and return immediately.
fn cancel_inner(inner: &Inner) {
    let callbacks = {
        let mut st = inner.lock();
        if st.cancelled {
            return;
        }
        st.cancelled = true;
        core::mem::take(&mut st.callbacks)
    };
    // Wake *all* waiters, not just one.
    inner.cvar.notify_all();
    if callbacks.is_empty() {
        return;
    }
    // Execute callbacks outside the lock so they may reenter the token.
    let mut receipts = Vec::with_capacity(callbacks.len());
    for (id, cb) in callbacks {
        // A panicking callback is just another kind of failure: it must not
        // stop the remaining notifications.
        let outcome = catch_unwind(AssertUnwindSafe(cb))
            .unwrap_or_else(|payload| Err(CallbackFailure::panicked(payload)));
        receipts.push(CallbackReceipt { id, outcome });
    }
    let mut st = inner.lock();
    st.receipts.extend(receipts);
}

/// Runs a callback that was registered after cancellation, and records its
/// receipt if the shared state is still alive.
fn run_now(inner: Option<&Inner>, id: u64, cb: Callback) {
    let outcome = catch_unwind(AssertUnwindSafe(cb))
        .unwrap_or_else(|payload| Err(CallbackFailure::panicked(payload)));
    if let Some(inner) = inner {
        let mut st = inner.lock();
        st.receipts.push(CallbackReceipt { id, outcome });
    }
}

/// The owner side of a cancellation token.
///
/// Explicitly calling [`cancel`](CancelToken::cancel) — or dropping the last
/// owner — cancels the token and wakes **all** waiters and callbacks.
///
/// # Examples
///
/// ```
/// use crossbeam_utils::sync::CancelToken;
/// use std::sync::{Arc, Barrier};
/// use std::thread;
///
/// let owner = CancelToken::new();
/// let barrier = Arc::new(Barrier::new(2));
///
/// let token = owner.token();
/// let b = barrier.clone();
/// let handle = thread::spawn(move || {
///     b.wait(); // make sure the waiter is parked before we cancel
///     token.wait();
/// });
///
/// barrier.wait();
/// owner.cancel(); // wakes every waiter at once
/// handle.join().unwrap();
/// ```
pub struct CancelToken {
    inner: Arc<Inner>,
}

impl CancelToken {
    /// Creates a new, non-cancelled token owner.
    pub fn new() -> Self {
        CancelToken {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    cancelled: false,
                    callbacks: Vec::new(),
                    receipts: Vec::new(),
                    next_id: 0,
                }),
                cvar: Condvar::new(),
                owners: AtomicUsize::new(1),
            }),
        }
    }

    /// Creates a weak observer token.
    ///
    /// The returned [`Token`] can wait and register callbacks, but it does
    /// **not** keep the owner alive: when the last `CancelToken` is dropped,
    /// the token is cancelled regardless of how many observers exist.
    pub fn token(&self) -> Token {
        Token {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Cancels the token, waking all waiters and running all pending
    /// callbacks (outside the lock, in registration order).
    ///
    /// Idempotent: subsequent calls — including reentrant calls from inside
    /// a callback — return immediately without deadlocking.
    pub fn cancel(&self) {
        cancel_inner(&self.inner);
    }

    /// Returns `true` if the token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.lock().cancelled
    }

    /// Drains the receipts of all callbacks executed so far.
    ///
    /// Each executed callback leaves exactly one independent receipt, so a
    /// failing callback's failure is preserved alongside — and does not hide
    /// — the receipts of the others.
    pub fn take_receipts(&self) -> Vec<CallbackReceipt> {
        let mut st = self.inner.lock();
        core::mem::take(&mut st.receipts)
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CancelToken {
    /// Cloning creates another owner, delaying the implicit cancel-on-drop.
    fn clone(&self) -> Self {
        self.inner.owners.fetch_add(1, Ordering::Relaxed);
        CancelToken {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for CancelToken {
    fn drop(&mut self) {
        // Last owner exiting cancels the token, exactly like `cancel()`.
        if self.inner.owners.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            cancel_inner(&self.inner);
        }
    }
}

impl fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// A weak observer of a [`CancelToken`].
///
/// A `Token` can wait for cancellation and register callbacks, but never
/// extends the lifetime of the owner: once the last [`CancelToken`] is
/// dropped, every `Token` observes cancellation.
#[derive(Clone)]
pub struct Token {
    inner: Weak<Inner>,
}

impl Token {
    /// Blocks the current thread until the token is cancelled.
    ///
    /// Returns immediately if the token is already cancelled or every owner
    /// has been dropped.
    pub fn wait(&self) {
        let inner = match self.inner.upgrade() {
            Some(inner) => inner,
            // All owners (and the state) are gone: that only happens after
            // cancellation completed.
            None => return,
        };
        let mut st = inner.lock();
        while !st.cancelled {
            st = inner.cvar.wait(st).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Returns `true` if the token has been cancelled (or every owner is
    /// gone, which implies cancellation).
    pub fn is_cancelled(&self) -> bool {
        match self.inner.upgrade() {
            Some(inner) => inner.lock().cancelled,
            None => true,
        }
    }

    /// Registers a callback to run when the token is cancelled.
    ///
    /// * If the token is already cancelled, the callback runs immediately on
    ///   the current thread.
    /// * Otherwise the callback runs at most once, outside the internal
    ///   lock, unless the returned [`Registration`] is revoked first.
    pub fn register<F, E>(&self, f: F) -> Registration
    where
        F: FnOnce() -> Result<(), E> + Send + 'static,
        E: Send + 'static,
    {
        let cb: Callback = Box::new(move || f().map_err(CallbackFailure::new));
        let inner = match self.inner.upgrade() {
            // Owners are gone, hence cancelled: notify immediately. There is
            // no state left to store the receipt in.
            None => {
                run_now(None, u64::MAX, cb);
                return Registration {
                    inner: Weak::new(),
                    id: u64::MAX,
                };
            }
            Some(inner) => inner,
        };
        let id = {
            let mut st = inner.lock();
            let id = st.next_id;
            st.next_id += 1;
            if st.cancelled {
                // Already cancelled: notify immediately, outside the lock.
                drop(st);
                run_now(Some(&inner), id, cb);
            } else {
                st.callbacks.push((id, cb));
            }
            id
        };
        Registration {
            inner: self.inner.clone(),
            id,
        }
    }

    /// Drains the receipts of all callbacks executed so far.
    ///
    /// Returns an empty vector if the shared state is already gone.
    pub fn take_receipts(&self) -> Vec<CallbackReceipt> {
        match self.inner.upgrade() {
            Some(inner) => {
                let mut st = inner.lock();
                core::mem::take(&mut st.receipts)
            }
            None => Vec::new(),
        }
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// A handle that can revoke a registered callback before it starts running.
pub struct Registration {
    inner: Weak<Inner>,
    id: u64,
}

impl Registration {
    /// The id of the registered callback, matching [`CallbackReceipt::id`].
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Revokes the registration.
    ///
    /// Returns `true` if the revocation won the race against cancellation:
    /// the callback is then guaranteed to never be called. Returns `false`
    /// if the callback was already taken for execution (or has already run).
    pub fn revoke(self) -> bool {
        if let Some(inner) = self.inner.upgrade() {
            let mut st = inner.lock();
            if let Some(pos) = st.callbacks.iter().position(|(id, _)| *id == self.id) {
                // Dropping the entry drops the callback without calling it.
                drop(st.callbacks.remove(pos));
                return true;
            }
        }
        false
    }
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("id", &self.id)
            .finish()
    }
}

/// A failure reported by a cancellation callback.
///
/// Wraps either the `Err` value returned by the callback or the payload of a
/// panic that was caught while running it.
pub struct CallbackFailure {
    payload: Box<dyn Any + Send + 'static>,
}

impl CallbackFailure {
    /// Creates a failure from any `Send` payload.
    pub fn new(payload: impl Any + Send + 'static) -> Self {
        CallbackFailure {
            payload: Box::new(payload),
        }
    }

    fn panicked(payload: Box<dyn Any + Send + 'static>) -> Self {
        CallbackFailure { payload }
    }

    /// Returns the failure payload.
    pub fn payload(&self) -> &(dyn Any + Send + 'static) {
        &*self.payload
    }

    /// Consumes the failure and returns the payload.
    pub fn into_payload(self) -> Box<dyn Any + Send + 'static> {
        self.payload
    }
}

impl fmt::Debug for CallbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackFailure").finish_non_exhaustive()
    }
}

impl fmt::Display for CallbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(s) = self.payload.downcast_ref::<&str>() {
            write!(f, "callback failed: {s}")
        } else if let Some(s) = self.payload.downcast_ref::<String>() {
            write!(f, "callback failed: {s}")
        } else {
            write!(f, "callback failed")
        }
    }
}

/// The independent receipt left by a single executed callback.
///
/// Every callback that runs leaves exactly one receipt, so a failure is
/// preserved per-callback and never masks the outcome of the others.
pub struct CallbackReceipt {
    id: u64,
    outcome: Result<(), CallbackFailure>,
}

impl CallbackReceipt {
    /// The id of the callback, matching [`Registration::id`].
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns `true` if the callback completed successfully.
    pub fn is_ok(&self) -> bool {
        self.outcome.is_ok()
    }

    /// Returns `true` if the callback failed or panicked.
    pub fn is_err(&self) -> bool {
        self.outcome.is_err()
    }

    /// Returns the outcome by reference.
    pub fn outcome(&self) -> &Result<(), CallbackFailure> {
        &self.outcome
    }

    /// Consumes the receipt and returns the outcome.
    pub fn into_outcome(self) -> Result<(), CallbackFailure> {
        self.outcome
    }
}

impl fmt::Debug for CallbackReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackReceipt")
            .field("id", &self.id)
            .field("is_ok", &self.is_ok())
            .finish()
    }
}
