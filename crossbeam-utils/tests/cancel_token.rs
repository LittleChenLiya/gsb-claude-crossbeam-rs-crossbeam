use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crossbeam_utils::sync::CancelToken;

/// One explicit `cancel()` must wake *all* waiters, not just one.
#[test]
fn cancel_wakes_all_waiters() {
    const WAITERS: usize = 3;

    let owner = CancelToken::new();
    // Barrier 1: every waiter is about to park. Barrier 2: every waiter woke.
    let start = Arc::new(Barrier::new(WAITERS + 1));
    let done = Arc::new(Barrier::new(WAITERS + 1));
    let woke = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..WAITERS)
        .map(|_| {
            let token = owner.token();
            let start = start.clone();
            let done = done.clone();
            let woke = woke.clone();
            thread::spawn(move || {
                start.wait();
                token.wait();
                woke.fetch_add(1, Ordering::SeqCst);
                done.wait();
            })
        })
        .collect();

    // All waiters are parked (or about to check the flag); a single cancel
    // must release every one of them.
    start.wait();
    owner.cancel();
    assert!(owner.is_cancelled());

    // If cancel only woke a single waiter, this barrier would never complete.
    done.wait();
    assert_eq!(woke.load(Ordering::SeqCst), WAITERS);
    for h in handles {
        h.join().unwrap();
    }
}

/// A revoked callback must run 0 times and leave no receipt.
#[test]
fn revoked_callback_never_runs() {
    let owner = CancelToken::new();
    let token = owner.token();

    let ran = Arc::new(AtomicUsize::new(0));
    let ran2 = ran.clone();
    let reg = token.register(move || -> Result<(), ()> {
        ran2.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });

    // Revoke wins the race before cancellation starts executing callbacks.
    assert!(reg.revoke());

    owner.cancel();
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(owner.take_receipts().is_empty());

    // Revoking after cancellation loses the race.
    let reg = token.register(|| -> Result<(), ()> { Ok(()) });
    owner.cancel(); // idempotent: second cancel is a no-op
    assert!(!reg.revoke());
}

/// A callback that reenters `cancel()` must not deadlock.
#[test]
fn reentrant_cancel_does_not_deadlock() {
    let owner = CancelToken::new();
    let token = owner.token();

    let owner_in_cb = owner.clone();
    let token_in_cb = owner.token();
    token.register(move || -> Result<(), ()> {
        // Reentrant cancel from inside a callback: callbacks run outside the
        // lock, so this returns immediately instead of deadlocking.
        owner_in_cb.cancel();
        // Reentrant wait also returns immediately (already cancelled).
        token_in_cb.wait();
        Ok(())
    });

    owner.cancel();
    let receipts = owner.take_receipts();
    assert_eq!(receipts.len(), 1);
    assert!(receipts[0].is_ok());
}

/// Dropping the last owner cancels the token and wakes all waiters.
#[test]
fn last_owner_drop_cancels() {
    const WAITERS: usize = 3;

    let owner = CancelToken::new();
    let start = Arc::new(Barrier::new(WAITERS + 1));
    let done = Arc::new(Barrier::new(WAITERS + 1));

    let handles: Vec<_> = (0..WAITERS)
        .map(|_| {
            let token = owner.token();
            let start = start.clone();
            let done = done.clone();
            thread::spawn(move || {
                start.wait();
                token.wait();
                done.wait();
            })
        })
        .collect();

    // Tokens held by the waiters must not extend the owner's lifetime:
    // dropping every owner (here: the only one) cancels the token.
    start.wait();
    drop(owner);

    done.wait();
    for h in handles {
        h.join().unwrap();
    }
}

/// Tokens are weak observers: they never keep the owner alive.
#[test]
fn token_does_not_extend_owner_lifetime() {
    let token = {
        let owner = CancelToken::new();
        let token = owner.token();
        assert!(!token.is_cancelled());
        drop(owner);
        token
    };
    // The owner is gone, so the token observes cancellation even though the
    // token itself is still alive.
    assert!(token.is_cancelled());
    token.wait(); // returns immediately
}

/// A failing callback keeps its own independent receipt and does not stop
/// the other callbacks from running.
#[test]
fn failure_keeps_independent_receipt() {
    let owner = CancelToken::new();
    let token = owner.token();

    let ran = Arc::new(AtomicUsize::new(0));

    let r1 = token.register({
        let ran = ran.clone();
        move || -> Result<(), ()> {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let r2 = token.register(|| -> Result<(), String> { Err("boom".to_string()) });
    let r3 = token.register({
        let ran = ran.clone();
        move || -> Result<(), ()> {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

    owner.cancel();

    // The failure did not prevent the other notifications.
    assert_eq!(ran.load(Ordering::SeqCst), 2);

    // Each executed callback left exactly one independent receipt.
    let receipts = owner.take_receipts();
    assert_eq!(receipts.len(), 3);
    assert_eq!(receipts[0].id(), r1.id());
    assert!(receipts[0].is_ok());
    assert_eq!(receipts[1].id(), r2.id());
    assert!(receipts[1].is_err());
    assert_eq!(receipts[2].id(), r3.id());
    assert!(receipts[2].is_ok());

    // The failure payload is preserved in its own receipt.
    let err = receipts[1].outcome().as_ref().unwrap_err();
    assert_eq!(
        err.payload().downcast_ref::<String>().map(String::as_str),
        Some("boom")
    );
    assert!(owner.take_receipts().is_empty());
}

/// A panicking callback is caught, recorded as a failed receipt, and does
/// not stop the remaining callbacks.
#[test]
fn panicking_callback_does_not_stop_others() {
    let owner = CancelToken::new();
    let token = owner.token();

    let ran = Arc::new(AtomicUsize::new(0));
    token.register(|| -> Result<(), ()> { panic!("callback panic") });
    token.register({
        let ran = ran.clone();
        move || -> Result<(), ()> {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

    owner.cancel();
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    let receipts = owner.take_receipts();
    assert_eq!(receipts.len(), 2);
    assert!(receipts[0].is_err());
    assert!(receipts[1].is_ok());
}

/// Registering on an already-cancelled token notifies immediately.
#[test]
fn register_after_cancel_runs_immediately() {
    let owner = CancelToken::new();
    let token = owner.token();
    owner.cancel();

    let ran = Arc::new(AtomicUsize::new(0));
    let reg = token.register({
        let ran = ran.clone();
        move || -> Result<(), ()> {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    // Ran synchronously, before `register` even returned.
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    // Too late to revoke: the callback already fired.
    assert!(!reg.revoke());

    let receipts = owner.take_receipts();
    assert_eq!(receipts.len(), 1);
    assert!(receipts[0].is_ok());
}

/// Waiters blocked in `wait()` observe cancellation from another thread.
#[test]
fn wait_blocks_until_cancel() {
    let owner = CancelToken::new();
    let token = owner.token();
    let (tx, rx) = std::sync::mpsc::channel();

    let handle = thread::spawn(move || {
        token.wait();
        tx.send(()).unwrap();
    });

    thread::sleep(Duration::from_millis(100));
    assert!(rx.try_recv().is_err());

    owner.cancel();
    rx.recv().unwrap();
    handle.join().unwrap();
}
