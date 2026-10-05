//! Tests for the cancellation token.
//!
//! The tests use [`Barrier`]s to deterministically synchronize the waiting
//! threads: every waiter passes the barrier immediately before calling
//! [`CancellationToken::wait`], so by the time the main thread reaches the
//! barrier all waiters are about to park.

use std::{
    any::Any,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use crossbeam_utils::sync::CancellationTokenSource;

/// Waits for a message, failing the test if it takes too long.
fn recv_timeout<T>(rx: &mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5))
        .expect("timed out waiting for a waiting thread to wake up")
}

/// Three waiters are all woken up by a single explicit cancel.
#[test]
fn cancel_wakes_all_three_waiters() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let (tx, rx) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(4)); // 3 waiters + main

    for _ in 0..3 {
        let token = token.clone();
        let tx = tx.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            token.wait();
            tx.send(()).unwrap();
        });
    }
    drop(tx);

    barrier.wait();
    // All waiters are now about to park. Give them a moment to actually block.
    thread::sleep(Duration::from_millis(50));

    // No waiter should have woken up yet.
    assert!(rx.try_recv().is_err());

    source.cancel();

    // All three waiters must be woken up by the single cancel.
    for _ in 0..3 {
        recv_timeout(&rx);
    }
    assert!(rx.try_recv().is_err());
}

/// A token can also be cancelled directly.
#[test]
fn token_cancel_wakes_waiters() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let (tx, rx) = mpsc::channel();

    let waiter = token.clone();
    let t = thread::spawn(move || {
        waiter.wait();
        tx.send(()).unwrap();
    });

    thread::sleep(Duration::from_millis(50));
    token.cancel();
    recv_timeout(&rx);
    t.join().unwrap();
}

/// A revoked callback is never called: it runs zero times.
#[test]
fn revoked_callback_runs_zero_times() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..3 {
        let counter = counter.clone();
        let handle = token.on_cancel(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        // Revocation wins before the callback starts running.
        assert!(handle.revoke());
        assert!(!handle.revoke()); // revoking twice is a no-op
        handles.push(handle);
    }

    source.cancel();
    thread::sleep(Duration::from_millis(50));

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    for handle in handles {
        assert!(handle.join().unwrap_err().is_revoked());
    }
}

/// A callback runs at most once, even if cancel is called repeatedly.
#[test]
fn callback_runs_at_most_once() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter2 = counter.clone();
    let handle = token.on_cancel(move || {
        counter2.fetch_add(1, Ordering::SeqCst);
    });

    source.cancel();
    source.cancel();
    source.token().cancel();

    handle.join().unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

/// Reentrant cancellation from within a callback does not deadlock.
#[test]
fn reentrant_cancel_does_not_deadlock() {
    let source = CancellationTokenSource::new();
    let token = source.token();

    let reentrant = token.clone();
    let handle = token.on_cancel(move || {
        // Re-enter cancellation from within the callback.
        reentrant.cancel();
        // Registering another callback during cancellation must also work and
        // run immediately, without taking the lock.
        let reentrant2 = reentrant.clone();
        reentrant.on_cancel(move || {
            reentrant2.is_cancelled();
        });
    });

    let (tx, rx) = mpsc::channel();
    let canceller = thread::spawn(move || {
        source.cancel();
        tx.send(()).unwrap();
    });

    recv_timeout(&rx); // returns only if cancel() did not deadlock
    handle.join().unwrap();
    canceller.join().unwrap();
}

/// When the last owner exits, the token is cancelled.
#[test]
fn last_owner_drop_cancels() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let (tx, rx) = mpsc::channel();

    let t = thread::spawn(move || {
        token.wait();
        tx.send(()).unwrap();
    });

    thread::sleep(Duration::from_millis(50));
    drop(source); // the last owner exits ...
    recv_timeout(&rx); // ... and the waiter is woken up
    t.join().unwrap();
}

/// Dropping one of several owners does not cancel; dropping the last one does.
#[test]
fn multiple_owners() {
    let source1 = CancellationTokenSource::new();
    let source2 = source1.clone();
    let source3 = source2.clone();
    let token = source1.token();

    drop(source1);
    assert!(!token.is_cancelled());
    drop(source2);
    assert!(!token.is_cancelled());
    drop(source3);
    assert!(token.is_cancelled());

    // Waiting after cancellation returns immediately.
    token.wait();
}

/// Tokens do not extend the owner's lifetime: dropping the last owner cancels
/// the token even while tokens are still alive, and the tokens remain usable.
#[test]
fn tokens_do_not_extend_owner_lifetime() {
    let token = {
        let source = CancellationTokenSource::new();
        source.token()
        // source is dropped here; the token must be cancelled ...
    };
    assert!(token.is_cancelled());
    // ... and the token remains usable afterwards.
    token.wait();
    token.cancel();
    assert!(token.is_cancelled());
}

/// A panicking callback keeps an independent receipt: its panic is captured and
/// returned from join, and it does not prevent other callbacks from running.
#[test]
fn callback_failure_keeps_independent_receipt() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let ran = Arc::new(AtomicUsize::new(0));

    let ran1 = ran.clone();
    let panicked = token.on_cancel(move || {
        ran1.fetch_add(1, Ordering::SeqCst);
        panic!("boom");
    });

    let ran2 = ran.clone();
    let ok = token.on_cancel(move || {
        ran2.fetch_add(1, Ordering::SeqCst);
    });

    source.cancel();

    // Both callbacks ran, despite the first one panicking.
    assert_eq!(ran.load(Ordering::SeqCst), 2);

    // The failed callback keeps an independent receipt.
    let err = panicked.join().unwrap_err();
    assert!(err.is_panic());
    let payload: Box<dyn Any + Send> = err.into_panic_payload().unwrap();
    assert_eq!(*payload.downcast::<&'static str>().unwrap(), "boom");

    ok.join().unwrap();
}

/// A callback registered after cancellation is notified immediately.
#[test]
fn register_after_cancel_notifies_immediately() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    source.cancel();

    let ran = Arc::new(AtomicUsize::new(0));
    let ran2 = ran.clone();
    let handle = token.on_cancel(move || {
        ran2.fetch_add(1, Ordering::SeqCst);
    });

    handle.join().unwrap();
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}

/// A callback that already ran cannot be revoked anymore.
#[test]
fn revoke_after_run_returns_false() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let handle = token.on_cancel(|| {});
    source.cancel();
    assert!(!handle.revoke()); // too late: the callback already ran
    handle.join().unwrap();
}

/// A waiting thread and a callback are both woken up by the same cancel.
#[test]
fn waiters_and_callbacks_are_all_woken() {
    let source = CancellationTokenSource::new();
    let token = source.token();
    let (tx, rx) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(2));

    let waiter_tx = tx.clone();
    let waiter = token.clone();
    let barrier2 = barrier.clone();
    let t = thread::spawn(move || {
        barrier2.wait();
        waiter.wait();
        waiter_tx.send("waiter").unwrap();
    });

    let callback_tx = tx.clone();
    let callback_token = token.clone();
    let handle = token.on_cancel(move || {
        callback_token.is_cancelled();
        callback_tx.send("callback").unwrap();
    });

    barrier.wait();
    thread::sleep(Duration::from_millis(50));
    source.cancel();

    let mut messages = [recv_timeout(&rx), recv_timeout(&rx)];
    messages.sort_unstable();
    assert_eq!(messages, ["callback", "waiter"]);
    handle.join().unwrap();
    t.join().unwrap();
}

/// Revocation racing with cancellation: if revocation wins, the callback is
/// never called; if cancellation wins, the callback runs exactly once.
#[test]
fn revoke_race() {
    for _ in 0..100 {
        let source = CancellationTokenSource::new();
        let token = source.token();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter2 = counter.clone();
        let handle = token.on_cancel(move || {
            counter2.fetch_add(1, Ordering::SeqCst);
        });

        let revoker = thread::spawn(move || handle.revoke());
        source.cancel();
        let revoked = revoker.join().unwrap();

        // If revocation won, the callback must never run. If cancellation won,
        // it must run exactly once.
        if revoked {
            assert_eq!(counter.load(Ordering::SeqCst), 0);
        } else {
            assert_eq!(counter.load(Ordering::SeqCst), 1);
        }
    }
}
