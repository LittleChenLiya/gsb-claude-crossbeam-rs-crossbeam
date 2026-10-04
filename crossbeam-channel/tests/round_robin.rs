//! Tests for the explicit round-robin receiver (`RoundRobin`).

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use crossbeam_channel::{unbounded, RoundRobin, RoundRobinError};

/// Three channels preloaded with two messages each must be consumed in the
/// strict rotation 0, 1, 2, 0, 1, 2 (the pre-ready round-robin guarantee).
#[test]
fn preloaded_channels_are_consumed_in_rotation() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    for (i, s) in [&s0, &s1, &s2].iter().enumerate() {
        s.send(format!("{i}-first")).unwrap();
        s.send(format!("{i}-second")).unwrap();
    }

    let mut rr = RoundRobin::new(vec![r0, r1, r2]);

    let mut indices = Vec::new();
    let mut values = Vec::new();
    for _ in 0..6 {
        let (i, msg) = rr.recv().unwrap();
        indices.push(i);
        values.push(msg);
    }

    assert_eq!(indices, vec![0, 1, 2, 0, 1, 2]);
    assert_eq!(
        values,
        vec![
            "0-first".to_string(),
            "1-first".to_string(),
            "2-first".to_string(),
            "0-second".to_string(),
            "1-second".to_string(),
            "2-second".to_string(),
        ]
    );
}

/// A permanently-ready first channel must not starve the later ones.
#[test]
fn ready_first_channel_does_not_starve_others() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    for _ in 0..3 {
        s0.send("from-0").unwrap();
    }
    s1.send("from-1").unwrap();
    s2.send("from-2").unwrap();

    let mut rr = RoundRobin::new(vec![r0, r1, r2]);
    let order: Vec<usize> = (0..5).map(|_| rr.recv().unwrap().0).collect();
    assert_eq!(order, vec![0, 1, 2, 0, 0]);
}

/// Disconnected channels are drained of queued messages before removal, and
/// once every channel is gone `recv` reports `Disconnected` explicitly.
#[test]
fn disconnected_channels_drain_then_report_disconnected() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    s0.send("a0").unwrap();
    s0.send("a1").unwrap();
    s2.send("c0").unwrap();
    s2.send("c1").unwrap();

    // Close all senders: channel 1 is empty, channels 0 and 2 still hold
    // queued messages that must not be lost because of the disconnect.
    drop(s0);
    drop(s1);
    drop(s2);

    let mut rr = RoundRobin::new(vec![r0, r1, r2]);

    // Channel 1 (empty + disconnected) is removed on the first scan; the
    // remaining two channels keep rotating in their compacted slots.
    let got: Vec<(usize, &str)> = (0..4).map(|_| rr.recv().unwrap()).collect();
    assert_eq!(
        got,
        vec![(0, "a0"), (1, "c0"), (0, "a1"), (1, "c1")]
    );

    // Everything is drained and disconnected now: explicit final answer.
    assert_eq!(rr.recv(), Err(RoundRobinError::Disconnected));
    assert_eq!(rr.recv(), Err(RoundRobinError::Disconnected));
    assert!(rr.is_empty());
    assert_eq!(rr.len(), 0);
}

/// An empty channel set never blocks and reports `Disconnected` right away.
#[test]
fn empty_set_returns_disconnected_immediately() {
    let mut rr = RoundRobin::<i32>::new(Vec::new());
    assert_eq!(rr.recv(), Err(RoundRobinError::Disconnected));
    assert!(rr.is_empty());
}

/// A blocked `recv` is woken up by `shutdown`, triggered from another thread
/// after both sides rendezvous on a barrier. Shutdown is latched.
#[test]
fn shutdown_wakes_blocked_recv_via_barrier() {
    let (_s, r) = unbounded::<i32>();
    let mut rr = RoundRobin::new(vec![r]);
    let handle = rr.shutdown_handle();

    let barrier = Arc::new(Barrier::new(2));
    let other = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            // Rendezvous with the main thread, then trigger the shutdown
            // that must wake its blocked `recv`.
            barrier.wait();
            handle.shutdown();
        })
    };

    barrier.wait();
    assert_eq!(rr.recv(), Err(RoundRobinError::Shutdown));
    other.join().unwrap();

    // Shutdown is latched: later receives never block again.
    assert!(rr.is_shutdown());
    assert_eq!(rr.recv(), Err(RoundRobinError::Shutdown));
}

/// `RoundRobin::shutdown` (without a handle) also wakes a blocked `recv`.
#[test]
fn shutdown_method_wakes_blocked_recv_via_barrier() {
    let (_s, r) = unbounded::<i32>();
    let mut rr = RoundRobin::new(vec![r]);

    let barrier = Arc::new(Barrier::new(2));
    let other = {
        let barrier = barrier.clone();
        let handle = rr.shutdown_handle();
        thread::spawn(move || {
            barrier.wait();
            handle.shutdown();
        })
    };

    barrier.wait();
    rr.shutdown();
    assert_eq!(rr.recv(), Err(RoundRobinError::Shutdown));
    other.join().unwrap();
}

/// When a wakeup from the blocking `Select` phase delivers a message, the
/// cursor moves past the winning channel and the strict rotation resumes
/// from there for subsequent pre-ready receives.
#[test]
fn blocking_wakeup_updates_cursor() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();

    let mut rr = RoundRobin::new(vec![r0, r1]);

    let barrier = Arc::new(Barrier::new(2));
    let producer = {
        let barrier = barrier.clone();
        let s1 = s1.clone();
        thread::spawn(move || {
            barrier.wait();
            s1.send("woken").unwrap();
        })
    };

    // Both channels are empty: this receive blocks in `Select` and is woken
    // by the send on channel 1.
    barrier.wait();
    assert_eq!(rr.recv(), Ok((1, "woken")));
    producer.join().unwrap();

    // The cursor now sits past channel 1, so the next pre-ready rotation
    // starts at channel 0 again.
    s0.send("a").unwrap();
    s1.send("b").unwrap();
    assert_eq!(rr.recv(), Ok((0, "a")));
    assert_eq!(rr.recv(), Ok((1, "b")));
}

/// A channel that disconnects while `recv` is blocked in `Select` is
/// removed and the receive retries instead of getting stuck.
#[test]
fn disconnect_during_blocking_recv_is_retried() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded::<i32>();

    let mut rr = RoundRobin::new(vec![r0, r1]);

    let barrier = Arc::new(Barrier::new(2));
    let producer = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            // Disconnect channel 0 while `recv` is blocked, then deliver
            // the message through channel 1.
            drop(s0);
            s1.send(7).unwrap();
        })
    };

    barrier.wait();
    assert_eq!(rr.recv(), Ok((0, 7)));
    producer.join().unwrap();
    assert_eq!(rr.len(), 1);
}
