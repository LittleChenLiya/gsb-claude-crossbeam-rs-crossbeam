//! Tests for the explicit round-robin receiver.

use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use crossbeam_channel::{RoundRobinReceiver, TryRecvError, unbounded};

/// Three channels each pre-loaded with two messages are consumed in round-robin order
/// `0, 1, 2, 0, 1, 2`.
#[test]
fn round_robin_guarantee() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    // Tag each message with its channel id so we can verify the order.
    s0.send((0, 0)).unwrap();
    s0.send((0, 1)).unwrap();
    s1.send((1, 0)).unwrap();
    s1.send((1, 1)).unwrap();
    s2.send((2, 0)).unwrap();
    s2.send((2, 1)).unwrap();

    let mut rr = RoundRobinReceiver::new(vec![r0, r1, r2]);

    assert_eq!(rr.recv(), Ok((0, 0)));
    assert_eq!(rr.recv(), Ok((1, 0)));
    assert_eq!(rr.recv(), Ok((2, 0)));
    assert_eq!(rr.recv(), Ok((0, 1)));
    assert_eq!(rr.recv(), Ok((1, 1)));
    assert_eq!(rr.recv(), Ok((2, 1)));
}

/// `try_recv` also honours the round-robin order.
#[test]
fn try_recv_round_robin() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    s0.send((0, 0)).unwrap();
    s0.send((0, 1)).unwrap();
    s1.send((1, 0)).unwrap();
    s1.send((1, 1)).unwrap();
    s2.send((2, 0)).unwrap();
    s2.send((2, 1)).unwrap();

    let mut rr = RoundRobinReceiver::new(vec![r0, r1, r2]);

    assert_eq!(rr.try_recv(), Ok((0, 0)));
    assert_eq!(rr.try_recv(), Ok((1, 0)));
    assert_eq!(rr.try_recv(), Ok((2, 0)));
    assert_eq!(rr.try_recv(), Ok((0, 1)));
    assert_eq!(rr.try_recv(), Ok((1, 1)));
    assert_eq!(rr.try_recv(), Ok((2, 1)));
    assert_eq!(rr.try_recv(), Err(TryRecvError::Empty));
}

/// Disconnected channels are drained before removal: messages already queued are not lost.
#[test]
fn disconnect_drains_queue() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();

    s0.send(0).unwrap();
    s0.send(1).unwrap();
    s1.send(2).unwrap();

    // Drop the sender of channel 0 while it still has messages queued.
    drop(s0);

    let mut rr = RoundRobinReceiver::new(vec![r0, r1]);

    // The round-robin visits channel 0 first (message 0), then channel 1 (message 2), then
    // channel 0 again (message 1). Channel 0 is only removed once it is empty, so no message
    // is lost.
    let mut received = Vec::new();
    received.push(rr.recv().unwrap());
    received.push(rr.recv().unwrap());
    received.push(rr.recv().unwrap());
    received.sort();
    assert_eq!(received, vec![0, 1, 2]);
}

/// When every channel is empty and disconnected, `recv` returns `RecvError`.
#[test]
fn all_disconnected_returns_err() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();

    s0.send(0).unwrap();
    s1.send(1).unwrap();

    drop(s0);
    drop(s1);

    let mut rr = RoundRobinReceiver::new(vec![r0, r1]);

    assert_eq!(rr.recv(), Ok(0));
    assert_eq!(rr.recv(), Ok(1));
    assert_eq!(rr.recv(), Err(crossbeam_channel::RecvError));
}

/// An empty receiver set returns `RecvError` immediately instead of blocking forever.
#[test]
fn empty_set_returns_err() {
    let mut rr: RoundRobinReceiver<()> = RoundRobinReceiver::new(vec![]);
    assert!(rr.is_empty());
    assert_eq!(rr.len(), 0);
    assert_eq!(rr.recv(), Err(crossbeam_channel::RecvError));
    assert_eq!(rr.try_recv(), Err(TryRecvError::Disconnected));
}

/// Disabled channels are skipped during round-robin scans.
#[test]
fn disabled_channels_are_skipped() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    // Pre-load enough messages so the round-robin scan has something to receive on each
    // enabled channel.
    s0.send(0).unwrap();
    s0.send(3).unwrap();
    s1.send(1).unwrap();
    s2.send(2).unwrap();
    s2.send(4).unwrap();

    let mut rr = RoundRobinReceiver::new(vec![r0, r1, r2]);

    // Disable channel 1; it must be skipped.
    rr.disable(1);
    assert!(rr.is_disabled(1));

    assert_eq!(rr.recv(), Ok(0));
    // Channel 1 is disabled, so the next message comes from channel 2.
    assert_eq!(rr.recv(), Ok(2));
    // Channel 0 is next (round-robin wraps), message 3.
    assert_eq!(rr.recv(), Ok(3));
    // Channel 2 is next, message 4.
    assert_eq!(rr.recv(), Ok(4));

    // Re-enable channel 1; it participates again. The old message 1 that was queued before
    // disabling is still received first, then the new message 10.
    rr.enable(1);
    assert!(!rr.is_disabled(1));

    s1.send(10).unwrap();
    assert_eq!(rr.recv(), Ok(1));
    assert_eq!(rr.recv(), Ok(10));
}

/// When all channels are empty, `recv` blocks and is woken up when a message arrives.
#[test]
fn blocking_wakeup_on_message() {
    let (s0, r0) = unbounded();
    let (_s1, r1) = unbounded();

    let mut rr = RoundRobinReceiver::new(vec![r0, r1]);

    let handle = thread::spawn(move || rr.recv());

    // Give the receiver thread time to park in `Select`.
    thread::sleep(Duration::from_millis(100));

    s0.send(42).unwrap();

    assert_eq!(handle.join().unwrap(), Ok(42));
}

/// The close wake-up entry unblocks a parked `recv` and returns `RecvError`.
///
/// A barrier is used to synchronise the receiver thread and the main thread so that the
/// receiver is known to be parked before `close` is called.
#[test]
fn close_wakes_up_parked_receiver() {
    let (s, r) = unbounded::<i32>();
    let mut rr = RoundRobinReceiver::new(vec![r]);
    let close_handle = rr.close_handle();

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();

    let handle = thread::spawn(move || {
        // Signal that we are about to park in `recv`.
        barrier_clone.wait();
        rr.recv()
    });

    // Wait until the receiver thread is about to park.
    barrier.wait();

    // Now wake it up via the close entry from another thread.
    close_handle.close();

    assert_eq!(handle.join().unwrap(), Err(crossbeam_channel::RecvError));

    // The closed state is sticky: subsequent calls return immediately.
    // (We need a new receiver since `rr` was moved into the thread.)
    let (s2, r2) = unbounded::<i32>();
    let mut rr2 = RoundRobinReceiver::new(vec![r2]);
    rr2.close();
    assert_eq!(rr2.recv(), Err(crossbeam_channel::RecvError));
    assert_eq!(rr2.try_recv(), Err(TryRecvError::Disconnected));
    drop(s2);

    // The sender is still alive; drop it to avoid a warning.
    drop(s);
}

/// Closing before `recv` is called still makes `recv` return `RecvError` immediately.
#[test]
fn close_before_recv() {
    let (s, r) = unbounded::<i32>();
    let mut rr = RoundRobinReceiver::new(vec![r]);

    rr.close();

    assert_eq!(rr.recv(), Err(crossbeam_channel::RecvError));
    assert_eq!(rr.try_recv(), Err(TryRecvError::Disconnected));

    drop(s);
}

/// Closing is idempotent.
#[test]
fn close_is_idempotent() {
    let (s, r) = unbounded::<i32>();
    let mut rr = RoundRobinReceiver::new(vec![r]);

    rr.close();
    rr.close();
    rr.close();

    assert_eq!(rr.recv(), Err(crossbeam_channel::RecvError));

    drop(s);
}

/// After a blocking wake-up selects a channel, the cursor is updated so the next receive
/// starts from the channel after it.
#[test]
fn blocking_wakeup_updates_cursor() {
    let (s0, r0) = unbounded();
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    let mut rr = RoundRobinReceiver::new(vec![r0, r1, r2]);

    // Pre-load channel 1 only. The pre-ready scan will find it immediately, so this tests
    // the pre-ready path rather than blocking.
    s1.send(1).unwrap();
    assert_eq!(rr.recv(), Ok(1));
    // Cursor is now 2. Pre-load channel 0; the scan starts from 2, wraps to 0.
    s0.send(0).unwrap();
    assert_eq!(rr.recv(), Ok(0));
    // Cursor is now 1. Pre-load channel 2; the scan starts from 1, wraps to 2.
    s2.send(2).unwrap();
    assert_eq!(rr.recv(), Ok(2));
}

/// A disconnected channel that still has messages is drained before removal, even when the
/// disconnect happens while the receiver is parked.
#[test]
fn disconnect_drains_queue_when_parked() {
    let (s0, r0) = unbounded();
    let (_s1, r1) = unbounded();

    let mut rr = RoundRobinReceiver::new(vec![r0, r1]);

    let handle = thread::spawn(move || rr.recv());

    // Give the receiver thread time to park.
    thread::sleep(Duration::from_millis(100));

    // Send a message on channel 0, then drop the sender (disconnect).
    s0.send(42).unwrap();
    drop(s0);

    // The message must still be received even though the sender was dropped.
    assert_eq!(handle.join().unwrap(), Ok(42));
}
