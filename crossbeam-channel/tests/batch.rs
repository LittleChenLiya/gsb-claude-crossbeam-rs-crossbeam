//! Tests for the explicit batch channel (`crossbeam_channel::batch`).
//!
//! Run with: cargo test -p crossbeam-channel --test batch

use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::batch::{
    SendBatchError, SendBatchTimeoutError, TrySendBatchError, batch_channel,
};
use crossbeam_channel::{RecvError, TryRecvError};

const WAIT: Duration = Duration::from_millis(200);

#[test]
fn no_partial_accept_then_second_batch_fits() {
    // Capacity is counted in messages, not batches.
    let (s, r) = batch_channel(3);
    assert_eq!(s.capacity(), 3);

    // First batch of two messages occupies 2 of 3 slots.
    s.send_batch(vec![1, 2]).unwrap();
    assert_eq!(s.len(), 2);

    // A second batch of two messages does not fit completely: it is rejected as a
    // whole, never partially accepted, and the original Vec is returned.
    match s.try_send_batch(vec![3, 4]) {
        Err(TrySendBatchError::Full(batch)) => assert_eq!(batch, vec![3, 4]),
        other => panic!("expected Full, got {:?}", other.map(|_| ())),
    }
    // Quota is untouched by the failed send.
    assert_eq!(s.len(), 2);

    // A blocking send of the second batch stays blocked while the channel is full.
    let s2 = s.clone();
    let handle = thread::spawn(move || s2.send_batch(vec![3, 4]));
    thread::sleep(WAIT);
    assert!(!handle.is_finished(), "second batch must wait for quota");

    // Taking out the first batch releases its quota...
    assert_eq!(r.recv_batch(), Ok(vec![1, 2]));
    // ...so the second batch now succeeds completely.
    handle.join().unwrap().unwrap();
    assert_eq!(r.recv_batch(), Ok(vec![3, 4]));

    // All quota has been released.
    assert_eq!(s.len(), 0);
    assert!(s.is_empty());
}

#[test]
fn disconnect_wakes_waiter_with_original_vec() {
    let (s, r) = batch_channel::<i32>(2);

    // Fill the channel completely.
    s.send_batch(vec![1, 2]).unwrap();

    // This sender blocks because there is no quota left.
    let s2 = s.clone();
    let handle = thread::spawn(move || s2.send_batch(vec![7, 8]));
    thread::sleep(WAIT);
    assert!(!handle.is_finished());

    // Dropping the last receiver wakes the blocked sender, which gets its
    // original Vec back unchanged.
    drop(r);
    match handle.join().unwrap() {
        Err(SendBatchError::Disconnected(batch)) => assert_eq!(batch, vec![7, 8]),
        other => panic!("expected Disconnected, got {:?}", other.map(|_| ())),
    }

    // New sends fail immediately and return the batch.
    match s.send_batch(vec![9]) {
        Err(SendBatchError::Disconnected(batch)) => assert_eq!(batch, vec![9]),
        other => panic!("expected Disconnected, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn timeout_returns_vec_and_does_not_leak_quota() {
    let (s, r) = batch_channel(2);
    s.send_batch(vec![1, 2]).unwrap();
    assert_eq!(s.len(), 2);

    // The channel is full, so the send times out and returns the original Vec.
    let start = Instant::now();
    match s.send_batch_timeout(vec![3], Duration::from_millis(100)) {
        Err(SendBatchTimeoutError::Timeout(batch)) => assert_eq!(batch, vec![3]),
        other => panic!("expected Timeout, got {:?}", other.map(|_| ())),
    }
    assert!(start.elapsed() >= Duration::from_millis(90));

    // The timed-out send never reserved quota: the channel still holds exactly
    // the first batch, and after receiving it the full capacity is available.
    assert_eq!(s.len(), 2);
    assert_eq!(r.recv_batch(), Ok(vec![1, 2]));
    assert_eq!(s.len(), 0);
    s.send_batch(vec![3, 4]).unwrap();
    assert_eq!(r.recv_batch(), Ok(vec![3, 4]));
    assert_eq!(s.len(), 0);
}

#[test]
fn empty_and_oversized_batches_rejected_upfront() {
    let (s, _r) = batch_channel::<i32>(3);

    match s.send_batch(vec![]) {
        Err(SendBatchError::Empty(batch)) => assert!(batch.is_empty()),
        other => panic!("expected Empty, got {:?}", other.map(|_| ())),
    }
    match s.try_send_batch(vec![1, 2, 3, 4]) {
        Err(TrySendBatchError::TooLarge(batch)) => assert_eq!(batch, vec![1, 2, 3, 4]),
        other => panic!("expected TooLarge, got {:?}", other.map(|_| ())),
    }
    match s.send_batch_timeout(vec![1, 2, 3, 4], Duration::from_millis(1)) {
        Err(SendBatchTimeoutError::TooLarge(batch)) => assert_eq!(batch, vec![1, 2, 3, 4]),
        other => panic!("expected TooLarge, got {:?}", other.map(|_| ())),
    }

    // Rejected batches never touch the quota.
    assert_eq!(s.len(), 0);
}

#[test]
fn messages_do_not_need_clone() {
    struct NoClone(u32);

    let (s, r) = batch_channel(4);
    s.send_batch(vec![NoClone(1), NoClone(2)]).unwrap();
    s.send_batch(vec![NoClone(3)]).unwrap();

    let first = r.recv_batch().unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].0, 1);
    assert_eq!(first[1].0, 2);
    let second = r.recv_batch().unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, 3);
    assert_eq!(s.len(), 0);
}

#[test]
fn recv_releases_quota_for_waiting_senders() {
    let (s, r) = batch_channel(4);

    let s2 = s.clone();
    let producer = thread::spawn(move || {
        for i in 0..10 {
            // Each batch needs 2 of the 4 slots; sends block when full.
            s2.send_batch(vec![i, i]).unwrap();
        }
    });

    for _ in 0..10 {
        let batch = r.recv_batch().unwrap();
        assert_eq!(batch.len(), 2);
    }
    producer.join().unwrap();

    // Every received batch released its quota: nothing leaked.
    assert_eq!(s.len(), 0);
}

#[test]
fn receiver_drop_releases_nothing_but_disconnects() {
    let (s, r) = batch_channel::<i32>(1);
    let r2 = r.clone();
    drop(r);
    // One receiver is still alive, so sends still work.
    s.send_batch(vec![1]).unwrap();
    assert_eq!(r2.recv_batch(), Ok(vec![1]));

    // Dropping the last receiver disconnects the channel.
    drop(r2);
    assert!(matches!(
        s.send_batch(vec![2]),
        Err(SendBatchError::Disconnected(_))
    ));
}

#[test]
fn recv_errors() {
    let (s, r) = batch_channel::<i32>(1);
    assert_eq!(r.try_recv_batch(), Err(TryRecvError::Empty));
    drop(s);
    assert_eq!(r.recv_batch(), Err(RecvError));
}
