//! Tests for the explicit batch channel.

use std::thread;
use std::time::Duration;

use crossbeam_channel::{
    RecvBatchError, RecvBatchTimeoutError, SendBatchError, SendBatchTimeoutError,
    TryRecvBatchError, TrySendBatchError, batch_channel,
};

#[test]
fn basic_send_recv() {
    let (s, r) = batch_channel::<i32>(3);
    s.send_batch(vec![1, 2, 3]).unwrap();
    assert_eq!(r.recv_batch().unwrap(), vec![1, 2, 3]);
}

#[test]
fn capacity_is_by_message_count_not_batch_count() {
    // Capacity is 3 messages, not 3 batches.
    let (s, r) = batch_channel::<i32>(3);

    // Send a batch of 2 messages.
    s.send_batch(vec![1, 2]).unwrap();
    assert_eq!(s.len(), 2);

    // Sending another batch of 2 must fail (2 + 2 > 3), not be partially accepted.
    let err = s.try_send_batch(vec![3, 4]).unwrap_err();
    assert!(err.is_full());
    assert_eq!(err.into_inner(), vec![3, 4]);
    assert_eq!(s.len(), 2);

    // Receiving the first batch frees 2 messages of quota.
    assert_eq!(r.recv_batch().unwrap(), vec![1, 2]);
    assert_eq!(s.len(), 0);

    // Now the second batch can be sent in full.
    s.send_batch(vec![3, 4]).unwrap();
    assert_eq!(s.len(), 2);
    assert_eq!(r.recv_batch().unwrap(), vec![3, 4]);
}

#[test]
fn empty_batch_is_rejected() {
    let (s, _r) = batch_channel::<i32>(3);
    let err = s.try_send_batch(vec![]).unwrap_err();
    assert!(err.is_empty());
    assert_eq!(err.into_inner(), Vec::<i32>::new());

    let err = s.send_batch(vec![]).unwrap_err();
    assert!(err.is_empty());
    assert_eq!(err.into_inner(), Vec::<i32>::new());
}

#[test]
fn batch_exceeding_total_capacity_is_rejected() {
    let (s, _r) = batch_channel::<i32>(3);
    let err = s.try_send_batch(vec![1, 2, 3, 4]).unwrap_err();
    assert!(err.is_too_large());
    assert_eq!(err.into_inner(), vec![1, 2, 3, 4]);

    let err = s.send_batch(vec![1, 2, 3, 4]).unwrap_err();
    assert!(err.is_too_large());
    assert_eq!(err.into_inner(), vec![1, 2, 3, 4]);
}

#[test]
fn batch_equal_to_capacity_can_be_sent() {
    let (s, r) = batch_channel::<i32>(3);
    s.send_batch(vec![1, 2, 3]).unwrap();
    assert_eq!(r.recv_batch().unwrap(), vec![1, 2, 3]);
}

#[test]
fn disconnected_sender_gets_batch_back() {
    let (s, r) = batch_channel::<i32>(3);
    drop(r);

    let err = s.send_batch(vec![1, 2]).unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![1, 2]);

    let err = s.try_send_batch(vec![1, 2]).unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![1, 2]);
}

#[test]
fn disconnected_waiter_gets_original_vec_back() {
    let (s, r) = batch_channel::<i32>(3);

    // Fill the channel so the next send blocks.
    s.send_batch(vec![1, 2]).unwrap();

    let handle = thread::spawn(move || {
        // This will block because the channel is full.
        s.send_batch(vec![3, 4, 5])
    });

    // Give the sender time to block.
    thread::sleep(Duration::from_millis(50));

    // Drop the receiver, disconnecting the channel.
    drop(r);

    // The blocked sender should wake up and get its original Vec back.
    let result = handle.join().unwrap();
    let err = result.unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![3, 4, 5]);
}

#[test]
fn quota_does_not_leak_on_failure() {
    let (s, r) = batch_channel::<i32>(3);

    // Send a batch of 2.
    s.send_batch(vec![1, 2]).unwrap();
    assert_eq!(s.len(), 2);

    // Try to send another batch of 2. It must fail with Full, not leak quota.
    let err = s.try_send_batch(vec![3, 4]).unwrap_err();
    assert!(err.is_full());
    assert_eq!(err.into_inner(), vec![3, 4]);
    assert_eq!(s.len(), 2);

    // Receive the first batch, freeing quota.
    assert_eq!(r.recv_batch().unwrap(), vec![1, 2]);
    assert_eq!(s.len(), 0);

    // Now a batch of 3 (full capacity) can be sent.
    s.send_batch(vec![5, 6, 7]).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(r.recv_batch().unwrap(), vec![5, 6, 7]);
}

#[test]
fn multiple_batches_are_received_in_order() {
    let (s, r) = batch_channel::<i32>(10);
    s.send_batch(vec![1, 2]).unwrap();
    s.send_batch(vec![3, 4, 5]).unwrap();
    s.send_batch(vec![6]).unwrap();

    assert_eq!(r.recv_batch().unwrap(), vec![1, 2]);
    assert_eq!(r.recv_batch().unwrap(), vec![3, 4, 5]);
    assert_eq!(r.recv_batch().unwrap(), vec![6]);
}

#[test]
fn try_recv_on_empty_channel() {
    let (s, r) = batch_channel::<i32>(3);
    assert_eq!(r.try_recv_batch(), Err(TryRecvBatchError::Empty));
    drop(s);
    assert_eq!(r.try_recv_batch(), Err(TryRecvBatchError::Disconnected));
}

#[test]
fn recv_on_disconnected_empty_channel() {
    let (s, r) = batch_channel::<i32>(3);
    drop(s);
    assert_eq!(r.recv_batch(), Err(RecvBatchError));
}

#[test]
fn recv_timeout_on_empty_channel() {
    let (_s, r) = batch_channel::<i32>(3);
    let err = r.recv_batch_timeout(Duration::from_millis(50)).unwrap_err();
    assert!(err.is_timeout());
}

#[test]
fn send_timeout_on_full_channel() {
    let (s, _r) = batch_channel::<i32>(3);
    s.send_batch(vec![1, 2, 3]).unwrap();
    let err = s
        .send_batch_timeout(vec![4], Duration::from_millis(50))
        .unwrap_err();
    assert!(err.is_timeout());
    assert_eq!(err.into_inner(), vec![4]);
}

#[test]
fn send_timeout_returns_batch_on_timeout() {
    let (s, _r) = batch_channel::<i32>(3);
    s.send_batch(vec![1, 2, 3]).unwrap();
    let err = s
        .send_batch_timeout(vec![4, 5], Duration::from_millis(50))
        .unwrap_err();
    assert!(err.is_timeout());
    assert_eq!(err.into_inner(), vec![4, 5]);
}

#[test]
fn recv_timeout_wakes_up_on_send() {
    let (s, r) = batch_channel::<i32>(3);
    let handle = thread::spawn(move || r.recv_batch_timeout(Duration::from_secs(2)));

    thread::sleep(Duration::from_millis(50));
    s.send_batch(vec![42]).unwrap();

    let result = handle.join().unwrap();
    assert_eq!(result.unwrap(), vec![42]);
}

#[test]
fn send_timeout_wakes_up_on_recv() {
    let (s, r) = batch_channel::<i32>(3);
    s.send_batch(vec![1, 2, 3]).unwrap();

    let handle = thread::spawn(move || {
        s.send_batch_timeout(vec![4, 5], Duration::from_secs(2))
    });

    thread::sleep(Duration::from_millis(50));
    assert_eq!(r.recv_batch().unwrap(), vec![1, 2, 3]);

    let result = handle.join().unwrap();
    assert!(result.is_ok());
}

#[test]
fn cloned_senders_and_receivers() {
    let (s, r) = batch_channel::<i32>(10);
    let s2 = s.clone();
    let r2 = r.clone();

    s.send_batch(vec![1, 2]).unwrap();
    s2.send_batch(vec![3, 4]).unwrap();

    assert_eq!(r.recv_batch().unwrap(), vec![1, 2]);
    assert_eq!(r2.recv_batch().unwrap(), vec![3, 4]);
}

#[test]
fn capacity_and_len() {
    let (s, r) = batch_channel::<i32>(5);
    assert_eq!(s.capacity(), 5);
    assert_eq!(r.capacity(), 5);
    assert_eq!(s.len(), 0);
    assert!(s.is_empty());
    assert!(!s.is_full());

    s.send_batch(vec![1, 2, 3, 4, 5]).unwrap();
    assert_eq!(s.len(), 5);
    assert!(!s.is_empty());
    assert!(s.is_full());

    r.recv_batch().unwrap();
    assert_eq!(s.len(), 0);
    assert!(s.is_empty());
}

#[test]
fn messages_do_not_need_clone() {
    // A type that does not implement Clone.
    struct NoClone(i32);

    let (s, r) = batch_channel::<NoClone>(3);
    s.send_batch(vec![NoClone(1), NoClone(2)]).unwrap();
    let batch = r.recv_batch().unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].0, 1);
    assert_eq!(batch[1].0, 2);
}

#[test]
fn drop_sender_wakes_up_receiver() {
    let (s, r) = batch_channel::<i32>(3);
    let handle = thread::spawn(move || r.recv_batch());

    thread::sleep(Duration::from_millis(50));
    drop(s);

    let result = handle.join().unwrap();
    assert_eq!(result, Err(RecvBatchError));
}

#[test]
fn drop_receiver_wakes_up_sender() {
    let (s, r) = batch_channel::<i32>(3);
    s.send_batch(vec![1, 2, 3]).unwrap();

    let handle = thread::spawn(move || s.send_batch(vec![4, 5]));

    thread::sleep(Duration::from_millis(50));
    drop(r);

    let result = handle.join().unwrap();
    let err = result.unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![4, 5]);
}

#[test]
fn send_batch_error_types() {
    let (s, _r) = batch_channel::<i32>(3);

    // SendBatchError (blocking) - disconnected.
    drop(_r);
    let err: SendBatchError<Vec<i32>> = s.send_batch(vec![1]).unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![1]);
}

#[test]
fn try_send_batch_error_types() {
    let (s, _r) = batch_channel::<i32>(3);

    // Full.
    s.send_batch(vec![1, 2, 3]).unwrap();
    let err: TrySendBatchError<Vec<i32>> = s.try_send_batch(vec![4]).unwrap_err();
    assert!(err.is_full());
    assert_eq!(err.into_inner(), vec![4]);

    // Disconnected.
    drop(_r);
    let err: TrySendBatchError<Vec<i32>> = s.try_send_batch(vec![4]).unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![4]);
}

#[test]
fn send_batch_timeout_error_types() {
    let (s, _r) = batch_channel::<i32>(3);

    // Timeout.
    s.send_batch(vec![1, 2, 3]).unwrap();
    let err: SendBatchTimeoutError<Vec<i32>> =
        s.send_batch_timeout(vec![4], Duration::from_millis(10)).unwrap_err();
    assert!(err.is_timeout());
    assert_eq!(err.into_inner(), vec![4]);

    // Disconnected.
    drop(_r);
    let err: SendBatchTimeoutError<Vec<i32>> =
        s.send_batch_timeout(vec![4], Duration::from_millis(10)).unwrap_err();
    assert!(err.is_disconnected());
    assert_eq!(err.into_inner(), vec![4]);
}

#[test]
fn recv_batch_timeout_error_types() {
    let (s, r) = batch_channel::<i32>(3);

    // Timeout.
    let err: RecvBatchTimeoutError = r.recv_batch_timeout(Duration::from_millis(10)).unwrap_err();
    assert!(err.is_timeout());

    // Disconnected.
    drop(s);
    let err: RecvBatchTimeoutError = r.recv_batch_timeout(Duration::from_millis(10)).unwrap_err();
    assert!(err.is_disconnected());
}

#[test]
fn concurrent_senders_and_receivers() {
    let (s, r) = batch_channel::<usize>(100);

    let senders: Vec<_> = (0..4)
        .map(|i| {
            let s = s.clone();
            thread::spawn(move || {
                for j in 0..10 {
                    s.send_batch(vec![i * 100 + j]).unwrap();
                }
            })
        })
        .collect();

    let receivers: Vec<_> = (0..2)
        .map(|_| {
            let r = r.clone();
            thread::spawn(move || {
                let mut count = 0;
                while count < 20 {
                    if let Ok(batch) = r.recv_batch() {
                        count += batch.len();
                    }
                }
                count
            })
        })
        .collect();

    // Drop the original handles so the channel can disconnect.
    drop(s);
    drop(r);

    for h in senders {
        h.join().unwrap();
    }

    let total: usize = receivers.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total, 40);
}
