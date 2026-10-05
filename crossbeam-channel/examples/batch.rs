//! Dual-batch example for the explicit batch channel.
//!
//! A channel with capacity 3 (counted in messages) takes a first batch of two
//! messages. A second batch of two messages cannot be partially accepted, so
//! the sending thread blocks until the first batch is received; then the
//! second batch is enqueued completely.
//!
//! Run with: cargo run -p crossbeam-channel --example batch

use std::thread;
use std::time::Duration;

use crossbeam_channel::batch::batch_channel;

fn main() {
    let (s, r) = batch_channel(3);

    // First batch: occupies 2 of the 3 message slots.
    s.send_batch(vec![1, 2]).unwrap();
    println!("sent first batch [1, 2]; {} of 3 slots used", s.len());

    // Second batch of two messages does not fit yet (2 + 2 > 3), so this
    // thread blocks instead of partially accepting the batch.
    let s2 = s.clone();
    let producer = thread::spawn(move || {
        s2.send_batch(vec![3, 4]).unwrap();
        println!("sent second batch [3, 4] after quota was released");
    });

    // Give the producer a moment to block on the full channel.
    thread::sleep(Duration::from_millis(100));
    assert!(!producer.is_finished());

    // Receiving the first batch releases its quota...
    let first = r.recv_batch().unwrap();
    println!("received {:?}", first);

    // ...so the second batch is now enqueued completely.
    producer.join().unwrap();
    let second = r.recv_batch().unwrap();
    println!("received {:?}", second);

    assert_eq!(first, vec![1, 2]);
    assert_eq!(second, vec![3, 4]);
    assert_eq!(s.len(), 0);
}
