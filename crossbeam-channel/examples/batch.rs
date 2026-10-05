//! Demonstrates the explicit batch channel.
//!
//! A batch channel sends and receives whole `Vec<T>` batches. A batch is either
//! enqueued in full or returned as-is. Capacity is measured by the total number
//! of messages, not the number of batches.
//!
//! This example shows the "double-batch" scenario: with capacity 3, sending two
//! batches of 2 messages each. The second batch cannot be partially accepted;
//! it succeeds only after the first batch is received.

use std::thread;

use crossbeam_channel::batch_channel;

fn main() {
    // Capacity is 3 messages, not 3 batches.
    let (s, r) = batch_channel::<i32>(3);

    // Send the first batch of 2 messages.
    s.send_batch(vec![1, 2]).unwrap();
    println!("sent first batch: [1, 2] (used: {})", s.len());

    // Try to send a second batch of 2 messages. There is only 1 message of
    // quota left, so this must fail with `Full` -- not be partially accepted.
    match s.try_send_batch(vec![3, 4]) {
        Ok(()) => println!("unexpected: second batch was accepted"),
        Err(err) => {
            let returned = err.into_inner();
            println!(
                "second batch rejected (returned: {:?})",
                returned
            );
        }
    }
    println!("used after failed send attempt: {}", s.len());

    // Receive the first batch, freeing 2 messages of quota.
    let first = r.recv_batch().unwrap();
    println!("received first batch: {:?} (used: {})", first, s.len());

    // Now the second batch can be sent in full.
    s.send_batch(vec![3, 4]).unwrap();
    println!("sent second batch: [3, 4] (used: {})", s.len());

    // Receive the second batch.
    let second = r.recv_batch().unwrap();
    println!("received second batch: {:?} (used: {})", second, s.len());

    // Demonstrate that a blocked sender gets its original batch back when the
    // receiver is dropped.
    let (s2, r2) = batch_channel::<i32>(3);
    s2.send_batch(vec![1, 2, 3]).unwrap();

    let handle = thread::spawn(move || {
        s2.send_batch(vec![4, 5])
    });

    thread::sleep(std::time::Duration::from_millis(50));
    drop(r2);

    match handle.join().unwrap() {
        Ok(()) => println!("unexpected: blocked sender succeeded after disconnect"),
        Err(err) => {
            println!(
                "blocked sender got batch back after disconnect: {:?}",
                err.into_inner()
            );
        }
    }

    println!("done");
}
