use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const QUEUE_CAPACITY: usize = 5;
const WORKER_COUNT: usize = 2;
const TOTAL_CHUNKS: usize = 30;

fn main() {
    let (tx, rx) = mpsc::sync_channel::<usize>(QUEUE_CAPACITY);

    let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

    let mut workers = Vec::new();

    for worker_id in 0..WORKER_COUNT {
        let rx = std::sync::Arc::clone(&rx);

        let worker = thread::spawn(move || {
            loop {
                let chunk = {
                    let receiver = rx.lock().unwrap();

                    match receiver.recv() {
                        Ok(chunk) => chunk,
                        Err(_) => break,
                    }
                };

                println!("Worker {} processing chunk {}", worker_id, chunk);

                thread::sleep(Duration::from_millis(1000));

                println!("Worker {} finished chunk {}", worker_id, chunk);
            }
        });

        workers.push(worker);
    }

    let mut sent = 0;
    let mut dropped = 0;

    for chunk_id in 0..TOTAL_CHUNKS {
        match tx.try_send(chunk_id) {
            Ok(_) => {
                sent += 1;

                println!("Producer queued chunk {}", chunk_id);
            }

            Err(mpsc::TrySendError::Full(chunk_id)) => {
                dropped += 1;

                println!("BACKPRESSURE: queue full, dropped chunk {}", chunk_id);
            }

            Err(mpsc::TrySendError::Disconnected(chunk_id)) => {
                println!("Queue disconnected, chunk {} not sent", chunk_id);

                break;
            }
        }

        thread::sleep(Duration::from_millis(100));
    }

    drop(tx);

    for worker in workers {
        worker.join().unwrap();
    }

    println!();
    println!("==============================");
    println!("Backpressure test completed");
    println!("==============================");
    println!("Total chunks: {}", TOTAL_CHUNKS);
    println!("Chunks queued: {}", sent);
    println!("Chunks dropped: {}", dropped);
    println!("Queue capacity: {}", QUEUE_CAPACITY);
}
