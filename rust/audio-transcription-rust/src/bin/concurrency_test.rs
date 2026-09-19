use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[derive(Debug)]
struct AudioChunk {
    chunk_id: u64,
}

#[derive(Debug)]
struct TranscriptionResult {
    chunk_id: u64,
    text: String,
}

fn main() {
    let worker_count = 3;

    let (tx, rx) = mpsc::channel::<AudioChunk>();
    let (result_tx, result_rx) = mpsc::channel::<TranscriptionResult>();

    let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

    let mut workers = Vec::new();

    for worker_id in 0..worker_count {
        let rx = std::sync::Arc::clone(&rx);
        let result_tx = result_tx.clone();

        let worker = thread::spawn(move || {
            loop {
                let chunk = {
                    let receiver = rx.lock().unwrap();

                    match receiver.recv() {
                        Ok(chunk) => chunk,
                        Err(_) => break,
                    }
                };

                println!("Worker {} processing chunk {}", worker_id, chunk.chunk_id);

                let delay = match chunk.chunk_id {
                    0 => 3000,
                    1 => 1000,
                    2 => 2000,
                    3 => 500,
                    4 => 1500,
                    _ => 1000,
                };

                thread::sleep(Duration::from_millis(delay));

                let result = TranscriptionResult {
                    chunk_id: chunk.chunk_id,
                    text: format!("Transcript for chunk {}", chunk.chunk_id),
                };

                println!("Worker {} finished chunk {}", worker_id, chunk.chunk_id);

                result_tx.send(result).unwrap();
            }
        });

        workers.push(worker);
    }

    drop(result_tx);

    for chunk_id in 0..5 {
        tx.send(AudioChunk { chunk_id }).unwrap();
    }

    drop(tx);

    let mut results = Vec::new();

    for result in result_rx {
        println!("Result received: chunk {}", result.chunk_id);

        results.push(result);
    }

    for worker in workers {
        worker.join().unwrap();
    }

    println!();
    println!("Results in completion order:");

    for result in &results {
        println!("Chunk {} -> {}", result.chunk_id, result.text);
    }

    println!();
    println!("Results reordered by chunk_id:");

    results.sort_by_key(|result| result.chunk_id);

    for result in &results {
        println!("Chunk {} -> {}", result.chunk_id, result.text);
    }

    println!();
    println!("Concurrency test completed successfully.");
}
