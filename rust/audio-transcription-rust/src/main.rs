use std::collections::BTreeMap;
use std::env;
use std::io::BufRead;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
};
use std::thread;
use std::time::Duration;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{SampleFormat, StreamConfig};
use reqwest::blocking::Client;
use reqwest::blocking::multipart;
use serde::Deserialize;

const TARGET_SAMPLE_RATE: u32 = 16_000;
const CHUNK_DURATION_MS: u64 = 1_000;
const CHUNK_SAMPLES: usize = 16_000;

const AUDIO_QUEUE_CAPACITY: usize = 10;
const TRANSCRIPTION_QUEUE_CAPACITY: usize = 5;
const WORKER_QUEUE_CAPACITY: usize = 2;
const RESULT_QUEUE_CAPACITY: usize = 10;

const TRANSCRIPTION_WORKERS: usize = 3;

const API_TIMEOUT_SECONDS: u64 = 30;
const MAX_TRANSCRIPTION_ATTEMPTS: usize = 3;

const SPEECH_START_THRESHOLD: f64 = 800.0;
const SPEECH_CONTINUE_THRESHOLD: f64 = 500.0;
const SILENT_CHUNKS_TO_END_SPEECH: u32 = 3;

struct AudioChunk {
    chunk_id: u64,
    start_ms: u64,
    end_ms: u64,
    samples: Vec<i16>,
}

enum ChunkOutcome {
    Transcript(String),
    Error(String),
    Dropped(String),
    Skipped(String),
}

struct TranscriptionResult {
    chunk_id: u64,
    start_ms: u64,
    end_ms: u64,
    outcome: ChunkOutcome,
}

#[derive(Debug, Deserialize)]
struct TranscriptionResponse {
    text: Option<String>,
    error: Option<String>,
}

struct VadState {
    speech_active: bool,
    consecutive_silent_chunks: u32,
}

fn main() {
    println!("Starting Rust Audio Transcription Pipeline");

    let stop_flag = Arc::new(AtomicBool::new(false));

    let stop_flag_stdin = Arc::clone(&stop_flag);

    thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = std::io::BufReader::new(stdin);
        let mut line = String::new();

        loop {
            line.clear();

            match reader.read_line(&mut line) {
                Ok(0) => {
                    return;
                }

                Ok(_) => {
                    let line = line.trim();

                    if line.contains("\"command\"") && line.contains("\"stop\"") {
                        println!("STOP command received from Electron");

                        stop_flag_stdin.store(true, Ordering::SeqCst);

                        return;
                    }
                }

                Err(error) => {
                    eprintln!("Error reading stdin: {}", error);

                    return;
                }
            }
        }
    });

    let host = cpal::default_host();

    let device = match host.default_input_device() {
        Some(device) => device,

        None => {
            send_error_event(0, "No microphone input device available");

            return;
        }
    };

    println!("Microphone: {}", device.name().unwrap_or_default());

    let supported_config = match device.default_input_config() {
        Ok(config) => config,

        Err(error) => {
            send_error_event(
                0,
                &format!("Could not get microphone configuration: {}", error),
            );

            return;
        }
    };

    let sample_rate = supported_config.sample_rate().0;
    let channels = supported_config.channels();
    let sample_format = supported_config.sample_format();

    println!("Sample rate: {} Hz", sample_rate);
    println!("Channels: {}", channels);
    println!("Sample format: {:?}", sample_format);

    let stream_config: StreamConfig = supported_config.clone().into();

    let (tx_audio, rx_audio) = mpsc::sync_channel::<Vec<i16>>(AUDIO_QUEUE_CAPACITY);

    let (tx_chunks, rx_chunks) = mpsc::sync_channel::<AudioChunk>(TRANSCRIPTION_QUEUE_CAPACITY);

    let (tx_results, rx_results) = mpsc::sync_channel::<TranscriptionResult>(RESULT_QUEUE_CAPACITY);

    let processor_stop_flag = Arc::clone(&stop_flag);
    let tx_chunks_processor = tx_chunks.clone();
    let tx_results_processor = tx_results.clone();

    let processor_handle = thread::spawn(move || {
        process_audio(
            rx_audio,
            tx_chunks_processor,
            tx_results_processor,
            sample_rate,
            channels,
            processor_stop_flag,
        );
    });

    let mut worker_handles = Vec::new();
    let mut worker_senders = Vec::new();

    for worker_id in 0..TRANSCRIPTION_WORKERS {
        let (worker_tx, worker_rx) = mpsc::sync_channel::<AudioChunk>(WORKER_QUEUE_CAPACITY);

        let tx_results_worker = tx_results.clone();

        let handle = thread::spawn(move || {
            transcription_worker(worker_id, worker_rx, tx_results_worker);
        });

        worker_senders.push(worker_tx);
        worker_handles.push(handle);
    }

    let dispatcher_senders: Vec<SyncSender<AudioChunk>> = worker_senders.to_vec();
    let tx_results_dispatcher = tx_results.clone();

    let dispatcher_handle = thread::spawn(move || {
        dispatch_chunks(rx_chunks, dispatcher_senders, tx_results_dispatcher);
    });

    let ordered_handle = thread::spawn(move || {
        ordered_result_worker(rx_results);
    });

    let stream = match build_input_stream(&device, &stream_config, sample_format, tx_audio.clone())
    {
        Ok(stream) => stream,

        Err(error) => {
            send_error_event(0, &format!("Failed to create microphone stream: {}", error));

            return;
        }
    };

    if let Err(error) = stream.play() {
        send_error_event(0, &format!("Failed to start microphone stream: {}", error));

        return;
    }

    println!("Recording started");

    println!(
        "EVENT:{}",
        serde_json::json!({
            "event": "recording_started"
        })
    );

    while !stop_flag.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(100));
    }

    println!("Stopping recording...");

    drop(stream);

    println!("Microphone stream stopped");

    drop(tx_audio);

    println!("Audio input queue closed");

    println!("Waiting for audio processing worker...");

    if let Err(error) = processor_handle.join() {
        eprintln!("Audio processing worker failed: {:?}", error);
    }

    drop(tx_chunks);

    println!("Waiting for transcription dispatcher...");

    if let Err(error) = dispatcher_handle.join() {
        eprintln!("Dispatcher worker failed: {:?}", error);
    }

    drop(worker_senders);

    println!("Worker queues closed");

    drop(tx_results);

    println!("Waiting for transcription workers...");

    for handle in worker_handles {
        if let Err(error) = handle.join() {
            eprintln!("Transcription worker failed: {:?}", error);
        }
    }

    println!("All transcription workers stopped");

    println!("Waiting for ordered result worker...");

    if let Err(error) = ordered_handle.join() {
        eprintln!("Ordered result worker failed: {:?}", error);
    }

    println!("Ordered result worker stopped");

    println!(
        "EVENT:{}",
        serde_json::json!({
            "event": "recording_stopped"
        })
    );

    println!("Rust Audio Transcription Pipeline stopped");
}

fn build_input_stream(
    device: &cpal::Device,
    config: &StreamConfig,
    sample_format: SampleFormat,
    tx_audio: SyncSender<Vec<i16>>,
) -> Result<cpal::Stream, cpal::BuildStreamError> {
    let error_callback = |error| {
        eprintln!("Audio stream error: {}", error);
    };

    match sample_format {
        SampleFormat::F32 => {
            let tx = tx_audio;

            device.build_input_stream(
                config,
                move |data: &[f32], _| {
                    let samples: Vec<i16> = data
                        .iter()
                        .map(|sample| {
                            let clamped = sample.clamp(-1.0, 1.0);

                            (clamped * i16::MAX as f32) as i16
                        })
                        .collect();

                    match tx.try_send(samples) {
                        Ok(_) => {}

                        Err(TrySendError::Full(_)) => {
                            eprintln!("WARNING: Audio queue full. Dropping audio buffer.");
                        }

                        Err(TrySendError::Disconnected(_)) => {
                            eprintln!("Audio queue disconnected.");
                        }
                    }
                },
                error_callback,
                None,
            )
        }

        SampleFormat::I16 => {
            let tx = tx_audio;

            device.build_input_stream(
                config,
                move |data: &[i16], _| {
                    let samples = data.to_vec();

                    match tx.try_send(samples) {
                        Ok(_) => {}

                        Err(TrySendError::Full(_)) => {
                            eprintln!("WARNING: Audio queue full. Dropping audio buffer.");
                        }

                        Err(TrySendError::Disconnected(_)) => {
                            eprintln!("Audio queue disconnected.");
                        }
                    }
                },
                error_callback,
                None,
            )
        }

        SampleFormat::U16 => {
            let tx = tx_audio;

            device.build_input_stream(
                config,
                move |data: &[u16], _| {
                    let samples: Vec<i16> = data
                        .iter()
                        .map(|sample| (*sample as i32 - 32768) as i16)
                        .collect();

                    match tx.try_send(samples) {
                        Ok(_) => {}

                        Err(TrySendError::Full(_)) => {
                            eprintln!("WARNING: Audio queue full. Dropping audio buffer.");
                        }

                        Err(TrySendError::Disconnected(_)) => {
                            eprintln!("Audio queue disconnected.");
                        }
                    }
                },
                error_callback,
                None,
            )
        }

        _ => {
            panic!("Unsupported microphone sample format: {:?}", sample_format);
        }
    }
}

fn process_audio(
    rx_audio: Receiver<Vec<i16>>,
    tx_chunks: SyncSender<AudioChunk>,
    tx_results: SyncSender<TranscriptionResult>,
    input_sample_rate: u32,
    channels: u16,
    stop_flag: Arc<AtomicBool>,
) {
    println!("Audio processing worker started");

    let mut audio_buffer: Vec<i16> = Vec::new();

    let mut vad_state = VadState {
        speech_active: false,
        consecutive_silent_chunks: 0,
    };

    let mut chunk_id: u64 = 0;

    loop {
        match rx_audio.recv_timeout(Duration::from_millis(100)) {
            Ok(raw_samples) => {
                let mono_samples = stereo_to_mono(&raw_samples, channels as usize);

                let resampled_samples =
                    resample_linear(&mono_samples, input_sample_rate, TARGET_SAMPLE_RATE);

                audio_buffer.extend(resampled_samples);

                while audio_buffer.len() >= CHUNK_SAMPLES {
                    let samples: Vec<i16> = audio_buffer.drain(..CHUNK_SAMPLES).collect();

                    let start_ms = chunk_id * CHUNK_DURATION_MS;
                    let end_ms = start_ms + CHUNK_DURATION_MS;

                    create_and_send_chunk(
                        chunk_id,
                        start_ms,
                        end_ms,
                        samples,
                        &tx_chunks,
                        &tx_results,
                        &mut vad_state,
                    );

                    chunk_id += 1;
                }
            }

            Err(RecvTimeoutError::Timeout) => {
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
            }

            Err(RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    while let Ok(raw_samples) = rx_audio.try_recv() {
        let mono_samples = stereo_to_mono(&raw_samples, channels as usize);

        let resampled_samples =
            resample_linear(&mono_samples, input_sample_rate, TARGET_SAMPLE_RATE);

        audio_buffer.extend(resampled_samples);

        while audio_buffer.len() >= CHUNK_SAMPLES {
            let samples: Vec<i16> = audio_buffer.drain(..CHUNK_SAMPLES).collect();

            let start_ms = chunk_id * CHUNK_DURATION_MS;
            let end_ms = start_ms + CHUNK_DURATION_MS;

            create_and_send_chunk(
                chunk_id,
                start_ms,
                end_ms,
                samples,
                &tx_chunks,
                &tx_results,
                &mut vad_state,
            );

            chunk_id += 1;
        }
    }

    if !audio_buffer.is_empty() {
        println!("Creating final partial chunk");

        let remaining_samples = audio_buffer.len();

        let duration_ms = ((remaining_samples as u64 * 1000) / TARGET_SAMPLE_RATE as u64).max(1);

        let start_ms = chunk_id * CHUNK_DURATION_MS;
        let end_ms = start_ms + duration_ms;

        let final_samples = std::mem::take(&mut audio_buffer);

        create_and_send_chunk(
            chunk_id,
            start_ms,
            end_ms,
            final_samples,
            &tx_chunks,
            &tx_results,
            &mut vad_state,
        );
    }

    println!("Audio processing worker stopped");
}

fn detect_voice_activity(samples: &[i16], vad_state: &mut VadState) -> bool {
    if samples.is_empty() {
        return false;
    }

    let mut sum_squares = 0.0_f64;

    for &sample in samples {
        let value = sample as f64;
        sum_squares += value * value;
    }

    let rms = (sum_squares / samples.len() as f64).sqrt();

    println!(
        "VAD DEBUG: RMS = {:.2}, Start = {:.2}, Continue = {:.2}, Silent chunks = {}",
        rms, SPEECH_START_THRESHOLD, SPEECH_CONTINUE_THRESHOLD, vad_state.consecutive_silent_chunks
    );

    if vad_state.speech_active {
        if rms >= SPEECH_CONTINUE_THRESHOLD {
            vad_state.consecutive_silent_chunks = 0;
        } else {
            vad_state.consecutive_silent_chunks += 1;

            if vad_state.consecutive_silent_chunks >= SILENT_CHUNKS_TO_END_SPEECH {
                vad_state.speech_active = false;
                vad_state.consecutive_silent_chunks = 0;
            }
        }
    } else {
        vad_state.consecutive_silent_chunks = 0;

        if rms >= SPEECH_START_THRESHOLD {
            vad_state.speech_active = true;
        }
    }

    if vad_state.speech_active {
        println!("VAD: SPEECH");
    } else {
        println!("VAD: SILENCE");
    }

    vad_state.speech_active
}

fn create_and_send_chunk(
    chunk_id: u64,
    start_ms: u64,
    end_ms: u64,
    samples: Vec<i16>,
    tx_chunks: &SyncSender<AudioChunk>,
    tx_results: &SyncSender<TranscriptionResult>,
    vad_state: &mut VadState,
) {
    let has_speech = detect_voice_activity(&samples, vad_state);

    println!(
        "VAD: chunk {} → {}",
        chunk_id,
        if has_speech { "SPEECH" } else { "SILENCE" }
    );

    if !has_speech {
        println!("VAD: skipping silent chunk {}", chunk_id);

        let result = TranscriptionResult {
            chunk_id,
            start_ms,
            end_ms,
            outcome: ChunkOutcome::Skipped("Silence detected by VAD".to_string()),
        };

        if tx_results.try_send(result).is_err() {
            eprintln!("VAD: failed to send skipped chunk {} result", chunk_id);
        }

        return;
    }

    println!();
    println!("================================");
    println!("Audio chunk created");
    println!("================================");

    println!("Chunk ID: {}", chunk_id);
    println!("Start: {} ms", start_ms);
    println!("End: {} ms", end_ms);
    println!("Samples: {}", samples.len());
    println!("Sample rate: {} Hz", TARGET_SAMPLE_RATE);
    println!("Channels: 1");

    let max_amplitude = samples
        .iter()
        .map(|sample| sample.abs() as i32)
        .max()
        .unwrap_or(0);

    let average_amplitude = if samples.is_empty() {
        0
    } else {
        samples
            .iter()
            .map(|sample| sample.unsigned_abs() as u64)
            .sum::<u64>()
            / samples.len() as u64
    };

    println!("Max amplitude: {}", max_amplitude);

    println!("Average amplitude: {}", average_amplitude);

    let chunk = AudioChunk {
        chunk_id,
        start_ms,
        end_ms,
        samples,
    };

    match tx_chunks.try_send(chunk) {
        Ok(_) => {
            println!("Chunk {} added to transcription queue", chunk_id);
        }

        Err(TrySendError::Full(chunk)) => {
            println!(
                "WARNING: Transcription queue full. Dropping chunk {}",
                chunk_id
            );

            send_dropped_result(
                tx_results,
                chunk.chunk_id,
                chunk.start_ms,
                chunk.end_ms,
                "Transcription queue full",
            );
        }

        Err(TrySendError::Disconnected(chunk)) => {
            println!(
                "WARNING: Transcription queue disconnected. Dropping chunk {}",
                chunk_id
            );

            send_dropped_result(
                tx_results,
                chunk.chunk_id,
                chunk.start_ms,
                chunk.end_ms,
                "Transcription queue disconnected",
            );
        }
    }
}

fn dispatch_chunks(
    rx_chunks: Receiver<AudioChunk>,
    worker_senders: Vec<SyncSender<AudioChunk>>,
    tx_results: SyncSender<TranscriptionResult>,
) {
    println!("Transcription dispatcher started");

    let worker_count = worker_senders.len();

    if worker_count == 0 {
        eprintln!("No transcription workers available");

        return;
    }

    let mut next_worker: usize = 0;

    while let Ok(chunk) = rx_chunks.recv() {
        let mut chunk = Some(chunk);
        let mut dispatched = false;

        for attempt in 0..worker_count {
            let worker_index = (next_worker + attempt) % worker_count;

            let current_chunk = chunk
                .take()
                .expect("Chunk should exist before worker attempt");

            match worker_senders[worker_index].try_send(current_chunk) {
                Ok(_) => {
                    println!("Chunk dispatched to transcription worker {}", worker_index);

                    next_worker = (worker_index + 1) % worker_count;

                    dispatched = true;

                    break;
                }

                Err(TrySendError::Full(returned_chunk)) => {
                    chunk = Some(returned_chunk);
                }

                Err(TrySendError::Disconnected(returned_chunk)) => {
                    chunk = Some(returned_chunk);
                }
            }
        }

        if !dispatched {
            let chunk = chunk.expect("Chunk must exist when dispatch fails");

            println!(
                "WARNING: All transcription workers are busy. Dropping chunk {}",
                chunk.chunk_id
            );

            send_dropped_result(
                &tx_results,
                chunk.chunk_id,
                chunk.start_ms,
                chunk.end_ms,
                "All transcription workers are busy",
            );
        }
    }

    println!("Transcription dispatcher stopped");
}

fn send_dropped_result(
    tx_results: &SyncSender<TranscriptionResult>,
    chunk_id: u64,
    start_ms: u64,
    end_ms: u64,
    reason: &str,
) {
    let result = TranscriptionResult {
        chunk_id,
        start_ms,
        end_ms,
        outcome: ChunkOutcome::Dropped(reason.to_string()),
    };

    if tx_results.send(result).is_err() {
        eprintln!("Failed to send dropped chunk result for {}", chunk_id);
    }
}

fn transcription_worker(
    worker_id: usize,
    rx: Receiver<AudioChunk>,
    tx_results: SyncSender<TranscriptionResult>,
) {
    println!("Transcription worker {} started", worker_id);

    while let Ok(chunk) = rx.recv() {
        println!("Worker {} received chunk {}", worker_id, chunk.chunk_id);

        println!(
            "Sending chunk {} to transcription service...",
            chunk.chunk_id
        );

        let outcome = match transcribe_chunk(&chunk) {
            Ok(text) => ChunkOutcome::Transcript(text),

            Err(error) => ChunkOutcome::Error(error),
        };

        let result = TranscriptionResult {
            chunk_id: chunk.chunk_id,
            start_ms: chunk.start_ms,
            end_ms: chunk.end_ms,
            outcome,
        };

        if tx_results.send(result).is_err() {
            eprintln!("Result queue disconnected for worker {}", worker_id);

            break;
        }
    }

    println!("Transcription worker {} stopped", worker_id);
}

fn ordered_result_worker(rx_results: Receiver<TranscriptionResult>) {
    println!("Ordered result worker started");

    let mut pending_results: BTreeMap<u64, TranscriptionResult> = BTreeMap::new();

    let mut expected_chunk_id: u64 = 0;

    while let Ok(result) = rx_results.recv() {
        println!("Result received for chunk {}", result.chunk_id);

        pending_results.insert(result.chunk_id, result);

        while let Some(result) = pending_results.remove(&expected_chunk_id) {
            emit_ordered_result(&result);

            expected_chunk_id += 1;
        }
    }

    for (_, result) in pending_results {
        emit_ordered_result(&result);
    }
}

fn emit_ordered_result(result: &TranscriptionResult) {
    match &result.outcome {
        ChunkOutcome::Transcript(text) => {
            if text.trim().is_empty() {
                println!("No speech detected in chunk {}", result.chunk_id);
            } else {
                println!("Transcript received for chunk {}:", result.chunk_id);

                println!("\"{}\"", text);
            }

            println!(
                "EVENT:{}",
                serde_json::json!({
                    "event": "transcript",
                    "chunk_id": result.chunk_id,
                    "start_ms": result.start_ms,
                    "end_ms": result.end_ms,
                    "text": text
                })
            );
        }

        ChunkOutcome::Error(error) => {
            eprintln!(
                "Transcription error for chunk {}: {}",
                result.chunk_id, error
            );

            println!(
                "EVENT:{}",
                serde_json::json!({
                    "event": "error",
                    "chunk_id": result.chunk_id,
                    "start_ms": result.start_ms,
                    "end_ms": result.end_ms,
                    "message": error
                })
            );
        }

        ChunkOutcome::Dropped(reason) => {
            eprintln!("Chunk {} dropped: {}", result.chunk_id, reason);

            println!(
                "EVENT:{}",
                serde_json::json!({
                    "event": "error",
                    "chunk_id": result.chunk_id,
                    "start_ms": result.start_ms,
                    "end_ms": result.end_ms,
                    "message": reason,
                    "type": "backpressure"
                })
            );
        }

        ChunkOutcome::Skipped(reason) => {
            println!("Chunk {} skipped: {}", result.chunk_id, reason);

            println!(
                "EVENT:{}",
                serde_json::json!({
                    "event": "skipped",
                    "chunk_id": result.chunk_id,
                    "start_ms": result.start_ms,
                    "end_ms": result.end_ms,
                    "reason": reason
                })
            );
        }
    }
}

fn transcribe_chunk(chunk: &AudioChunk) -> Result<String, String> {
    let api_key = env::var("ASSEMBLYAI_API_KEY")
        .map_err(|_| "ASSEMBLYAI_API_KEY environment variable is not set".to_string())?;

    let wav_data = create_wav(&chunk.samples);

    println!("WAV data created: {} bytes", wav_data.len());

    let client = Client::builder()
        .timeout(Duration::from_secs(API_TIMEOUT_SECONDS))
        .build()
        .map_err(|error| format!("Failed to create HTTP client: {}", error))?;

    let mut last_error = String::from("Unknown transcription error");

    for attempt in 1..=MAX_TRANSCRIPTION_ATTEMPTS {
        println!(
            "Transcription attempt {}/{} for chunk {}",
            attempt, MAX_TRANSCRIPTION_ATTEMPTS, chunk.chunk_id
        );

        match send_transcription_request(&client, &api_key, &wav_data) {
            Ok(text) => {
                return Ok(text);
            }

            Err(error) => {
                eprintln!(
                    "Transcription attempt {} failed for chunk {}: {}",
                    attempt, chunk.chunk_id, error
                );

                last_error = error;

                if attempt < MAX_TRANSCRIPTION_ATTEMPTS {
                    let backoff_seconds = attempt as u64;

                    println!(
                        "Retrying chunk {} after {} second(s)...",
                        chunk.chunk_id, backoff_seconds
                    );

                    thread::sleep(Duration::from_secs(backoff_seconds));
                }
            }
        }
    }

    Err(last_error)
}

fn send_transcription_request(
    client: &Client,
    api_key: &str,
    wav_data: &[u8],
) -> Result<String, String> {
    let part = multipart::Part::bytes(wav_data.to_vec())
        .file_name("chunk.wav")
        .mime_str("audio/wav")
        .map_err(|error| format!("Failed to create multipart audio: {}", error))?;

    let form = multipart::Form::new().part("audio", part);

    let response = client
        .post(
            env::var("TRANSCRIPTION_URL")
                .unwrap_or_else(|_| "https://sync.assemblyai.com/transcribe".to_string()),
        )
        .header("Authorization", api_key)
        .header("X-AAI-Model", "universal-3-5-pro")
        .multipart(form)
        .send()
        .map_err(|error| format!("HTTP request failed: {}", error))?;

    println!("Transcription HTTP status: {}", response.status());

    let status = response.status();

    let body = response
        .text()
        .map_err(|error| format!("Failed to read API response: {}", error))?;

    if !status.is_success() {
        return Err(format!("Transcription API returned {}: {}", status, body));
    }

    let parsed: TranscriptionResponse = serde_json::from_str(&body)
        .map_err(|error| format!("Failed to parse transcription response: {}", error))?;

    if let Some(error) = parsed.error {
        return Err(format!("Transcription service error: {}", error));
    }

    Ok(parsed.text.unwrap_or_default())
}

fn send_error_event(chunk_id: u64, message: &str) {
    println!(
        "EVENT:{}",
        serde_json::json!({
            "event": "error",
            "chunk_id": chunk_id,
            "message": message
        })
    );
}

fn stereo_to_mono(samples: &[i16], channels: usize) -> Vec<i16> {
    if channels <= 1 {
        return samples.to_vec();
    }

    let frame_count = samples.len() / channels;

    let mut mono = Vec::with_capacity(frame_count);

    for frame in 0..frame_count {
        let start = frame * channels;

        let end = start + channels;

        let frame_samples = &samples[start..end];

        let sum: i32 = frame_samples.iter().map(|&sample| sample as i32).sum();

        let average = sum / channels as i32;

        mono.push(average as i16);
    }

    mono
}

fn resample_linear(samples: &[i16], input_sample_rate: u32, output_sample_rate: u32) -> Vec<i16> {
    if samples.is_empty() {
        return Vec::new();
    }

    if input_sample_rate == output_sample_rate {
        return samples.to_vec();
    }

    let input_rate = input_sample_rate as f64;

    let output_rate = output_sample_rate as f64;

    let output_length = ((samples.len() as f64) * output_rate / input_rate).ceil() as usize;

    let mut output = Vec::with_capacity(output_length);

    let ratio = input_rate / output_rate;

    for i in 0..output_length {
        let source_position = i as f64 * ratio;

        let index = source_position.floor() as usize;

        let fraction = source_position - index as f64;

        if index + 1 < samples.len() {
            let sample1 = samples[index] as f64;

            let sample2 = samples[index + 1] as f64;

            let interpolated = sample1 + (sample2 - sample1) * fraction;

            let clamped = interpolated.clamp(i16::MIN as f64, i16::MAX as f64);

            output.push(clamped as i16);
        } else {
            output.push(samples[samples.len() - 1]);
        }
    }

    output
}

fn create_wav(samples: &[i16]) -> Vec<u8> {
    let sample_rate = TARGET_SAMPLE_RATE;

    let channels: u16 = 1;

    let bits_per_sample: u16 = 16;

    let bytes_per_sample = bits_per_sample / 8;

    let data_size = (samples.len() * bytes_per_sample as usize) as u32;

    let file_size = 36 + data_size;

    let byte_rate = sample_rate * channels as u32 * bytes_per_sample as u32;

    let block_align = channels * bytes_per_sample;

    let mut wav = Vec::with_capacity(file_size as usize + 8);

    wav.extend_from_slice(b"RIFF");

    wav.extend_from_slice(&file_size.to_le_bytes());

    wav.extend_from_slice(b"WAVE");

    wav.extend_from_slice(b"fmt ");

    wav.extend_from_slice(&16u32.to_le_bytes());

    wav.extend_from_slice(&1u16.to_le_bytes());

    wav.extend_from_slice(&channels.to_le_bytes());

    wav.extend_from_slice(&sample_rate.to_le_bytes());

    wav.extend_from_slice(&byte_rate.to_le_bytes());

    wav.extend_from_slice(&block_align.to_le_bytes());

    wav.extend_from_slice(&bits_per_sample.to_le_bytes());

    wav.extend_from_slice(b"data");

    wav.extend_from_slice(&data_size.to_le_bytes());

    for &sample in samples {
        wav.extend_from_slice(&sample.to_le_bytes());
    }

    wav
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stereo_to_mono() {
        let samples = vec![1000, 3000, 2000, 4000, 3000, 5000];

        let mono = stereo_to_mono(&samples, 2);

        assert_eq!(mono, vec![2000, 3000, 4000]);
    }

    #[test]
    fn test_stereo_to_mono_single_channel() {
        let samples = vec![1000, 2000, 3000];

        let mono = stereo_to_mono(&samples, 1);

        assert_eq!(mono, samples);
    }

    #[test]
    fn test_resample_linear() {
        let samples = vec![0, 1000, 2000, 3000];

        let output = resample_linear(&samples, 8000, 16000);

        assert!(!output.is_empty());
        assert!(output.len() > samples.len());
    }

    #[test]
    fn test_create_wav() {
        let samples = vec![0i16; 16000];

        let wav = create_wav(&samples);

        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");

        assert_eq!(wav.len(), 32044);
    }

    #[test]
    fn test_empty_wav() {
        let samples: Vec<i16> = Vec::new();

        let wav = create_wav(&samples);

        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
    }
}
