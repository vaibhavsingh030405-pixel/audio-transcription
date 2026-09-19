# Rust + Electron Audio Transcription Pipeline

A real-time audio transcription application built using **Rust, CPAL, Electron, and AssemblyAI**.

The application captures microphone audio, processes it into the required format, divides it into chunks, sends speech chunks for transcription, and displays the transcript in an Electron UI.

---

## Architecture

```text
Electron UI
    |
    | IPC
    v
Electron Main Process
    |
    v
Rust Audio Module
    |
    v
Microphone
    |
    v
Audio Capture
    |
    v
Audio Processing
    |
    +--> Stereo -> Mono
    +--> Resampling -> 16 kHz
    +--> VAD
    |
    v
1-Second Chunks
    |
    v
Bounded Queue
    |
    v
Transcription Workers
    |
    v
AssemblyAI
    |
    v
Result Ordering
    |
    v
Rust Events
    |
    | IPC
    v
Electron UI
    |
    v
Transcript
```

---

## Project Structure

```text
D:\audio-transcription
│
├── README.md
│
├── rust
│   └── audio-transcription-rust
│       ├── Cargo.toml
│       └── src
│           ├── main.rs
│           └── bin
│               ├── concurrency_test.rs
│               └── backpressure_test.rs
│
└── electron
    ├── package.json
    └── src
        ├── main.js
        ├── preload.js
        ├── renderer.js
        ├── index.html
        └── index.css
```

---

## Setup

### Requirements

Install the following before running the project:

* Rust and Cargo
* Microsoft C++ Build Tools with MSVC
* Node.js and npm
* Electron dependencies
* A working microphone
* AssemblyAI API key

### Rust Setup

Open PowerShell:

```powershell
cd D:\audio-transcription\rust\audio-transcription-rust
```

Check Rust:

```powershell
rustc --version
cargo --version
```

Build the project:

```powershell
cargo build
```

Run tests:

```powershell
cargo test
```

### Electron Setup

Open another PowerShell window:

```powershell
cd D:\audio-transcription\electron
```

Install dependencies:

```powershell
npm install
```

### API Key Setup

Set the AssemblyAI API key in PowerShell:

```powershell
$env:ASSEMBLYAI_API_KEY="YOUR_API_KEY"
```

Optional transcription endpoint:

```powershell
$env:TRANSCRIPTION_URL="YOUR_TRANSCRIPTION_ENDPOINT"
```

Do not hardcode API credentials in the source code or commit them to Git.

---

## Running the Application

Start the Rust application:

```powershell
cd D:\audio-transcription\rust\audio-transcription-rust
cargo run
```

Start Electron in another terminal:

```powershell
cd D:\audio-transcription\electron
npm start
```

The application workflow is:

```text
Start
  ↓
Electron
  ↓
Rust
  ↓
Microphone
  ↓
Audio Processing
  ↓
VAD
  ↓
Audio Chunks
  ↓
Transcription
  ↓
Transcript
  ↓
Electron UI
```

---

## Audio Processing

The pipeline converts microphone audio to:

```text
Sample rate: 16000 Hz
Channels:    1
Format:      16-bit PCM
```

Processing includes:

* Stereo-to-mono conversion
* Sample-rate conversion
* Voice Activity Detection
* Audio chunking

Audio is divided into approximately **1-second chunks**, containing approximately **16,000 samples**.

---

## Voice Activity Detection

The project uses RMS-based VAD.

```text
Speech start threshold:    800
Speech continue threshold: 500
Silent chunks to end:        3
```

Silent chunks can be skipped instead of being sent for transcription.

The current VAD is amplitude-based, so loud background noise can sometimes be detected as speech.

---

## Concurrency

The transcription pipeline uses **3 workers**.

```text
             Dispatcher
            /     |     \
           v      v      v
       Worker 0 Worker 1 Worker 2
           \      |      /
            \     |     /
             v    v    v
          AssemblyAI
```

Multiple workers allow transcription requests to run concurrently.

Because workers can finish in different orders, results are reordered using `chunk_id`.

---

## Backpressure

The application uses bounded queues to prevent unlimited memory growth.

```text
Audio queue:          10
Transcription queue:   5
Worker queue:          2
Result queue:         10
```

When a queue becomes full, the current policy is to drop the new chunk and report backpressure.

Example:

```text
BACKPRESSURE: queue full, dropped chunk 7
```

Backpressure test:

```text
Total chunks: 30
Chunks queued: 11
Chunks dropped: 19
Queue capacity: 5
```

---

## Transcription

Processed audio is converted to WAV and sent to AssemblyAI.

The API key is provided through:

```text
ASSEMBLYAI_API_KEY
```

The transcription URL can be configured using:

```text
TRANSCRIPTION_URL
```

Transcription requests support:

```text
Maximum attempts: 3
Timeout: 30 seconds
```

---

## Electron ↔ Rust Communication

Electron controls the Rust process through the Electron main process.

```text
Renderer
   |
   | IPC
   v
Electron Main
   |
   v
Rust
```

Rust sends structured events through stdout.

Important events include:

```text
recording_started
recording_stopped
transcript
error
skipped
backpressure
```

Electron receives these events and updates the UI.

---

## Testing

### Audio Processing

```powershell
cd D:\audio-transcription\rust\audio-transcription-rust
cargo test
```

Current result:

```text
5 passed
0 failed
```

Tests cover:

* Stereo-to-mono conversion
* Single-channel handling
* Resampling
* WAV generation
* Empty WAV generation

### Concurrency

```powershell
cargo run --bin concurrency_test
```

Verifies concurrent workers and result ordering.

### Backpressure

```powershell
cargo run --bin backpressure_test
```

Verifies bounded queues and queue-full handling.

---

## Error Handling

The application handles:

* Microphone errors
* API errors
* Network failures
* Request timeouts
* Queue disconnection
* Transcription failures
* Shutdown conditions

Errors are reported through Rust events instead of intentionally crashing the application.

---

## Known Limitations

* Transcription uses approximately 1-second independent chunks.
* Words can sometimes be split between chunk boundaries.
* RMS-based VAD can mistake loud noise for speech.
* Chunks may be dropped when transcription queues are full.
* Transcription requires network connectivity.

---

## Future Improvements

* Streaming transcription
* Better VAD
* Exponential backoff
* Microphone switching
* Latency and performance metrics
* Reduced audio copying
* Improved transcript merging

---

## Conclusion

This project demonstrates a complete Rust and Electron audio-transcription pipeline with:

* Native microphone capture
* Audio processing
* 16 kHz mono PCM conversion
* VAD
* Audio chunking
* Concurrent transcription
* Result ordering
* Bounded queues
* Backpressure handling
* Error handling
* Electron ↔ Rust IPC
* Automated testing

The architecture separates audio processing from network transcription so that slow transcription requests do not directly block the audio pipeline.
