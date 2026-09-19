
const startButton = document.getElementById("startButton");
const stopButton = document.getElementById("stopButton");
const status = document.getElementById("status");
const transcript = document.getElementById("transcript");
const transcriptEmpty = document.getElementById("transcriptEmpty");
const chunkCount = document.getElementById("chunkCount");


// ==================================================
// DEBUG: CHECK HTML ELEMENTS
// ==================================================

console.log("startButton:", startButton);
console.log("stopButton:", stopButton);
console.log("status:", status);
console.log("transcript:", transcript);
console.log("transcriptEmpty:", transcriptEmpty);
console.log("chunkCount:", chunkCount);


// ==================================================
// INITIAL UI STATE
// ==================================================

if (!startButton || !stopButton) {
    console.error("Required buttons were not found in index.html");
} else {
    startButton.disabled = false;
    stopButton.disabled = true;
}


// ==================================================
// START RECORDING
// ==================================================

startButton.addEventListener("click", () => {

    console.log("Start button clicked");

    // Update status
    status.textContent = "Starting recording...";

    // Button state
    startButton.disabled = true;
    stopButton.disabled = false;

    // Clear previous transcript
    transcript.textContent = "";

    // Show empty transcript message
    if (transcriptEmpty) {
        transcriptEmpty.style.display = "block";
    }

    // Reset chunk counter
    if (chunkCount) {
        chunkCount.textContent = "Chunks processed: 0";
    }

    // Start Rust process
    window.audioAPI.startRecording();
});


// ==================================================
// STOP RECORDING
// ==================================================

stopButton.addEventListener("click", () => {

    console.log("Stop button clicked");

    // Update status
    status.textContent = "Stopping recording...";

    // Disable both buttons while Rust shuts down
    startButton.disabled = true;
    stopButton.disabled = true;

    // Tell Rust to stop
    window.audioAPI.stopRecording();
});


// ==================================================
// NORMAL MESSAGE FROM RUST
// ==================================================

window.audioAPI.onRustMessage((message) => {

    console.log("Message from Rust:", message);

});


// ==================================================
// RUST PROCESS ERROR
// ==================================================

window.audioAPI.onRustError((error) => {

    console.error("Rust error:", error);

    status.textContent = `Error: ${error}`;

    // Allow user to start again
    startButton.disabled = false;
    stopButton.disabled = true;

});


// ==================================================
// RUST PROCESS STOPPED
// ==================================================

window.audioAPI.onRustStopped(() => {

    console.log("Rust process stopped");

    status.textContent = "Recording stopped";

    // Allow another recording
    startButton.disabled = false;
    stopButton.disabled = true;

});


// ==================================================
// STRUCTURED EVENTS FROM RUST
// ==================================================

window.audioAPI.onRustEvent((event) => {

    console.log("Rust Event received:", event);


    // ==================================================
    // RECORDING STARTED
    // ==================================================

    if (event.event === "recording_started") {

        status.textContent = "Recording started";

    }


    // ==================================================
    // TRANSCRIPT RECEIVED
    // ==================================================

    if (event.event === "transcript") {

        // Safely read transcript text
        const newText = event.text?.trim() || "";


        // ------------------------------------------------
        // EMPTY TRANSCRIPT
        // ------------------------------------------------

        if (newText === "") {

            console.log(
                `No speech detected in chunk ${event.chunk_id}`
            );

            return;
        }


        // ------------------------------------------------
        // DEBUG
        // ------------------------------------------------

        console.log(
            `Transcript received from chunk ${event.chunk_id}:`,
            newText
        );


        // ------------------------------------------------
        // UPDATE STATUS
        // ------------------------------------------------

        status.textContent = "Transcription received";


        // ------------------------------------------------
        // HIDE EMPTY TRANSCRIPT MESSAGE
        // ------------------------------------------------

        if (transcriptEmpty) {

            transcriptEmpty.style.display = "none";

        }


        // ------------------------------------------------
        // APPEND TRANSCRIPT
        // ------------------------------------------------
        //
        // IMPORTANT:
        // We DO NOT display the chunk ID.
        //
        // Instead of:
        //
        // Chunk 5: Hello
        // Chunk 6: my name is Vaibhav
        //
        // We display:
        //
        // Hello my name is Vaibhav
        //
        // ------------------------------------------------

        const currentText = transcript.textContent.trim();


        if (currentText === "") {

            transcript.textContent = newText;

        } else {

            transcript.textContent =
                `${currentText} ${newText}`;

        }


        // ------------------------------------------------
        // UPDATE CHUNK COUNTER
        // ------------------------------------------------

        if (chunkCount) {

            chunkCount.textContent =
                `Chunks processed: ${event.chunk_id + 1}`;

        }

    }


    // ==================================================
    // TRANSCRIPTION ERROR
    // ==================================================

    if (event.event === "error") {


        // ------------------------------------------------
        // BACKPRESSURE
        // ------------------------------------------------
        //
        // Backpressure is expected when the queues are
        // full. It should NOT break the whole application.
        // ------------------------------------------------

        if (event.type === "backpressure") {

            console.warn(
                `Backpressure: chunk ${event.chunk_id} dropped: ${event.message}`
            );

            return;
        }


        // ------------------------------------------------
        // REAL TRANSCRIPTION/API ERROR
        // ------------------------------------------------

        console.error(
            `Chunk ${event.chunk_id} error:`,
            event.message
        );


        status.textContent =
            `Transcription error: ${event.message}`;

    }


    // ==================================================
    // CHUNK SKIPPED
    // ==================================================

    if (event.event === "skipped") {

        console.log(
            `Chunk ${event.chunk_id} skipped: ${event.reason}`
        );

    }


    // ==================================================
    // RECORDING STOPPED
    // ==================================================

    if (event.event === "recording_stopped") {

        console.log("Recording stopped event received");

        status.textContent = "Recording stopped";

        // Allow another recording
        startButton.disabled = false;
        stopButton.disabled = true;

    }

});

