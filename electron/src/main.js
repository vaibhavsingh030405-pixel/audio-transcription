const { app, BrowserWindow, ipcMain } = require("electron");
const { spawn } = require("node:child_process");

let mainWindow;
let rustProcess = null;
let rustOutputBuffer = "";

// ============================================================
// CREATE ELECTRON WINDOWA
// ============================================================

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1000,
    height: 700,

    webPreferences: {
      preload: MAIN_WINDOW_PRELOAD_WEBPACK_ENTRY,
    },
  });

  mainWindow.loadURL(MAIN_WINDOW_WEBPACK_ENTRY);

  // Open DevTools during development
  mainWindow.webContents.openDevTools();
}

// ============================================================
// ELECTRON READY
// ============================================================

app.whenReady().then(() => {
  createWindow();

  // ==========================================================
  // START RECORDING
  // ==========================================================

  ipcMain.on("start-recording", () => {
    console.log("Electron: Start Recording");

    // Prevent starting Rust twice
    if (rustProcess) {
      console.log("Rust process is already running");
      return;
    }
    rustOutputBuffer = "";

    // Path to Rust executable
    const rustPath =
      "D:\\audio-transcription\\rust\\audio-transcription-rust\\target\\debug\\audio-transcription-rust.exe";

    console.log("Starting Rust:");
    console.log(rustPath);

    // Start Rust process
    rustProcess = spawn(rustPath);

    // ========================================================
    // RUST STDOUT
    // ========================================================

    

rustProcess.stdout.on("data", (data) => {
  const message = data.toString();

  console.log("Rust:", message);

  // Send raw Rust output to renderer if needed
  mainWindow.webContents.send("rust-message", message);

  // Add new data to our buffer
  rustOutputBuffer += message;

  // Split complete lines
  const lines = rustOutputBuffer.split(/\r?\n/);

  // Keep the last incomplete line in the buffer
  rustOutputBuffer = lines.pop() || "";

  for (const line of lines) {
    if (!line.startsWith("EVENT:")) {
      continue;
    }

    const jsonText = line.substring("EVENT:".length);

    try {
      const event = JSON.parse(jsonText);

      console.log("Rust Event:", event);

      // Send parsed event to renderer
      mainWindow.webContents.send("rust-event", event);
    } catch (error) {
      console.error(
        "Invalid Rust event:",
        jsonText
      );

      mainWindow.webContents.send(
        "rust-error",
        `Invalid Rust event: ${error.message}`
      );
    }
  }
});

    // ========================================================
    // RUST STDERR
    // ========================================================

    rustProcess.stderr.on("data", (data) => {
  const message = data.toString().trim();

  if (message) {
    console.warn("Rust:", message);
  }
});

    // ========================================================
    // RUST PROCESS EXITED
    // ========================================================

    rustProcess.on("close", (code) => {
      console.log(
        "Rust process exited with code:",
        code
      );

      // Rust is no longer running
      rustProcess = null;
      rustOutputBuffer = "";

      // Tell renderer that Rust stopped
      mainWindow.webContents.send("rust-stopped");
    });

    // ========================================================
    // RUST PROCESS ERROR
    // ========================================================

    rustProcess.on("error", (error) => {
      console.error(
        "Failed to start Rust:",
        error
      );

      mainWindow.webContents.send(
        "rust-error",
        `Failed to start Rust: ${error.message}`
      );

      rustProcess = null;
    });
  });

  // ==========================================================
  // STOP RECORDING
  // ==========================================================

  ipcMain.on("stop-recording", () => {
    console.log("Electron: Stop Recording");

    if (!rustProcess) {
      console.log("Rust process is not running");
      return;
    }

    // Check whether stdin is available
    if (
      rustProcess.stdin &&
      !rustProcess.stdin.destroyed
    ) {
      // Send STOP command to Rust
      rustProcess.stdin.write(
        '{"command":"stop"}\n'
      );

      console.log(
        "Electron: STOP command sent to Rust"
      );
    } else {
      console.log(
        "Rust stdin is not available"
      );
    }
  });
});

// ============================================================
// CLOSE ELECTRON
// ============================================================

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});