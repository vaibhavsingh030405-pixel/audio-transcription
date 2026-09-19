const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("audioAPI", {
  startRecording: () => {
    ipcRenderer.send("start-recording");
  },

  stopRecording: () => {
    ipcRenderer.send("stop-recording");
  },

  onRustMessage: (callback) => {
    ipcRenderer.on("rust-message", (event, message) => {
      callback(message);
    });
  },

  onRustEvent: (callback) => {
    ipcRenderer.on("rust-event", (event, data) => {
      callback(data);
    });
  },

  onRustError: (callback) => {
    ipcRenderer.on("rust-error", (event, error) => {
      callback(error);
    });
  },

  onRustStopped: (callback) => {
    ipcRenderer.on("rust-stopped", () => {
      callback();
    });
  },
});