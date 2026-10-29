// The operating system's requests to stop a server process, for a server's main().
#pragma once

namespace chargewire {

// On POSIX: SIGINT and SIGTERM. On Windows: the console's Ctrl+C, Ctrl+Break, close and
// shutdown events (Ctrl+Break is what a parent sends to a child it started in its own
// process group, as `kill` sends SIGTERM). A close or shutdown event counts as two
// requests, so a server that drains on the first stops at once: Windows ends the process
// a few seconds after either, whatever it is doing.
//
// Construct one in main() before any other thread starts: on POSIX it blocks the signals
// in the calling thread, so every thread started afterwards inherits the block and only
// Wait() receives them. One per process.
class StopSignals {
 public:
  StopSignals();
  // On Windows, a close or shutdown event waits for this (a few seconds at most), so the
  // process can finish stopping before Windows ends it.
  ~StopSignals();
  StopSignals(const StopSignals&) = delete;
  StopSignals& operator=(const StopSignals&) = delete;

  // Blocks until the next request (each request is delivered to one Wait()) or Wake().
  void Wait();
  // Ends one Wait() as a request would; safe from any thread.
  void Wake();
};

}  // namespace chargewire
