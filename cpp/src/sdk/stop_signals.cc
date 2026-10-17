#include "warpsql/stop_signals.hpp"

#ifdef _WIN32
#include <windows.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#else
#include <pthread.h>
#include <unistd.h>

#include <csignal>
#endif

namespace warpsql {

#ifdef _WIN32

namespace {

// Process-wide, never destroyed: the console's handler runs on threads of its own and
// may outlive main().
struct Requests {
  std::mutex mutex;
  std::condition_variable changed;
  int pending = 0;
  bool finished = false;
};

Requests& State() {
  static auto* state = new Requests();
  return *state;
}

void Deliver(int count) {
  auto& state = State();
  {
    std::lock_guard lock(state.mutex);
    state.pending += count;
  }
  state.changed.notify_all();
}

BOOL WINAPI OnConsoleEvent(DWORD event) {
  switch (event) {
    case CTRL_C_EVENT:
    case CTRL_BREAK_EVENT:
      Deliver(1);
      return TRUE;
    case CTRL_CLOSE_EVENT:
    case CTRL_SHUTDOWN_EVENT: {
      // Windows ends the process when this returns (or after a few seconds): stop at
      // once, and return once the process has stopped.
      Deliver(2);
      auto& state = State();
      std::unique_lock lock(state.mutex);
      state.changed.wait_for(lock, std::chrono::seconds(10), [&] { return state.finished; });
      return TRUE;
    }
    default:
      return FALSE;
  }
}

}  // namespace

StopSignals::StopSignals() {
  State();
  SetConsoleCtrlHandler(OnConsoleEvent, TRUE);
}

StopSignals::~StopSignals() {
  auto& state = State();
  {
    std::lock_guard lock(state.mutex);
    state.finished = true;
  }
  state.changed.notify_all();
}

void StopSignals::Wait() {
  auto& state = State();
  std::unique_lock lock(state.mutex);
  state.changed.wait(lock, [&] { return state.pending > 0; });
  state.pending--;
}

void StopSignals::Wake() { Deliver(1); }

#else

namespace {

sigset_t StopSet() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  return set;
}

}  // namespace

StopSignals::StopSignals() {
  const sigset_t set = StopSet();
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

StopSignals::~StopSignals() = default;

void StopSignals::Wait() {
  const sigset_t set = StopSet();
  int signal = 0;
  sigwait(&set, &signal);
}

// The signals are blocked in every thread, so this one stays pending until Wait() takes it.
void StopSignals::Wake() { kill(getpid(), SIGTERM); }

#endif

}  // namespace warpsql
