// Rate limiting of SecurityEvents (spec §4.4): refusals come for free to an attacker, so
// each source (client address, plus the certificate it presented) gets its first events
// of every minute in full, and then one record per minute that counts the rest. Sources
// are kept in a bounded LRU; one evicted with a count still pending reports it first, so
// no flood goes unrecorded and none can push another source's records out.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.hpp"
#include "warpsql/server.hpp"

namespace warpsql::detail {

class SecurityEventLimiter {
 public:
  struct Limits {
    std::size_t per_source = 10;          // events in full per source and window
    std::chrono::seconds window{60};
    std::size_t sources = 4096;           // sources remembered (LRU)
  };
  SecurityEventLimiter() = default;
  explicit SecurityEventLimiter(Limits limits) : limits_(limits) {}

  // The events to report now for `event` (none, the event itself, and/or summaries).
  std::vector<SecurityEvent> Admit(const SecurityEvent& event, Clock::time_point now = Clock::now());
  // Summaries of the windows that ended with events suppressed (call now and then).
  std::vector<SecurityEvent> Flush(Clock::time_point now = Clock::now());

 private:
  struct Source {
    std::string key;
    Clock::time_point window_start;
    std::size_t reported = 0;
    int64_t suppressed = 0;
    SecurityEvent last;  // the latest suppressed one, for the summary
  };
  static SecurityEvent Summary(const Source& source);

  Limits limits_;
  std::mutex mutex_;
  std::list<Source> lru_;  // most recent first
  std::unordered_map<std::string, std::list<Source>::iterator> by_key_;
};

// The client's address without its port ("127.0.0.1:5000" -> "127.0.0.1", "[::1]:5" -> "::1").
std::string PeerHost(const std::string& peer);

}  // namespace warpsql::detail
