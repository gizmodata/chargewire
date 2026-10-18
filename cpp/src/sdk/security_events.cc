#include "security_events.hpp"

namespace warpsql::detail {

std::string PeerHost(const std::string& peer) {
  const auto colon = peer.rfind(':');
  return colon == std::string::npos ? peer : peer.substr(0, colon);
}

SecurityEvent SecurityEventLimiter::Summary(const Source& source) {
  SecurityEvent summary = source.last;
  summary.suppressed = source.suppressed;
  summary.detail = std::to_string(source.suppressed) + " more refusals from this source in the last " +
                   "window were not reported one by one (the latest: " + source.last.detail + ")";
  return summary;
}

std::vector<SecurityEvent> SecurityEventLimiter::Admit(const SecurityEvent& event, Clock::time_point now) {
  std::vector<SecurityEvent> out;
  const std::string key = PeerHost(event.peer) + "|" + event.presented_thumbprint;
  std::lock_guard lock(mutex_);
  auto found = by_key_.find(key);
  if (found == by_key_.end()) {
    if (lru_.size() >= limits_.sources) {
      // The least recent source makes room, reporting what it still holds back.
      const Source& evicted = lru_.back();
      if (evicted.suppressed > 0) out.push_back(Summary(evicted));
      by_key_.erase(evicted.key);
      lru_.pop_back();
    }
    lru_.push_front(Source{key, now, 0, 0, {}});
    found = by_key_.emplace(key, lru_.begin()).first;
  } else {
    lru_.splice(lru_.begin(), lru_, found->second);
  }
  Source& source = *found->second;
  if (now - source.window_start >= limits_.window) {
    if (source.suppressed > 0) out.push_back(Summary(source));
    source.window_start = now;
    source.reported = 0;
    source.suppressed = 0;
  }
  if (source.reported < limits_.per_source) {
    source.reported++;
    out.push_back(event);
  } else {
    source.suppressed++;
    source.last = event;
  }
  return out;
}

std::vector<SecurityEvent> SecurityEventLimiter::Flush(Clock::time_point now) {
  std::vector<SecurityEvent> out;
  std::lock_guard lock(mutex_);
  for (auto& source : lru_) {
    if (source.suppressed > 0 && now - source.window_start >= limits_.window) {
      out.push_back(Summary(source));
      source.window_start = now;
      source.reported = 0;
      source.suppressed = 0;
    }
  }
  return out;
}

}  // namespace warpsql::detail
