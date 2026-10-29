#include "security_events.hpp"

namespace chargewire::detail {

std::string SourceKey(const SecurityEvent& event) {
  std::string host = PeerHost(event.peer);
  boost::system::error_code ec;
  const auto address = net::ip::make_address(host, ec);
  if (!ec && address.is_v6() && !address.to_v6().is_v4_mapped()) {
    auto bytes = address.to_v6().to_bytes();
    std::fill(bytes.begin() + 8, bytes.end(), 0);  // the /64
    host = net::ip::address_v6(bytes).to_string() + "/64";
  }
  if (event.code == "client_certificate_refused") return host;
  return host + "|" + event.presented_thumbprint;
}

namespace {

std::string Cap(std::string text) {
  if (text.size() <= kMaxSecurityEventField) return text;
  const std::size_t full = text.size();
  text.resize(kMaxSecurityEventField);
  return text + "... (" + std::to_string(full) + " bytes)";
}

}  // namespace

SecurityEvent Capped(SecurityEvent event) {
  for (std::string* field : {&event.code, &event.detail, &event.user, &event.token_id, &event.presented_thumbprint,
                             &event.certificate_subject, &event.peer, &event.proxy, &event.request_id}) {
    *field = Cap(std::move(*field));
  }
  return event;
}

SecurityEvent SecurityEventLimiter::Summary(const Source& source) {
  SecurityEvent summary = source.last;
  summary.suppressed = source.suppressed;
  summary.detail = std::to_string(source.suppressed) + " more refusals from this source in the last " +
                   "window were not reported one by one (the latest: " + source.last.detail + ")";
  return Capped(std::move(summary));
}

std::vector<SecurityEvent> SecurityEventLimiter::Admit(const SecurityEvent& event, Clock::time_point now) {
  std::vector<SecurityEvent> out;
  const std::string key = SourceKey(event);
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
    out.push_back(Capped(event));
  } else {
    source.suppressed++;
    source.last = Capped(event);  // what the LRU keeps is bounded too
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

}  // namespace chargewire::detail
