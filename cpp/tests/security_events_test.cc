// SecurityEventLimiter (spec §4.4): every refusal is accounted for, in full or in a summary;
// sources are bounded and keyed so a client can't make itself new ones; fields are capped;
// Admit and Flush are safe together (CI runs this under ThreadSanitizer). Based on the
// third security review's probe.
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "security_events.hpp"

namespace {

using chargewire::SecurityEvent;
using chargewire::detail::Clock;
using chargewire::detail::SecurityEventLimiter;

SecurityEvent Event(const std::string& peer, const std::string& thumbprint,
                    const std::string& code = "certificate_mismatch") {
  SecurityEvent event;
  event.code = code;
  event.detail = "d";
  event.peer = peer;
  event.presented_thumbprint = thumbprint;
  return event;
}

int64_t Accounted(const std::vector<SecurityEvent>& events) {
  int64_t n = 0;
  for (const auto& event : events) n += event.suppressed > 0 ? event.suppressed : 1;
  return n;
}

TEST(SecurityEventLimiter, EveryRefusalIsAccountedForAcrossWindows) {
  SecurityEventLimiter limiter({.per_source = 10, .window = std::chrono::seconds(60), .sources = 4});
  const auto t = Clock::now();
  int64_t reported = 0, full = 0;
  for (int i = 0; i < 1000; i++) {
    const auto out = limiter.Admit(Event("1.2.3.4:" + std::to_string(i), "T"), t);
    reported += Accounted(out);
    full += static_cast<int64_t>(out.size());
  }
  EXPECT_EQ(full, 10);  // the first ten in full; the port is no part of the source
  reported += Accounted(limiter.Flush(t + std::chrono::seconds(61)));
  EXPECT_EQ(reported, 1000);
}

TEST(SecurityEventLimiter, AnEvictedSourceReportsWhatItHeldBack) {
  SecurityEventLimiter limiter({.per_source = 2, .window = std::chrono::seconds(60), .sources = 4});
  const auto t = Clock::now();
  int64_t reported = 0, admitted = 0;
  for (int round = 0; round < 3; round++) {
    for (int source = 0; source < 5; source++) {  // five sources over a four-slot LRU
      for (int i = 0; i < 5; i++) {
        reported += Accounted(limiter.Admit(Event("10.0.0." + std::to_string(source) + ":1", "T"), t));
        admitted++;
      }
    }
  }
  reported += Accounted(limiter.Flush(t + std::chrono::seconds(61)));
  EXPECT_EQ(reported, admitted);
}

TEST(SecurityEventLimiter, HandshakeRefusalsCountByAddressNotCertificate) {
  SecurityEventLimiter limiter({.per_source = 10, .window = std::chrono::seconds(60), .sources = 64});
  const auto t = Clock::now();
  std::size_t full = 0;
  for (int i = 0; i < 60; i++) {  // a new (unverified) certificate every time
    full += limiter.Admit(Event("1.2.3.4:5", "thumb-" + std::to_string(i), "client_certificate_refused"), t).size();
  }
  EXPECT_EQ(full, 10u);
  // A verified certificate's events still count per certificate (the twin).
  std::size_t verified = 0;
  for (int i = 0; i < 3; i++) verified += limiter.Admit(Event("1.2.3.4:5", "cert-" + std::to_string(i)), t).size();
  EXPECT_EQ(verified, 3u);
}

TEST(SecurityEventLimiter, AnIpv6HostIsOneSourceByItsSlash64) {
  SecurityEventLimiter limiter({.per_source = 10, .window = std::chrono::seconds(60), .sources = 64});
  const auto t = Clock::now();
  std::size_t full = 0;
  for (int i = 0; i < 40; i++) {
    full += limiter.Admit(Event("[2001:db8:1:2::" + std::to_string(i + 1) + "]:443", "", "client_certificate_refused"),
                          t).size();
  }
  EXPECT_EQ(full, 10u);
  EXPECT_EQ(limiter.Admit(Event("[2001:db8:1:3::1]:443", "", "client_certificate_refused"), t).size(), 1u);
}

TEST(SecurityEventLimiter, ClientSuppliedFieldsAreCapped) {
  SecurityEventLimiter limiter({.per_source = 1, .window = std::chrono::seconds(60), .sources = 4});
  const auto t = Clock::now();
  SecurityEvent big = Event("1.2.3.4:5", "", "client_certificate_refused");
  big.certificate_subject = std::string(100000, 'u');
  big.user = std::string(1000000, 'x');
  big.detail = std::string(5000, 'd');
  const auto first = limiter.Admit(big, t);
  ASSERT_EQ(first.size(), 1u);
  for (const std::string* field : {&first[0].certificate_subject, &first[0].user, &first[0].detail}) {
    EXPECT_LT(field->size(), 600u);
  }
  EXPECT_NE(first[0].user.find("(1000000 bytes)"), std::string::npos);
  EXPECT_TRUE(limiter.Admit(big, t).empty());  // held back, and stored capped
  const auto summary = limiter.Flush(t + std::chrono::seconds(61));
  ASSERT_EQ(summary.size(), 1u);
  EXPECT_LT(summary[0].certificate_subject.size(), 600u);
  EXPECT_LT(summary[0].detail.size(), 600u);
}

TEST(SecurityEventLimiter, AdmitAndFlushAreSafeTogether) {
  SecurityEventLimiter limiter({.per_source = 10, .window = std::chrono::seconds(0), .sources = 64});
  std::atomic<int64_t> reported{0};
  std::atomic<bool> stop{false};
  std::thread flusher([&] {
    while (!stop) reported += Accounted(limiter.Flush());
  });
  std::vector<std::thread> workers;
  for (int w = 0; w < 8; w++) {
    workers.emplace_back([&, w] {
      for (int i = 0; i < 20000; i++) {
        reported += Accounted(
            limiter.Admit(Event("10.1." + std::to_string(w) + "." + std::to_string(i % 100) + ":1", "T")));
      }
    });
  }
  for (auto& worker : workers) worker.join();
  stop = true;
  flusher.join();
  reported += Accounted(limiter.Flush(Clock::now() + std::chrono::hours(1)));
  EXPECT_EQ(reported.load(), 160000);
}

}  // namespace
