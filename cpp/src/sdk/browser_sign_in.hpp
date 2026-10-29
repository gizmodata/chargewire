// Browser sign-in flows (spec §4.3): created by a client, completed at the callback by the
// identity provider's redirect, collected once by a poll with the flow's secret. Bounded in
// number (in all and per client address) and in time.
#pragma once

#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common.hpp"
#include "chargewire/backend.hpp"

namespace chargewire::detail {

// Whether `return_to` is a path on the server's own origin that no browser reads as
// another host (spec §4.3.3): at most 2048 printable ASCII characters, starting with one
// '/', with no '\', '#', whitespace or control character.
bool ValidReturnTo(std::string_view return_to);

// The URL fragment a return_to redirect carries the flow id in (spec §4.3.3).
inline constexpr std::string_view kReturnFragmentKey = "chargewire_oauth_flow";

class BrowserFlows {
 public:
  struct Limits {
    std::chrono::seconds lifetime{600};
    std::size_t max_flows = 10000;
    std::size_t max_per_address = 100;  // 0: no per-address bound
  };
  explicit BrowserFlows(Limits limits) : limits_(limits) {}

  struct Created {
    std::string flow_id;
    std::string poll_secret;
    std::string state;           // the identity provider's `state`, prefix included
    std::string code_challenge;  // PKCE S256 of the flow's verifier
    std::string nonce;
    WallClock::time_point expires_at;
  };
  // A new pending flow for a client at `address`. Throws Error(kRateLimited) beyond the
  // limits and Error(kInvalidReturnTo) for a return_to ValidReturnTo() refuses.
  Created Create(std::string_view address, std::string return_to, std::string_view state_prefix);

  struct Exchange {
    std::string flow_id;
    std::string code_verifier;
    std::string nonce;
    std::string return_to;  // empty: none
  };
  enum class CallbackOutcome { kStarted, kUnknown, kNotPending };
  // The callback for `state`: a pending flow moves on to exchanging its code (kStarted,
  // with *exchange filled in); a state no flow has (or an expired flow's) is kUnknown; a
  // flow past pending (a second callback) is kNotPending and keeps its outcome.
  CallbackOutcome BeginCallback(std::string_view state, Exchange* exchange);
  // The outcome of a flow whose callback started (no-ops for a flow that is gone).
  void Complete(const std::string& flow_id, Principal principal);
  void Fail(const std::string& flow_id, std::string code, std::string detail);

  struct Poll {
    enum class State { kPending, kComplete, kFailed } state = State::kPending;
    Principal principal;     // kComplete
    std::string error_code;  // kFailed
    std::string error_detail;
  };
  // A poll with the flow's secret. A finished flow is handed out once and forgotten; a
  // flow that doesn't exist, has expired or is given the wrong secret throws
  // Error(kFlowNotFound). `accept` sees a complete flow's principal before it is handed
  // out: when it throws, the flow stays complete for another poll.
  Poll Collect(std::string_view flow_id, std::string_view poll_secret,
               const std::function<void(const Principal&)>& accept = nullptr);

  std::size_t size() const;

 private:
  enum class Stage { kPending, kExchanging, kComplete, kFailed };
  struct Flow {
    std::string secret_hash;  // SHA-256 of the poll secret
    std::string state;
    std::string code_verifier;
    std::string nonce;
    std::string return_to;
    std::string address;
    Clock::time_point expires;
    Stage stage = Stage::kPending;
    Principal principal;
    std::string error_code;
    std::string error_detail;
  };

  // Forgets expired flows (at most once a second). Holds mutex_.
  void SweepLocked(Clock::time_point now);
  void EraseLocked(std::unordered_map<std::string, Flow>::iterator it);

  const Limits limits_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Flow> flows_;               // by flow id
  std::unordered_map<std::string, std::string> by_state_;     // state -> flow id
  std::unordered_map<std::string, std::size_t> per_address_;  // flows held per address
  Clock::time_point last_sweep_{};
};

}  // namespace chargewire::detail
