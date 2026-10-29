#include "browser_sign_in.hpp"

#include <openssl/evp.h>

#include <array>
#include <stdexcept>

#include "ids.hpp"

namespace chargewire::detail {

namespace {

// base64url without padding (RFC 4648 §5), as PKCE's S256 challenge is written.
std::string Base64UrlSha256(std::string_view data) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int length = 0;
  if (EVP_Digest(data.data(), data.size(), digest.data(), &length, EVP_sha256(), nullptr) != 1) {
    throw std::runtime_error("SHA-256 failed");
  }
  std::string out = Base64(std::string_view(reinterpret_cast<const char*>(digest.data()), length));
  for (char& c : out) {
    if (c == '+') c = '-';
    if (c == '/') c = '_';
  }
  while (!out.empty() && out.back() == '=') out.pop_back();
  return out;
}

}  // namespace

bool ValidReturnTo(std::string_view return_to) {
  if (return_to.empty() || return_to.size() > 2048 || return_to[0] != '/') return false;
  // "//host" and "/\host" are other hosts to a browser (which also drops tabs and newlines,
  // so "/\t/host" is "//host"): no second slash first, no backslash, nothing invisible.
  if (return_to.size() > 1 && return_to[1] == '/') return false;
  for (const char c : return_to) {
    const auto byte = static_cast<unsigned char>(c);
    if (byte <= 0x20 || byte >= 0x7F || c == '\\' || c == '#') return false;
  }
  return true;
}

BrowserFlows::Created BrowserFlows::Create(std::string_view address, std::string return_to,
                                           std::string_view state_prefix) {
  if (!return_to.empty() && !ValidReturnTo(return_to)) {
    throw Error(ErrorCode::kInvalidReturnTo,
                "return_to must be a path on this server (\"/...\"), not a URL, with no '//' at its start, "
                "no backslash, fragment, whitespace or non-ASCII character");
  }
  Created created{.flow_id = NewId("f"),
                  .poll_secret = NewSecret(),
                  .state = std::string(state_prefix) + NewSecret(),
                  .code_challenge = {},
                  .nonce = NewSecret(),
                  .expires_at = WallClock::now() + limits_.lifetime};
  Flow flow;
  flow.secret_hash = Sha256Hex(created.poll_secret);
  flow.state = created.state;
  flow.code_verifier = NewSecret();  // 52 characters of [a-z2-7]: a valid PKCE verifier
  created.code_challenge = Base64UrlSha256(flow.code_verifier);
  flow.nonce = created.nonce;
  flow.return_to = std::move(return_to);
  flow.address = std::string(address);
  const auto now = Clock::now();
  flow.expires = now + limits_.lifetime;

  std::lock_guard lock(mutex_);
  SweepLocked(now);
  if (flows_.size() >= limits_.max_flows) {
    throw Error(ErrorCode::kRateLimited, "too many browser sign-ins are in progress; try again shortly");
  }
  std::size_t& held = per_address_[flow.address];
  if (limits_.max_per_address > 0 && held >= limits_.max_per_address) {
    throw Error(ErrorCode::kRateLimited,
                "too many browser sign-ins are in progress from this address; try again shortly");
  }
  held++;
  by_state_.emplace(flow.state, created.flow_id);
  flows_.emplace(created.flow_id, std::move(flow));
  return created;
}

BrowserFlows::CallbackOutcome BrowserFlows::BeginCallback(std::string_view state, Exchange* exchange) {
  std::lock_guard lock(mutex_);
  SweepLocked(Clock::now());
  auto by_state = by_state_.find(std::string(state));
  if (by_state == by_state_.end()) return CallbackOutcome::kUnknown;
  auto it = flows_.find(by_state->second);
  if (it == flows_.end() || Clock::now() >= it->second.expires) return CallbackOutcome::kUnknown;
  Flow& flow = it->second;
  if (flow.stage != Stage::kPending) return CallbackOutcome::kNotPending;
  flow.stage = Stage::kExchanging;
  *exchange = {.flow_id = it->first, .code_verifier = flow.code_verifier, .nonce = flow.nonce,
               .return_to = flow.return_to};
  return CallbackOutcome::kStarted;
}

void BrowserFlows::Complete(const std::string& flow_id, Principal principal) {
  std::lock_guard lock(mutex_);
  auto it = flows_.find(flow_id);
  if (it == flows_.end() || it->second.stage != Stage::kExchanging) return;
  it->second.stage = Stage::kComplete;
  it->second.principal = std::move(principal);
}

void BrowserFlows::Fail(const std::string& flow_id, std::string code, std::string detail) {
  std::lock_guard lock(mutex_);
  auto it = flows_.find(flow_id);
  if (it == flows_.end() || (it->second.stage != Stage::kExchanging && it->second.stage != Stage::kPending)) return;
  it->second.stage = Stage::kFailed;
  it->second.error_code = std::move(code);
  it->second.error_detail = std::move(detail);
}

BrowserFlows::Poll BrowserFlows::Collect(std::string_view flow_id, std::string_view poll_secret,
                                         const std::function<void(const Principal&)>& accept) {
  std::lock_guard lock(mutex_);
  const auto now = Clock::now();
  SweepLocked(now);
  auto it = flows_.find(std::string(flow_id));
  // A wrong secret is the same 404 as no flow, so flow ids can't be probed (spec §4.3.2).
  // The digests are compared in constant time.
  if (it == flows_.end() || now >= it->second.expires ||
      !ConstantTimeEquals(Sha256Hex(poll_secret), it->second.secret_hash)) {
    throw Error(ErrorCode::kFlowNotFound, "no such browser sign-in (it may have expired or been collected)");
  }
  Poll poll;
  switch (it->second.stage) {
    case Stage::kPending:
    case Stage::kExchanging:
      return poll;
    case Stage::kComplete:
      if (accept) accept(it->second.principal);  // throws: the flow stays for another poll
      poll.state = Poll::State::kComplete;
      poll.principal = std::move(it->second.principal);
      break;
    case Stage::kFailed:
      poll.state = Poll::State::kFailed;
      poll.error_code = std::move(it->second.error_code);
      poll.error_detail = std::move(it->second.error_detail);
      break;
  }
  EraseLocked(it);
  return poll;
}

std::size_t BrowserFlows::size() const {
  std::lock_guard lock(mutex_);
  return flows_.size();
}

void BrowserFlows::SweepLocked(Clock::time_point now) {
  if (now - last_sweep_ < std::chrono::seconds(1)) return;
  last_sweep_ = now;
  for (auto it = flows_.begin(); it != flows_.end();) {
    if (now >= it->second.expires) {
      auto next = std::next(it);
      EraseLocked(it);
      it = next;
    } else {
      ++it;
    }
  }
}

void BrowserFlows::EraseLocked(std::unordered_map<std::string, Flow>::iterator it) {
  by_state_.erase(it->second.state);
  if (auto held = per_address_.find(it->second.address); held != per_address_.end() && --held->second == 0) {
    per_address_.erase(held);
  }
  flows_.erase(it);
}

}  // namespace chargewire::detail
