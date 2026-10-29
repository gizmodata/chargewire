// TLS over a beast::tcp_stream that encrypts large writes in one go.
//
// Asio's ssl::stream moves ciphertext through a 17 KB BIO pair, so every TLS record (at
// most 16 KB of plaintext) costs one socket write and one completion handler: a result
// stream spent most of its time handing records between threads, and TLS capped it at
// about a third of plaintext. This stream gives OpenSSL in-memory BIOs that grow as
// needed: a write encrypts all of its buffers (up to kMaxWrite) into one ciphertext
// buffer, which then goes out in one socket write. Reads decrypt from a buffer filled by
// large socket reads. The records on the wire are ordinary TLS records.
//
// Writes overlap: a write encrypts its buffers while the previous write's ciphertext is
// still being sent, and completes once that send is under way, so a sender that writes
// in large pieces keeps one core encrypting and the socket busy at the same time.
//
// It is an Asio AsyncStream (async_read_some, async_write_some) for Beast, plus
// async_handshake (server side) and async_shutdown. One read and one write may be in
// flight at a time, as for any Asio stream, and all of its operations must run on one
// strand. A send can still be under way when a write completes: async_shutdown waits
// for it, so a stream is destroyed only after async_shutdown (or once its socket failed).
#pragma once

#include <openssl/ssl.h>

#include <boost/asio/co_composed.hpp>
#include <boost/asio/ssl/error.hpp>

#include <string>
#include <vector>

#include "common.hpp"

namespace chargewire::detail {

class TlsStream {
 public:
  using next_layer_type = beast::tcp_stream;
  using executor_type = next_layer_type::executor_type;

  // Most plaintext one write encrypts; a larger write is completed in several.
  static constexpr std::size_t kMaxWrite = 1024 * 1024;

  TlsStream(next_layer_type next, SSL_CTX* context);
  ~TlsStream();
  TlsStream(const TlsStream&) = delete;
  TlsStream& operator=(const TlsStream&) = delete;

  executor_type get_executor() { return next_.get_executor(); }
  next_layer_type& next_layer() { return next_; }
  const next_layer_type& next_layer() const { return next_; }
  SSL* native_handle() { return ssl_; }

 private:
  // Awaited operations inside the composed operations report errors as values.
  static constexpr auto kTuple = net::as_tuple(net::deferred);

  // Starts sending the ciphertext produced so far, in the background: the next write
  // encrypts while it goes out. At most one send is in flight.
  void StartSend() {
    sending_.clear();
    sending_.swap(output_);
    sending_in_flight_ = true;
    net::async_write(next_, net::buffer(sending_), [this](boost::system::error_code ec, std::size_t) {
      sending_in_flight_ = false;
      if (ec && !send_error_) send_error_ = ec;
      send_done_.cancel();
    });
  }
  // Waits for the send in flight, if any; reports the first send that failed.
  template <typename Token = net::deferred_t>
  auto AwaitSend(Token&& token = {}) {
    return net::async_initiate<Token, void(boost::system::error_code)>(
        net::co_composed(
            [](auto state, TlsStream* self) -> void {
              while (self->sending_in_flight_) {
                self->send_done_.expires_at(net::steady_timer::time_point::max());
                co_await self->send_done_.async_wait(kTuple);
              }
              co_yield state.complete(self->send_error_);
            },
            next_),
        token, this);
  }
  // Sends everything produced so far and waits until it has gone.
  template <typename Token = net::deferred_t>
  auto Flush(Token&& token = {}) {
    return net::async_initiate<Token, void(boost::system::error_code)>(
        net::co_composed(
            [](auto state, TlsStream* self) -> void {
              if (auto ec = std::get<0>(co_await self->AwaitSend(kTuple)); ec) co_yield state.complete(ec);
              if (!self->output_.empty()) {
                self->StartSend();
                co_yield state.complete(std::get<0>(co_await self->AwaitSend(kTuple)));
              }
              co_yield state.complete(boost::system::error_code{});
            },
            next_),
        token, this);
  }
  // Reads more ciphertext from the socket.
  template <typename Token = net::deferred_t>
  auto FillInput(Token&& token = {}) {
    return net::async_initiate<Token, void(boost::system::error_code)>(
        net::co_composed(
            [](auto state, TlsStream* self) -> void {
              self->PrepareInput();
              auto [ec, n] = co_await self->next_.async_read_some(
                  net::buffer(self->input_.data() + self->input_end_,
                              self->input_.size() - self->input_end_),
                  kTuple);
              if (ec == net::error::eof) {
                // The peer closed without close_notify.
                co_yield state.complete(net::ssl::error::stream_truncated);
              }
              if (ec) co_yield state.complete(ec);
              self->input_end_ += n;
              co_yield state.complete(boost::system::error_code{});
            },
            next_),
        token, this);
  }

 public:

  template <typename Token>
  auto async_handshake(Token&& token) {
    return net::async_initiate<Token, void(boost::system::error_code)>(
        net::co_composed(
            [](auto state, TlsStream* self) -> void {
              state.reset_cancellation_state(net::enable_terminal_cancellation());
              for (;;) {
                const Step step = self->Handshake();
                if (auto ec = std::get<0>(co_await self->Flush(kTuple)); ec) co_yield state.complete(ec);
                if (step.error) co_yield state.complete(step.error);
                if (step.done) co_yield state.complete(boost::system::error_code{});
                if (auto ec = std::get<0>(co_await self->FillInput(kTuple)); ec) co_yield state.complete(ec);
              }
            },
            next_),
        token, this);
  }

  // Sends close_notify. Doesn't wait for the peer's: the connection closes next.
  template <typename Token>
  auto async_shutdown(Token&& token) {
    return net::async_initiate<Token, void(boost::system::error_code)>(
        net::co_composed(
            [](auto state, TlsStream* self) -> void {
              state.reset_cancellation_state(net::enable_terminal_cancellation());
              self->Shutdown();
              co_yield state.complete(std::get<0>(co_await self->Flush(kTuple)));
            },
            next_),
        token, this);
  }

  template <typename MutableBuffers, typename Token>
  auto async_read_some(const MutableBuffers& buffers, Token&& token) {
    return net::async_initiate<Token, void(boost::system::error_code, std::size_t)>(
        net::co_composed(
            [](auto state, TlsStream* self, MutableBuffers buffers) -> void {
              state.reset_cancellation_state(net::enable_terminal_cancellation());
              net::mutable_buffer target;
              for (auto it = net::buffer_sequence_begin(buffers);
                   it != net::buffer_sequence_end(buffers); ++it) {
                if (net::mutable_buffer piece(*it); piece.size() > 0) {
                  target = piece;
                  break;
                }
              }
              bool waited = false;  // whether the operation has suspended yet
              for (;;) {
                Step step;
                // Decrypt as much as the buffers hold of what has arrived, record by record.
                while (step.bytes < target.size()) {
                  const Step more = self->Decrypt(target + step.bytes);
                  step.bytes += more.bytes;
                  if (more.error) {
                    if (step.bytes == 0) step.error = more.error;
                    break;
                  }
                  if (more.bytes == 0) break;
                }
                // Reading can produce output (a key update's answer, an alert): it goes
                // now, unless a write is under way, which then sends it.
                if (!self->writing_ && !self->sending_in_flight_ && !self->output_.empty()) {
                  self->StartSend();
                }
                if (step.error || step.bytes > 0 || target.size() == 0) {
                  // An answer from what was already read completes asynchronously all the
                  // same: completing inline would let a caller that reads in a loop (Beast's
                  // parser) recurse until the stack runs out.
                  if (!waited) co_await net::post(self->next_.get_executor(), kTuple);
                  co_yield state.complete(step.error, step.error ? 0 : step.bytes);
                }
                waited = true;
                if (auto ec = std::get<0>(co_await self->FillInput(kTuple)); ec) co_yield state.complete(ec, 0);
              }
            },
            next_),
        token, this, buffers);
  }

  template <typename ConstBuffers, typename Token>
  auto async_write_some(const ConstBuffers& buffers, Token&& token) {
    return net::async_initiate<Token, void(boost::system::error_code, std::size_t)>(
        net::co_composed(
            [](auto state, TlsStream* self, ConstBuffers buffers) -> void {
              state.reset_cancellation_state(net::enable_terminal_cancellation());
              self->writing_ = true;
              const auto started = std::chrono::steady_clock::now();
              std::size_t consumed = 0;
              boost::system::error_code ec;
              self->output_.reserve(self->output_.size() +
                                    std::min(net::buffer_size(buffers), kMaxWrite) * 102 / 100 + 1024);
              for (auto it = net::buffer_sequence_begin(buffers);
                   it != net::buffer_sequence_end(buffers) && consumed < kMaxWrite; ++it) {
                net::const_buffer piece(*it);
                if (piece.size() > kMaxWrite - consumed) piece = net::buffer(piece.data(), kMaxWrite - consumed);
                ec = self->Encrypt(piece);
                if (ec) break;
                consumed += piece.size();
              }
              if (!ec) ec = self->EndEncrypt();
              self->encrypt_time_ += std::chrono::steady_clock::now() - started;
              self->encrypted_ += consumed;
              // This write was encrypted while the previous one went out; now it goes.
              const auto send_started = std::chrono::steady_clock::now();
              if (!self->sending_in_flight_) {
                co_await net::post(self->next_.get_executor(), kTuple);  // never complete inline
              }
              if (!ec) ec = std::get<0>(co_await self->AwaitSend(kTuple));
              self->send_time_ += std::chrono::steady_clock::now() - send_started;
              if (!ec && !self->output_.empty()) self->StartSend();
              self->writing_ = false;
              co_yield state.complete(ec, ec ? 0 : consumed);
            },
            next_),
        token, this, buffers);
  }

 private:
  struct Step {
    boost::system::error_code error;
    bool done = false;       // handshake finished
    std::size_t bytes = 0;   // plaintext read
  };

  Step Handshake();
  Step Decrypt(net::mutable_buffer target);
  // Plaintext pieces are gathered into records-worth of input, so small pieces (an HTTP
  // chunk header) don't each become a record of their own.
  boost::system::error_code Encrypt(net::const_buffer plaintext);
  boost::system::error_code EndEncrypt();
  void Shutdown();
  boost::system::error_code SslError(int result);

  void PrepareInput();

  friend struct TlsBio;

  next_layer_type next_;
  SSL* ssl_ = nullptr;
  bool writing_ = false;
  bool sending_in_flight_ = false;
  boost::system::error_code send_error_;
  net::steady_timer send_done_;  // cancelled when the send in flight ends
  std::string output_;   // ciphertext not yet sent
  std::string sending_;  // ciphertext being sent
  std::string staged_;   // small plaintext pieces waiting to be encrypted together
  std::vector<char> input_;  // ciphertext read, [input_begin_, input_end_) not yet decrypted
  std::size_t input_begin_ = 0;
  std::size_t input_end_ = 0;
  // For CHARGEWIRE_STREAM_STATS: time spent encrypting and sending, and plaintext written.
  std::chrono::steady_clock::duration encrypt_time_{};
  std::chrono::steady_clock::duration send_time_{};
  std::size_t encrypted_ = 0;
};

}  // namespace chargewire::detail
