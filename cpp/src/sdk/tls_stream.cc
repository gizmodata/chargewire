#include "tls_stream.hpp"

#include <openssl/bio.h>
#include <openssl/err.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <stdexcept>

namespace chargewire::detail {

namespace {

// Plaintext gathered before it is encrypted: one full TLS record.
constexpr std::size_t kRecordPlaintext = 16 * 1024;
// The read buffer: large enough that one socket read takes what a fast sender has queued.
constexpr std::size_t kInputBuffer = 512 * 1024;

}  // namespace

// The BIO OpenSSL reads ciphertext from and writes it to: reads take from the stream's
// input buffer, writes append to its output, which grows as needed.
struct TlsBio {
  static TlsStream* Stream(BIO* bio) { return static_cast<TlsStream*>(BIO_get_data(bio)); }

  static int Write(BIO* bio, const char* data, size_t length, size_t* written) {
    Stream(bio)->output_.append(data, length);
    *written = length;
    return 1;
  }

  static int Read(BIO* bio, char* data, size_t length, size_t* read) {
    TlsStream* stream = Stream(bio);
    BIO_clear_retry_flags(bio);
    const std::size_t available = stream->input_end_ - stream->input_begin_;
    if (available == 0) {
      BIO_set_retry_read(bio);
      *read = 0;
      return 0;
    }
    const std::size_t take = std::min(available, length);
    std::memcpy(data, stream->input_.data() + stream->input_begin_, take);
    stream->input_begin_ += take;
    *read = take;
    return 1;
  }

  static long Control(BIO* bio, int command, long, void*) {
    switch (command) {
      case BIO_CTRL_FLUSH:
        return 1;
      case BIO_CTRL_PENDING: {
        TlsStream* stream = Stream(bio);
        return static_cast<long>(stream->input_end_ - stream->input_begin_);
      }
      case BIO_CTRL_WPENDING:
        return 0;
      default:
        return 0;
    }
  }

  static int Create(BIO* bio) {
    BIO_set_init(bio, 1);
    return 1;
  }

  static BIO_METHOD* Method() {
    static BIO_METHOD* method = [] {
      BIO_METHOD* m = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "chargewire-tls");
      if (m == nullptr) throw std::runtime_error("BIO_meth_new failed");
      BIO_meth_set_write_ex(m, Write);
      BIO_meth_set_read_ex(m, Read);
      BIO_meth_set_ctrl(m, Control);
      BIO_meth_set_create(m, Create);
      return m;
    }();
    return method;
  }
};

TlsStream::TlsStream(next_layer_type next, SSL_CTX* context)
    : next_(std::move(next)), send_done_(next_.get_executor()) {
  ssl_ = SSL_new(context);
  if (ssl_ == nullptr) throw std::runtime_error("SSL_new failed");
  BIO* bio = BIO_new(TlsBio::Method());
  if (bio == nullptr) {
    SSL_free(ssl_);
    throw std::runtime_error("BIO_new failed");
  }
  BIO_set_data(bio, this);
  BIO_up_ref(bio);  // SSL_set_bio takes one reference for reading and one for writing
  SSL_set_bio(ssl_, bio, bio);
  SSL_set_accept_state(ssl_);
}

TlsStream::~TlsStream() {
  if (const char* stats = std::getenv("CHARGEWIRE_STREAM_STATS");
      stats != nullptr && std::string_view(stats) == "1" && encrypted_ > (1 << 20)) {
    const double encrypt = std::chrono::duration<double>(encrypt_time_).count();
    const double send = std::chrono::duration<double>(send_time_).count();
    std::fprintf(stderr,
                 "chargewire tls: %.1f MB written; encrypting %.3f s (%.0f MB/s), sending %.3f s "
                 "(%.0f MB/s), %s\n",
                 encrypted_ / 1e6, encrypt, encrypt > 0 ? encrypted_ / 1e6 / encrypt : 0.0, send,
                 send > 0 ? encrypted_ / 1e6 / send : 0.0, SSL_get_cipher_name(ssl_));
  }
  if (ssl_ != nullptr) SSL_free(ssl_);
}

boost::system::error_code TlsStream::SslError(int result) {
  const int reason = SSL_get_error(ssl_, result);
  if (reason == SSL_ERROR_ZERO_RETURN) {
    return net::error::eof;  // the peer's close_notify
  }
  const unsigned long code = ERR_get_error();
  ERR_clear_error();
  if (code != 0) {
    return {static_cast<int>(code), net::error::get_ssl_category()};
  }
  return net::ssl::error::stream_truncated;
}

TlsStream::Step TlsStream::Handshake() {
  ERR_clear_error();
  const int result = SSL_do_handshake(ssl_);
  if (result == 1) return {.done = true};
  if (SSL_get_error(ssl_, result) == SSL_ERROR_WANT_READ) return {};
  return {.error = SslError(result)};
}

TlsStream::Step TlsStream::Decrypt(net::mutable_buffer target) {
  ERR_clear_error();
  std::size_t read = 0;
  const int result = SSL_read_ex(ssl_, target.data(), target.size(), &read);
  if (result == 1) return {.bytes = read};
  if (SSL_get_error(ssl_, result) == SSL_ERROR_WANT_READ) return {};
  return {.error = SslError(result)};
}

boost::system::error_code TlsStream::Encrypt(net::const_buffer plaintext) {
  auto write = [this](const void* data, std::size_t size) -> boost::system::error_code {
    ERR_clear_error();
    std::size_t written = 0;
    // With a BIO that never refuses a write, SSL_write_ex encrypts everything at once.
    const int result = SSL_write_ex(ssl_, data, size, &written);
    if (result != 1 || written != size) return SslError(result);
    return {};
  };
  if (plaintext.size() >= kRecordPlaintext) {
    if (!staged_.empty()) {
      if (auto ec = write(staged_.data(), staged_.size())) return ec;
      staged_.clear();
    }
    return write(plaintext.data(), plaintext.size());
  }
  staged_.append(static_cast<const char*>(plaintext.data()), plaintext.size());
  if (staged_.size() >= kRecordPlaintext) {
    auto ec = write(staged_.data(), staged_.size());
    staged_.clear();
    return ec;
  }
  return {};
}

boost::system::error_code TlsStream::EndEncrypt() {
  if (staged_.empty()) return {};
  ERR_clear_error();
  std::size_t written = 0;
  const int result = SSL_write_ex(ssl_, staged_.data(), staged_.size(), &written);
  const bool ok = result == 1 && written == staged_.size();
  staged_.clear();
  return ok ? boost::system::error_code{} : SslError(result);
}

void TlsStream::Shutdown() {
  ERR_clear_error();
  SSL_shutdown(ssl_);
  ERR_clear_error();
}

void TlsStream::PrepareInput() {
  if (input_.empty()) input_.resize(kInputBuffer);
  if (input_begin_ == input_end_) {
    input_begin_ = input_end_ = 0;
  } else if (input_begin_ > 0 && input_end_ == input_.size()) {
    std::memmove(input_.data(), input_.data() + input_begin_, input_end_ - input_begin_);
    input_end_ -= input_begin_;
    input_begin_ = 0;
  }
}

}  // namespace chargewire::detail
