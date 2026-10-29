#include "chargewire/client.hpp"

#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <format>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <boost/url/parse.hpp>

#include "legacy_dialect.hpp"
#include "transport.hpp"

namespace chargewire::client {

using detail::BodyReader;
using detail::ChunkSource;
using detail::Reply;
using detail::Transport;
namespace http = detail::http;

namespace {

constexpr const char* kArrowStream = "application/vnd.apache.arrow.stream";
constexpr auto kNoTimeout = std::chrono::milliseconds(0);

// The code of an error answer that carries none (no problem document, as a proxy's own
// error page): the client's, so it can't be taken for a server's code (spec §11).
std::string CodeForStatus(int status) { return "client_http_" + std::to_string(status); }

std::string DefaultUserAgent() { return "chargewire-client-cpp/" CHARGEWIRE_VERSION; }

std::string RandomKey() {
  unsigned char bytes[16];
  if (RAND_bytes(bytes, sizeof(bytes)) != 1) throw Error(0, "client_protocol_error", "no random bytes for an idempotency key");
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  for (unsigned char b : bytes) {
    out += kHex[b >> 4];
    out += kHex[b & 15];
  }
  return out;
}

bool IsJson(const std::string& content_type) {
  return content_type == "application/json" || content_type == "application/problem+json";
}

std::optional<json::object> ParseObject(const std::string& text) {
  boost::system::error_code ec;
  json::value value = json::parse(text, ec);
  if (ec || !value.is_object()) return std::nullopt;
  return std::move(value.as_object());
}

std::string String(const json::object& object, const char* key) {
  if (auto* value = object.if_contains(key); value != nullptr && value->is_string()) {
    return std::string(value->as_string());
  }
  return {};
}

std::optional<int64_t> Integer(const json::object& object, const char* key) {
  if (auto* value = object.if_contains(key); value != nullptr) {
    if (value->is_int64()) return value->as_int64();
    if (value->is_uint64()) return static_cast<int64_t>(value->as_uint64());
    if (value->is_double()) return static_cast<int64_t>(value->as_double());
  }
  return std::nullopt;
}

Error ProblemOf(int status, const std::string& body, const std::string& content_type) {
  if (IsJson(content_type)) {
    if (auto object = ParseObject(body)) return Error::FromProblem(status, *object);
  }
  std::string detail = body.empty() ? "HTTP " + std::to_string(status) : body.substr(0, 500);
  return Error(status, CodeForStatus(status), detail);
}

Error ProblemOf(const Reply& reply) { return ProblemOf(reply.status, reply.body, reply.ContentType()); }

std::string Base64Decode(std::string_view text) {
  static const auto kTable = [] {
    std::array<int, 256> table{};
    table.fill(-1);
    const char* alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for (int i = 0; i < 64; i++) table[static_cast<unsigned char>(alphabet[i])] = i;
    return table;
  }();
  std::string out;
  int value = 0;
  int bits = -8;
  for (unsigned char c : text) {
    if (c == '=' || std::isspace(c)) continue;
    if (kTable[c] < 0) throw Error(0, "client_protocol_error", "an embedded Arrow schema is not base64");
    value = (value << 6) + kTable[c];
    bits += 6;
    if (bits >= 0) {
      out.push_back(static_cast<char>((value >> bits) & 0xFF));
      bits -= 8;
    }
  }
  return out;
}

// An arrow::io::OutputStream that collects what is written, taken piece by piece.
class StringSink : public arrow::io::OutputStream {
 public:
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return position_; }
  arrow::Status Write(const void* data, int64_t nbytes) override {
    pending_.append(static_cast<const char*>(data), static_cast<size_t>(nbytes));
    position_ += nbytes;
    return arrow::Status::OK();
  }
  std::string Take() { return std::exchange(pending_, {}); }

 private:
  std::string pending_;
  int64_t position_ = 0;
  bool closed_ = false;
};

// The chunks of an Arrow IPC stream of `reader`'s batches, one per batch.
class StreamChunks {
 public:
  explicit StreamChunks(arrow::RecordBatchReader& reader) : reader_(reader) {}

  std::optional<std::string> operator()() {
    if (done_) return std::nullopt;
    auto check = [](const arrow::Status& status) {
      if (!status.ok()) throw Error(0, "client_invalid_argument", "cannot write the Arrow stream: " + status.ToString());
    };
    if (!writer_) {
      sink_ = std::make_shared<StringSink>();
      auto writer = arrow::ipc::MakeStreamWriter(sink_, reader_.schema());
      check(writer.status());
      writer_ = *writer;
      return sink_->Take();
    }
    std::shared_ptr<arrow::RecordBatch> batch;
    check(reader_.ReadNext(&batch));
    if (batch) {
      check(writer_->WriteRecordBatch(*batch));
      return sink_->Take();
    }
    check(writer_->Close());
    done_ = true;
    return sink_->Take();
  }

 private:
  arrow::RecordBatchReader& reader_;
  std::shared_ptr<StringSink> sink_;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_;
  bool done_ = false;
};

// A response body as an arrow::io::InputStream, remembering its last 8 bytes so the
// reader can tell a complete stream (it ends with Arrow's end-of-stream marker) from one
// the server cut short.
class BodyStream : public arrow::io::InputStream {
 public:
  explicit BodyStream(BodyReader* body) : body_(body) {}

  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return position_; }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    int64_t got = 0;
    char* target = static_cast<char*>(out);
    try {
      while (got < nbytes) {
        const std::size_t n = body_->Read(target + got, static_cast<std::size_t>(nbytes - got));
        if (n == 0) break;
        got += static_cast<int64_t>(n);
      }
    } catch (const Error& e) {
      error_ = e;
      return arrow::Status::IOError(e.what());
    }
    Remember(target, got);
    position_ += got;
    return got;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t got, Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(got));
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
  }

  bool EndedWithEos() const {
    static constexpr unsigned char kEos[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0};
    return tail_.size() == 8 && std::memcmp(tail_.data(), kEos, 8) == 0;
  }
  const std::optional<Error>& error() const { return error_; }

 private:
  void Remember(const char* data, int64_t n) {
    if (n >= 8) {
      tail_.assign(data + n - 8, data + n);
    } else {
      tail_.append(data, static_cast<size_t>(n));
      if (tail_.size() > 8) tail_.erase(0, tail_.size() - 8);
    }
  }

  BodyReader* body_;
  int64_t position_ = 0;
  bool closed_ = false;
  std::string tail_;
  std::optional<Error> error_;
};

}  // namespace

// ---- Error ----

Error::Error(int status, std::string code, std::string detail)
    : std::runtime_error(detail), status_(status), code_(std::move(code)), detail_(std::move(detail)) {}

Error Error::FromProblem(int status, const json::object& problem) {
  int effective = status;
  if (auto value = Integer(problem, "status"); value && status == 0) effective = static_cast<int>(*value);
  std::string code = String(problem, "code");
  if (code.empty()) code = CodeForStatus(effective);
  std::string detail = String(problem, "detail");
  if (detail.empty()) detail = String(problem, "title");
  if (detail.empty()) detail = code;
  Error error(effective, code, detail);
  error.sqlstate_ = String(problem, "sqlstate");
  error.request_id_ = String(problem, "request_id");
  error.reason_ = String(problem, "reason");
  error.cancel_reason_ = String(problem, "cancel_reason");
  return error;
}

std::string BaseUrl(const ConnectOptions& options) {
  const bool v6 = options.host.find(':') != std::string::npos && options.host.front() != '[';
  return std::string(options.tls.enabled ? "https://" : "http://") + (v6 ? "[" + options.host + "]" : options.host) +
         ":" + std::to_string(options.port) + options.base_path;
}

bool Discovery::Has(const std::string& capability) const {
  return std::find(capabilities.begin(), capabilities.end(), capability) != capabilities.end();
}

std::optional<int64_t> StatementDocument::TotalRows() const {
  if (partitions.empty()) return std::nullopt;
  int64_t total = 0;
  for (const auto& partition : partitions) {
    if (!partition.rows) return std::nullopt;
    total += *partition.rows;
  }
  return total;
}

std::shared_ptr<arrow::Schema> DecodeSchema(const std::string& base64) {
  if (base64.empty()) return nullptr;
  const std::string bytes = Base64Decode(base64);
  arrow::io::BufferReader input(arrow::Buffer::FromString(bytes));
  arrow::ipc::DictionaryMemo memo;
  auto schema = arrow::ipc::ReadSchema(&input, &memo);
  if (!schema.ok()) throw Error(0, "client_protocol_error", "cannot read an embedded Arrow schema: " + schema.status().ToString());
  return *schema;
}

std::shared_ptr<arrow::Table> ReadStream(const std::string& bytes) {
  auto input = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(bytes));
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(input);
  if (!reader.ok()) throw Error(0, "client_protocol_error", "cannot read an Arrow stream: " + reader.status().ToString());
  auto table = (*reader)->ToTable();
  if (!table.ok()) throw Error(0, "client_protocol_error", "cannot read an Arrow stream: " + table.status().ToString());
  return *table;
}

// ---- ClientCore ----

class ClientCore {
 public:
  explicit ClientCore(ConnectOptions o) : options(std::move(o)) {
    if (options.base_path.empty()) options.base_path = "/chargewire";
    while (options.base_path.size() > 1 && options.base_path.back() == '/') options.base_path.pop_back();
    if (options.base_path.front() != '/') options.base_path = "/" + options.base_path;
    if (detail::IsLegacyRoot(options.base_path)) options.base_path = "/chargewire";
    may_fall_back = options.base_path == "/chargewire";
    if (options.tls.enabled) tls = detail::MakeTlsContext(options.tls);
    const bool v6 = options.host.find(':') != std::string::npos && options.host.front() != '[';
    host_header = (v6 ? "[" + options.host + "]" : options.host) + ":" + std::to_string(options.port);
  }

  std::unique_ptr<Transport> NewTransport(std::chrono::milliseconds connect_timeout = kNoTimeout) const {
    return std::make_unique<Transport>(options.host, options.port, tls,
                                       connect_timeout.count() > 0 ? connect_timeout : options.connect_timeout,
                                       legacy);
  }

  // A server released before the protocol's rename has no discovery document at the
  // current root, only at its old one; the client then speaks the old spelling to it
  // (legacy_dialect.hpp). Asked once, before the first request that names a path; a
  // failure to ask is thrown, and the next request asks again.
  void ResolveDialect() {
    if (!may_fall_back) return;
    std::call_once(dialect_once, [this] {
      Transport probe(options.host, options.port, tls, options.connect_timeout);
      Reply current = probe.Exchange(Request(http::verb::get, options.base_path, false));
      auto object = current.status == 200 ? ParseObject(current.body) : std::nullopt;
      if (object && String(*object, "protocol") == "chargewire") return;
      Reply old = probe.Exchange(Request(http::verb::get, detail::LegacyRoot(), false));
      object = old.status == 200 ? ParseObject(old.body) : std::nullopt;
      if (object && detail::IsLegacyProtocol(String(*object, "protocol"))) {
        options.base_path = detail::LegacyRoot();
        legacy->store(true);
      }
    });
  }

  const std::string& BasePath() {
    ResolveDialect();
    return options.base_path;
  }

  std::string V1(std::string_view path) { return BasePath() + "/v1" + std::string(path); }

  http::request<http::string_body> Request(http::verb verb, const std::string& target, bool authorized) {
    http::request<http::string_body> request(verb, target, 11);
    request.set(http::field::host, host_header);
    request.set(http::field::user_agent, options.user_agent.empty() ? DefaultUserAgent() : options.user_agent);
    request.set(http::field::accept, "application/json");
    if (authorized) {
      const std::string bearer = Token();
      if (!bearer.empty()) request.set(http::field::authorization, "Bearer " + bearer);
    }
    return request;
  }

  static void SetJson(http::request<http::string_body>& request, const json::value& body) {
    request.set(http::field::content_type, "application/json");
    request.body() = json::serialize(body);
  }

  void SignIn(const json::object& body) {
    auto request = Request(http::verb::post, V1("/auth/token"), false);
    SetJson(request, body);
    std::lock_guard lock(auth_mutex);
    if (!auth_transport) auth_transport = NewTransport();
    Reply reply = auth_transport->Exchange(std::move(request));
    if (reply.status != 200) throw ProblemOf(reply);
    auto object = ParseObject(reply.body);
    if (!object || String(*object, "access_token").empty()) {
      throw Error(reply.status, "client_protocol_error", "the sign-in answer has no access_token");
    }
    std::lock_guard token_lock(token_mutex);
    token = String(*object, "access_token");
    const int64_t lifetime = Integer(*object, "expires_in").value_or(0);
    expires_at = lifetime > 0 ? std::chrono::steady_clock::now() + std::chrono::seconds(lifetime)
                              : std::chrono::steady_clock::time_point{};
  }

  std::string Token() {
    std::optional<json::object> again;
    {
      std::lock_guard lock(token_mutex);
      const bool expiring = expires_at != std::chrono::steady_clock::time_point{} &&
                            expires_at - std::chrono::steady_clock::now() < std::chrono::seconds(60);
      if (!expiring || !renew_grant) return token;
      again = renew_grant;
    }
    try {
      SignIn(*again);
    } catch (const Error&) {
      // The old token is still sent; the server's answer says what went wrong.
    }
    std::lock_guard lock(token_mutex);
    return token;
  }

  ConnectOptions options;
  std::shared_ptr<detail::ssl::context> tls;
  std::string host_header;

  // Whether this client speaks the pre-rename spelling (ResolveDialect), shared with its
  // transports.
  bool may_fall_back = false;
  std::once_flag dialect_once;
  std::shared_ptr<std::atomic<bool>> legacy = std::make_shared<std::atomic<bool>>(false);

  std::mutex token_mutex;
  std::string token;
  std::chrono::steady_clock::time_point expires_at{};
  std::optional<json::object> renew_grant;  // a grant to repeat before the token expires

  std::mutex auth_mutex;
  std::unique_ptr<Transport> auth_transport;

  // Whether the server lists `capability` in its discovery document (asked once).
  bool Offers(std::string_view capability) {
    std::lock_guard lock(auth_mutex);
    if (!capabilities) {
      if (!auth_transport) auth_transport = NewTransport();
      Reply reply = auth_transport->Exchange(Request(http::verb::get, BasePath(), false));
      capabilities.emplace();
      if (auto object = reply.status == 200 ? ParseObject(reply.body) : std::nullopt) {
        if (auto* caps = object->if_contains("capabilities"); caps != nullptr && caps->is_array()) {
          for (const auto& cap : caps->as_array()) {
            if (cap.is_string()) capabilities->push_back(std::string(cap.as_string()));
          }
        }
      }
    }
    return std::find(capabilities->begin(), capabilities->end(), capability) != capabilities->end();
  }
  bool OffersPartitions() { return Offers("result_partitions"); }
  std::optional<std::vector<std::string>> capabilities;
};

// ---- SessionCore ----

class SessionCore : public std::enable_shared_from_this<SessionCore> {
 public:
  SessionCore(std::shared_ptr<ClientCore> c, std::string session_id, int64_t idle)
      : client(std::move(c)), id(std::move(session_id)), idle_timeout_s(idle), transport(client->NewTransport()) {}

  std::string Path(std::string_view rest) const { return client->V1("/sessions/" + id + std::string(rest)); }

  void CheckOpen() const {
    if (closed) throw Error(0, "client_session_closed", "the session is closed");
  }

  Reply Call(http::verb verb, const std::string& target, const json::value* body = nullptr,
             std::chrono::milliseconds timeout = kNoTimeout) {
    CheckOpen();
    auto request = client->Request(verb, target, true);
    if (body != nullptr) ClientCore::SetJson(request, *body);
    return transport->Exchange(std::move(request), nullptr, timeout);
  }

  json::object CallJson(http::verb verb, const std::string& target, const json::value* body,
                        std::initializer_list<int> ok) {
    Reply reply = Call(verb, target, body);
    if (std::find(ok.begin(), ok.end(), reply.status) == ok.end()) throw ProblemOf(reply);
    if (reply.body.empty()) return {};
    auto object = ParseObject(reply.body);
    if (!object) throw Error(reply.status, "client_protocol_error", "the server's answer is not a JSON object");
    return *object;
  }

  std::shared_ptr<arrow::Table> Metadata(const std::string& kind, const std::vector<std::pair<std::string, std::string>>& query) {
    std::string target = Path("/metadata/" + kind);
    char separator = '?';
    for (const auto& [key, value] : query) {
      if (value.empty()) continue;
      target += separator + key + "=" + detail::UrlEncode(value);
      separator = '&';
    }
    CheckOpen();
    auto request = client->Request(http::verb::get, target, true);
    request.set(http::field::accept, kArrowStream);
    Reply reply = transport->Exchange(std::move(request));
    if (reply.status != 200) throw ProblemOf(reply);
    return ReadStream(reply.body);
  }

  static StatementDocument ParseStatement(const json::object& object) {
    StatementDocument doc;
    doc.document = object;
    doc.statement_id = String(object, "statement_id");
    doc.state = String(object, "state");
    if (auto* error = object.if_contains("error"); error != nullptr && error->is_object()) {
      Error e = Error::FromProblem(0, error->as_object());
      e.set_cancel_reason(String(object, "cancel_reason"));
      doc.error = e;
    } else if (doc.state == "failed" || doc.state == "cancelled") {
      Error e(0, doc.state == "failed" ? "execution_error" : "statement_cancelled", "the statement " + doc.state);
      e.set_cancel_reason(String(object, "cancel_reason"));
      doc.error = e;
    }
    if (auto* result = object.if_contains("result"); result != nullptr && result->is_object()) {
      const json::object& r = result->as_object();
      doc.result_kind = String(r, "kind");
      doc.rows_affected = Integer(r, "rows_affected");
      if (auto schema = String(r, "schema"); !schema.empty()) {
        try {
          doc.schema = DecodeSchema(schema);
        } catch (const Error&) {
        }
      }
      if (auto* retained = r.if_contains("retained"); retained != nullptr && retained->is_bool()) {
        doc.retained = retained->as_bool();
      }
      doc.expires_at = String(r, "expires_at");
      doc.partition_order = String(r, "partition_order");
      if (auto* parts = r.if_contains("partitions"); parts != nullptr && parts->is_array()) {
        for (const auto& item : parts->as_array()) {
          if (!item.is_object()) continue;
          const json::object& p = item.as_object();
          Partition partition;
          partition.index = Integer(p, "index").value_or(0);
          partition.location = String(p, "location");
          partition.batches = Integer(p, "batches");
          partition.rows = Integer(p, "rows");
          doc.partitions.push_back(std::move(partition));
        }
      }
    }
    return doc;
  }

  StatementDocument Status(const std::string& statement_id, int wait_s, Transport* on = nullptr) {
    CheckOpen();
    auto request = client->Request(http::verb::get, Path("/statements/" + statement_id), true);
    if (wait_s > 0) request.set(http::field::prefer, "wait=" + std::to_string(wait_s));
    Reply reply = (on != nullptr ? *on : *transport).Exchange(std::move(request));
    if (reply.status != 200) throw ProblemOf(reply);
    auto object = ParseObject(reply.body);
    if (!object) throw Error(reply.status, "client_protocol_error", "the statement document is not JSON");
    return ParseStatement(*object);
  }

  StatementDocument Wait(const std::string& statement_id) {
    while (true) {
      StatementDocument doc = Status(statement_id, 10);
      if (doc.Terminal() || doc.result_kind == "update") return doc;
    }
  }

  // Why the result of `statement_id` failed, from its statement document (spec §6.6).
  Error ErrorOf(const std::string& statement_id, const std::string& fallback) {
    if (statement_id.empty()) return Error(0, "client_protocol_error", fallback);
    try {
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while (true) {
        StatementDocument doc = Status(statement_id, 2);
        if (doc.error) return *doc.error;
        if (doc.Terminal() || std::chrono::steady_clock::now() > deadline) break;
      }
    } catch (const Error& e) {
      if (e.IsTransport()) return e;
    }
    return Error(0, "client_protocol_error", "the result of statement " + statement_id + " ended early: " + fallback);
  }

  // ---- Statements running now, for CancelRunning ----

  uint64_t BeginRun() {
    std::lock_guard lock(run_mutex);
    running = true;
    running_id.clear();
    return ++run_serial;
  }
  void SetRunning(uint64_t serial, const std::string& statement_id) {
    std::lock_guard lock(run_mutex);
    if (serial == run_serial) running_id = statement_id;
  }
  void EndRun(uint64_t serial) {
    std::lock_guard lock(run_mutex);
    if (serial == run_serial) {
      running = false;
      running_id.clear();
    }
  }

  void CancelRunning() {
    std::string statement_id;
    {
      std::lock_guard lock(run_mutex);
      if (!running || closed) return;
      statement_id = running_id;
    }
    try {
      auto cancel = client->NewTransport(std::chrono::seconds(2));
      const std::string target = statement_id.empty() ? Path("/cancel") : Path("/statements/" + statement_id + "/cancel");
      auto request = client->Request(http::verb::post, target, true);
      cancel->Exchange(std::move(request), nullptr, std::chrono::seconds(5));
    } catch (const Error&) {
      // Best effort: the statement may have finished, or the server gone.
    }
  }

  ExecuteResult Execute(const std::string& target, const json::value* body, arrow::RecordBatchReader* parameters,
                        const StatementOptions& options);
  // `ipc_compression` is the Chargewire-IPC-Compression the request sends (spec §8.2).
  std::shared_ptr<ResultReader> Fetch(const std::string& statement_id, int64_t partition, int64_t from_batch,
                                      uint64_t serial, const std::string& ipc_compression);
  ExecuteResult Finish(StatementDocument doc, uint64_t serial, const std::string& ipc_compression);
  // A reader of every partition of a split result at once (spec §6.10). `first`, when
  // set, is partition 0's response; otherwise partition 0 is requested too.
  std::shared_ptr<ResultReader> MakePartitioned(const std::string& statement_id, uint64_t serial, int partitions,
                                                bool any_order, std::unique_ptr<BodyReader> first,
                                                const std::string& ipc_compression);

  std::shared_ptr<ClientCore> client;
  std::string id;
  int64_t idle_timeout_s;
  std::unique_ptr<Transport> transport;
  bool closed = false;

  std::mutex run_mutex;
  uint64_t run_serial = 0;
  bool running = false;
  std::string running_id;
};

namespace {

class StreamResultReader : public ResultReader {
 public:
  StreamResultReader(std::shared_ptr<SessionCore> core, std::unique_ptr<BodyReader> body, std::string statement_id,
                     uint64_t serial)
      : core_(std::move(core)), body_(std::move(body)), statement_id_(std::move(statement_id)), serial_(serial) {
    input_ = std::make_shared<BodyStream>(body_.get());
    auto opened = arrow::ipc::RecordBatchStreamReader::Open(input_);
    if (!opened.ok()) Fail(opened.status());
    reader_ = *opened;
  }

  ~StreamResultReader() override {
    try {
      Close();
    } catch (...) {
    }
  }

  const std::string& statement_id() const override { return statement_id_; }
  std::shared_ptr<arrow::Schema> schema() const override { return reader_->schema(); }

  std::shared_ptr<arrow::RecordBatch> Next() override {
    if (finished_) return nullptr;
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status status = reader_->ReadNext(&batch);
    if (!status.ok()) Fail(status);
    if (batch) return batch;
    if (!input_->EndedWithEos()) Fail(arrow::Status::IOError("the stream ended without its end-of-stream marker"));
    try {
      body_->ReadAll();  // the rest of the HTTP body (normally nothing), so the connection is reusable
    } catch (const Error&) {
    }
    Finish();
    return nullptr;
  }

  void Close() override {
    if (finished_) return;
    body_.reset();  // drops the connection: the server cancels an unretained query
    Finish();
  }

 private:
  void Finish() {
    finished_ = true;
    core_->EndRun(serial_);
  }

  [[noreturn]] void Fail(const arrow::Status& status) {
    const std::string fallback = input_->error() ? input_->error()->what() : status.message();
    body_.reset();
    finished_ = true;
    Error error = core_->ErrorOf(statement_id_, fallback);
    core_->EndRun(serial_);
    throw error;
  }

  std::shared_ptr<SessionCore> core_;
  std::unique_ptr<BodyReader> body_;
  std::shared_ptr<BodyStream> input_;
  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader_;
  std::string statement_id_;
  uint64_t serial_;
  bool finished_ = false;
};

// One partition's Arrow stream, read by a pump thread of PartitionedResultReader.
class PartStream {
 public:
  PartStream(std::unique_ptr<Transport> own, std::unique_ptr<BodyReader> body)
      : own_(std::move(own)), body_(std::move(body)), input_(std::make_shared<BodyStream>(body_.get())) {}

  // Reads the schema message; throws std::runtime_error when there is none.
  std::shared_ptr<arrow::Schema> Open() {
    auto opened = arrow::ipc::RecordBatchStreamReader::Open(input_);
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    reader_ = *opened;
    return reader_->schema();
  }
  // The next batch; nullptr at the end of a complete stream. Throws std::runtime_error
  // when the stream fails or ends without its end-of-stream marker.
  std::shared_ptr<arrow::RecordBatch> Next() {
    std::shared_ptr<arrow::RecordBatch> batch;
    if (auto status = reader_->ReadNext(&batch); !status.ok()) throw std::runtime_error(status.message());
    if (batch) return batch;
    if (!input_->EndedWithEos()) throw std::runtime_error("the stream ended without its end-of-stream marker");
    try {
      body_->ReadAll();
    } catch (const Error&) {
    }
    return nullptr;
  }
  void Drop() {
    reader_.reset();
    input_.reset();
    body_.reset();
    own_.reset();
  }

 private:
  std::unique_ptr<Transport> own_;  // the partition's connection (partition 0 uses the session's)
  std::unique_ptr<BodyReader> body_;
  std::shared_ptr<BodyStream> input_;
  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader_;
};

// Every partition of a split result (spec §6.10), read at once by one thread each and
// merged: round robin for "interleaved" (the result's order), as batches arrive for
// "any". The result is complete only when every partition ended with its end-of-stream
// marker; the first partition that fails fails the result with the statement's error.
class PartitionedResultReader : public ResultReader {
 public:
  PartitionedResultReader(std::shared_ptr<SessionCore> core, std::string statement_id, uint64_t serial,
                          std::vector<std::unique_ptr<PartStream>> parts, bool any_order,
                          std::function<void()> cancel)
      : core_(std::move(core)), statement_id_(std::move(statement_id)), serial_(serial), any_(any_order),
        cancel_(std::move(cancel)), parts_(std::move(parts)), queues_(parts_.size()), ended_(parts_.size(), false) {
    for (std::size_t i = 0; i < parts_.size(); i++) {
      std::shared_ptr<arrow::Schema> schema;
      try {
        schema = parts_[i]->Open();
      } catch (const std::exception& e) {
        RecordFailure(e.what());
        continue;
      }
      if (!schema_) {
        schema_ = schema;
      } else if (!schema->Equals(*schema_)) {
        RecordFailure("partition " + std::to_string(i) + " has another schema than partition 0");
      }
    }
    if (failure_) {
      Stop();
      Fail();
    }
    for (std::size_t i = 0; i < parts_.size(); i++) {
      pumps_.emplace_back([this, i] { Pump(i); });
    }
  }

  ~PartitionedResultReader() override {
    try {
      Close();
    } catch (...) {
    }
  }

  const std::string& statement_id() const override { return statement_id_; }
  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  std::shared_ptr<arrow::RecordBatch> Next() override {
    if (finished_) return nullptr;
    std::unique_lock lock(mutex_);
    for (;;) {
      if (failure_) {
        lock.unlock();
        Stop();
        Fail();
      }
      if (any_) {
        for (auto& queue : queues_) {
          if (!queue.empty()) return Take(queue, lock);
        }
        if (std::all_of(ended_.begin(), ended_.end(), [](bool e) { return e; })) break;
      } else {
        auto& queue = queues_[next_];
        if (!queue.empty()) {
          next_ = (next_ + 1) % queues_.size();
          return Take(queue, lock);
        }
        if (ended_[next_]) {
          // Round robin: once a partition has no next batch, none has (§6.10).
          ready_.wait(lock, [this] {
            return failure_ || std::all_of(ended_.begin(), ended_.end(), [](bool e) { return e; });
          });
          if (failure_) continue;
          for (const auto& rest : queues_) {
            if (!rest.empty()) {
              failure_ = "a partition has more batches than an interleaved result allows";
              break;
            }
          }
          if (failure_) continue;
          break;
        }
      }
      ready_.wait(lock);
    }
    lock.unlock();
    Stop();
    finished_ = true;
    core_->EndRun(serial_);
    return nullptr;
  }

  void Close() override {
    if (finished_) return;
    {
      std::lock_guard lock(mutex_);
      if (!failure_) failure_ = "closed";
    }
    // Pumps blocked reading a socket end when the statement does.
    if (cancel_) cancel_();
    Stop();
    finished_ = true;
    core_->EndRun(serial_);
  }

 private:
  static constexpr std::size_t kBuffer = 2;  // batches each partition reads ahead

  void Pump(std::size_t i) {
    try {
      for (;;) {
        {
          std::unique_lock lock(mutex_);
          room_.wait(lock, [&] { return failure_ || queues_[i].size() < kBuffer; });
          if (failure_) break;
        }
        auto batch = parts_[i]->Next();
        std::lock_guard lock(mutex_);
        if (!batch) {
          ended_[i] = true;
          ready_.notify_all();
          break;
        }
        queues_[i].push_back(std::move(batch));
        ready_.notify_all();
      }
    } catch (const std::exception& e) {
      RecordFailure(e.what());
    }
    std::lock_guard lock(mutex_);
    ended_[i] = true;
    ready_.notify_all();
  }

  std::shared_ptr<arrow::RecordBatch> Take(std::deque<std::shared_ptr<arrow::RecordBatch>>& queue,
                                           std::unique_lock<std::mutex>&) {
    auto batch = std::move(queue.front());
    queue.pop_front();
    room_.notify_all();
    return batch;
  }

  void RecordFailure(const std::string& what) {
    std::lock_guard lock(mutex_);
    if (!failure_) failure_ = what;
    room_.notify_all();
    ready_.notify_all();
  }

  // Waits for every pump to end and drops the connections.
  void Stop() {
    {
      std::lock_guard lock(mutex_);
      if (!failure_ && !std::all_of(ended_.begin(), ended_.end(), [](bool e) { return e; })) failure_ = "stopped";
      room_.notify_all();
    }
    for (auto& pump : pumps_) {
      if (pump.joinable()) pump.join();
    }
    for (auto& part : parts_) part->Drop();
  }

  [[noreturn]] void Fail() {
    finished_ = true;
    std::string fallback;
    {
      std::lock_guard lock(mutex_);
      fallback = failure_.value_or("failed");
    }
    Error error = core_->ErrorOf(statement_id_, fallback);  // every pump has stopped: the session's connection is free
    core_->EndRun(serial_);
    throw error;
  }

  std::shared_ptr<SessionCore> core_;
  std::string statement_id_;
  uint64_t serial_;
  bool any_;
  std::function<void()> cancel_;
  std::vector<std::unique_ptr<PartStream>> parts_;
  std::shared_ptr<arrow::Schema> schema_;

  std::mutex mutex_;
  std::condition_variable ready_;  // a batch arrived, or a partition ended
  std::condition_variable room_;   // a batch was taken
  std::vector<std::deque<std::shared_ptr<arrow::RecordBatch>>> queues_;
  std::vector<bool> ended_;
  std::optional<std::string> failure_;
  std::size_t next_ = 0;
  std::vector<std::thread> pumps_;
  bool finished_ = false;
};

class ReaderAdapter : public arrow::RecordBatchReader {
 public:
  explicit ReaderAdapter(ResultReader* reader) : reader_(reader) {}
  std::shared_ptr<arrow::Schema> schema() const override { return reader_->schema(); }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    try {
      *batch = reader_->Next();
      return arrow::Status::OK();
    } catch (const Error& e) {
      if (e.IsCancelled()) return arrow::Status::Cancelled(e.what());
      return arrow::Status::IOError(e.what());
    }
  }
  arrow::Status Close() override {
    reader_->Close();
    return arrow::Status::OK();
  }

 private:
  ResultReader* reader_;
};

}  // namespace

std::shared_ptr<arrow::Table> ResultReader::ToTable() { return ToTable(0, nullptr); }

std::shared_ptr<arrow::Table> ResultReader::ToTable(int64_t limit, bool* more) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  int64_t rows = 0;
  if (more != nullptr) *more = false;
  while (auto batch = Next()) {
    if (limit > 0 && rows + batch->num_rows() >= limit) {
      const int64_t needed = limit - rows;
      batches.push_back(batch->Slice(0, needed));
      rows += needed;
      const bool left = batch->num_rows() > needed || Next() != nullptr;
      if (more != nullptr) *more = left;
      Close();
      break;
    }
    rows += batch->num_rows();
    batches.push_back(std::move(batch));
  }
  auto table = arrow::Table::FromRecordBatches(schema(), batches);
  if (!table.ok()) throw Error(0, "client_protocol_error", table.status().ToString());
  return *table;
}

std::shared_ptr<arrow::RecordBatchReader> ResultReader::AsRecordBatchReader() {
  return std::make_shared<ReaderAdapter>(this);
}

ExecuteResult SessionCore::Finish(StatementDocument doc, uint64_t serial, const std::string& ipc_compression) {
  SetRunning(serial, doc.statement_id);
  try {
    if (!doc.Terminal() && doc.result_kind != "update") doc = Wait(doc.statement_id);
    if (doc.error && doc.state != "succeeded") throw *doc.error;
    if (doc.result_kind == "rows") {
      ExecuteResult result;
      result.statement_id = doc.statement_id;
      if (doc.partitions.size() > 1 && (doc.partition_order == "interleaved" || doc.partition_order == "any")) {
        result.reader = MakePartitioned(doc.statement_id, serial, static_cast<int>(doc.partitions.size()),
                                        doc.partition_order == "any", nullptr, ipc_compression);
      } else {
        result.reader = Fetch(doc.statement_id, 0, 0, serial, ipc_compression);
      }
      return result;
    }
  } catch (...) {
    EndRun(serial);
    throw;
  }
  EndRun(serial);
  ExecuteResult result;
  result.statement_id = doc.statement_id;
  result.document = std::move(doc);
  return result;
}

ExecuteResult SessionCore::Execute(const std::string& target, const json::value* body,
                                   arrow::RecordBatchReader* parameters, const StatementOptions& options) {
  CheckOpen();
  auto request = client->Request(http::verb::post, target, true);
  request.set(http::field::accept, kArrowStream);
  request.set("Idempotency-Key", options.idempotency_key.empty() ? RandomKey() : options.idempotency_key);
  if (options.timeout_ms) request.set("Chargewire-Statement-Timeout", std::to_string(*options.timeout_ms));
  if (options.retain_result) request.set("Chargewire-Retain-Result", *options.retain_result ? "true" : "false");
  if (!options.query_tag.empty()) request.set("Chargewire-Query-Tag", options.query_tag);
  if (!options.ipc_compression.empty()) request.set("Chargewire-IPC-Compression", options.ipc_compression);
  if (options.result_partitions > 1 && client->OffersPartitions()) {
    request.set("Chargewire-Result-Partitions", std::to_string(std::min(options.result_partitions, 256)));
    if (options.any_order) request.set("Chargewire-Result-Order", "any");
  }
  std::optional<StreamChunks> chunks;
  ChunkSource source;
  if (parameters != nullptr) {
    request.set(http::field::content_type, kArrowStream);
    chunks.emplace(*parameters);
    source = [&chunks] { return (*chunks)(); };
  } else if (body != nullptr) {
    ClientCore::SetJson(request, *body);
  }

  const uint64_t serial = BeginRun();
  std::unique_ptr<BodyReader> reply;
  try {
    reply = transport->Open(std::move(request), parameters != nullptr ? &source : nullptr);
  } catch (...) {
    EndRun(serial);
    throw;
  }
  const int status = reply->status();
  const std::string content_type = reply->ContentType();
  if (status == 200 && content_type == kArrowStream) {
    const std::string statement_id = reply->Header("Chargewire-Statement-Id");
    SetRunning(serial, statement_id);
    ExecuteResult result;
    result.statement_id = statement_id;
    try {
      int partitions = 1;
      try {
        partitions = std::stoi(reply->Header("Chargewire-Result-Partitions"));
      } catch (...) {
      }
      if (partitions > 1 && partitions <= 256) {
        // A split result (spec §6.10): this body is partition 0; the rest are read at once.
        const bool any = reply->Header("Chargewire-Result-Order") != "interleaved";
        result.reader = MakePartitioned(statement_id, serial, partitions, any, std::move(reply),
                                        options.ipc_compression);
      } else {
        result.reader = std::make_shared<StreamResultReader>(shared_from_this(), std::move(reply), statement_id, serial);
      }
    } catch (...) {
      EndRun(serial);
      throw;
    }
    return result;
  }
  std::string text;
  try {
    text = reply->ReadAll();
  } catch (...) {
    EndRun(serial);
    throw;
  }
  if ((status == 200 || status == 201) && IsJson(content_type)) {
    auto object = ParseObject(text);
    if (!object) {
      EndRun(serial);
      throw Error(status, "client_protocol_error", "the statement document is not JSON");
    }
    return Finish(ParseStatement(*object), serial, options.ipc_compression);
  }
  EndRun(serial);
  throw ProblemOf(status, text, content_type);
}

std::shared_ptr<ResultReader> SessionCore::MakePartitioned(const std::string& statement_id, uint64_t serial,
                                                           int partitions, bool any_order,
                                                           std::unique_ptr<BodyReader> first,
                                                           const std::string& ipc_compression) {
  auto open = [&](int index, Transport& on) {
    auto request = client->Request(
        http::verb::get, Path("/statements/" + statement_id + "/result?partition=" + std::to_string(index)), true);
    request.set(http::field::accept, kArrowStream);
    if (!ipc_compression.empty()) request.set("Chargewire-IPC-Compression", ipc_compression);
    auto reply = on.Open(std::move(request));
    if (reply->status() != 200 || reply->ContentType() != kArrowStream) {
      const int status = reply->status();
      const std::string content_type = reply->ContentType();
      throw ProblemOf(status, reply->ReadAll(), content_type);
    }
    return reply;
  };
  std::vector<std::unique_ptr<PartStream>> parts;
  parts.push_back(std::make_unique<PartStream>(nullptr, first ? std::move(first) : open(0, *transport)));
  for (int i = 1; i < partitions; i++) {
    auto own = client->NewTransport();
    auto reply = open(i, *own);
    parts.push_back(std::make_unique<PartStream>(std::move(own), std::move(reply)));
  }
  // Closing early cancels the statement, which ends every partition's stream.
  auto self = shared_from_this();
  auto cancel = [self, statement_id] {
    try {
      auto t = self->client->NewTransport(std::chrono::seconds(2));
      t->Exchange(self->client->Request(http::verb::post, self->Path("/statements/" + statement_id + "/cancel"), true),
                  nullptr, std::chrono::seconds(5));
    } catch (const Error&) {
    }
  };
  return std::make_shared<PartitionedResultReader>(self, statement_id, serial, std::move(parts), any_order,
                                                   std::move(cancel));
}

std::shared_ptr<ResultReader> SessionCore::Fetch(const std::string& statement_id, int64_t partition,
                                                 int64_t from_batch, uint64_t serial,
                                                 const std::string& ipc_compression) {
  auto backoff = std::chrono::milliseconds(50);
  while (true) {
    CheckOpen();
    auto request = client->Request(
        http::verb::get,
        Path("/statements/" + statement_id + "/result?partition=" + std::to_string(partition) +
             "&from_batch=" + std::to_string(from_batch)),
        true);
    request.set(http::field::accept, kArrowStream);
    if (!ipc_compression.empty()) request.set("Chargewire-IPC-Compression", ipc_compression);
    auto reply = transport->Open(std::move(request));
    if (reply->status() == 200 && reply->ContentType() == kArrowStream) {
      return std::make_shared<StreamResultReader>(shared_from_this(), std::move(reply), statement_id, serial);
    }
    const int status = reply->status();
    const std::string content_type = reply->ContentType();
    const std::string text = reply->ReadAll();
    Error error = ProblemOf(status, text, content_type);
    if (error.code() != "result_not_ready") throw error;
    std::this_thread::sleep_for(backoff);
    backoff = std::min(backoff * 2, std::chrono::milliseconds(1000));
  }
}

// ---- PreparedStatement ----

PreparedStatement::PreparedStatement(std::shared_ptr<SessionCore> core, std::string id,
                                     std::shared_ptr<arrow::Schema> parameter_schema,
                                     std::shared_ptr<arrow::Schema> result_schema, std::string result_kind)
    : core_(std::move(core)), id_(std::move(id)), parameter_schema_(std::move(parameter_schema)),
      result_schema_(std::move(result_schema)), result_kind_(std::move(result_kind)) {}

PreparedStatement::~PreparedStatement() {
  try {
    Close();
  } catch (...) {
  }
}

ExecuteResult PreparedStatement::Execute(const StatementOptions& options) {
  json::object body;
  if (!options.parameters.is_null()) body["parameters"] = options.parameters;
  json::value value = std::move(body);
  return core_->Execute(core_->Path("/prepared/" + id_ + "/execute"), &value, nullptr, options);
}

ExecuteResult PreparedStatement::Execute(arrow::RecordBatchReader& parameters, const StatementOptions& options) {
  return core_->Execute(core_->Path("/prepared/" + id_ + "/execute"), nullptr, &parameters, options);
}

void PreparedStatement::Close() {
  if (closed_ || core_->closed) return;
  closed_ = true;
  Reply reply = core_->Call(http::verb::delete_, core_->Path("/prepared/" + id_));
  if (reply.status != 204 && reply.status != 200 && reply.status != 404) throw ProblemOf(reply);
}

// ---- Session ----

Session::Session(std::shared_ptr<SessionCore> core) : core_(std::move(core)) {}

Session::~Session() {
  try {
    Close();
  } catch (...) {
  }
}

const std::string& Session::id() const { return core_->id; }
int64_t Session::idle_timeout_s() const { return core_->idle_timeout_s; }

json::object Session::Get() { return core_->CallJson(http::verb::get, core_->Path(""), nullptr, {200}); }

json::object Session::SetOptions(const json::object& options) {
  json::value body = json::object{{"options", options}};
  return core_->CallJson(http::verb::patch, core_->Path(""), &body, {200});
}

ExecuteResult Session::Execute(const std::string& sql, const StatementOptions& options) {
  json::object body{{"sql", sql}};
  if (!options.parameters.is_null()) body["parameters"] = options.parameters;
  json::value value = std::move(body);
  return core_->Execute(core_->Path("/statements"), &value, nullptr, options);
}

StatementDocument Session::Submit(const std::string& sql, int wait_s, const StatementOptions& options) {
  core_->CheckOpen();
  auto request = core_->client->Request(http::verb::post, core_->Path("/statements"), true);
  json::object body{{"sql", sql}};
  if (!options.parameters.is_null()) body["parameters"] = options.parameters;
  ClientCore::SetJson(request, body);
  request.set(http::field::prefer, wait_s > 0 ? "wait=" + std::to_string(wait_s) : std::string("respond-async"));
  request.set("Idempotency-Key", options.idempotency_key.empty() ? RandomKey() : options.idempotency_key);
  if (options.timeout_ms) request.set("Chargewire-Statement-Timeout", std::to_string(*options.timeout_ms));
  if (options.retain_result) request.set("Chargewire-Retain-Result", *options.retain_result ? "true" : "false");
  if (!options.query_tag.empty()) request.set("Chargewire-Query-Tag", options.query_tag);
  if (options.result_partitions > 1 && core_->client->OffersPartitions()) {
    request.set("Chargewire-Result-Partitions", std::to_string(std::min(options.result_partitions, 256)));
    if (options.any_order) request.set("Chargewire-Result-Order", "any");
  }
  Reply reply = core_->transport->Exchange(std::move(request));
  if (reply.status != 200 && reply.status != 201) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  if (!object) throw Error(reply.status, "client_protocol_error", "the statement document is not JSON");
  return SessionCore::ParseStatement(*object);
}

StatementDocument Session::Status(const std::string& statement_id, int wait_s) {
  return core_->Status(statement_id, wait_s);
}

StatementDocument Session::Wait(const std::string& statement_id) {
  const uint64_t serial = core_->BeginRun();
  core_->SetRunning(serial, statement_id);
  try {
    StatementDocument doc = core_->Wait(statement_id);
    core_->EndRun(serial);
    return doc;
  } catch (...) {
    core_->EndRun(serial);
    throw;
  }
}

std::shared_ptr<ResultReader> Session::FetchResult(const std::string& statement_id, int64_t partition,
                                                   int64_t from_batch, const std::string& ipc_compression) {
  const uint64_t serial = core_->BeginRun();
  core_->SetRunning(serial, statement_id);
  try {
    return core_->Fetch(statement_id, partition, from_batch, serial, ipc_compression);
  } catch (...) {
    core_->EndRun(serial);
    throw;
  }
}

void Session::CancelStatement(const std::string& statement_id) {
  core_->CallJson(http::verb::post, core_->Path("/statements/" + statement_id + "/cancel"), nullptr, {200, 202, 204});
}

void Session::DeleteStatement(const std::string& statement_id) {
  core_->CallJson(http::verb::delete_, core_->Path("/statements/" + statement_id), nullptr, {200, 204});
}

void Session::CancelRunning() { core_->CancelRunning(); }

void Session::Commit() { core_->CallJson(http::verb::post, core_->Path("/commit"), nullptr, {200, 204}); }
void Session::Rollback() { core_->CallJson(http::verb::post, core_->Path("/rollback"), nullptr, {200, 204}); }

std::unique_ptr<PreparedStatement> Session::Prepare(const std::string& sql) {
  json::value body = json::object{{"sql", sql}};
  json::object answer = core_->CallJson(http::verb::post, core_->Path("/prepared"), &body, {200, 201});
  return std::make_unique<PreparedStatement>(core_, String(answer, "prepared_id"),
                                             DecodeSchema(String(answer, "parameter_schema")),
                                             DecodeSchema(String(answer, "result_schema")),
                                             String(answer, "result_kind"));
}

namespace {

// An upload's batches, shared by the threads that send a partitioned ingest's parts:
// each takes the next batch (the first ones may have been read ahead already).
class SharedBatches {
 public:
  SharedBatches(arrow::RecordBatchReader& source, std::vector<std::shared_ptr<arrow::RecordBatch>> ahead)
      : source_(source), ahead_(ahead.begin(), ahead.end()) {}
  std::shared_ptr<arrow::Schema> schema() const { return source_.schema(); }
  arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* batch) {
    std::lock_guard lock(mutex_);
    if (!ahead_.empty()) {
      *batch = std::move(ahead_.front());
      ahead_.pop_front();
      return arrow::Status::OK();
    }
    return source_.ReadNext(batch);
  }

 private:
  arrow::RecordBatchReader& source_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> ahead_;
  std::mutex mutex_;
};

// One part's (or one stream's) batches: from SharedBatches.
class PartBatches : public arrow::RecordBatchReader {
 public:
  explicit PartBatches(SharedBatches& shared) : shared_(shared) {}
  std::shared_ptr<arrow::Schema> schema() const override { return shared_.schema(); }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override { return shared_.Next(batch); }

 private:
  SharedBatches& shared_;
};

// A partitioned ingest (spec §8.3): open, send the parts at once, commit.
int64_t IngestParts(SessionCore& core, const std::string& table, SharedBatches& shared,
                    const IngestOptions& options) {
  json::object target{{"table", table}, {"mode", options.mode.empty() ? "create_append" : options.mode},
                      {"temporary", options.temporary}};
  if (!options.catalog.empty()) target["catalog"] = options.catalog;
  if (!options.schema.empty()) target["schema"] = options.schema;
  json::value body = std::move(target);
  auto open = core.client->Request(http::verb::post, core.Path("/ingests"), true);
  ClientCore::SetJson(open, body);
  open.set("Idempotency-Key", options.idempotency_key.empty() ? RandomKey() : options.idempotency_key);
  Reply opened = core.transport->Exchange(std::move(open));
  if (opened.status != 201 && opened.status != 200) throw ProblemOf(opened);
  auto doc = ParseObject(opened.body);
  const std::string ingest_id = doc ? String(*doc, "ingest_id") : std::string();
  if (ingest_id.empty()) throw Error(opened.status, "client_protocol_error", "the ingest answer has no ingest_id");
  const std::string base = core.Path("/ingests/" + ingest_id);

  const int parts = std::clamp(options.parts, 1, 256);
  std::mutex mutex;
  std::optional<Error> failure;
  std::vector<std::thread> senders;
  for (int part = 0; part < parts; part++) {
    senders.emplace_back([&, part] {
      try {
        auto transport = core.client->NewTransport();
        auto request = core.client->Request(http::verb::post, base + "/parts/" + std::to_string(part), true);
        request.set(http::field::content_type, kArrowStream);
        request.set("Idempotency-Key", RandomKey());
        if (!options.query_tag.empty()) request.set("Chargewire-Query-Tag", options.query_tag);
        PartBatches rows(shared);
        StreamChunks chunks(rows);
        ChunkSource source = [&chunks] { return chunks(); };
        Reply reply = transport->Exchange(std::move(request), &source);
        if (reply.status != 200) throw ProblemOf(reply);
      } catch (const Error& e) {
        std::lock_guard lock(mutex);
        if (!failure) failure = e;
      } catch (const std::exception& e) {
        std::lock_guard lock(mutex);
        if (!failure) failure = Error(0, "client_error", std::string("a part's upload failed: ") + e.what());
      }
    });
  }
  for (auto& sender : senders) sender.join();
  auto abort = [&] {
    try {
      core.CallJson(http::verb::delete_, base, nullptr, {204, 404});  // nothing is written
    } catch (const Error&) {
    }
  };
  if (failure) {
    abort();
    throw *failure;
  }
  json::value commit = json::object{{"part_count", parts}};
  try {
    json::object answer = core.CallJson(http::verb::post, base + "/commit", &commit, {200});
    return Integer(answer, "rows_ingested").value_or(0);
  } catch (const Error&) {
    // The answer may have been lost after the commit: ask before aborting.
    try {
      json::object state = core.CallJson(http::verb::get, base, nullptr, {200});
      if (String(state, "state") == "committed") return Integer(state, "rows_ingested").value_or(0);
    } catch (const Error&) {
    }
    abort();
    throw;
  }
}

}  // namespace

int64_t Session::Ingest(const std::string& table, arrow::RecordBatchReader& data, const IngestOptions& options) {
  core_->CheckOpen();
  // Read up to two batches ahead: an upload of one batch (or none) goes as one stream.
  std::vector<std::shared_ptr<arrow::RecordBatch>> ahead;
  const bool may_split = options.parts > 1 && core_->client->Offers("partitioned_ingest");
  while (may_split && ahead.size() < 2) {
    std::shared_ptr<arrow::RecordBatch> batch;
    if (auto status = data.ReadNext(&batch); !status.ok()) {
      throw Error(0, "client_invalid_argument", "cannot read the rows to ingest: " + status.ToString());
    }
    if (!batch) break;
    ahead.push_back(std::move(batch));
  }
  const bool split = may_split && ahead.size() >= 2;
  SharedBatches shared(data, std::move(ahead));
  if (!split) {
    std::string target = core_->Path("/ingest") + "?table=" + detail::UrlEncode(table);
    if (!options.catalog.empty()) target += "&catalog=" + detail::UrlEncode(options.catalog);
    if (!options.schema.empty()) target += "&schema=" + detail::UrlEncode(options.schema);
    if (!options.mode.empty()) target += "&mode=" + detail::UrlEncode(options.mode);
    if (options.temporary) target += "&temporary=true";
    auto request = core_->client->Request(http::verb::post, target, true);
    request.set(http::field::content_type, kArrowStream);
    request.set("Idempotency-Key", options.idempotency_key.empty() ? RandomKey() : options.idempotency_key);
    if (!options.query_tag.empty()) request.set("Chargewire-Query-Tag", options.query_tag);
    PartBatches rows(shared);
    StreamChunks chunks(rows);
    ChunkSource source = [&chunks] { return chunks(); };
    Reply reply = core_->transport->Exchange(std::move(request), &source);
    if (reply.status != 200) throw ProblemOf(reply);
    auto object = ParseObject(reply.body);
    if (!object) throw Error(reply.status, "client_protocol_error", "the ingest answer is not JSON");
    return Integer(*object, "rows_ingested").value_or(0);
  }
  return IngestParts(*core_, table, shared, options);
}

std::shared_ptr<arrow::Table> Session::Catalogs() { return core_->Metadata("catalogs", {}); }

std::shared_ptr<arrow::Table> Session::DbSchemas(const std::string& catalog, const std::string& pattern) {
  return core_->Metadata("db-schemas", {{"catalog", catalog}, {"db_schema_filter_pattern", pattern}});
}

std::shared_ptr<arrow::Table> Session::Tables(const TablesRequest& request) {
  std::string types;
  for (const auto& type : request.table_types) types += (types.empty() ? "" : ",") + type;
  return core_->Metadata("tables", {{"catalog", request.catalog},
                                    {"db_schema_filter_pattern", request.db_schema_filter_pattern},
                                    {"table_name_filter_pattern", request.table_name_filter_pattern},
                                    {"table_types", types},
                                    {"include_schema", request.include_schema ? "true" : ""}});
}

std::shared_ptr<arrow::Table> Session::TableTypes() { return core_->Metadata("table-types", {}); }

std::shared_ptr<arrow::Table> Session::PrimaryKeys(const std::string& catalog, const std::string& db_schema,
                                                   const std::string& table) {
  return core_->Metadata("primary-keys", {{"catalog", catalog}, {"db_schema", db_schema}, {"table", table}});
}

std::shared_ptr<arrow::Table> Session::ImportedKeys(const std::string& catalog, const std::string& db_schema,
                                                    const std::string& table) {
  return core_->Metadata("imported-keys", {{"catalog", catalog}, {"db_schema", db_schema}, {"table", table}});
}

std::shared_ptr<arrow::Table> Session::ExportedKeys(const std::string& catalog, const std::string& db_schema,
                                                    const std::string& table) {
  return core_->Metadata("exported-keys", {{"catalog", catalog}, {"db_schema", db_schema}, {"table", table}});
}

std::shared_ptr<arrow::Table> Session::TypeInfo(std::optional<int> data_type) {
  return core_->Metadata("type-info", {{"data_type", data_type ? std::to_string(*data_type) : ""}});
}

json::object Session::ServerInfo() {
  return core_->CallJson(http::verb::get, core_->Path("/metadata/server-info"), nullptr, {200});
}

void Session::Close() {
  if (core_->closed) return;
  try {
    // Bounded: a client that exits must not hang on a server that is gone.
    core_->Call(http::verb::delete_, core_->Path(""), nullptr, std::chrono::seconds(3));
  } catch (const Error&) {
  }
  core_->closed = true;
  core_->transport->Close();
}

// ---- Client ----

Client::Client(ConnectOptions options) : core_(std::make_shared<ClientCore>(std::move(options))) {}
Client::~Client() = default;

const ConnectOptions& Client::options() const { return core_->options; }

Discovery Client::Discover() {
  auto transport = core_->NewTransport();
  Reply reply = transport->Exchange(core_->Request(http::verb::get, core_->BasePath(), false));
  if (reply.status != 200) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  const std::string protocol = object ? String(*object, "protocol") : std::string();
  if (protocol != "chargewire" && !(core_->legacy->load() && detail::IsLegacyProtocol(protocol))) {
    throw Error(reply.status, "client_protocol_error", BaseUrl(core_->options) + " is not a Chargewire server");
  }
  Discovery discovery;
  discovery.document = *object;
  if (auto* server = object->if_contains("server"); server != nullptr && server->is_object()) {
    discovery.server_name = String(server->as_object(), "name");
    discovery.server_version = String(server->as_object(), "version");
  }
  if (auto* caps = object->if_contains("capabilities"); caps != nullptr && caps->is_array()) {
    for (const auto& c : caps->as_array()) {
      if (c.is_string()) discovery.capabilities.emplace_back(c.as_string());
    }
  }
  if (auto* auth = object->if_contains("auth"); auth != nullptr && auth->is_object()) {
    if (auto* grants = auth->as_object().if_contains("grant_types"); grants != nullptr && grants->is_array()) {
      for (const auto& g : grants->as_array()) {
        if (g.is_string()) discovery.grant_types.emplace_back(g.as_string());
      }
    }
  }
  return discovery;
}

json::object Client::Health() {
  auto transport = core_->NewTransport();
  Reply reply = transport->Exchange(core_->Request(http::verb::get, core_->V1("/health"), false));
  auto object = ParseObject(reply.body);
  if ((reply.status != 200 && reply.status != 503) || !object) throw ProblemOf(reply);
  return *object;
}

void Client::SignInWithPassword(const std::string& username, const std::string& password) {
  json::object grant{{"grant_type", "password"}, {"username", username}, {"password", password}};
  core_->SignIn(grant);
  std::lock_guard lock(core_->token_mutex);
  core_->renew_grant = std::move(grant);
}

void Client::SignInWithToken(const std::string& subject_token) {
  core_->SignIn(json::object{{"grant_type", "token_exchange"}, {"subject_token", subject_token}});
  std::lock_guard lock(core_->token_mutex);
  core_->renew_grant.reset();
}

void Client::SignInWithClientCertificate() {
  json::object grant{{"grant_type", "client_certificate"}};
  core_->SignIn(grant);
  std::lock_guard lock(core_->token_mutex);
  core_->renew_grant = std::move(grant);
}

BrowserSignInFlow Client::StartBrowserSignIn(const std::string& return_to) {
  auto request = core_->Request(http::verb::post, core_->V1("/auth/oauth/flows"), false);
  json::object body;
  if (!return_to.empty()) body["return_to"] = return_to;
  ClientCore::SetJson(request, body);
  std::lock_guard lock(core_->auth_mutex);
  if (!core_->auth_transport) core_->auth_transport = core_->NewTransport();
  Reply reply = core_->auth_transport->Exchange(std::move(request));
  if (reply.status != 201) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  BrowserSignInFlow flow;
  if (object) {
    flow.flow_id = String(*object, "flow_id");
    flow.poll_secret = String(*object, "poll_secret");
    flow.authorize_url = String(*object, "authorize_url");
    flow.poll_interval_s = std::max<int64_t>(1, Integer(*object, "poll_interval_s").value_or(2));
    flow.expires_at = String(*object, "expires_at");
  }
  if (flow.flow_id.empty() || flow.poll_secret.empty() || flow.authorize_url.empty()) {
    throw Error(reply.status, "client_protocol_error", "the browser sign-in answer lacks flow_id, poll_secret or authorize_url");
  }
  return flow;
}

bool Client::PollBrowserSignIn(const BrowserSignInFlow& flow) {
  auto request = core_->Request(http::verb::post, core_->V1("/auth/oauth/flows/" + flow.flow_id), false);
  ClientCore::SetJson(request, json::object{{"poll_secret", flow.poll_secret}});
  Reply reply;
  {
    std::lock_guard lock(core_->auth_mutex);
    if (!core_->auth_transport) core_->auth_transport = core_->NewTransport();
    reply = core_->auth_transport->Exchange(std::move(request));
  }
  if (reply.status != 200) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  const std::string state = object ? String(*object, "state") : "";
  if (state == "pending") return false;
  if (state == "failed") {
    const json::object* error = nullptr;
    if (const auto* value = object->if_contains("error"); value != nullptr && value->is_object()) {
      error = &value->as_object();
    }
    throw Error(401, error ? String(*error, "code") : "invalid_credentials",
                error ? String(*error, "detail") : "the browser sign-in failed");
  }
  // A state this client doesn't know is not an end (spec §12): poll again.
  if (state != "complete") return false;
  if (String(*object, "access_token").empty()) {
    throw Error(reply.status, "client_protocol_error", "a completed browser sign-in answer has no access_token");
  }
  std::lock_guard token_lock(core_->token_mutex);
  core_->token = String(*object, "access_token");
  const int64_t lifetime = Integer(*object, "expires_in").value_or(0);
  core_->expires_at = lifetime > 0 ? std::chrono::steady_clock::now() + std::chrono::seconds(lifetime)
                                   : std::chrono::steady_clock::time_point{};
  core_->renew_grant.reset();  // a browser sign-in can't repeat by itself
  return true;
}

void Client::SignInWithBrowser(const std::function<void(const std::string&)>& open, std::chrono::seconds timeout) {
  const BrowserSignInFlow flow = StartBrowserSignIn();
  open(flow.authorize_url);
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!PollBrowserSignIn(flow)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw Error(0, "timeout", "the browser sign-in was not completed within " + std::to_string(timeout.count()) + " s");
    }
    std::this_thread::sleep_for(std::chrono::seconds(flow.poll_interval_s));
  }
}

std::string Client::BrowserSignInProviderName(const Discovery& discovery) {
  const auto* auth = discovery.document.if_contains("auth");
  if (auth == nullptr || !auth->is_object()) return {};
  const auto* browser = auth->as_object().if_contains("oauth_browser");
  if (browser == nullptr || !browser->is_object()) return {};
  return String(browser->as_object(), "provider");
}

void Client::SetAccessToken(const std::string& access_token) {
  std::lock_guard lock(core_->token_mutex);
  core_->token = access_token;
  core_->expires_at = {};
  core_->renew_grant.reset();
}

bool Client::SignedIn() const {
  std::lock_guard lock(core_->token_mutex);
  return !core_->token.empty();
}

std::string Client::access_token() { return core_->Token(); }

std::unique_ptr<Session> Client::CreateSession(const json::object& options) {
  auto transport = core_->NewTransport();
  auto request = core_->Request(http::verb::post, core_->V1("/sessions"), true);
  ClientCore::SetJson(request, json::object{{"options", options}});
  Reply reply = transport->Exchange(std::move(request));
  if (reply.status != 201 && reply.status != 200) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  if (!object || String(*object, "session_id").empty()) {
    throw Error(reply.status, "client_protocol_error", "the session answer has no session_id");
  }
  auto core = std::make_shared<SessionCore>(core_, String(*object, "session_id"),
                                            Integer(*object, "idle_timeout_s").value_or(0));
  // Keep the connection that created the session for its statements.
  core->transport = std::move(transport);
  return std::make_unique<Session>(std::move(core));
}

// ---- GetJson, PostForm ----

namespace {

std::pair<int, json::value> ExchangeJson(http::verb method, const std::string& url, std::string form_body,
                                         const TlsOptions& tls, std::chrono::milliseconds timeout) {
  auto parsed = boost::urls::parse_uri(url);
  if (!parsed) throw Error(0, "client_invalid_argument", "not a URL: " + url);
  const std::string scheme(parsed->scheme());
  if (scheme != "http" && scheme != "https") throw Error(0, "client_invalid_argument", "not an http(s) URL: " + url);
  std::string host(parsed->encoded_host_address());
  if (host.size() > 2 && host.front() == '[') host = host.substr(1, host.size() - 2);
  const std::string port_text(parsed->port());
  const uint16_t port = port_text.empty() ? (scheme == "https" ? 443 : 80) : static_cast<uint16_t>(std::stoi(port_text));
  std::string target(parsed->encoded_resource());
  if (target.empty()) target = "/";
  std::shared_ptr<detail::ssl::context> context;
  if (scheme == "https") {
    TlsOptions options = tls;
    options.enabled = true;
    context = detail::MakeTlsContext(options);
  }
  Transport transport(host, port, context, timeout);
  http::request<http::string_body> request(method, target, 11);
  request.set(http::field::host, std::string(parsed->encoded_authority()));
  request.set(http::field::user_agent, DefaultUserAgent());
  request.set(http::field::accept, "application/json");
  if (method == http::verb::post) {
    request.set(http::field::content_type, "application/x-www-form-urlencoded");
    request.body() = std::move(form_body);
    request.prepare_payload();
  }
  Reply reply = transport.Exchange(std::move(request), nullptr, timeout);
  boost::system::error_code ec;
  json::value body = json::parse(reply.body, ec);
  if (ec) body = nullptr;
  return {reply.status, std::move(body)};
}

}  // namespace

std::pair<int, json::value> GetJson(const std::string& url, const TlsOptions& tls, std::chrono::milliseconds timeout) {
  return ExchangeJson(http::verb::get, url, {}, tls, timeout);
}

std::pair<int, json::value> PostForm(const std::string& url,
                                     const std::vector<std::pair<std::string, std::string>>& fields,
                                     const TlsOptions& tls, std::chrono::milliseconds timeout) {
  // Every byte but the unreserved ones percent-encoded, so '+' never reads as a space.
  auto encode = [](std::string_view text) {
    std::string out;
    for (const unsigned char c : text) {
      if (std::isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~') {
        out.push_back(static_cast<char>(c));
      } else {
        out += std::format("%{:02X}", c);
      }
    }
    return out;
  };
  std::string body;
  for (const auto& [name, value] : fields) {
    if (!body.empty()) body.push_back('&');
    body += encode(name) + "=" + encode(value);
  }
  return ExchangeJson(http::verb::post, url, std::move(body), tls, timeout);
}

}  // namespace chargewire::client
