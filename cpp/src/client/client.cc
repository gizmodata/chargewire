#include "warpsql/client.hpp"

#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <thread>
#include <utility>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <boost/url/parse.hpp>

#include "transport.hpp"

namespace warpsql::client {

using detail::BodyReader;
using detail::ChunkSource;
using detail::Reply;
using detail::Transport;
namespace http = detail::http;

namespace {

constexpr const char* kArrowStream = "application/vnd.apache.arrow.stream";
constexpr auto kNoTimeout = std::chrono::milliseconds(0);

std::string CodeForStatus(int status) {
  switch (status) {
    case 400: return "bad_request";
    case 401: return "invalid_token";
    case 403: return "permission_denied";
    case 404: return "not_found";
    case 405: return "method_not_allowed";
    case 409: return "conflict";
    case 410: return "result_expired";
    case 413: return "payload_too_large";
    case 415: return "unsupported_arrow_type";
    case 421: return "session_elsewhere";
    case 422: return "idempotency_key_reused";
    case 429: return "rate_limited";
    case 431: return "headers_too_large";
    case 500: return "execution_error";
    case 501: return "not_implemented";
    case 503: return "server_draining";
    default: return "http_" + std::to_string(status);
  }
}

std::string RandomKey() {
  unsigned char bytes[16];
  if (RAND_bytes(bytes, sizeof(bytes)) != 1) throw Error(0, "protocol_error", "no random bytes for an idempotency key");
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
    if (kTable[c] < 0) throw Error(0, "protocol_error", "an embedded Arrow schema is not base64");
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
      if (!status.ok()) throw Error(0, "bad_request", "cannot write the Arrow stream: " + status.ToString());
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
  if (!schema.ok()) throw Error(0, "protocol_error", "cannot read an embedded Arrow schema: " + schema.status().ToString());
  return *schema;
}

std::shared_ptr<arrow::Table> ReadStream(const std::string& bytes) {
  auto input = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(bytes));
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(input);
  if (!reader.ok()) throw Error(0, "protocol_error", "cannot read an Arrow stream: " + reader.status().ToString());
  auto table = (*reader)->ToTable();
  if (!table.ok()) throw Error(0, "protocol_error", "cannot read an Arrow stream: " + table.status().ToString());
  return *table;
}

// ---- ClientCore ----

class ClientCore {
 public:
  explicit ClientCore(ConnectOptions o) : options(std::move(o)) {
    if (options.base_path.empty()) options.base_path = "/warp";
    while (options.base_path.size() > 1 && options.base_path.back() == '/') options.base_path.pop_back();
    if (options.base_path.front() != '/') options.base_path = "/" + options.base_path;
    if (options.tls.enabled) tls = detail::MakeTlsContext(options.tls);
    const bool v6 = options.host.find(':') != std::string::npos && options.host.front() != '[';
    host_header = (v6 ? "[" + options.host + "]" : options.host) + ":" + std::to_string(options.port);
  }

  std::unique_ptr<Transport> NewTransport(std::chrono::milliseconds connect_timeout = kNoTimeout) const {
    return std::make_unique<Transport>(options.host, options.port, tls,
                                       connect_timeout.count() > 0 ? connect_timeout : options.connect_timeout);
  }

  std::string V1(std::string_view path) const { return options.base_path + "/v1" + std::string(path); }

  http::request<http::string_body> Request(http::verb verb, const std::string& target, bool authorized) {
    http::request<http::string_body> request(verb, target, 11);
    request.set(http::field::host, host_header);
    request.set(http::field::user_agent, options.user_agent);
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
      throw Error(reply.status, "protocol_error", "the sign-in answer has no access_token");
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
      if (!expiring || !password_grant) return token;
      again = password_grant;
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

  std::mutex token_mutex;
  std::string token;
  std::chrono::steady_clock::time_point expires_at{};
  std::optional<json::object> password_grant;  // to sign in again before the token expires

  std::mutex auth_mutex;
  std::unique_ptr<Transport> auth_transport;
};

// ---- SessionCore ----

class SessionCore : public std::enable_shared_from_this<SessionCore> {
 public:
  SessionCore(std::shared_ptr<ClientCore> c, std::string session_id, int64_t idle)
      : client(std::move(c)), id(std::move(session_id)), idle_timeout_s(idle), transport(client->NewTransport()) {}

  std::string Path(std::string_view rest) const { return client->V1("/sessions/" + id + std::string(rest)); }

  void CheckOpen() const {
    if (closed) throw Error(0, "session_closed", "the session is closed");
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
    if (!object) throw Error(reply.status, "protocol_error", "the server's answer is not a JSON object");
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
    if (!object) throw Error(reply.status, "protocol_error", "the statement document is not JSON");
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
    if (statement_id.empty()) return Error(0, "protocol_error", fallback);
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
    return Error(0, "protocol_error", "the result of statement " + statement_id + " ended early: " + fallback);
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
  std::shared_ptr<ResultReader> Fetch(const std::string& statement_id, int64_t partition, int64_t from_batch,
                                      uint64_t serial);
  ExecuteResult Finish(StatementDocument doc, uint64_t serial);

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
  if (!table.ok()) throw Error(0, "protocol_error", table.status().ToString());
  return *table;
}

std::shared_ptr<arrow::RecordBatchReader> ResultReader::AsRecordBatchReader() {
  return std::make_shared<ReaderAdapter>(this);
}

ExecuteResult SessionCore::Finish(StatementDocument doc, uint64_t serial) {
  SetRunning(serial, doc.statement_id);
  try {
    if (!doc.Terminal() && doc.result_kind != "update") doc = Wait(doc.statement_id);
    if (doc.error && doc.state != "succeeded") throw *doc.error;
    if (doc.result_kind == "rows") {
      ExecuteResult result;
      result.statement_id = doc.statement_id;
      result.reader = Fetch(doc.statement_id, 0, 0, serial);
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
  if (options.timeout_ms) request.set("Warp-Statement-Timeout", std::to_string(*options.timeout_ms));
  if (options.retain_result) request.set("Warp-Retain-Result", *options.retain_result ? "true" : "false");
  if (!options.query_tag.empty()) request.set("Warp-Query-Tag", options.query_tag);
  if (options.accept_zstd) request.set("Warp-IPC-Compression", "zstd");
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
    const std::string statement_id = reply->Header("Warp-Statement-Id");
    SetRunning(serial, statement_id);
    ExecuteResult result;
    result.statement_id = statement_id;
    try {
      result.reader = std::make_shared<StreamResultReader>(shared_from_this(), std::move(reply), statement_id, serial);
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
      throw Error(status, "protocol_error", "the statement document is not JSON");
    }
    return Finish(ParseStatement(*object), serial);
  }
  EndRun(serial);
  throw ProblemOf(status, text, content_type);
}

std::shared_ptr<ResultReader> SessionCore::Fetch(const std::string& statement_id, int64_t partition,
                                                 int64_t from_batch, uint64_t serial) {
  auto backoff = std::chrono::milliseconds(50);
  while (true) {
    CheckOpen();
    auto request = client->Request(
        http::verb::get,
        Path("/statements/" + statement_id + "/result?partition=" + std::to_string(partition) +
             "&from_batch=" + std::to_string(from_batch)),
        true);
    request.set(http::field::accept, kArrowStream);
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
  if (options.timeout_ms) request.set("Warp-Statement-Timeout", std::to_string(*options.timeout_ms));
  if (options.retain_result) request.set("Warp-Retain-Result", *options.retain_result ? "true" : "false");
  if (!options.query_tag.empty()) request.set("Warp-Query-Tag", options.query_tag);
  Reply reply = core_->transport->Exchange(std::move(request));
  if (reply.status != 200 && reply.status != 201) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  if (!object) throw Error(reply.status, "protocol_error", "the statement document is not JSON");
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
                                                   int64_t from_batch) {
  const uint64_t serial = core_->BeginRun();
  core_->SetRunning(serial, statement_id);
  try {
    return core_->Fetch(statement_id, partition, from_batch, serial);
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

int64_t Session::Ingest(const std::string& table, arrow::RecordBatchReader& data, const IngestOptions& options) {
  core_->CheckOpen();
  std::string target = core_->Path("/ingest") + "?table=" + detail::UrlEncode(table);
  if (!options.catalog.empty()) target += "&catalog=" + detail::UrlEncode(options.catalog);
  if (!options.schema.empty()) target += "&schema=" + detail::UrlEncode(options.schema);
  if (!options.mode.empty()) target += "&mode=" + detail::UrlEncode(options.mode);
  if (options.temporary) target += "&temporary=true";
  auto request = core_->client->Request(http::verb::post, target, true);
  request.set(http::field::content_type, kArrowStream);
  request.set("Idempotency-Key", options.idempotency_key.empty() ? RandomKey() : options.idempotency_key);
  if (!options.query_tag.empty()) request.set("Warp-Query-Tag", options.query_tag);
  StreamChunks chunks(data);
  ChunkSource source = [&chunks] { return chunks(); };
  Reply reply = core_->transport->Exchange(std::move(request), &source);
  if (reply.status != 200) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  if (!object) throw Error(reply.status, "protocol_error", "the ingest answer is not JSON");
  return Integer(*object, "rows_ingested").value_or(0);
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
  Reply reply = transport->Exchange(core_->Request(http::verb::get, core_->options.base_path, false));
  if (reply.status != 200) throw ProblemOf(reply);
  auto object = ParseObject(reply.body);
  if (!object || String(*object, "protocol") != "warp-sql") {
    throw Error(reply.status, "protocol_error", BaseUrl(core_->options) + " is not a Warp SQL server");
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
  core_->password_grant = std::move(grant);
}

void Client::SignInWithToken(const std::string& subject_token) {
  core_->SignIn(json::object{{"grant_type", "token_exchange"}, {"subject_token", subject_token}});
  std::lock_guard lock(core_->token_mutex);
  core_->password_grant.reset();
}

void Client::SetAccessToken(const std::string& access_token) {
  std::lock_guard lock(core_->token_mutex);
  core_->token = access_token;
  core_->expires_at = {};
  core_->password_grant.reset();
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
    throw Error(reply.status, "protocol_error", "the session answer has no session_id");
  }
  auto core = std::make_shared<SessionCore>(core_, String(*object, "session_id"),
                                            Integer(*object, "idle_timeout_s").value_or(0));
  // Keep the connection that created the session for its statements.
  core->transport = std::move(transport);
  return std::make_unique<Session>(std::move(core));
}

// ---- GetJson ----

std::pair<int, json::value> GetJson(const std::string& url, const TlsOptions& tls, std::chrono::milliseconds timeout) {
  auto parsed = boost::urls::parse_uri(url);
  if (!parsed) throw Error(0, "bad_request", "not a URL: " + url);
  const std::string scheme(parsed->scheme());
  if (scheme != "http" && scheme != "https") throw Error(0, "bad_request", "not an http(s) URL: " + url);
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
  http::request<http::string_body> request(http::verb::get, target, 11);
  request.set(http::field::host, std::string(parsed->encoded_authority()));
  request.set(http::field::user_agent, "warpsql-client-cpp");
  request.set(http::field::accept, "application/json");
  Reply reply = transport.Exchange(std::move(request), nullptr, timeout);
  boost::system::error_code ec;
  json::value body = json::parse(reply.body, ec);
  if (ec) body = nullptr;
  return {reply.status, std::move(body)};
}

}  // namespace warpsql::client
