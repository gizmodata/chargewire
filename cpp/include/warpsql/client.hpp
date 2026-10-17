// The Warp SQL client library (libwarpsql_client): the protocol's client side, over
// HTTP/1.1 and TLS on Boost.Beast, with results as Arrow record batches.
//
//   warpsql::client::Client client({.host = "db.example.com", .port = 31337,
//                                   .tls = {.enabled = true}});
//   client.SignInWithPassword("joe", password);
//   auto session = client.CreateSession();
//   auto result = session->Execute("SELECT 42 AS answer");
//   auto table = result.reader->ToTable();
//
// Every call throws warpsql::client::Error on failure: a problem document from the
// server (spec §11) or a transport failure (status 0). A Session is used by one thread
// at a time, except Session::CancelRunning, which any thread may call while another
// thread waits on a statement (it is what a command-line client's Ctrl-C calls).
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <boost/json.hpp>

namespace warpsql::client {

namespace json = boost::json;

// ---- Options ----

struct TlsOptions {
  bool enabled = false;
  // PEM file of the certificate authorities to trust; empty: the system's CA bundle.
  std::string root_certs_file;
  // Accept any certificate and host name. Development only.
  bool skip_verify = false;
  // Mutual TLS: the client's certificate chain and private key (PEM).
  std::string cert_file;
  std::string key_file;
};

struct ConnectOptions {
  std::string host = "localhost";
  uint16_t port = 31337;
  std::string base_path = "/warp";  // where the server mounts the protocol (spec §2.1)
  TlsOptions tls;
  std::chrono::milliseconds connect_timeout{10000};
  std::string user_agent = "warpsql-client-cpp";
};

// The base URL these options name: http(s)://host:port/warp.
std::string BaseUrl(const ConnectOptions& options);

// ---- Errors ----

class Error : public std::runtime_error {
 public:
  Error(int status, std::string code, std::string detail);

  int status() const { return status_; }        // HTTP status; 0 for a transport failure
  const std::string& code() const { return code_; }  // problem code, or "connection_failed",
                                                     // "tls_failed", "protocol_error"
  const std::string& detail() const { return detail_; }
  const std::string& sqlstate() const { return sqlstate_; }
  const std::string& request_id() const { return request_id_; }
  const std::string& reason() const { return reason_; }  // session_ended's reason (§5.6)
  const std::string& cancel_reason() const { return cancel_reason_; }  // a cancelled statement's

  // A statement that was cancelled (by a client, a timeout, a kill, ...).
  bool IsCancelled() const { return code_ == "statement_cancelled" || code_ == "statement_timeout"; }
  // A failure to reach the server at all (nothing was sent or answered).
  bool IsTransport() const { return status_ == 0; }

  // A problem document (spec §11) as an Error.
  static Error FromProblem(int status, const json::object& problem);
  void set_cancel_reason(std::string reason) { cancel_reason_ = std::move(reason); }

 private:
  int status_;
  std::string code_;
  std::string detail_;
  std::string sqlstate_;
  std::string request_id_;
  std::string reason_;
  std::string cancel_reason_;
};

// ---- Documents ----

struct Discovery {
  json::object document;
  std::string server_name;
  std::string server_version;
  std::vector<std::string> capabilities;
  std::vector<std::string> grant_types;
  bool Has(const std::string& capability) const;
};

struct Partition {
  int64_t index = 0;
  std::string location;  // empty: this server
  std::optional<int64_t> batches;
  std::optional<int64_t> rows;
};

// A statement document (spec §6.2).
struct StatementDocument {
  json::object document;
  std::string statement_id;
  std::string state;  // queued, running, succeeded, failed, cancelled
  std::optional<Error> error;  // failed or cancelled
  std::string result_kind;     // "rows", "update", or empty while unknown
  std::optional<int64_t> rows_affected;
  std::shared_ptr<arrow::Schema> schema;  // a rows result's
  std::vector<Partition> partitions;
  std::string partition_order;  // "interleaved" or "any" for a result split on this server (§6.10)
  bool retained = false;
  std::string expires_at;

  bool Terminal() const { return state == "succeeded" || state == "failed" || state == "cancelled"; }
  // Rows of every partition, when the server has counted them all.
  std::optional<int64_t> TotalRows() const;
};

// ---- Statements ----

struct StatementOptions {
  // Bind parameters (spec §6.1): an array (positional) or an object (by name); null: none.
  json::value parameters;
  std::optional<int64_t> timeout_ms;   // Warp-Statement-Timeout
  std::optional<bool> retain_result;   // Warp-Retain-Result
  std::string query_tag;               // Warp-Query-Tag: a JSON object's text
  std::string idempotency_key;         // empty: a random one per request
  bool accept_zstd = false;            // Warp-IPC-Compression: zstd
  // Split results (spec §6.10): up to this many partitions, read at once over separate
  // connections (sent only to a server with the result_partitions capability). With
  // any_order the batches come as they arrive; without, in the result's order.
  int result_partitions = 8;           // Warp-Result-Partitions (1: one stream)
  bool any_order = false;              // Warp-Result-Order: any
};

// The record batches of a result, read from the network as they arrive. A stream that
// ends without Arrow's end-of-stream marker failed (spec §6.6): Next() then throws the
// statement's own error, read from its statement document.
class ResultReader {
 public:
  virtual ~ResultReader() = default;
  virtual const std::string& statement_id() const = 0;
  virtual std::shared_ptr<arrow::Schema> schema() const = 0;
  // The next batch, or nullptr at the end of the result.
  virtual std::shared_ptr<arrow::RecordBatch> Next() = 0;
  // Stops reading. A result not read to its end is abandoned: the connection closes and
  // the server cancels an unretained query (spec §6.5).
  virtual void Close() = 0;
  // Every remaining batch, as a table.
  std::shared_ptr<arrow::Table> ToTable();
  // At most `limit` more rows (0: all), as a table; *more is set when rows were left.
  std::shared_ptr<arrow::Table> ToTable(int64_t limit, bool* more);
  // This reader as an arrow::RecordBatchReader (errors become arrow::Status).
  std::shared_ptr<arrow::RecordBatchReader> AsRecordBatchReader();
};

// What executing a statement returned: rows to read, or the document of a statement
// that returns none (DML, DDL, SET, ...).
struct ExecuteResult {
  std::string statement_id;
  std::shared_ptr<ResultReader> reader;     // rows
  std::optional<StatementDocument> document;  // no rows
  bool HasRows() const { return reader != nullptr; }
};

struct IngestOptions {
  std::string catalog;
  std::string schema;
  std::string mode = "create_append";  // create, append, replace, create_append (spec §8.1)
  bool temporary = false;
  std::string idempotency_key;
  std::string query_tag;
  // A partitioned ingest (spec §8.3): an upload of two batches or more goes over up to
  // this many connections at once, committed as one, on a server with the
  // partitioned_ingest capability. 1: one stream (§8.1). Rows then land in no defined
  // order; use 1 to keep the upload's order.
  int parts = 8;
};

struct TablesRequest {
  std::string catalog;
  std::string db_schema_filter_pattern;
  std::string table_name_filter_pattern;
  std::vector<std::string> table_types;
  bool include_schema = false;
};

class SessionCore;  // shared by a session and the readers it hands out

class PreparedStatement {
 public:
  PreparedStatement(std::shared_ptr<SessionCore> core, std::string id,
                    std::shared_ptr<arrow::Schema> parameter_schema,
                    std::shared_ptr<arrow::Schema> result_schema, std::string result_kind);
  ~PreparedStatement();
  PreparedStatement(const PreparedStatement&) = delete;
  PreparedStatement& operator=(const PreparedStatement&) = delete;

  const std::string& id() const { return id_; }
  std::shared_ptr<arrow::Schema> parameter_schema() const { return parameter_schema_; }
  std::shared_ptr<arrow::Schema> result_schema() const { return result_schema_; }  // may be null
  const std::string& result_kind() const { return result_kind_; }  // rows, update or empty

  // Runs it once with JSON parameters (options.parameters), streaming any rows.
  ExecuteResult Execute(const StatementOptions& options = {});
  // Runs it once per row of `parameters` (an Arrow stream matching parameter_schema).
  ExecuteResult Execute(arrow::RecordBatchReader& parameters, const StatementOptions& options = {});
  void Close();

 private:
  std::shared_ptr<SessionCore> core_;
  std::string id_;
  std::shared_ptr<arrow::Schema> parameter_schema_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::string result_kind_;
  bool closed_ = false;
};

class Session {
 public:
  explicit Session(std::shared_ptr<SessionCore> core);
  ~Session();  // closes the session (best effort)
  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;

  const std::string& id() const;
  int64_t idle_timeout_s() const;
  // The session document (§5.2): its current options.
  json::object Get();
  // Sets options (§5.3); returns the server's report ({"options", "errors"}).
  json::object SetOptions(const json::object& options);

  // Runs a statement on the streamed path (§6.1): rows stream as they are produced.
  ExecuteResult Execute(const std::string& sql, const StatementOptions& options = {});
  // Submits a statement on the status-document path; `wait_s` > 0 sends Prefer:
  // wait=N, 0 sends respond-async.
  StatementDocument Submit(const std::string& sql, int wait_s = 0, const StatementOptions& options = {});
  // A statement's document; `wait_s` > 0 long-polls (capability long_poll).
  StatementDocument Status(const std::string& statement_id, int wait_s = 0);
  // Polls until the statement finishes; returns its final document.
  StatementDocument Wait(const std::string& statement_id);
  // A partition of a statement's result (§6.4), from batch `from_batch`.
  std::shared_ptr<ResultReader> FetchResult(const std::string& statement_id, int64_t partition = 0,
                                            int64_t from_batch = 0);
  void CancelStatement(const std::string& statement_id);
  void DeleteStatement(const std::string& statement_id);
  // Cancels what this session runs now (§6.8): the statement being executed, when its
  // id is known, else every statement of the session. Safe from any thread, while
  // another thread waits in Execute or reads its result.
  void CancelRunning();

  void Commit();
  void Rollback();

  std::unique_ptr<PreparedStatement> Prepare(const std::string& sql);

  // Bulk ingest (§8): uploads `data` as one chunked Arrow stream; returns rows_ingested.
  int64_t Ingest(const std::string& table, arrow::RecordBatchReader& data, const IngestOptions& options = {});

  // Metadata (§10), as Flight SQL's result schemas.
  std::shared_ptr<arrow::Table> Catalogs();
  std::shared_ptr<arrow::Table> DbSchemas(const std::string& catalog = "",
                                          const std::string& db_schema_filter_pattern = "");
  std::shared_ptr<arrow::Table> Tables(const TablesRequest& request = {});
  std::shared_ptr<arrow::Table> TableTypes();
  std::shared_ptr<arrow::Table> PrimaryKeys(const std::string& catalog, const std::string& db_schema,
                                            const std::string& table);
  std::shared_ptr<arrow::Table> ImportedKeys(const std::string& catalog, const std::string& db_schema,
                                             const std::string& table);
  std::shared_ptr<arrow::Table> ExportedKeys(const std::string& catalog, const std::string& db_schema,
                                             const std::string& table);
  std::shared_ptr<arrow::Table> TypeInfo(std::optional<int> data_type = std::nullopt);
  json::object ServerInfo();

  // Ends the session (§5.4). Later calls fail.
  void Close();

 private:
  std::shared_ptr<SessionCore> core_;
};

class ClientCore;

class Client {
 public:
  explicit Client(ConnectOptions options);
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  const ConnectOptions& options() const;

  // GET /warp (§3.1) and GET /warp/v1/health (§3.2); no sign-in needed.
  Discovery Discover();
  json::object Health();

  // Sign-in (§4.1). The password grant is repeated by itself shortly before its token
  // expires, so a long-lived session keeps working.
  void SignInWithPassword(const std::string& username, const std::string& password);
  // The token_exchange grant: a token from an identity provider the server trusts.
  void SignInWithToken(const std::string& subject_token);
  // Uses an access token obtained elsewhere.
  void SetAccessToken(const std::string& access_token);
  bool SignedIn() const;
  // The current access token (renewed first when a password sign-in is about to expire).
  std::string access_token();

  // A new session (§5.1) with these options (catalog, schema, autocommit, ...).
  std::unique_ptr<Session> CreateSession(const json::object& options = {});

 private:
  std::shared_ptr<ClientCore> core_;
};

// GET a JSON document from any http(s) URL with these TLS settings (for helpers such as
// a browser sign-in server that lives beside a Warp SQL server). Returns the status and
// the parsed body (null when it isn't JSON).
std::pair<int, json::value> GetJson(const std::string& url, const TlsOptions& tls,
                                    std::chrono::milliseconds timeout = std::chrono::milliseconds(10000));

// Decodes an Arrow schema embedded in JSON (spec §2.2: base64 IPC stream).
std::shared_ptr<arrow::Schema> DecodeSchema(const std::string& base64);
// Reads a complete Arrow IPC stream held in memory into a table.
std::shared_ptr<arrow::Table> ReadStream(const std::string& bytes);

}  // namespace warpsql::client
