// Read-only sessions (spec §5.8) on DuckDB, against a server started in the test process:
// what each of the adapter's layers holds (duckdb_read_only.hpp), one case for each way
// through a single layer that was found, the doors back to read-write, what a reader keeps,
// and that a server embedding the adapter decides the lists. The conformance suite
// (conformance/tests/test_read_only*.py) has the longer lists and the extensions; these run
// in CI, need no network, and never touch the home directory of whoever runs them.
//
// A refusal that is answered after the change was made is worse than none. So every refusal
// here (Refused()) is followed by a look at what a second session sees of the server (the
// rows, the catalog with every sequence's last value, the attached databases, every setting,
// logging, the secrets), at the read-only session's own settings and temporary objects, and
// at the files of the test's scratch directory: nothing may differ from when the test last
// said Remember().
#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "duckdb_backend.hpp"
#include "duckdb_read_only.hpp"
#include "duckdb_session.hpp"
#include "chargewire/server.hpp"
#include "update_outcome.hpp"

namespace {

namespace json = boost::json;
namespace http = boost::beast::http;
using chargewire_duckdb::ReadOnlyPolicy;
using update_outcome::Answer;
using update_outcome::Wire;

// DuckDB keeps installed extensions and persistent secrets under the home directory. The
// tests' process gets one of its own before any DuckDB starts, and it is removed at the end:
// a test that once got an INSTALL or a CREATE PERSISTENT SECRET through must not leave it
// where every later run, of every build, would find it.
class ScratchHome : public ::testing::Environment {
 public:
  static const std::filesystem::path& Directory() { return directory_; }

  void SetUp() override {
    if (!directory_.empty()) return;
    std::random_device random;
    directory_ = std::filesystem::temp_directory_path() / ("chargewire-duckdb-tests-" + std::to_string(random()) + std::to_string(random()));
    std::filesystem::create_directories(directory_);
#ifdef _WIN32
    _putenv_s("USERPROFILE", directory_.string().c_str());
    _putenv_s("HOME", directory_.string().c_str());
#else
    setenv("HOME", directory_.string().c_str(), 1);
#endif
  }
  void TearDown() override {
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
    directory_.clear();
  }

 private:
  static inline std::filesystem::path directory_;
};
[[maybe_unused]] const ::testing::Environment* const kScratchHome = ::testing::AddGlobalTestEnvironment(new ScratchHome);

// A server that embeds the adapter decides the lists: a backend whose sessions have the
// adapter's policy as `edit` left it.
class PolicySession : public chargewire_duckdb::DuckDBSession {
 public:
  PolicySession(duckdb_v2_instance_handle instance, const std::function<void(ReadOnlyPolicy&)>& edit) : DuckDBSession(instance, 0) {
    auto policy = std::make_shared<ReadOnlyPolicy>(ReadOnlyPolicy::Default());
    edit(*policy);
    set_read_only_policy(std::move(policy));
  }
};

class PolicyDuckDB : public chargewire_duckdb::DuckDBBackend {
 public:
  explicit PolicyDuckDB(std::function<void(ReadOnlyPolicy&)> edit) : DuckDBBackend(Options{}), edit_(std::move(edit)) {}

 protected:
  std::unique_ptr<chargewire::EngineSession> NewSession(const chargewire::Principal&, const std::string&) override {
    return std::make_unique<PolicySession>(instance(), edit_);
  }

 private:
  std::function<void(ReadOnlyPolicy&)> edit_;
};

// DuckDB's catalog as one hash: tables and their columns, views and their text, schemas,
// indexes, sequences with their last value, macros and types. WHERE picks the databases.
std::string CatalogHash(const std::string& where) {
  std::string sql = R"(SELECT md5(coalesce(string_agg(entry, '|' ORDER BY entry ASC), '')) FROM (
    SELECT 'table ' || database_name || '.' || schema_name || '.' || table_name || ' ' || column_count::VARCHAR || ' ' || coalesce(comment, '') AS entry
      FROM duckdb_tables() WHERE {W}
    UNION ALL SELECT 'column ' || database_name || '.' || schema_name || '.' || table_name || '.' || column_name || ' ' || data_type
      FROM duckdb_columns() WHERE {W} AND NOT internal
    UNION ALL SELECT 'view ' || database_name || '.' || schema_name || '.' || view_name || ' ' || coalesce(sql, '') FROM duckdb_views() WHERE {W} AND NOT internal
    UNION ALL SELECT 'schema ' || database_name || '.' || schema_name FROM duckdb_schemas() WHERE {W} AND NOT internal
    UNION ALL SELECT 'index ' || database_name || '.' || schema_name || '.' || index_name FROM duckdb_indexes() WHERE {W}
    UNION ALL SELECT 'sequence ' || database_name || '.' || schema_name || '.' || sequence_name || ' ' || coalesce(last_value::VARCHAR, 'unused')
      FROM duckdb_sequences() WHERE {W}
    UNION ALL SELECT 'function ' || database_name || '.' || schema_name || '.' || function_name FROM duckdb_functions() WHERE {W} AND NOT internal
    UNION ALL SELECT 'type ' || database_name || '.' || schema_name || '.' || type_name FROM duckdb_types() WHERE {W} AND NOT internal
  ) AS entries)";
  for (auto at = sql.find("{W}"); at != std::string::npos; at = sql.find("{W}", at + where.size())) sql.replace(at, 3, where);
  return sql;
}

const char kEverySetting[] = "SELECT md5(string_agg(name || '=' || value, '|' ORDER BY name ASC)) FROM duckdb_settings()";

// What a second session sees of the server: a name for the failure message, and a query for
// one value.
std::vector<std::pair<std::string, std::string>> ServerState() {
  return {
      {"the rows of t", "SELECT md5(coalesce(string_agg(id::VARCHAR || ':' || v, ',' ORDER BY id ASC, v ASC), '')) FROM memory.main.t"},
      {"the catalog (tables, columns, views, schemas, indexes, sequences and their last values, macros, types)",
       CatalogHash("database_name NOT IN ('system', 'temp')")},
      {"the attached databases",
       "SELECT string_agg(database_name || ' at ' || coalesce(path, '-') || ' read-only ' || readonly::VARCHAR, ', ' ORDER BY database_name ASC) "
       "FROM duckdb_databases()"},
      {"a setting, as a second session sees them", kEverySetting},
      {"logging", "SELECT current_setting('enable_logging')::VARCHAR || ' at ' || current_setting('logging_level')::VARCHAR"},
      {"the secrets", "SELECT coalesce(string_agg(name || ' persistent ' || persistent::VARCHAR, ', ' ORDER BY name ASC), '') FROM duckdb_secrets()"},
  };
}

// What a session has of its own: its settings and its temporary objects, as one value.
std::string SessionState() {
  return "SELECT (" + std::string(kEverySetting) + ") || ' ' || (" + CatalogHash("database_name = 'temp'") + ")";
}

std::string OneColumnOfIntegers(int rows) {
  arrow::Int64Builder builder;
  for (int i = 0; i < rows; i++) EXPECT_TRUE(builder.Append(i).ok());
  auto array = builder.Finish().ValueOrDie();
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("i", arrow::int64())}), rows, {array});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(sink, batch->schema()).ValueOrDie();
  EXPECT_TRUE(writer->WriteRecordBatch(*batch).ok());
  EXPECT_TRUE(writer->Close().ok());
  return sink->Finish().ValueOrDie()->ToString();
}

// The column names metadata/tables?include_schema=true gives for each table and view: read
// from the answer's Arrow stream (table_name, and table_schema as a serialized schema).
std::map<std::string, std::vector<std::string>> ListedColumns(const std::string& body) {
  std::map<std::string, std::vector<std::string>> listed;
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(body)));
  if (!reader.ok()) {
    ADD_FAILURE() << "the listing is not an Arrow stream: " << reader.status().ToString();
    return listed;
  }
  auto table = (*reader)->ToTable().ValueOrDie();
  auto combined = table->CombineChunksToBatch().ValueOrDie();
  auto names = std::static_pointer_cast<arrow::StringArray>(combined->GetColumnByName("table_name"));
  auto schemas = std::static_pointer_cast<arrow::BinaryArray>(combined->GetColumnByName("table_schema"));
  for (int64_t row = 0; row < combined->num_rows(); row++) {
    arrow::io::BufferReader serialized(arrow::Buffer::FromString(schemas->GetString(row)));
    arrow::ipc::DictionaryMemo memo;
    auto schema = arrow::ipc::ReadSchema(&serialized, &memo).ValueOrDie();
    listed[names->GetString(row)] = schema->field_names();
  }
  return listed;
}

class DuckDBReadOnly : public ::testing::Test {
 protected:
  // Which session's own state a snapshot holds besides the server's.
  enum class Own { kReaders, kNoSessions };

  void Start(std::shared_ptr<chargewire::Backend> backend) {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::move(backend),
                                                   std::make_shared<update_outcome::AnyPassword>());
    server_->Start();
    writer_ = Wire::Open(server_->port());
    reader_ = Session(R"({"read_only": true})");
    writer_.Run("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'b')) v(id, v)");
    Remember();
  }
  void SetUp() override {
    static int next = 0;
    scratch_ = ScratchHome::Directory() / ("test-" + std::to_string(++next));
    std::filesystem::create_directories(scratch_);
    Start(std::make_shared<chargewire_duckdb::DuckDBBackend>(chargewire_duckdb::DuckDBBackend::Options{}));
  }
  void TearDown() override {
    if (server_) server_->Stop();
    std::error_code ignored;
    std::filesystem::remove_all(scratch_, ignored);
  }

  // A further session of the same client, on the test's connection.
  Wire Session(const std::string& options = "{}") {
    Wire session = writer_;
    const Answer created = writer_.Post("/chargewire/v1/sessions", R"({"options": )" + options + "}");
    EXPECT_EQ(created.status, 201) << created.body;
    session.session = "/chargewire/v1/sessions/" + std::string(created.Json().at("session_id").as_string());
    return session;
  }
  std::string File(const std::string& name) const { return (scratch_ / name).string(); }

  static Answer Submit(const Wire& wire, const std::string& sql, const json::value& parameters = nullptr) {
    json::object body{{"sql", sql}};
    if (!parameters.is_null()) body["parameters"] = parameters;
    return wire.Post(wire.session + "/statements", json::serialize(body), {{"Prefer", "wait=30"}});
  }
  static std::string CodeOf(const Answer& answer) {
    const json::object problem = answer.Json();
    const json::value* code = problem.if_contains("code");
    return code != nullptr && code->is_string() ? std::string(code->as_string()) : std::string();
  }
  // 403 read_only with SQLSTATE 25006; the answer.
  static Answer ExpectReadOnly(const Answer& answer, const std::string& what) {
    EXPECT_EQ(answer.status, 403) << what << ": " << answer.body;
    EXPECT_EQ(CodeOf(answer), "read_only") << what << ": " << answer.body;
    const json::object problem = answer.Json();
    EXPECT_EQ(problem.contains("sqlstate") ? json::serialize(problem.at("sqlstate")) : "", "\"25006\"") << what << ": " << answer.body;
    return answer;
  }
  // Refused with 403 read_only in the read-only session (or `wire`), and nothing changed.
  Answer Refused(const std::string& sql, const Wire* wire = nullptr) {
    const Answer answer = ExpectReadOnly(Submit(wire ? *wire : reader_, sql), sql);
    ExpectUnchanged(sql);
    return answer;
  }
  // Runs to a succeeded statement in the read-only session.
  void Reads(const std::string& sql, const json::value& parameters = nullptr) {
    const Answer answer = Submit(reader_, sql, parameters);
    EXPECT_EQ(answer.status, 201) << sql << ": " << answer.body;
    EXPECT_EQ(Wire::StateOf(answer.Json()), "succeeded") << sql << ": " << answer.body;
  }
  // A small count, read by the read-write session, as text. (An UPDATE's count carries a
  // number without reading Arrow: the query's value becomes the row count of a table.)
  std::string Value(const std::string& sql) {
    const Answer answer = writer_.Post(writer_.session + "/statements",
                                       json::serialize(json::object{{"sql", "CREATE OR REPLACE TEMP TABLE value_of AS SELECT * FROM range(CAST((" + sql + ") AS BIGINT))"}}),
                                       {{"Prefer", "wait=30"}});
    EXPECT_EQ(answer.status, 201) << sql << ": " << answer.body;
    const json::object doc = answer.Json();
    if (!doc.contains("result") || !doc.at("result").is_object()) return "?";
    return json::serialize(doc.at("result").as_object().at("rows_affected"));
  }
  std::string Rows() { return Value("SELECT count(*) FROM t"); }
  static Answer Patch(const Wire& wire, const std::string& options) {
    return wire.connection->Exchange(http::verb::patch, wire.session, wire.With({}), R"({"options": )" + options + "}");
  }
  static std::string OptionError(const Answer& patched, const std::string& option) {
    const json::object errors = patched.Json().at("errors").as_object();
    const json::value* error = errors.if_contains(option);
    return error ? std::string(error->as_object().at("code").as_string()) : std::string();
  }
  static std::string Transaction(const Wire& wire) { return std::string(wire.Session().at("transaction").as_string()); }
  static Answer Prepare(const Wire& wire, const std::string& sql) {
    return wire.Post(wire.session + "/prepared", json::serialize(json::object{{"sql", sql}}));
  }
  static std::string Prepared(const Wire& wire, const std::string& sql) {
    const Answer prepared = Prepare(wire, sql);
    EXPECT_EQ(prepared.status, 201) << sql << ": " << prepared.body;
    const json::object doc = prepared.Json();
    return doc.contains("prepared_id") ? wire.session + "/prepared/" + std::string(doc.at("prepared_id").as_string()) + "/execute" : "";
  }
  static Answer Execute(const Wire& wire, const std::string& prepared, const std::string& parameters = "[]") {
    return wire.Post(prepared, R"({"parameters": )" + parameters + "}", {{"Prefer", "wait=30"}});
  }

  // ---- The snapshot ----

  std::vector<std::string> Files() const {
    std::vector<std::string> found;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(scratch_)) {
      if (entry.is_regular_file()) found.push_back(entry.path().lexically_relative(scratch_).string() + " " + std::to_string(entry.file_size()));
    }
    std::sort(found.begin(), found.end());
    return found;
  }
  // Takes the snapshot that every Refused() after it is held to. `subject`: the session whose
  // own settings and temporary objects are part of it.
  void Remember(const Wire& subject) {
    RememberServer();
    subject_ = subject;
    const Answer kept = Submit(subject, "SET VARIABLE cw_own_before = (" + SessionState() + ")");
    EXPECT_EQ(kept.status, 201) << kept.body;
  }
  void Remember(Own own = Own::kReaders) {
    if (own == Own::kReaders) return Remember(reader_);
    RememberServer();
    subject_.reset();
  }
  void RememberServer() {
    std::string sql;
    int n = 0;
    for (const auto& [name, query] : ServerState()) sql += "SET VARIABLE cw_before_" + std::to_string(n++) + " = (" + query + "); ";
    writer_.Run(sql + "SELECT 1");
    files_ = Files();
  }
  void ExpectServerUnchanged(const std::string& after) {
    const auto state = ServerState();
    std::string sql = "SELECT CASE";
    for (std::size_t n = 0; n < state.size(); n++) {
      sql += " WHEN getvariable('cw_before_" + std::to_string(n) + "') IS DISTINCT FROM (" + state[n].second + ") THEN " + std::to_string(n + 1);
    }
    const std::string changed = Value(sql + " ELSE 0 END");
    if (changed != "0") {
      const std::size_t which = changed == "?" ? 0 : static_cast<std::size_t>(std::stoi(changed));
      ADD_FAILURE() << "after: " << after << "\n  changed: " << (which >= 1 && which <= state.size() ? state[which - 1].first : "(the state could not be read)");
    }
    EXPECT_EQ(Files(), files_) << "after: " << after << "\n  a file was written";
  }
  void ExpectUnchanged(const std::string& after) {
    ExpectServerUnchanged(after);
    if (!subject_) return;
    const Answer own = Submit(*subject_, "SELECT CASE WHEN getvariable('cw_own_before') = (" + SessionState() + ") THEN 1 ELSE error('changed') END");
    EXPECT_EQ(own.status, 201) << "after: " << after << "\n  changed: the session's own settings or temporary objects (" << own.body << ")";
  }
  bool LoggingIsOn() { return Value("SELECT current_setting('enable_logging')::BOOLEAN::INT") == "1"; }

  std::unique_ptr<chargewire::Server> server_;
  Wire writer_;
  Wire reader_;
  std::optional<Wire> subject_;
  std::filesystem::path scratch_;
  std::vector<std::string> files_;
};

// These tests send INSTALL and CREATE PERSISTENT SECRET to be refused. A build that lets one
// through must leave it under the scratch home directory, which this shows DuckDB to use.
TEST_F(DuckDBReadOnly, DuckDBKeepsItsExtensionsAndSecretsUnderTheScratchHomeDirectory) {
  const std::string home = ScratchHome::Directory().string();
  ASSERT_FALSE(home.empty());
  Reads("SELECT CASE WHEN starts_with(current_setting('secret_directory'), '" + home + "') THEN 1 ELSE error(current_setting('secret_directory')) END");
  writer_.Run("CREATE OR REPLACE PERSISTENT SECRET home_probe (TYPE http, BEARER_TOKEN 'not-a-token')");
  EXPECT_TRUE(std::filesystem::exists(ScratchHome::Directory() / ".duckdb" / "stored_secrets" / "home_probe.duckdb_secret"));
  writer_.Run("DROP PERSISTENT SECRET home_probe");
  EXPECT_FALSE(std::filesystem::exists(ScratchHome::Directory() / ".duckdb" / "stored_secrets" / "home_probe.duckdb_secret"));
  std::error_code ignored;
  std::filesystem::remove_all(ScratchHome::Directory() / ".duckdb", ignored);
}

// ---- Layer 2: the kinds of statement that never run ----

TEST_F(DuckDBReadOnly, StatementsOfARefusedKind) {
  const std::string file = File("out.csv");
  writer_.Run("COPY (SELECT 9 AS id, 'copied' AS v) TO '" + File("in.csv") + "'");
  Remember();
  for (const std::string& sql : std::vector<std::string>{
           "INSERT INTO t VALUES (4, 'c')", "UPDATE t SET v = 'z'", "DELETE FROM t", "TRUNCATE t",
           "MERGE INTO t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE",
           "CREATE TABLE n (i INTEGER)", "CREATE TABLE n AS SELECT 1 AS i", "CREATE OR REPLACE TABLE t AS SELECT 0 AS id, 'r' AS v",
           "CREATE VIEW n AS SELECT 1", "CREATE SCHEMA n", "CREATE SEQUENCE n", "CREATE MACRO n(a) AS a + 1", "CREATE TYPE n AS ENUM ('a')",
           "CREATE INDEX n ON t (id)", "CREATE TEMP TABLE n AS SELECT 1 AS i", "CREATE TEMP VIEW n AS SELECT 1", "CREATE TEMP MACRO m(a) AS a + 1",
           "CREATE TEMP SEQUENCE n", "DROP TABLE t", "ALTER TABLE t ADD COLUMN x INTEGER", "ALTER TABLE t RENAME TO renamed", "COMMENT ON TABLE t IS 'x'",
           "COPY t TO '" + file + "'", "COPY (SELECT 1) TO '" + file + "'", "COPY t FROM '" + File("in.csv") + "'",
           "EXPLAIN ANALYZE COPY t TO '" + file + "'", "EXPORT DATABASE '" + File("exported") + "'",
           "ATTACH ':memory:' AS other", "ATTACH '" + File("attached.duckdb") + "' AS other", "DETACH memory", "LOAD tpch", "INSTALL tpch",
           "UPDATE EXTENSIONS", "CHECKPOINT", "FORCE CHECKPOINT", "CALL checkpoint()", "VACUUM", "ANALYZE",
           "CREATE SECRET s (TYPE s3, KEY_ID 'not-a-key', SECRET 'not-a-secret')",
           "CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID 'not-a-key', SECRET 'not-a-secret')", "DROP SECRET s",
           "PREPARE p AS SELECT 1", "PREPARE p AS COPY t TO '" + file + "'", "EXECUTE p", "DEALLOCATE p",
           "SELECT 1; INSERT INTO t VALUES (4, 'c')", "EXPLAIN ANALYZE CREATE TEMP TABLE n AS SELECT 1 AS i"}) {
    Refused(sql);
  }
  // Nothing was installed or stored under the (scratch) home directory either.
  EXPECT_FALSE(std::filesystem::exists(ScratchHome::Directory() / ".duckdb" / "stored_secrets"));
  EXPECT_FALSE(std::filesystem::exists(ScratchHome::Directory() / ".duckdb" / "extensions"));
  // The twins: a sample of each family works in a read-write session.
  for (const std::string& sql : std::vector<std::string>{
           "CREATE TEMP TABLE n AS SELECT 1 AS i", "DROP TABLE n", "COPY t TO '" + file + "'", "ATTACH ':memory:' AS other", "DETACH other",
           "CHECKPOINT", "CALL checkpoint()", "ANALYZE", "PREPARE p AS SELECT 1", "EXECUTE p", "DEALLOCATE p", "INSERT INTO t VALUES (4, 'c')"}) {
    EXPECT_EQ(Wire::StateOf(Submit(writer_, sql).Json()), "succeeded") << sql;
  }
  EXPECT_TRUE(std::filesystem::exists(file));
  EXPECT_EQ(Rows(), "4");
}

// The kind is the parser's word, and the refusal comes before DuckDB binds the statement:
// binding resolves names and evaluates the arguments of table functions. So a write to a
// table that is not there is refused as a write, where a read-write session is told that
// the table is missing.
TEST_F(DuckDBReadOnly, ARefusedKindIsRefusedBeforeAnythingOfTheStatementIsLookedUp) {
  for (const std::string& sql : std::vector<std::string>{
           "INSERT INTO no_such_table VALUES (1)", "UPDATE no_such_table SET i = 1", "DELETE FROM no_such_table", "DROP TABLE no_such_table",
           "ALTER TABLE no_such_table ADD COLUMN j INTEGER", "CREATE VIEW n AS SELECT * FROM no_such_table",
           "COPY no_such_table TO '" + File("missing.csv") + "'", "EXECUTE no_such_prepared", "CALL no_such_function()",
           "PIVOT no_such_table ON a USING sum(b)", "EXPLAIN PIVOT no_such_table ON a USING sum(b)"}) {
    Refused(sql);
    const Answer twin = Submit(writer_, sql);
    EXPECT_EQ(twin.status, 400) << sql << ": " << twin.body;
    EXPECT_EQ(CodeOf(twin), "sql_error") << sql << ": " << twin.body;
  }
}

// A PIVOT that takes its values from the data is two statements to DuckDB: a query for the
// values (it makes a temporary enum type of them), then the pivot. An earlier server checked
// only the second, and any table function in the first ran unchecked. It is refused by its
// kind; with its values written out it is a plain query.
TEST_F(DuckDBReadOnly, APivotThatTakesItsValuesFromTheData) {
  const std::vector<std::string> forms = {
      "PIVOT t ON v USING count(*)", "PIVOT t ON v USING count(*) GROUP BY id", "PIVOT_WIDER t ON v USING count(*)",
      "FROM (PIVOT t ON v USING count(*))", "SELECT * FROM (PIVOT t ON v USING count(*)) p", "WITH c AS (PIVOT t ON v USING count(*)) SELECT * FROM c",
      "PIVOT t ON v IN (SELECT v FROM t) USING count(*)", "EXPLAIN PIVOT t ON v USING count(*)", "EXPLAIN ANALYZE PIVOT t ON v USING count(*)",
      "DESCRIBE PIVOT t ON v USING count(*)", "SUMMARIZE PIVOT t ON v USING count(*)",
      "SET VARIABLE pivoted = (SELECT count(*) FROM (PIVOT t ON v USING count(*)))"};
  for (const std::string& sql : forms) {
    const Answer refused = Refused(sql);  // the session's temporary objects too: no enum type is left
    const std::string detail = refused.Json().contains("detail") ? std::string(refused.Json().at("detail").as_string()) : "";
    EXPECT_NE(detail.find("PIVOT"), std::string::npos) << sql << ": " << detail;  // it says what to write instead
    EXPECT_NE(detail.find("IN ("), std::string::npos) << sql << ": " << detail;
    ExpectReadOnly(Prepare(reader_, sql), "prepare " + sql);
  }
  // A table function in the part DuckDB runs first, in the list of values or in the source.
  for (const char* function : {"enable_logging(level = 'debug')", "disable_logging()", "truncate_duckdb_logs()", "checkpoint()",
                               "enable_profiling()", "duckdb_secrets()", "range(1)"}) {
    const std::string f(function);
    for (const std::string& sql : std::vector<std::string>{
             "PIVOT t ON v IN (SELECT 'k' FROM (SELECT count(*) FROM " + f + ")) USING count(*)",
             "PIVOT t ON v IN (SELECT v FROM t, (SELECT count(*) FROM " + f + ")) USING count(*)",
             "PIVOT (SELECT id, v FROM t, (SELECT count(*) AS c FROM " + f + ") e) ON v USING count(*)",
             "DESCRIBE PIVOT t ON v IN (SELECT v FROM t, (SELECT count(*) FROM " + f + ")) USING count(*)",
             "EXPLAIN PIVOT t ON v IN (SELECT 'k' FROM (SELECT count(*) FROM " + f + ")) USING count(*)",
             "SET VARIABLE pivoted = (SELECT count(*) FROM (PIVOT t ON v IN (SELECT v FROM t, (SELECT count(*) FROM " + f + ")) USING count(*)))"}) {
      Refused(sql);
    }
  }
  // With the values written out it runs, in every spelling.
  for (const char* sql : {"PIVOT t ON v IN ('a', 'b') USING count(*)", "SELECT * FROM t PIVOT (count(*) FOR v IN ('a', 'b'))",
                          "FROM (PIVOT t ON v IN ('a', 'b') USING count(*))", "EXPLAIN PIVOT t ON v IN ('a', 'b') USING count(*)",
                          "EXPLAIN ANALYZE PIVOT t ON v IN ('a', 'b') USING count(*)", "DESCRIBE PIVOT t ON v IN ('a', 'b') USING count(*)",
                          "UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b", "UNPIVOT (SELECT 1 AS a, 2 AS b) ON COLUMNS(*)"}) {
    Reads(sql);
  }
  // The twins: a read-write session runs each form (with a function that reads).
  for (const std::string& sql : forms) EXPECT_EQ(Wire::StateOf(Submit(writer_, sql).Json()), "succeeded") << sql;
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "PIVOT t ON v IN (SELECT 'k' FROM (SELECT count(*) FROM range(1))) USING count(*)").Json()), "succeeded");
}

// ---- Layers 1 and 3: writes inside a statement whose kind runs ----

TEST_F(DuckDBReadOnly, WritesInsideAQuery) {
  writer_.Run("CREATE SEQUENCE seq");
  writer_.Run("CREATE VIEW seq_view AS SELECT nextval('seq') AS n");
  writer_.Run("CREATE MACRO seq_macro() AS nextval('seq')");
  Remember();  // the snapshot has the sequence's last value
  for (const char* sql : {"WITH w AS (INSERT INTO t VALUES (4, 'c') RETURNING id) SELECT count(*) FROM w",
                          "WITH w AS (DELETE FROM t RETURNING id) SELECT count(*) FROM w",
                          "WITH w AS (UPDATE t SET v = 'z' RETURNING id) SELECT count(*) FROM w",
                          "SELECT nextval('seq')", "SELECT * FROM seq_view", "SELECT seq_macro()", "SELECT * FROM query('SELECT nextval(''seq'')')",
                          "SET VARIABLE x = (SELECT nextval('seq'))", "SELECT setval('seq', 1000)", "SELECT setval('seq', 1000, false)",
                          "SELECT list(nextval('seq')) FROM range(3)", "SELECT id FROM t WHERE nextval('seq') > 0", "SELECT id FROM t ORDER BY nextval('seq')",
                          "EXPLAIN ANALYZE SELECT nextval('seq')", "EXPLAIN SELECT nextval('seq')",
                          "EXPLAIN ANALYZE WITH w AS (INSERT INTO t VALUES (4, 'c') RETURNING id) SELECT count(*) FROM w",
                          // A plain EXPLAIN runs nothing, but not of a statement DuckDB plans as a change of a
                          // catalog, the session's temporary one included (which no READ ONLY transaction covers):
                          "EXPLAIN INSERT INTO t VALUES (4, 'c')", "EXPLAIN DELETE FROM t", "EXPLAIN UPDATE t SET v = 'z'",
                          "EXPLAIN CREATE TABLE n AS SELECT 1 AS i", "EXPLAIN CREATE TEMP TABLE n AS SELECT 1 AS i"}) {
    Refused(sql);
  }
  EXPECT_EQ(Rows(), "3");
  // The twins, and the proof that nothing advanced the sequence.
  EXPECT_EQ(Value("SELECT nextval('seq')"), "1");
  EXPECT_EQ(Value("SELECT setval('seq', 7)"), "7");
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "EXPLAIN CREATE TEMP TABLE n AS SELECT 1 AS i").Json()), "succeeded");
}

// DuckDB evaluates some expressions while it binds a statement, before any plan exists that
// the guard could refuse: the arguments of a table function, the value of a setting, the
// parameters of a pragma, a time-travel clause. A sequence advanced or set there stays so
// although the statement is then refused. Before DuckDB binds anything, the adapter screens
// the statement: such an expression may hold plain values only. Wherever the table function
// stands in the statement, the answer is the refusal and the sequence is as it was.
TEST_F(DuckDBReadOnly, WhatDuckDBEvaluatesWhileItBindsHoldsPlainValuesOnly) {
  writer_.Run("CREATE SEQUENCE seq");
  writer_.Run("CREATE TABLE pv AS SELECT * FROM (VALUES ('x', 1), ('y', 2)) v(a, b)");
  Remember();  // the snapshot has the sequence's last value
  const std::string f = "range(nextval('seq'))";
  const std::vector<std::string> statements = {
      "SELECT * FROM " + f, "FROM " + f, "SELECT * FROM range(setval('seq', 50))", "SELECT * FROM range(setval('seq', 50, false))",
      "SELECT * FROM generate_series(1, nextval('seq'))", "SELECT * FROM repeat('x', nextval('seq'))", "SELECT * FROM unnest([nextval('seq')])",
      "SELECT * FROM t JOIN " + f + " r ON true", "SELECT * FROM " + f + " r JOIN t ON true", "SELECT * FROM (SELECT * FROM " + f + ") s",
      "WITH c AS (SELECT * FROM " + f + ") SELECT * FROM c", "WITH c AS MATERIALIZED (SELECT * FROM " + f + ") SELECT 1",
      "SELECT (SELECT count(*) FROM " + f + ")", "SELECT * FROM t WHERE id IN (SELECT * FROM " + f + ")",
      "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM " + f + ")", "SELECT id FROM t UNION ALL SELECT * FROM " + f,
      "SELECT * FROM " + f + " UNION ALL SELECT id FROM t", "SELECT * FROM t, LATERAL (SELECT * FROM " + f + ")",
      "SELECT * FROM t, LATERAL range(nextval('seq') + t.id)", "PIVOT (SELECT a, b FROM pv, " + f + ") ON a IN ('x', 'y') USING sum(b)",
      "SELECT * FROM (VALUES ((SELECT count(*) FROM " + f + "))) v(n)", "VALUES ((SELECT count(*) FROM " + f + "))",
      "SELECT * FROM t ORDER BY (SELECT count(*) FROM " + f + ")", "SELECT * FROM t LIMIT (SELECT count(*) FROM " + f + ")",
      "SELECT * FROM t QUALIFY row_number() OVER () IN (SELECT * FROM " + f + ")",
      "EXPLAIN SELECT * FROM " + f, "EXPLAIN ANALYZE SELECT * FROM " + f, "DESCRIBE SELECT * FROM " + f, "SUMMARIZE SELECT * FROM " + f,
      "SET VARIABLE x = (SELECT count(*) FROM " + f + ")", "SET progress_bar_time = nextval('seq')", "SET SESSION progress_bar_time = setval('seq', 9)",
      "PRAGMA table_info(nextval('seq'))", "SELECT * FROM read_csv('" + File("no_such_") + "' || nextval('seq') || '.csv')",
      "SELECT * FROM duckdb_table_sample('t' || nextval('seq'))", "SELECT * FROM json_each(json_array(nextval('seq')))",
      // what is not a plain value is refused whatever it is: no list of functions that change something is kept
      "SELECT * FROM range(CASE WHEN random() < 0.5 THEN 1 ELSE 2 END)", "SELECT * FROM range(greatest(1, 2))", "SELECT * FROM range(length('abc'))",
      // forty derived tables down
      "SELECT count(*) FROM " + [&] { std::string q = "(SELECT * FROM " + f + ") d0"; for (int i = 1; i < 40; i++) q = "(SELECT * FROM " + q + ") d" + std::to_string(i); return q; }(),
      // functions that take SQL text or a table's name: what DuckDB then binds is not in the statement
      "SELECT * FROM query('SELECT 1')", "SELECT * FROM query_table('t')", "SELECT * FROM query_table(t)",
      "SELECT * FROM query('SELECT * FROM range(nextval(''seq''))')", "SELECT (SELECT count(*) FROM query('SELECT * FROM range(nextval(''seq''))'))",
      // (not while binding, but refused by the plan all the same)
      "SELECT * FROM range((SELECT nextval('seq')))", "SELECT * FROM t LIMIT nextval('seq')", "SELECT unnest([nextval('seq')])"};
  for (const std::string& sql : statements) Refused(sql);
  // Preparing a statement describes it, and describing binds it.
  for (const char* sql : {"SELECT * FROM range(nextval('seq'))", "SELECT * FROM range(setval('seq', 50)) WHERE ? = 1",
                          "SELECT (SELECT count(*) FROM range(nextval('seq')))", "SELECT * FROM query('SELECT * FROM range(nextval(''seq''))')"}) {
    ExpectReadOnly(Prepare(reader_, sql), std::string("preparing ") + sql);
    ExpectUnchanged(std::string("preparing ") + sql);
  }
  // The twins, and the proof that nothing advanced the sequence.
  EXPECT_EQ(Value("SELECT nextval('seq')"), "1");
  for (const char* sql : {"SELECT * FROM range(nextval('seq'))", "SELECT * FROM query('SELECT 1')", "SELECT * FROM query_table('t')",
                          "SELECT * FROM range(greatest(1, 2))", "SELECT * FROM range(setval('seq', 50))"}) {
    EXPECT_EQ(Wire::StateOf(Submit(writer_, sql).Json()), "succeeded") << sql;
  }
  EXPECT_EQ(Value("SELECT nextval('seq')"), "51");
}

TEST_F(DuckDBReadOnly, NullParameterTypeInferenceIsScreenedBeforeBinding) {
  writer_.Run("CREATE SEQUENCE null_parameter_seq");
  Remember();
  for (const json::value& parameters : {json::value(json::array{nullptr}),
                                        json::value(json::object{{"p", nullptr}})}) {
    const std::string parameter = parameters.is_array() ? "?" : "$p";
    for (const std::string& sql : {
             "SELECT * FROM range(nextval('null_parameter_seq')) WHERE " + parameter + " IS NULL",
             "SELECT * FROM range(setval('null_parameter_seq', 50)) WHERE " + parameter + " IS NULL",
             "CREATE TEMP TABLE null_parameter_table AS SELECT " + parameter + "::INTEGER AS n"}) {
      ExpectReadOnly(Submit(reader_, sql, parameters), sql);
      ExpectUnchanged(sql);
    }
    Reads("SELECT " + parameter + "::INTEGER IS NULL AS is_null", parameters);
    Reads("SELECT * FROM range(3) WHERE " + parameter + "::INTEGER IS NULL", parameters);
  }
  EXPECT_EQ(Value("SELECT nextval('null_parameter_seq')"), "1");
}

// The time-travel clause of a table is evaluated while DuckDB binds, like an argument.
TEST_F(DuckDBReadOnly, ATimeTravelClauseHoldsPlainValuesOnly) {
  writer_.Run("CREATE SEQUENCE seq");
  Remember();
  for (const char* sql : {"SELECT * FROM t AT (VERSION => nextval('seq'))", "SELECT * FROM t AT (VERSION => setval('seq', 77))",
                          "SELECT * FROM t AT (TIMESTAMP => now() - INTERVAL (nextval('seq')) DAY)",
                          "SELECT (SELECT count(*) FROM t AT (VERSION => nextval('seq')))", "SELECT * FROM (SELECT * FROM t AT (VERSION => nextval('seq'))) s",
                          "SELECT * FROM t a JOIN (SELECT * FROM t AT (VERSION => nextval('seq'))) b USING (id)",
                          "EXPLAIN SELECT * FROM t AT (VERSION => nextval('seq'))", "DESCRIBE SELECT * FROM t AT (VERSION => nextval('seq'))"}) {
    Refused(sql);
  }
  ExpectReadOnly(Prepare(reader_, "SELECT * FROM t AT (VERSION => nextval('seq'))"), "preparing a time-travel read");
  ExpectUnchanged("preparing a time-travel read");
  // With plain values the clause passes the screen: DuckDB's own answer follows, the same in
  // both kinds of session (its own tables have no time travel; a lake's have).
  for (const char* sql : {"SELECT * FROM t AT (VERSION => 1)", "SELECT * FROM t AT (TIMESTAMP => now() - INTERVAL 1 DAY)",
                          "SELECT * FROM t AT (TIMESTAMP => TIMESTAMP '2020-01-01 00:00:00')"}) {
    for (const Wire* wire : {&reader_, &writer_}) {
      const Answer answer = Submit(*wire, sql);
      EXPECT_EQ(answer.status, 400) << sql << ": " << answer.body;
      EXPECT_EQ(CodeOf(answer), "sql_error") << sql << ": " << answer.body;
    }
  }
  EXPECT_EQ(Value("SELECT nextval('seq')"), "1");
}

// DuckDB expands views and macros into the statement while it binds, so what their
// definitions would evaluate then is screened as the statement's own: for every view and
// macro the statement names, and every one those name, whoever defined them.
TEST_F(DuckDBReadOnly, ViewsAndMacrosAreScreenedWithTheStatementThatReachesThem) {
  for (const char* sql : {
           "CREATE SEQUENCE seq",
           "CREATE VIEW bad_view AS SELECT * FROM range(nextval('seq'))",  // (creating it binds it: the sequence moves once, here)
           "CREATE VIEW bad_over AS SELECT count(*) AS n FROM bad_view", "CREATE VIEW bad_over_over AS SELECT n + 1 AS m FROM bad_over",
           "CREATE VIEW bad_join AS SELECT * FROM t JOIN generate_series(1, nextval('seq')) g(i) ON true",
           "CREATE VIEW bad_set AS SELECT * FROM range(setval('seq', 9))",
           "CREATE MACRO bad_runs(q) AS TABLE SELECT * FROM query(q)", "CREATE MACRO bad_table() AS TABLE SELECT * FROM range(setval('seq', 500))",
           "CREATE MACRO bad_calls_macro() AS TABLE SELECT * FROM bad_table()", "CREATE MACRO bad_scalar() AS (SELECT count(*) FROM range(nextval('seq')))",
           "CREATE MACRO bad_scalar_over_view() AS (SELECT max(n) FROM bad_over)", "CREATE MACRO nx() AS nextval('seq')",
           "CREATE MACRO tm(n) AS TABLE SELECT * FROM range(n)", "CREATE VIEW good_view AS SELECT id FROM t WHERE id > 1",
           "CREATE VIEW good_over AS SELECT count(*) AS n FROM good_view", "CREATE MACRO good_scalar(a) AS a + 1",
           "CREATE MACRO good_sub() AS (SELECT max(id) FROM t)", "CREATE MACRO good_table(n) AS TABLE SELECT * FROM good_view WHERE id <= n"}) {
    writer_.Run(sql);
  }
  Remember();
  for (const char* sql : {
           // views: read directly, qualified, quoted, described, through a view over them, anywhere in a statement
           "SELECT * FROM bad_view", "SELECT * FROM memory.main.bad_view", "SELECT * FROM \"BAD_VIEW\"", "FROM bad_view", "TABLE bad_view",
           "SELECT * FROM bad_over", "SELECT * FROM bad_over_over", "SELECT * FROM bad_join", "SELECT * FROM bad_set",
           "SELECT count(*) FROM t WHERE id IN (SELECT * FROM bad_view)", "SELECT (SELECT count(*) FROM bad_view)", "WITH c AS (SELECT * FROM bad_view) SELECT count(*) FROM c",
           "SELECT * FROM t a LEFT JOIN bad_view b ON true", "EXPLAIN SELECT * FROM bad_view", "DESCRIBE bad_view", "DESCRIBE TABLE bad_view",
           "DESCRIBE SELECT * FROM bad_view", "SUMMARIZE bad_view", "SET VARIABLE x = (SELECT count(*) FROM bad_view)",
           // table macros
           "SELECT * FROM bad_runs('SELECT 1')", "SELECT * FROM bad_runs('SELECT * FROM range(nextval(''seq''))')", "SELECT * FROM bad_table()",
           "SELECT * FROM bad_calls_macro()", "SELECT (SELECT count(*) FROM bad_table())", "SELECT * FROM tm(nx())", "SELECT * FROM tm(nextval('seq'))",
           // scalar macros, wherever an expression stands
           "SELECT bad_scalar()", "SELECT (SELECT bad_scalar())", "SELECT id, bad_scalar() FROM t", "SELECT * FROM t WHERE id > bad_scalar()",
           "SELECT * FROM t ORDER BY bad_scalar()", "SELECT bad_scalar_over_view()", "SELECT nx()"}) {
    Refused(sql);
  }
  for (const char* sql : {"SELECT * FROM bad_view", "SELECT * FROM bad_table()", "SELECT bad_scalar()", "SELECT * FROM tm(nx())"}) {
    ExpectReadOnly(Prepare(reader_, sql), std::string("preparing ") + sql);
    ExpectUnchanged(std::string("preparing ") + sql);
  }
  // Views and macros that only read are read, and a table macro takes a plain value.
  for (const char* sql : {"SELECT CASE WHEN count(*) = 2 THEN 1 ELSE error('good_view') END FROM good_view",
                          "SELECT CASE WHEN n = 2 THEN 1 ELSE error('good_over') END FROM good_over",
                          "SELECT CASE WHEN good_scalar(1) = 2 AND good_sub() = 3 THEN 1 ELSE error('scalar macros') END",
                          "SELECT CASE WHEN count(*) = 3 THEN 1 ELSE error('tm') END FROM tm(3)", "SELECT * FROM tm(1 + 2)",
                          "SELECT CASE WHEN count(*) = 1 THEN 1 ELSE error('good_table') END FROM good_table(2)"}) {
    Reads(sql);
  }
  ExpectUnchanged("reading through views and macros");
  // The twins: a read-write session reads each of them (and moves the sequence).
  for (const char* sql : {"SELECT * FROM bad_view", "SELECT * FROM bad_table()", "SELECT bad_scalar()", "SELECT * FROM bad_runs('SELECT 1')", "SELECT * FROM tm(nx())"}) {
    EXPECT_EQ(Wire::StateOf(Submit(writer_, sql).Json()), "succeeded") << sql;
  }
}

// The adapter keeps the views' and macros' definitions it has read for a session, and reads
// them again when a catalog has changed. What a writer defines or replaces after a reader's
// first statement is screened like what was there before it.
TEST_F(DuckDBReadOnly, DefinitionsMadeAfterTheSessionsFirstStatementAreScreenedToo) {
  for (const char* sql : {"CREATE SEQUENCE seq", "CREATE VIEW later_view AS SELECT 0::BIGINT AS n",
                          "CREATE MACRO later_macro(q) AS TABLE SELECT 0::BIGINT AS n"}) {
    writer_.Run(sql);
  }
  const std::string view_is = "SELECT CASE WHEN n = {} THEN 1 ELSE error('not the view as it is now') END FROM later_view";
  auto reads_view_as = [&](const char* n) {
    std::string sql = view_is;
    sql.replace(sql.find("{}"), 2, n);
    Reads(sql);
  };
  reads_view_as("0");  // the session has read the definitions
  Reads("SELECT * FROM later_macro('SELECT 1')");
  // Replaced by ones that evaluate while binding (the writer's own CREATE binds the view once).
  writer_.Run("CREATE OR REPLACE VIEW later_view AS SELECT range AS n FROM range(nextval('seq'))");
  writer_.Run("CREATE OR REPLACE MACRO later_macro(q) AS TABLE SELECT * FROM query(q)");
  Remember(Own::kNoSessions);  // (the reader's next statement is the read itself)
  Refused("SELECT * FROM later_view");
  Refused("SELECT * FROM later_macro('SELECT * FROM range(setval(''seq'', 100))')");
  // Made new, after the session has read the definitions twice.
  writer_.Run("CREATE VIEW newer_view AS SELECT * FROM range(setval('seq', 200))");
  writer_.Run("CREATE MACRO newer_macro() AS TABLE SELECT * FROM generate_series(1, nextval('seq'))");
  Remember(Own::kNoSessions);
  Refused("SELECT * FROM newer_view");
  Refused("SELECT * FROM newer_macro()");
  // And the reverse: replaced by a harmless one, a view is readable again.
  writer_.Run("CREATE OR REPLACE VIEW later_view AS SELECT 5::BIGINT AS n");
  writer_.Run("CREATE OR REPLACE MACRO later_macro(q) AS TABLE SELECT 5::BIGINT AS n");
  Remember(Own::kNoSessions);
  reads_view_as("5");
  Reads("SELECT CASE WHEN n = 5 THEN 1 ELSE error('not the macro as it is now') END FROM later_macro('x')");
  writer_.Run("CREATE OR REPLACE VIEW later_view AS SELECT range AS n FROM range(nextval('seq'))");
  Remember(Own::kNoSessions);
  Refused("SELECT * FROM later_view");
  // In a second database, attached after the reader's first statement.
  writer_.Run("ATTACH ':memory:' AS other");
  writer_.Run("CREATE SEQUENCE other.main.far_seq");
  writer_.Run("CREATE VIEW other.main.far_view AS SELECT * FROM range(nextval('other.main.far_seq'))");
  writer_.Run("CREATE MACRO other.main.far_macro() AS TABLE SELECT * FROM range(setval('other.main.far_seq', 300))");
  Remember(Own::kNoSessions);
  Refused("SELECT * FROM other.main.far_view");
  Refused("SELECT * FROM other.main.far_macro()");
  writer_.Run("CREATE OR REPLACE VIEW other.main.far_view AS SELECT 7::BIGINT AS n");
  Remember(Own::kNoSessions);
  Reads("SELECT CASE WHEN n = 7 THEN 1 ELSE error('far_view') END FROM other.main.far_view");
  ExpectServerUnchanged("reading the replaced view of the attached database");
  // Temporary views and macros the session made itself, before it became read-only. (Ones
  // that do nothing when they are made: DuckDB 2.0 ends a CREATE TEMP VIEW over
  // range(nextval('seq')) with a fatal error, in any session.)
  Wire session = Session();
  session.Run("SET VARIABLE one = 1");
  session.Run("CREATE TEMP VIEW mine_view AS SELECT * FROM query('SELECT 42 AS n')");
  session.Run("CREATE TEMP VIEW mine_other AS SELECT * FROM range(CASE WHEN getvariable('one') = 1 THEN 1 ELSE nextval('seq') END)");
  session.Run("CREATE TEMP MACRO mine_macro() AS TABLE SELECT * FROM range(setval('seq', 400))");
  session.Run("CREATE TEMP MACRO mine_scalar() AS (SELECT count(*) FROM range(nextval('seq')))");
  session.Run("CREATE TEMP VIEW mine_good AS SELECT 3::BIGINT AS n");
  EXPECT_EQ(Patch(session, R"({"read_only": true})").Json().at("errors").as_object().size(), 0u);
  Remember(Own::kNoSessions);
  Refused("SELECT * FROM mine_view", &session);
  Refused("SELECT * FROM mine_other", &session);
  Refused("SELECT * FROM mine_macro()", &session);
  Refused("SELECT mine_scalar()", &session);
  const Answer good = Submit(session, "SELECT CASE WHEN n = 3 THEN 1 ELSE error('mine_good') END FROM mine_good");
  EXPECT_EQ(Wire::StateOf(good.Json()), "succeeded") << good.body;
}

// A view is also reached without its name standing in the statement as a name: SHOW, and
// the functions and pragmas that take a table's name as a string and bind it to describe it.
// While a view exists whose definition does not pass the screen, a statement that names it
// in such a string is refused, and so is one whose string is computed (the adapter cannot
// tell which table it will name). The schema listing gives such a view no columns.
TEST_F(DuckDBReadOnly, AViewThatDoesNotPassTheScreenIsCaughtHoweverItIsNamed) {
  writer_.Run("CREATE SEQUENCE seq");
  writer_.Run("CREATE VIEW good_view AS SELECT id FROM t WHERE id > 1");
  const std::vector<std::string> computed = {"SELECT count(*) FROM pragma_table_info('good' || '_view')",
                                             "SELECT count(*) FROM pragma_table_info(getvariable('which'))"};
  const char* with_parameter = "SELECT count(*) FROM pragma_table_info(?)";
  const std::vector<std::string> good = {"SHOW good_view", "SELECT * FROM (SHOW good_view)", "PRAGMA table_info('good_view')",
                                         "SELECT count(*) FROM pragma_table_info('good_view')", "SELECT * FROM good_view", "SHOW TABLES",
                                         "SHOW ALL TABLES", "DESCRIBE", "SELECT * FROM range(-1 + 3)", "SELECT count(*) FROM duckdb_columns()",
                                         "SELECT count(*) FROM information_schema.columns", "SELECT count(*) FROM duckdb_views()"};
  Reads("SET VARIABLE which = 'good_view'");
  for (const std::string& sql : computed) Reads(sql);  // no view is unreadable: a computed name is a name like another
  Reads(with_parameter, json::array{"good_view"});
  writer_.Run("CREATE VIEW bad_view AS SELECT * FROM range(nextval('seq'))");  // (creating it binds it once)
  Remember();
  for (const char* sql : {"SHOW bad_view", "SELECT * FROM (SHOW bad_view)", "SHOW memory.main.bad_view", "SET VARIABLE x = (SELECT count(*) FROM (SHOW bad_view))",
                          "PRAGMA table_info('bad_view')", "PRAGMA table_info('main.bad_view')", "SELECT count(*) FROM pragma_table_info('bad_view')",
                          "SELECT count(*) FROM pragma_table_info('main.bad_view')", "SELECT count(*) FROM pragma_table_info('memory.main.bad_view')",
                          "SELECT (SELECT count(*) FROM pragma_table_info('bad_view'))", "SELECT count(*) FROM pragma_table_info('BAD_VIEW')",
                          "SELECT count(*) FROM pragma_table_info('bad' || '_view')", "DESCRIBE bad_view", "SUMMARIZE bad_view"}) {
    Refused(sql);
  }
  ExpectReadOnly(Submit(reader_, with_parameter, json::array{"bad_view"}), "the view's name as a parameter");
  // A computed name is a name like another, whatever views there are (an earlier adapter
  // refused every computed name while a view existed that it would not bind, anyone's view).
  for (const std::string& sql : computed) Reads(sql);
  Reads(with_parameter, json::array{"good_view"});
  // Prepared: refused then, or accepted (DuckDB binds nothing of the view to describe the
  // statement) and refused at every execution.
  for (const char* sql : {"SHOW bad_view", "SELECT * FROM pragma_table_info('bad_view')"}) {
    const Answer prepared = Prepare(reader_, sql);
    if (prepared.status == 201) {
      ExpectReadOnly(reader_.Post(reader_.session + "/prepared/" + std::string(prepared.Json().at("prepared_id").as_string()) + "/execute", "{}"),
                     std::string("executing the prepared ") + sql);
    } else {
      ExpectReadOnly(prepared, std::string("preparing ") + sql);
    }
  }
  ExpectUnchanged("naming the view in a parameter, and preparing");
  // The schema listing binds every view to describe it: an unreadable one is listed without columns.
  for (const char* listing : {"/metadata/tables", "/metadata/tables?include_schema=true"}) {
    const Answer tables = reader_.Get(reader_.session + listing);
    EXPECT_EQ(tables.status, 200) << listing << ": " << tables.body.substr(0, 300);
    ExpectUnchanged(listing);
  }
  // Everything else is read as before, also what names another view.
  for (const std::string& sql : good) Reads(sql);
  ExpectUnchanged("reading beside an unreadable view");
  // Replaced by a view that only reads, everything about it reads again.
  writer_.Run("CREATE OR REPLACE VIEW bad_view AS SELECT 1 AS one");
  Reads("SET VARIABLE which = 'bad_view'");
  for (const char* sql : {"SHOW bad_view", "SELECT * FROM (SHOW bad_view)", "PRAGMA table_info('bad_view')", "SELECT * FROM bad_view",
                          "SELECT CASE WHEN count(*) = 1 THEN 1 ELSE error('columns') END FROM pragma_table_info('bad' || '_view')",
                          "SELECT CASE WHEN count(*) = 1 THEN 1 ELSE error('columns') END FROM pragma_table_info(getvariable('which'))"}) {
    Reads(sql);
  }
  Reads(with_parameter, json::array{"bad_view"});
  EXPECT_EQ(reader_.Get(reader_.session + "/metadata/tables?include_schema=true").status, 200);
  // The twin: a read-write session describes a view by its name in a string. (Not the unreadable
  // one: DuckDB 2.0 ends that, in a read-write session, with a fatal error for the database.)
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT count(*) FROM pragma_table_info('good' || '_view')").Json()), "succeeded");
}

// A driver's column listing describes every table and view. For a read-only session each is
// only bound, never run: a view the session may not read (it advances a sequence, it calls a
// function that is not listed) is listed with its columns, one whose binding itself would
// evaluate something is listed without any, and no view takes the listing of the others away.
TEST_F(DuckDBReadOnly, TheSchemaListingIsAnsweredWhateverViewsThereAre) {
  using Columns = std::vector<std::string>;
  for (const char* sql : {"CREATE SEQUENCE seq", "CREATE VIEW seq_view AS SELECT nextval('seq') AS n",
                          "CREATE VIEW log_view AS SELECT count(*) AS c FROM enable_logging()", "CREATE VIEW plain_view AS SELECT v, id FROM t",
                          "CREATE VIEW bad_view AS SELECT * FROM range(nextval('seq'))"}) {  // (creating that one binds it once)
    writer_.Run(sql);
  }
  Remember();
  for (int again = 0; again < 3; again++) {
    const Answer listing = reader_.Get(reader_.session + "/metadata/tables?include_schema=true");
    ASSERT_EQ(listing.status, 200) << listing.body.substr(0, 300);
    const auto columns = ListedColumns(listing.body);
    EXPECT_EQ(columns.size(), 5u);
    EXPECT_EQ(columns.contains("t") ? columns.at("t") : Columns{"?"}, (Columns{"id", "v"}));
    EXPECT_EQ(columns.contains("seq_view") ? columns.at("seq_view") : Columns{"?"}, (Columns{"n"}));
    EXPECT_EQ(columns.contains("log_view") ? columns.at("log_view") : Columns{"?"}, (Columns{"c"}));
    EXPECT_EQ(columns.contains("plain_view") ? columns.at("plain_view") : Columns{"?"}, (Columns{"v", "id"}));
    EXPECT_EQ(columns.contains("bad_view") ? columns.at("bad_view") : Columns{"?"}, (Columns{}));
    ExpectUnchanged("the schema listing");
    EXPECT_EQ(reader_.Get(reader_.session + "/metadata/tables").status, 200);
    EXPECT_EQ(reader_.Get(reader_.session + "/metadata/tables?include_schema=true&table_name_filter_pattern=bad%25").status, 200);
    ExpectUnchanged("the schema listing, filtered");
  }
  for (const char* sql : {"SELECT * FROM seq_view", "SELECT * FROM log_view", "SELECT * FROM bad_view"}) Refused(sql);
  Reads("SELECT * FROM plain_view");
  // The twin: a read-write session is given the unreadable view's columns too.
  const auto for_a_writer = ListedColumns(writer_.Get(writer_.session + "/metadata/tables?include_schema=true").body);
  EXPECT_EQ(for_a_writer.contains("bad_view") ? for_a_writer.at("bad_view") : Columns{"?"}, (Columns{"range"}));
}

// A function called where DuckDB evaluates while binding is on the adapter's short list of
// ones that only compute. A macro of the same name, in any schema, could be what DuckDB
// calls there instead: while one exists, such a statement is refused.
// A macro named like a function that is evaluated while DuckDB binds: in a schema the
// statement does not reach it changes nothing for anyone, and where DuckDB does bind it, it
// is refused where it would advance the sequence (an earlier adapter refused every such
// statement of every reader while the macro existed).
TEST_F(DuckDBReadOnly, AMacroNamedLikeAFunctionChangesNothingThroughAReadOnlySession) {
  writer_.Run("CREATE SEQUENCE seq");
  writer_.Run("CREATE SCHEMA shadows");
  const std::vector<std::string> statements = {
      "SELECT * FROM generate_series(TIMESTAMP '2020-01-01', TIMESTAMP '2020-01-03', INTERVAL 1 DAY)",  // an interval literal is to_days(1)
      "SELECT * FROM unnest([1, 2])",                                                                     // a list literal is list_value(1, 2)
      "SELECT * FROM range(abs(-3))", "SET progress_bar_time = abs(-1000)"};
  for (const std::string& sql : statements) Reads(sql);
  Reads("RESET progress_bar_time");
  for (const char* macro : {"shadows.to_days(x)", "shadows.list_value(x, y)", "shadows.abs(x)"}) {
    writer_.Run(std::string("CREATE MACRO ") + macro + " AS nextval('seq')");
  }
  for (const std::string& sql : statements) Reads(sql);
  Reads("RESET progress_bar_time");
  Remember();
  Reads("SELECT abs(-3), [1, 2]");
  Refused("SELECT * FROM range(shadows.abs(-3))");
  Refused("SELECT shadows.abs(-3)");
  for (const char* macro : {"shadows.to_days", "shadows.list_value", "shadows.abs"}) writer_.Run(std::string("DROP MACRO ") + macro);
  for (const std::string& sql : statements) Reads(sql);
  EXPECT_EQ(Value("SELECT nextval('seq')"), "1");
}

// Preparing a statement describes it, and describing binds it. An earlier server bound a
// prepared `SELECT * FROM range(nextval('seq'))` inside its READ ONLY transaction and then
// committed that: to DuckDB a fatal error, after which no session of the server got an answer.
TEST_F(DuckDBReadOnly, PreparingAStatementNeitherChangesAnythingNorTakesTheDatabaseDown) {
  writer_.Run("CREATE SEQUENCE seq");
  writer_.Run("CREATE VIEW bad_view AS SELECT * FROM range(setval('seq', 5))");
  Remember();
  for (int round = 0; round < 3; round++) {
    for (const char* sql : {"SELECT * FROM range(nextval('seq'))", "SELECT * FROM bad_view", "SELECT * FROM t AT (VERSION => nextval('seq'))",
                            "SELECT * FROM range(nextval('seq')) WHERE ? = 1", "SET progress_bar_time = nextval('seq')"}) {
      ExpectReadOnly(Prepare(reader_, sql), std::string("preparing ") + sql);
    }
    ExpectReadOnly(Submit(reader_, "SELECT * FROM range(nextval('seq'))"), "the statement itself");
    Reads("BEGIN");
    ExpectReadOnly(Submit(reader_, "SELECT * FROM bad_view"), "inside the client's transaction");
    Reads("COMMIT");
    ExpectUnchanged("preparing and refusing, round " + std::to_string(round));
  }
  // Every session still gets its answers: this one, the read-write one, a new one.
  Reads("SELECT count(*) FROM t");
  writer_.Run("INSERT INTO t VALUES (4, 'c')");
  EXPECT_EQ(Rows(), "4");
  Wire fresh = Session();
  fresh.Run("INSERT INTO t VALUES (5, 'd')");
  Wire reader = Session(R"({"read_only": true})");
  EXPECT_EQ(Wire::StateOf(Submit(reader, "SELECT count(*) FROM t").Json()), "succeeded");
}

TEST_F(DuckDBReadOnly, WhatASessionHadBeforeItBecameReadOnly) {
  // A temporary table from before is read and not written.
  Wire session = Session();
  session.Run("CREATE TEMP TABLE mine AS SELECT 1 AS i");
  EXPECT_EQ(Patch(session, R"({"read_only": true})").Json().at("errors").as_object().size(), 0u);
  Remember(session);
  for (const char* sql : {"INSERT INTO mine VALUES (2)", "UPDATE mine SET i = 3", "DELETE FROM mine", "DROP TABLE mine", "TRUNCATE mine",
                          "ALTER TABLE mine ADD COLUMN j INTEGER", "WITH w AS (INSERT INTO mine VALUES (2) RETURNING i) SELECT count(*) FROM w",
                          "EXPLAIN INSERT INTO mine VALUES (2)", "EXPLAIN ANALYZE INSERT INTO mine VALUES (2)"}) {
    Refused(sql, &session);
  }
  EXPECT_EQ(Wire::StateOf(Submit(session, "SELECT CASE WHEN (SELECT sum(i) FROM mine) = 1 THEN 1 ELSE error('written') END").Json()), "succeeded");

  // A temporary sequence would be the one thing a statement could still move (no READ ONLY
  // transaction covers the temporary catalog, and advancing a sequence is not undone): the
  // session is told to drop it first, and stays as it was.
  Wire other = Session();
  other.Run("CREATE TEMP SEQUENCE mine START 1");
  other.Run("SELECT nextval('mine')");
  const Answer refused = Patch(other, R"({"read_only": true})");
  EXPECT_EQ(OptionError(refused, "read_only"), "invalid_option_value") << refused.body;
  EXPECT_NE(refused.body.find("temporary sequence"), std::string::npos) << refused.body;
  EXPECT_EQ(other.Session().at("options").as_object().at("read_only"), json::value(false));
  other.Run("SELECT nextval('mine')");  // still a session that writes
  other.Run("DROP SEQUENCE mine");
  const Answer accepted = Patch(other, R"({"read_only": true})");
  EXPECT_EQ(accepted.Json().at("errors").as_object().size(), 0u) << accepted.body;
  EXPECT_EQ(other.Session().at("options").as_object().at("read_only"), json::value(true));
  Remember(other);
  Refused("CREATE TEMP SEQUENCE mine", &other);
  Refused("INSERT INTO t VALUES (4, 'c')", &other);
}

// Functions that run SQL text of their own or do the server's housekeeping, reached every
// way a statement reaches a function: the plan names them wherever they stand in it.
TEST_F(DuckDBReadOnly, FunctionsThatChangeSomethingHoweverTheyAreCalled) {
  const std::string write = "WITH w AS (INSERT INTO t VALUES (9, ''z'') RETURNING id) SELECT count(*) FROM w";
  const std::string run = "json_execute_serialized_sql(json_serialize_sql('" + write + "'))";
  writer_.Run("CREATE MACRO run_it(q) AS TABLE SELECT * FROM json_execute_serialized_sql(json_serialize_sql(q))");
  writer_.Run("CREATE MACRO checkpoint_it() AS TABLE SELECT * FROM checkpoint()");
  writer_.Run("CREATE VIEW logging_it AS SELECT count(*) AS c FROM enable_logging()");
  Remember();
  for (const std::string& sql : std::vector<std::string>{
           "SELECT * FROM " + run, "SELECT * FROM system.main." + run, "SELECT * FROM run_it('" + write + "')",
           "SELECT (SELECT count(*) FROM " + run + ")", "WITH c AS (SELECT * FROM " + run + ") SELECT * FROM c",
           "EXPLAIN ANALYZE SELECT * FROM " + run, "SET VARIABLE x = (SELECT count(*) FROM " + run + ")",
           "SELECT * FROM checkpoint()", "SELECT * FROM force_checkpoint()", "SELECT * FROM system.main.checkpoint()",
           "SELECT * FROM query('SELECT * FROM checkpoint()')", "SELECT * FROM checkpoint_it()",
           // the function is the root of the plan ...
           "SELECT * FROM enable_logging()", "SELECT * FROM enable_logging(level = 'debug')", "SELECT * FROM disable_logging()",
           "SELECT * FROM truncate_duckdb_logs()", "SELECT * FROM enable_profiling()", "SELECT * FROM disable_profiling()",
           // ... or stands below other operators: an aggregate, a view, either side of a join, a
           // branch of a union, a subquery of every kind
           "SELECT count(*) FROM enable_logging()", "SELECT * FROM logging_it",
           "SELECT * FROM t a JOIN (SELECT count(*) AS c FROM enable_logging()) e ON true",
           "SELECT * FROM (SELECT count(*) AS c FROM enable_logging()) e JOIN t a ON true",
           "SELECT * FROM t a LEFT JOIN (SELECT count(*) AS c FROM enable_logging()) e ON a.id = e.c",
           "SELECT id FROM t UNION ALL SELECT count(*) FROM enable_logging()", "SELECT count(*) FROM enable_logging() UNION ALL SELECT id FROM t",
           "SELECT * FROM t WHERE id IN (SELECT count(*) FROM enable_logging())",
           "SELECT * FROM t a WHERE EXISTS (SELECT 1 FROM enable_logging() WHERE a.id > 0)",
           "SELECT * FROM t a, LATERAL (SELECT count(*) + a.id FROM enable_logging())",
           "SELECT id, (SELECT count(*) FROM enable_logging()) FROM t ORDER BY id LIMIT 1", "SUMMARIZE SELECT * FROM enable_logging()",
           "SELECT 1 AS first; SELECT * FROM enable_logging()"}) {
    Refused(sql);
  }
  EXPECT_EQ(Rows(), "3");
  EXPECT_FALSE(LoggingIsOn());
  // A plain EXPLAIN shows a plan and runs none of it.
  for (const std::string& sql : std::vector<std::string>{"EXPLAIN SELECT * FROM checkpoint()", "EXPLAIN SELECT * FROM enable_logging()",
                                                         "EXPLAIN COPY t TO '" + File("explained.csv") + "'", "EXPLAIN (SELECT 1)"}) {
    Reads(sql);
    ExpectUnchanged(sql);
  }
  // The twins: in a read-write session the same calls do what they are refused for.
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT * FROM " + run).Json()), "succeeded");
  EXPECT_EQ(Rows(), "4");
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT * FROM enable_logging()").Json()), "succeeded");
  EXPECT_TRUE(LoggingIsOn());
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT * FROM checkpoint()").Json()), "succeeded");
}

// What is checked is what runs. DuckDB evaluates the arguments of query() and query_table()
// while it binds, and they become the plan; an earlier server planned once to check and
// once to run, and with a coin in the argument the two plans differed in a quarter of the
// tries. Neither function is run for a read-only session at all now, and an argument that
// is not a plain value is refused before it is evaluated: every one of 40 tries of each
// form is refused, and the server's logging is off after each. (One try proves nothing here.)
TEST_F(DuckDBReadOnly, AVolatileArgumentIsRefusedEveryTime) {
  writer_.Run("CREATE VIEW harmless AS SELECT 0::BIGINT AS c");
  writer_.Run("CREATE VIEW harmful AS SELECT count(*) AS c FROM enable_logging()");
  Remember();
  const std::string either = "THEN 'SELECT 0::BIGINT AS c' ELSE 'SELECT count(*) AS c FROM enable_logging()' END";
  for (const std::string& sql : std::vector<std::string>{
           "SELECT * FROM query(CASE WHEN random() < 0.5 " + either + ")",
           "SELECT * FROM query(CASE WHEN uuid()::VARCHAR < '8' " + either + ")",
           "SELECT * FROM query_table(CASE WHEN random() < 0.5 THEN 'harmless' ELSE 'harmful' END)",
           "SELECT * FROM query_table([CASE WHEN random() < 0.5 THEN 'harmless' ELSE 'harmful' END])",
           "SELECT * FROM range(CASE WHEN random() < 0.5 THEN 1 ELSE 2 END)", "SELECT * FROM range((random() * 3)::INTEGER)"}) {
    for (int attempt = 0; attempt < 40; attempt++) Refused(sql);
  }
  // An argument that reads the session's own variable is a value: the statement runs, with
  // the plan that was checked.
  Reads("SET VARIABLE n = 3");
  Reads("SELECT CASE WHEN count(*) = 3 THEN 1 ELSE error('not the variable') END FROM range(getvariable('n'))");
  ExpectUnchanged("an argument that is the session's variable");
  // The twin: a read-write session runs them.
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT * FROM range((random() * 3)::INTEGER)").Json()), "succeeded");
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT * FROM query_table(CASE WHEN random() < 2 THEN 'harmless' ELSE 'harmful' END)").Json()), "succeeded");
}

// ---- The doors back to read-write ----

// Each got a write out of a READ ONLY transaction that a client had started itself.
TEST_F(DuckDBReadOnly, NothingMakesItReadWriteAgain) {
  Remember(Own::kNoSessions);  // (the session's own state cannot be read while its transaction is aborted)
  const std::string insert = "INSERT INTO t VALUES (4, 'c')";
  Reads("BEGIN");
  Reads("COMMIT");
  Refused(insert);
  Reads("BEGIN");
  Refused("COMMIT; " + insert);
  EXPECT_EQ(Transaction(reader_), "none");  // the COMMIT ran; the write found a READ ONLY transaction too
  Refused("BEGIN; ROLLBACK; BEGIN; " + insert + "; COMMIT");
  if (Transaction(reader_) != "none") Reads("ROLLBACK");
  Refused("BEGIN TRANSACTION READ WRITE");
  Refused("START TRANSACTION READ WRITE");
  Refused("BEGIN READ WRITE");
  EXPECT_EQ(Transaction(reader_), "none");
  for (const char* options : {R"({"read_only": false})", R"({"read_only": false, "autocommit": true})"}) {
    const Answer back = Patch(reader_, options);
    EXPECT_EQ(OptionError(back, "read_only"), "read_only") << back.body;
    EXPECT_EQ(back.Json().at("options").as_object().at("read_only"), json::value(true)) << back.body;
  }
  Refused(insert);
  // Manual commit: every transaction the commit and rollback calls start is READ ONLY.
  EXPECT_EQ(Patch(reader_, R"({"autocommit": false})").Json().at("errors").as_object().size(), 0u);
  for (const char* call : {"/commit", "/rollback"}) {
    Reads("SELECT count(*) FROM t");
    EXPECT_EQ(reader_.Post(reader_.session + call, "").status, 204);
    EXPECT_EQ(Transaction(reader_), "open");
    Refused(insert);
    Reads("COMMIT");
    Refused("WITH w AS (" + insert + " RETURNING id) SELECT count(*) FROM w");
    EXPECT_EQ(Transaction(reader_), "open");  // refused before DuckDB bound anything of it: the transaction is as it was
    Refused("SELECT * FROM checkpoint()");
    EXPECT_EQ(Transaction(reader_), "aborted");  // refused while DuckDB planned it: that aborts the transaction
    Reads("ROLLBACK");
  }
  EXPECT_EQ(Patch(reader_, R"({"autocommit": true})").Json().at("errors").as_object().size(), 0u);
  EXPECT_EQ(Transaction(reader_), "none");
  Refused(insert);
  // A failed statement and a refusal leave no transaction behind in autocommit.
  EXPECT_EQ(Submit(reader_, "SELECT * FROM no_such_table").status, 400);
  Refused("WITH w AS (" + insert + " RETURNING id) SELECT count(*) FROM w");
  Reads("SELECT count(*) FROM t");
  Refused(insert);
  EXPECT_EQ(Rows(), "3");
  EXPECT_EQ(reader_.Session().at("options").as_object().at("read_only"), json::value(true));
}

TEST_F(DuckDBReadOnly, ATransactionWithWorkInItRefusesTheSwitch) {
  Wire session = Session();
  session.Run("BEGIN");
  session.Run("INSERT INTO t VALUES (4, 'c')");
  const Answer refused = Patch(session, R"({"read_only": true})");
  ASSERT_EQ(refused.status, 200) << refused.body;
  EXPECT_EQ(OptionError(refused, "read_only"), "invalid_option_value") << refused.body;
  EXPECT_EQ(Transaction(session), "open");
  EXPECT_EQ(session.Session().at("options").as_object().at("read_only"), json::value(false));
  session.Run("COMMIT");  // the work was left for the client to end
  EXPECT_EQ(Rows(), "4");
  // A transaction in which a statement failed counts as one with work in it.
  session.Run("BEGIN");
  EXPECT_EQ(Submit(session, "SELECT * FROM no_such_table").status, 400);
  Patch(session, R"({"read_only": true})");  // (DuckDB answers no option request in an aborted transaction)
  EXPECT_EQ(session.Session().at("options").as_object().at("read_only"), json::value(false));
  session.Run("ROLLBACK");
  // read_only is applied before the request's other options (spec §5.8): with work in a
  // manual-commit transaction, {"autocommit": true, "read_only": true} finds the transaction
  // and refuses read_only. Applied after autocommit, which commits the work, it would be
  // accepted, and the answer would depend on the order of a JSON object's members.
  for (const char* both : {R"({"autocommit": true, "read_only": true})", R"({"read_only": true, "autocommit": true})"}) {
    EXPECT_EQ(Patch(session, R"({"autocommit": false})").Json().at("errors").as_object().size(), 0u);
    session.Run("INSERT INTO t VALUES (5, 'd')");
    const Answer answer = Patch(session, both);
    EXPECT_EQ(OptionError(answer, "read_only"), "invalid_option_value") << answer.body;
    EXPECT_EQ(answer.Json().at("errors").as_object().size(), 1u) << answer.body;
    EXPECT_EQ(answer.Json().at("options").as_object().at("read_only"), json::value(false)) << answer.body;
    EXPECT_EQ(answer.Json().at("options").as_object().at("autocommit"), json::value(true)) << answer.body;  // the other option was applied
    EXPECT_EQ(Value("SELECT count(*) FROM t WHERE id = 5"), "1");                                            // ... and committed the work
    session.Run("DELETE FROM t WHERE id = 5");                                                               // still a session that writes
  }
  // One in which nothing ran is replaced: manual commit and read-only in either order.
  EXPECT_EQ(Patch(session, R"({"autocommit": false})").Json().at("errors").as_object().size(), 0u);
  const Answer accepted = Patch(session, R"({"read_only": true})");
  EXPECT_EQ(accepted.Json().at("errors").as_object().size(), 0u) << accepted.body;
  EXPECT_EQ(Transaction(session), "open");
  Remember(Own::kNoSessions);
  Refused("INSERT INTO t VALUES (6, 'e')", &session);
  EXPECT_EQ(Rows(), "4");
}

// The options request is decided on the session's engine thread: after the request that was
// running when it came, which ends as it began, and before any that comes later.
TEST_F(DuckDBReadOnly, AnOptionsRequestWaitsForTheRunningRequest) {
  writer_.Run("CREATE TABLE later (i INTEGER)");
  Wire session = Session();
  // One request: a statement that takes a while, then a write.
  const Answer submitted = session.Post(session.session + "/statements",
                                        R"j({"sql": "SELECT sleep_ms(1500); INSERT INTO later VALUES (1)"})j", {{"Prefer", "respond-async"}});
  ASSERT_EQ(submitted.status, 201) << submitted.body;
  const std::string statement = std::string(submitted.Json().at("statement_id").as_string());
  ASSERT_FALSE(update_outcome::Terminal(Wire::StateOf(session.Status(statement))))
      << "the request had ended before the options request was sent: nothing would be tested";
  const Answer switched = Patch(session, R"({"read_only": true})");
  const json::object after = session.Status(statement);  // read at once, without waiting
  EXPECT_EQ(switched.Json().at("errors").as_object().size(), 0u) << switched.body;
  EXPECT_EQ(switched.Json().at("options").as_object().at("read_only"), json::value(true)) << switched.body;
  EXPECT_EQ(Wire::StateOf(after), "succeeded") << "the options request was answered before the running request had ended, or that "
                                                  "request did not end as it began: " << json::serialize(after);
  EXPECT_EQ(Value("SELECT count(*) FROM later"), "1") << "the running request's write is missing";
  Remember(session);
  Refused("INSERT INTO later VALUES (2)", &session);
  EXPECT_EQ(Value("SELECT count(*) FROM later"), "1");
}

// A partitioned ingest opened while the session wrote, then the session is made read-only:
// the SDK refuses new ingests, and the adapter the one that was on its way.
TEST_F(DuckDBReadOnly, AnIngestOpenedBeforeTheSwitchWritesNothing) {
  const std::string body = OneColumnOfIntegers(1000);
  auto run = [&](bool make_read_only, const std::string& table, Answer& part, Answer& commit) {
    Wire session = Session();
    const Answer opened = session.Post(session.session + "/ingests", json::serialize(json::object{{"table", table}}));
    ASSERT_EQ(opened.status, 201) << opened.body;
    const std::string ingest = session.session + "/ingests/" + std::string(opened.Json().at("ingest_id").as_string());
    if (make_read_only) {
      const Answer switched = Patch(session, R"({"read_only": true})");
      ASSERT_EQ(switched.Json().at("errors").as_object().size(), 0u) << switched.body;
    }
    part = session.Post(ingest + "/parts/0", body, {{"Content-Type", update_outcome::kArrow}});
    commit = session.Post(ingest + "/commit", R"({"part_count": 1})");
    session.connection->Exchange(http::verb::delete_, ingest, session.With({}));
  };
  Answer part, commit;
  run(true, "ingested", part, commit);
  ExpectReadOnly(part, "the part");
  ExpectReadOnly(commit, "the commit");
  ExpectServerUnchanged("an ingest of a session that became read-only");
  // The twin: the same requests without the switch write the table.
  run(false, "ingested", part, commit);
  EXPECT_EQ(part.status, 200) << part.body;
  EXPECT_EQ(commit.status, 200) << commit.body;
  EXPECT_EQ(Value("SELECT count(*) FROM ingested"), "1000");
}

// ---- Prepared statements (spec §7): planned, checked and run anew at each execution ----

TEST_F(DuckDBReadOnly, AStatementPreparedBeforeTheSwitchIsPlannedAnewAfterIt) {
  writer_.Run("CREATE VIEW w AS SELECT count(*) AS c FROM enable_logging()");
  Wire session = Session();
  const std::string insert = Prepared(session, "INSERT INTO t VALUES (?, 'p')");
  const std::string function = Prepared(session, "SELECT count(*) AS c FROM enable_logging()");
  const std::string with_parameter = Prepared(session, "SELECT count(*) AS c FROM enable_logging() WHERE ? = 1");
  const std::string through_a_view = Prepared(session, "SELECT CASE WHEN c = 5 THEN c ELSE error('not the new view') END FROM w");
  EXPECT_EQ(Execute(session, insert, "[4]").status, 201);  // the twin
  EXPECT_FALSE(LoggingIsOn());                             // preparing ran nothing
  EXPECT_EQ(Patch(session, R"({"read_only": true})").Json().at("errors").as_object().size(), 0u);
  Remember(session);
  for (int again = 0; again < 3; again++) {
    ExpectReadOnly(Execute(session, insert, "[5]"), "the prepared INSERT");
    ExpectReadOnly(Execute(session, function), "the prepared function");
    ExpectReadOnly(Execute(session, with_parameter, "[1]"), "the prepared function with a parameter");
    ExpectReadOnly(Execute(session, through_a_view), "the prepared view over the function");
    ExpectUnchanged("executions of statements prepared before the switch");
  }
  EXPECT_EQ(Rows(), "4");
  // Another session replaces the view by a harmless one: the next execution runs that, not
  // what DuckDB bound when the statement was prepared.
  writer_.Run("CREATE OR REPLACE VIEW w AS SELECT 5::BIGINT AS c");
  Remember(session);
  const Answer ran = Execute(session, through_a_view);
  EXPECT_EQ(ran.status, 201) << ran.body;
  EXPECT_EQ(Wire::StateOf(ran.Json()), "succeeded") << ran.body;
  ExpectUnchanged("the prepared view, replaced");
  // Preparing a write in a read-only session is refused there; a prepared read runs.
  ExpectReadOnly(Prepare(reader_, "INSERT INTO t VALUES (?, 'p')"), "preparing a write");
  const Answer read = Execute(reader_, Prepared(reader_, "SELECT count(*) FROM t WHERE id >= ?"), "[2]");
  EXPECT_EQ(read.status, 201) << read.body;
  EXPECT_EQ(Wire::StateOf(read.Json()), "succeeded") << read.body;
}

// An earlier server checked a prepared statement's text as its names resolved now, and ran
// the plan DuckDB had bound when it was prepared: prepared where a name was harmless and
// executed where it was not, those were two plans.
TEST_F(DuckDBReadOnly, APreparedStatementRunsUnderTheSessionAsItIsAtEachExecution) {
  for (const char* sql : {"CREATE SCHEMA zero", "CREATE SCHEMA seven", "CREATE SCHEMA harmful",
                          "CREATE VIEW zero.v AS SELECT 0::BIGINT AS c", "CREATE VIEW seven.v AS SELECT 7::BIGINT AS c",
                          "CREATE VIEW harmful.v AS SELECT count(*) AS c FROM enable_logging()",
                          "CREATE MACRO zero.m() AS TABLE SELECT 0::BIGINT AS c", "CREATE MACRO seven.m() AS TABLE SELECT 7::BIGINT AS c",
                          "CREATE MACRO harmful.m() AS TABLE SELECT count(*) AS c FROM enable_logging()"}) {
    writer_.Run(sql);
  }
  using Move = std::function<void(const Wire&, const std::string&)>;
  const std::vector<std::pair<std::string, Move>> moves = {
      {"USE", [&](const Wire& w, const std::string& s) { EXPECT_EQ(Submit(w, "USE " + s).status, 201); }},
      {"SET search_path", [&](const Wire& w, const std::string& s) { EXPECT_EQ(Submit(w, "SET search_path = '" + s + "'").status, 201); }},
      {"SET schema", [&](const Wire& w, const std::string& s) { EXPECT_EQ(Submit(w, "SET schema = '" + s + "'").status, 201); }},
      {"the schema option", [&](const Wire& w, const std::string& s) {
         EXPECT_EQ(Patch(w, R"({"schema": ")" + s + "\"}").Json().at("errors").as_object().size(), 0u);
       }}};
  for (const auto& [how, move] : moves) {
    for (const std::string object : {"v", "m()"}) {
      const std::string what = how + ", " + object;
      Wire session = Session(R"({"read_only": true})");
      move(session, "zero");
      // The statement tells which object it read: it fails unless c is what the parameter says.
      const std::string prepared = Prepared(session, "SELECT CASE WHEN c = ? THEN c ELSE error('another schema''s object ran') END FROM " + object);
      auto runs_as = [&](const char* expected) {
        const Answer answer = Execute(session, prepared, std::string("[") + expected + "]");
        EXPECT_EQ(answer.status, 201) << what << " expecting " << expected << ": " << answer.body;
        EXPECT_EQ(Wire::StateOf(answer.Json()), "succeeded") << what << " expecting " << expected << ": " << answer.body;
      };
      runs_as("0");
      move(session, "seven");
      runs_as("7");  // the name as it resolves now
      move(session, "harmful");
      Remember(session);
      for (int again = 0; again < 3; again++) {
        ExpectReadOnly(Execute(session, prepared, "[0]"), what);
        ExpectUnchanged(what + ", executed where the name is the function");
      }
      // Prepared where the name is the function: preparing runs nothing, executing is refused
      // there and works once the session has moved.
      const std::string there = Prepared(session, "SELECT c FROM " + object);
      ExpectUnchanged(what + ", prepared where the name is the function");
      ExpectReadOnly(Execute(session, there), what);
      ExpectUnchanged(what + ", prepared and executed where the name is the function");
      move(session, "zero");
      runs_as("0");
      EXPECT_EQ(Execute(session, there).status, 201) << what;
      EXPECT_FALSE(LoggingIsOn()) << what;
    }
  }
}

// A driver that prepares every statement prepares its BEGIN, its SET and its USE. An earlier
// server answered 403 to a prepared BEGIN and had opened the transaction.
TEST_F(DuckDBReadOnly, PreparedTransactionStatementsAndSettingsDoWhatTheirAnswerSays) {
  writer_.Run("CREATE SCHEMA other_schema");
  Remember(Own::kNoSessions);
  auto step = [&](const std::string& sql, int status, const char* transaction) {
    const Answer prepared = Prepare(reader_, sql);
    const Answer answer = prepared.status == 201
                              ? Execute(reader_, reader_.session + "/prepared/" + std::string(prepared.Json().at("prepared_id").as_string()) + "/execute")
                              : prepared;
    EXPECT_EQ(answer.status, status) << sql << ": " << answer.body;
    if (status == 201) EXPECT_EQ(Wire::StateOf(answer.Json()), "succeeded") << sql << ": " << answer.body;
    if (status == 403) EXPECT_EQ(CodeOf(answer), "read_only") << sql << ": " << answer.body;
    EXPECT_EQ(Transaction(reader_), transaction) << "after the prepared " << sql;
    ExpectServerUnchanged("the prepared " + sql);
  };
  step("BEGIN", 201, "open");
  step("SET TimeZone = 'Pacific/Chatham'", 201, "open");
  step("USE memory.other_schema", 201, "open");
  step("SET VARIABLE answer = 42", 201, "open");
  step("SELECT CASE WHEN current_setting('TimeZone') = 'Pacific/Chatham' AND current_schema() = 'other_schema' AND getvariable('answer') = 42 "
       "THEN 1 ELSE error('a prepared SET or USE took no effect') END",
       201, "open");
  EXPECT_EQ(reader_.Session().at("options").as_object().at("schema").as_string(), "other_schema");
  step("COMMIT", 201, "none");
  step("RESET TimeZone", 201, "none");
  step("SELECT CASE WHEN current_setting('TimeZone') <> 'Pacific/Chatham' THEN 1 ELSE error('a prepared RESET took no effect') END", 201, "none");
  step("BEGIN TRANSACTION READ ONLY", 201, "open");
  step("ROLLBACK", 201, "none");
  step("START TRANSACTION", 201, "open");
  step("SELECT 1", 201, "open");
  step("ROLLBACK", 201, "none");
  // What fails says so, and has done nothing.
  step("COMMIT", 422, "none");
  step("ROLLBACK", 422, "none");
  step("BEGIN TRANSACTION READ WRITE", 403, "none");
  step("SET memory_limit = '1GB'", 403, "none");
  step("SET GLOBAL default_order = 'desc'", 403, "none");
  step("PRAGMA enable_verification", 403, "none");
  step("INSERT INTO no_such_table VALUES (1)", 403, "none");
  step("BEGIN", 201, "open");
  step("BEGIN", 422, "open");  // as the plain statement: there is one already
  step("COMMIT", 201, "none");
}

// ---- Settings and pragmas ----

TEST_F(DuckDBReadOnly, SettingsThatReachBeyondTheSessionAreRefused) {
  for (const char* sql : {"SET GLOBAL default_order = 'desc'", "SET GLOBAL TimeZone = 'Pacific/Chatham'", "RESET GLOBAL default_order",
                          "SET memory_limit = '1GB'", "SET threads = 1", "PRAGMA threads = 1", "SET temp_directory = '/tmp/x'",
                          "SET log_query_path = '/tmp/chargewire_read_only_test.log'", "SET SESSION log_query_path = '/tmp/x.log'",
                          "SET enable_profiling = 'json'", "PRAGMA enable_profiling", "PRAGMA enable_profile", "SET profiling_output = '/tmp/x.json'",
                          "SET enable_logging = true", "SET explain_output = 'all'", "RESET explain_output", "SET enable_optimizer = false",
                          "PRAGMA disable_optimizer", "SET extension_directory = '/tmp/x'", "SET home_directory = '/tmp/x'",
                          "SET secret_directory = '/tmp/x'", "SET lock_configuration = true", "SET enable_external_access = false",
                          "SET allow_persistent_secrets = false", "SET autoinstall_known_extensions = false",
                          // the limits that keep one statement from exhausting the stack or the memory of the process
                          "SET max_expression_depth = 20000", "RESET max_expression_depth", "SET SESSION max_expression_depth = 20000",
                          "SET perfect_ht_threshold = 20", "SET nested_loop_join_threshold = 1", "SET merge_join_threshold = 1",
                          "SET ordered_aggregate_threshold = 1", "SET preserve_insertion_order = false", "SET pivot_limit = 5",
                          // what prints to the server's own output
                          "SET enable_progress_bar_print = true", "PRAGMA enable_print_progress_bar",
                          "SELECT 1; SET GLOBAL threads = 1"}) {
    Refused(sql);
  }
  // With max_expression_depth raised, a 10 KB SELECT ended the process for every session. The
  // setting was refused above, so the statement is still an error that DuckDB reports. (It is
  // sent only when the limit is seen to stand: this process is the server.)
  const Answer limit = Submit(reader_, "SELECT CASE WHEN current_setting('max_expression_depth') = 1000 THEN 1 ELSE error('raised') END");
  EXPECT_EQ(limit.status, 201) << "max_expression_depth was changed in a read-only session: " << limit.body;
  if (limit.status == 201) {
    std::string deep = "SELECT ";
    for (int i = 0; i < 2000; i++) deep += "abs(";
    deep += "1";
    for (int i = 0; i < 2000; i++) deep += ")";
    const Answer too_deep = Submit(reader_, deep);
    EXPECT_EQ(too_deep.status, 400) << too_deep.body.substr(0, 300);
    EXPECT_EQ(CodeOf(too_deep), "sql_error");
  }
  // DuckDB has no subqueries in SET, in any session: nothing of one runs.
  for (const char* sql : {"SET TimeZone = (SELECT 'UTC')", "SET default_null_order = (SELECT 'nulls_first' FROM enable_logging())"}) {
    EXPECT_EQ(Submit(reader_, sql).status, 400) << sql;
    EXPECT_EQ(Submit(writer_, sql).status, 400) << sql;
    ExpectUnchanged(sql);
  }
  Reads("SELECT 1");
}

// Every setting on the adapter's list, with a value that differs from its default. A setting
// added to the list needs a value here: this test is where someone decides that it only
// changes how the session's own statements run and how their results look.
TEST_F(DuckDBReadOnly, EveryListedSettingIsSetForTheSessionOnlyAndCanBeReset) {
  const std::map<std::string, std::string> values = {
      {"timezone", "'Pacific/Chatham'"}, {"calendar", "'japanese'"}, {"default_collation", "'nocase'"}, {"default_null_order", "'nulls_first'"},
      {"null_order", "'nulls_first'"}, {"default_order", "'desc'"}, {"integer_division", "true"}, {"ieee_floating_point_ops", "false"},
      {"error_on_division_by_zero", "false"}, {"order_by_non_integer_literal", "true"}, {"scalar_subquery_error_on_multiple_rows", "false"},
      {"preserve_identifier_case", "false"}, {"disable_timestamptz_casts", "true"}, {"errors_as_json", "true"},
      {"search_path", "'other_schema'"}, {"schema", "'other_schema'"}, {"arrow_lossless_conversion", "true"},
      {"arrow_large_buffer_size", "true"}, {"arrow_output_list_view", "true"}, {"arrow_output_version", "'1.4'"},
      {"binary_as_string", "true"}, {"enable_progress_bar", "true"}, {"progress_bar_time", "1000"}};
  const ReadOnlyPolicy policy = ReadOnlyPolicy::Default();
  std::set<std::string> listed(policy.settings.begin(), policy.settings.end()), valued;
  for (const auto& [name, value] : values) valued.insert(name);
  EXPECT_EQ(listed, valued) << "the list of a reader's settings and this test's values for them differ";
  writer_.Run("CREATE SCHEMA other_schema");
  Remember(Own::kNoSessions);
  for (const std::string& name : listed) {
    if (!values.contains(name)) continue;
    const std::string current = "current_setting('" + name + "')::VARCHAR";
    Reads("SET VARIABLE default_value = " + current);
    Reads("SET " + name + " = " + values.at(name));
    Reads("SELECT CASE WHEN " + current + " <> getvariable('default_value') THEN 1 ELSE error('the SET took no effect') END");
    // DuckDB's plain SET is for every session for some of these; here it is the session's.
    ExpectServerUnchanged("SET " + name);
    Reads("RESET " + name);
    Reads("SELECT CASE WHEN " + current + " = getvariable('default_value') THEN 1 ELSE error('the RESET took no effect') END");
    ExpectServerUnchanged("RESET " + name);
  }
}

TEST_F(DuckDBReadOnly, EverySpellingOfAListedSettingWorks) {
  writer_.Run("CREATE SCHEMA other_schema");
  Remember(Own::kNoSessions);
  // (statement, the setting, its value afterwards)
  const std::vector<std::vector<std::string>> spellings = {
      {"SET TIME ZONE 'Pacific/Chatham'", "TimeZone", "Pacific/Chatham"},        // the PostgreSQL spelling
      {"SET \"TimeZone\" = 'Pacific/Chatham'", "TimeZone", "Pacific/Chatham"},   // a quoted name
      {"SET timezone TO 'Pacific/Chatham'", "TimeZone", "Pacific/Chatham"},
      {"SET SESSION TimeZone = 'Pacific/Chatham'", "TimeZone", "Pacific/Chatham"},
      {"PRAGMA TimeZone = 'Pacific/Chatham'", "TimeZone", "Pacific/Chatham"},
      {"SET schema 'other_schema'", "schema", "other_schema"},
      {"SET default_null_order = (nulls_first)", "default_null_order", "NULLS_FIRST"},
      {"SET default_null_order TO 'nulls_first'", "default_null_order", "NULLS_FIRST"},
      {"PRAGMA default_collation = 'nocase'", "default_collation", "nocase"},
      {"SET default_order = 'desc'", "default_order", "DESC"},
      {"SET search_path = lower('OTHER_SCHEMA')", "search_path", "other_schema"}};  // a value that is an expression
  for (const auto& spelling : spellings) {
    const std::string &sql = spelling[0], &setting = spelling[1], &value = spelling[2];
    const std::string is = "SELECT CASE WHEN current_setting('" + setting + "')::VARCHAR = '" + value + "' THEN 1 ELSE error('not set') END";
    const std::string is_not = "SELECT CASE WHEN current_setting('" + setting + "')::VARCHAR <> '" + value + "' THEN 1 ELSE error('not reset') END";
    for (const std::string& reset : {"RESET " + setting, "RESET SESSION " + setting, "SET " + setting + " TO DEFAULT"}) {
      Reads(sql);
      Reads(is);
      ExpectServerUnchanged(sql);
      Reads(reset);
      Reads(is_not);
      ExpectServerUnchanged(reset);
    }
  }
  Reads("SET VARIABLE answer = 42");
  Reads("SET VARIABLE most = (SELECT max(id) FROM t)");
  Reads("SELECT CASE WHEN getvariable('answer') = 42 AND getvariable('most') = 3 THEN 1 ELSE error('variables') END");
  Reads("RESET VARIABLE answer");
  Reads("USE memory.other_schema");
  EXPECT_EQ(reader_.Session().at("options").as_object().at("schema").as_string(), "other_schema");
  // The statements of a request before a refused one have run: for the session, no one else.
  ExpectReadOnly(Submit(reader_, "SET \"TimeZone\" TO 'Pacific/Chatham'; SET GLOBAL threads = 1"), "a listed setting, then SET GLOBAL");
  Reads("SELECT CASE WHEN current_setting('TimeZone') = 'Pacific/Chatham' THEN 1 ELSE error('the first statement') END");
  ExpectServerUnchanged("a listed setting, then SET GLOBAL");
}

TEST_F(DuckDBReadOnly, PragmasThatAreNotOnTheListAreRefused) {
  for (const char* sql : {"PRAGMA enable_verification", "PRAGMA disable_verification", "PRAGMA enable_object_cache", "PRAGMA force_checkpoint",
                          "PRAGMA verify_parallelism", "PRAGMA enable_checkpoint_on_shutdown", "PRAGMA disable_checkpoint_on_shutdown",
                          "PRAGMA add_parquet_key('a_key', '0123456789112345')", "PRAGMA disable_profiling"}) {
    Refused(sql);
    Wire twin = Session();  // each in a session of its own: several are settings of the connection
    EXPECT_EQ(Wire::StateOf(Submit(twin, sql).Json()), "succeeded") << sql;
    Remember();
  }
  for (const char* sql : {"PRAGMA table_info('t')", "PRAGMA show_tables", "PRAGMA show_tables_expanded", "PRAGMA database_list",
                          "PRAGMA database_size", "PRAGMA storage_info('t')", "PRAGMA metadata_info", "PRAGMA version", "PRAGMA platform",
                          "PRAGMA collations", "PRAGMA user_agent", "PRAGMA extension_versions", "PRAGMA show_databases", "PRAGMA functions"}) {
    Reads(sql);
  }
}

// ---- Every transaction of a read-only session is READ ONLY ----

// The first of the three things that hold a read-only session: DuckDB itself refuses a write
// of the main catalog in a READ ONLY transaction, whatever else let it through. Seen in
// DuckDB's own log of the statements it is given: on the session's connection every
// transaction begins READ ONLY, whichever way it came to be started, and no transaction
// statement of the client's is passed on as it was written. And every one is ended by
// ROLLBACK, also where the client said COMMIT: a reader has nothing to commit, and DuckDB
// treats the commit of a READ ONLY transaction in which something was changed after all as
// fatal for the whole database.
TEST_F(DuckDBReadOnly, EveryTransactionIsStartedReadOnly) {
  writer_.Run("CALL enable_logging('QueryLog')");
  Reads("SELECT 'the-reader-marker'");  // autocommit
  Reads("BEGIN");                       // the client's own
  Reads("SELECT count(*) FROM t");
  Reads("COMMIT");
  Reads("START TRANSACTION");
  Reads("ROLLBACK");
  Reads("BEGIN TRANSACTION READ ONLY");
  Reads("END");
  Reads("BEGIN; SELECT count(*) FROM t; COMMIT; SELECT 1");  // in one request
  ExpectReadOnly(Submit(reader_, "INSERT INTO t VALUES (4, 'c')"), "a write");  // after a refusal
  EXPECT_EQ(Submit(reader_, "SELECT * FROM no_such_table").status, 400);        // after an error
  Reads("SELECT 1");
  EXPECT_EQ(Patch(reader_, R"({"autocommit": false})").Json().at("errors").as_object().size(), 0u);  // manual commit
  for (const char* call : {"/commit", "/rollback"}) {
    Reads("SELECT 1");
    EXPECT_EQ(reader_.Post(reader_.session + call, "").status, 204);
  }
  Reads("SELECT 1");
  EXPECT_EQ(Patch(reader_, R"({"autocommit": true})").Json().at("errors").as_object().size(), 0u);
  EXPECT_EQ(Execute(reader_, Prepared(reader_, "BEGIN")).status, 201);  // prepared
  Reads("ROLLBACK");
  EXPECT_EQ(reader_.Get(reader_.session + "/metadata/tables").status, 200);

  const std::string of_the_reader =
      "FROM duckdb_logs() l JOIN duckdb_log_contexts() c ON c.context_id = l.context_id WHERE l.type = 'QueryLog' AND c.connection_id IN ("
      "SELECT c2.connection_id FROM duckdb_logs() l2 JOIN duckdb_log_contexts() c2 ON c2.context_id = l2.context_id "
      "WHERE l2.type = 'QueryLog' AND l2.message = 'SELECT ''the-reader-marker''')";
  EXPECT_EQ(Value("SELECT count(*) " + of_the_reader + " AND l.message = 'SELECT ''the-reader-marker'''"), "1")
      << "the session's statements are not in DuckDB's query log";
  EXPECT_EQ(Value("SELECT (count(*) >= 12)::INT " + of_the_reader + " AND l.message = 'BEGIN TRANSACTION READ ONLY'"), "1");
  EXPECT_EQ(Value("SELECT count(*) " + of_the_reader + " AND regexp_matches(upper(l.message), '^(BEGIN|START|COMMIT|END|ROLLBACK|ABORT)') "
                  "AND l.message NOT IN ('BEGIN TRANSACTION READ ONLY', 'ROLLBACK')"),
            "0")
      << "a transaction of the read-only session was not started READ ONLY, or was committed, or a transaction statement was passed on to DuckDB";
  EXPECT_EQ(Value("SELECT (count(*) >= 12)::INT " + of_the_reader + " AND l.message = 'ROLLBACK'"), "1");
  // No statement ran outside a transaction the adapter began: the first one logged is its BEGIN.
  EXPECT_EQ(Value("SELECT (arg_min(l.message, l.context_id) = 'BEGIN TRANSACTION READ ONLY')::INT " + of_the_reader), "1");
}

// ---- What a reader keeps ----

TEST_F(DuckDBReadOnly, WhatAReaderUsesStillRuns) {
  writer_.Run("COPY t TO '" + File("data.csv") + "'");
  writer_.Run("COPY t TO '" + File("data.parquet") + "'");
  writer_.Run("COPY t TO '" + File("data.json") + "'");
  writer_.Run("CREATE MACRO tm(n) AS TABLE SELECT * FROM range(n)");
  writer_.Run("CREATE VIEW two_up AS SELECT id FROM t WHERE id > 1");
  writer_.Run("CREATE MACRO plus_one(a) AS a + 1");
  Remember();
  for (const std::string& sql : std::vector<std::string>{
           "SELECT * FROM t", "SELECT v, count(*), sum(id) FROM t GROUP BY v ORDER BY v", "SELECT DISTINCT v FROM t",
           // a view, a scalar macro and a table macro that only read
           "SELECT * FROM two_up", "SHOW two_up", "SELECT plus_one(id) FROM two_up", "SELECT * FROM tm(3)", "SELECT * FROM tm(1 + 2)",
           "SELECT * FROM tm(getvariable('no_such_variable'))", "SELECT * FROM pragma_table_info('two' || '_up')",
           "SELECT * FROM t a JOIN t b USING (id)", "SELECT * FROM t a LEFT JOIN t b ON a.id < b.id", "SELECT * FROM t a, t b",
           "SELECT * FROM t a FULL JOIN t b ON a.id = b.id AND a.v < b.v", "SELECT * FROM t a SEMI JOIN t b ON a.id = b.id",
           "SELECT * FROM t a ASOF JOIN t b ON a.id >= b.id", "SELECT * FROM t a POSITIONAL JOIN t b",
           "SELECT count(*) FROM range(60000) a(i) JOIN range(60000) b(j) ON a.i < b.j AND a.i > b.j - 3",
           "SELECT id, row_number() OVER (ORDER BY id) FROM t", "SELECT * FROM t QUALIFY row_number() OVER (PARTITION BY v ORDER BY id) = 1",
           "SELECT * FROM t UNION SELECT * FROM t", "SELECT * FROM t EXCEPT SELECT * FROM t", "SELECT * FROM t INTERSECT SELECT * FROM t",
           "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT sum(n) FROM r",
           "WITH c AS MATERIALIZED (SELECT * FROM t) SELECT * FROM c, c c2", "SELECT * FROM t WHERE id IN (SELECT id FROM t)",
           "SELECT (SELECT max(id) FROM t b WHERE b.id <= t.id) FROM t", "SELECT * FROM t ORDER BY id LIMIT 1 OFFSET 1",
           "SELECT * FROM t USING SAMPLE 50%", "SELECT * FROM t TABLESAMPLE reservoir(2)", "SELECT v, id, count(*) FROM t GROUP BY ROLLUP (v, id)",
           "VALUES (1), (2)", "SELECT * FROM range(10)", "SELECT unnest([1, 2, 3])", "SELECT * FROM json_each('[1, 2]')",
           // a table function called for each row of its input: held to the list as a scanned one is
           "SELECT * FROM (SELECT '[1, 2]' AS x) s, LATERAL json_each(s.x)", "SELECT * FROM (SELECT '{\"a\": [1]}' AS x) s, LATERAL json_tree(s.x)",
           "SELECT * FROM t, LATERAL range(t.id)", "SELECT * FROM t, range(t.id)", "SELECT * FROM t, LATERAL generate_series(1, t.id)",
           "SELECT * FROM (SELECT [1, 2] AS l) s, LATERAL unnest(s.l)", "SELECT * FROM t, LATERAL summary((SELECT t.id))",
           "PIVOT t ON v IN ('a', 'b') USING count(*)", "DESCRIBE t", "DESCRIBE SELECT * FROM t", "SHOW TABLES",
           "SHOW ALL TABLES", "SUMMARIZE t", "EXPLAIN SELECT * FROM t", "EXPLAIN ANALYZE SELECT count(*) FROM t",
           "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM t",
           // a parenthesized query: an earlier server took the parenthesis for EXPLAIN's option list
           "EXPLAIN (SELECT 1)", "EXPLAIN ANALYZE (SELECT 1) UNION ALL (SELECT 2)",
           "SELECT * FROM information_schema.tables", "SELECT * FROM pg_catalog.pg_tables", "SELECT * FROM sqlite_master",
           "SELECT * FROM duckdb_tables()", "SELECT * FROM duckdb_columns()", "SELECT * FROM duckdb_settings()",
           "SELECT * FROM pragma_table_info('t')",
           // arguments that are plain values: constants, arithmetic, casts, lists and structs, columns, the session's variable
           "SELECT * FROM range(1, 10, 2)", "SELECT * FROM range(-1 + 3)", "SELECT * FROM range(CAST('3' AS INTEGER))", "SELECT * FROM range('3'::INTEGER)",
           "SELECT * FROM range(getvariable('no_such_variable'))", "SELECT * FROM range(now()::DATE - today())", "SELECT * FROM unnest([1, 2] || [3])",
           "SELECT * FROM generate_series(TIMESTAMP '2020-01-01', TIMESTAMP '2020-01-03', INTERVAL 1 DAY)",
           "SELECT * FROM generate_series(DATE '2020-01-01', DATE '2020-01-03', INTERVAL '1 day')", "SELECT * FROM range(abs(-3))",
           "SELECT * FROM range((SELECT max(id) FROM t))", "SELECT * FROM generate_series(1, 3) g(i), LATERAL range(i + 1)",
           "SELECT * FROM repeat_row(1, 2, num_rows = 3)", "SELECT * FROM summary((SELECT 1))",
           "SELECT * FROM read_parquet(['" + File("data.parquet") + "', '" + File("data.parquet") + "'], filename = true)",
           "SELECT * FROM read_csv('" + File("data.csv") + "', columns = {'id': 'INTEGER', 'v': 'VARCHAR'})",
           "SELECT * FROM read_csv('" + File("data") + "' || '.csv')",
           // readers an earlier list left out
           "SELECT count(*) FROM duckdb_logs()", "SELECT count(*) FROM duckdb_log_contexts()", "SELECT count(*) FROM duckdb_secrets()",
           "SELECT count(*) FROM duckdb_extension_repositories()", "SELECT count(*) FROM duckdb_connection_count()",
           "SELECT count(*) FROM duckdb_external_file_cache()", "SELECT count(*) FROM duckdb_available_metrics()",
           "SELECT count(*) FROM duckdb_grammar_extensions()", "SELECT count(*) FROM duckdb_table_sample('t')",
           "SELECT count(*) FROM which_secret('s3://bucket/key', 's3')", "SELECT count(*) FROM sql_tokenize('SELECT 1')",
           "SELECT count(*) FROM duckdb_approx_database_count()", "SELECT count(*) FROM duckdb_eviction_queues()",
           "SELECT count(*) FROM duckdb_external_resources()", "SELECT count(*) FROM duckdb_external_resource_types()",
           "SELECT count(*) FROM test_all_types()",
           // files
           "SELECT * FROM read_csv('" + File("data.csv") + "')", "SELECT * FROM '" + File("data.csv") + "'",
           "SELECT * FROM read_parquet('" + File("data.parquet") + "')", "SELECT * FROM parquet_metadata('" + File("data.parquet") + "')",
           "SELECT * FROM read_json('" + File("data.json") + "')", "SELECT * FROM glob('" + File("*") + "')",
           "SELECT * FROM read_text('" + File("data.csv") + "')", "SELECT * FROM read_single_csv_file('" + File("data.csv") + "')",
           "SELECT * FROM read_single_parquet_file('" + File("data.parquet") + "')", "SELECT * FROM read_single_json_file('" + File("data.json") + "')"}) {
    Reads(sql);
  }
  ExpectUnchanged("reading");  // reading changes nothing either
  Reads("SELECT * FROM t WHERE id = ? AND v = ?", json::array{2, "b"});
  Reads("SELECT * FROM t WHERE id = $id", json::object{{"id", 2}});
  Reads("EXPLAIN ANALYZE SELECT * FROM t WHERE id = ?", json::array{2});
  Reads("SELECT CASE WHEN count(*) = 4 THEN 1 ELSE error('not the parameter') END FROM range(?::INTEGER)", json::array{4});
  Reads("SELECT * FROM range(?)", json::array{4});
  Reads("SELECT CASE WHEN count(*) = 3 THEN 1 ELSE error('not the parameter') END FROM tm(?::INTEGER)", json::array{3});
  Reads("SELECT * FROM pragma_table_info(?)", json::array{"two_up"});
  const Answer with_parameter = Execute(reader_, Prepared(reader_, "SELECT count(*) FROM range(?::INTEGER)"), "[5]");
  EXPECT_EQ(with_parameter.status, 201) << with_parameter.body;
  // A read-only session in autocommit holds no snapshot between statements.
  writer_.Run("INSERT INTO t VALUES (4, 'c')");
  Reads("SELECT CASE WHEN (SELECT count(*) FROM t) = 4 THEN 1 ELSE error('an old snapshot') END");
  const Answer tables = reader_.Get(reader_.session + "/metadata/tables");
  EXPECT_EQ(tables.status, 200) << tables.body;
}

// Plans as deep as a BI tool's or a generated query's: an earlier server could not read a
// plan more than 16 operators deep and refused these with read_only. (Sizes that DuckDB
// itself runs in any session with room to spare on every platform's thread stacks.)
TEST_F(DuckDBReadOnly, DeepPlansRun) {
  std::string left_joins, inner_joins, window_ctes = "c0 AS (SELECT id, v FROM t)", plain_ctes = "c0 AS (SELECT * FROM t)";
  for (int i = 1; i <= 40; i++) left_joins += " LEFT JOIN t t" + std::to_string(i) + " ON t" + std::to_string(i) + ".id = t0.id + " + std::to_string(i);
  for (int i = 1; i <= 32; i++) inner_joins += " JOIN t t" + std::to_string(i) + " ON t" + std::to_string(i) + ".id = t0.id";
  for (int i = 1; i <= 30; i++) {
    const std::string n = std::to_string(i);
    window_ctes += ", c" + n + " AS (SELECT id, v, row_number() OVER (PARTITION BY v ORDER BY id) AS rn" + n + " FROM c" + std::to_string(i - 1) +
                   " WHERE id > 0 QUALIFY rn" + n + " < 100)";
  }
  for (int i = 1; i <= 60; i++) plain_ctes += ", c" + std::to_string(i) + " AS (SELECT * FROM c" + std::to_string(i - 1) + ")";
  std::string nested = "SELECT * FROM t ORDER BY id LIMIT 100";
  for (int i = 0; i < 60; i++) nested = "SELECT * FROM (" + nested + ") s" + std::to_string(i) + " WHERE id > 0";
  std::string unions = "SELECT id FROM t";
  for (int i = 1; i < 200; i++) unions += " UNION ALL SELECT id FROM t";
  // Each fails unless it also gives the right answer.
  auto counts = [](const std::string& query, int expected) {
    return "SELECT CASE WHEN n = " + std::to_string(expected) + " THEN n ELSE error('a wrong answer') END FROM (" + query + ") answer";
  };
  for (const std::string& sql : std::vector<std::string>{
           counts("SELECT count(*) AS n FROM t t0" + left_joins, 3),                                      // a fact table with 40 LEFT JOINs
           counts("SELECT count(*) AS n FROM t t0" + inner_joins, 3),                                     // 33 tables joined
           counts("WITH " + window_ctes + " SELECT count(*) AS n FROM c30", 3),                           // 30 chained CTEs, a window function in each
           counts("WITH " + plain_ctes + " SELECT count(*) AS n FROM c60", 3),                            // 60 chained CTEs
           counts("SELECT count(*) AS n FROM (" + nested + ") deepest", 3),                               // 60 nested subqueries
           counts("SELECT count(*) AS n FROM (" + unions + ") u", 600),                                   // 200 branches of a UNION ALL
           "EXPLAIN ANALYZE SELECT count(*) AS n FROM t t0" + left_joins}) {
    // Within seconds. (The screen of what DuckDB evaluates while it binds once walked every
    // derived table twice: 2^60 steps for the sixty nested ones, on the session's engine thread,
    // where nothing can cancel it. A statement that is still running after ten seconds would
    // keep this process from ever ending, so the process ends here, as a failure.)
    const auto started = std::chrono::steady_clock::now();
    const Answer answer = reader_.Post(reader_.session + "/statements", json::serialize(json::object{{"sql", sql}}), {{"Prefer", "wait=10"}});
    const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
    EXPECT_EQ(answer.status, 201) << sql.substr(0, 200) << ": " << answer.body.substr(0, 300);
    if (answer.status == 201 && !update_outcome::Terminal(Wire::StateOf(answer.Json()))) {
      ADD_FAILURE() << "no answer within 10 s (a deep plan must not take longer to check than to run): " << sql.substr(0, 200);
      std::cout << "[  FAILED  ] DuckDBReadOnly.DeepPlansRun: a statement of a read-only session does not end; this process stops here\n" << std::flush;
      std::quick_exit(1);
    }
    EXPECT_EQ(Wire::StateOf(answer.Json()), "succeeded") << sql.substr(0, 200) << ": " << answer.body.substr(0, 300);
    EXPECT_LT(took, 5000) << sql.substr(0, 200);
    const Answer prepared = Execute(reader_, Prepared(reader_, sql));  // and prepared, as a driver sends it
    EXPECT_EQ(prepared.status, 201) << prepared.body.substr(0, 300);
    EXPECT_EQ(Wire::StateOf(Submit(writer_, sql).Json()), "succeeded");  // the twin
  }
}

// Multi-join aggregate queries in the shapes of TPC-H and TPC-DS, over small tables.
TEST_F(DuckDBReadOnly, WarehouseShapedQueriesRun) {
  for (const char* sql : {
           "CREATE TABLE region AS SELECT * FROM (VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA')) v(r_regionkey, r_name)",
           "CREATE TABLE nation AS SELECT * FROM (VALUES (0, 'ALGERIA', 0), (1, 'BRAZIL', 1), (2, 'CANADA', 1), (3, 'CHINA', 2), (4, 'INDIA', 2)) "
           "v(n_nationkey, n_name, n_regionkey)",
           "CREATE TABLE supplier AS SELECT i AS s_suppkey, 'Supplier#' || i AS s_name, i % 5 AS s_nationkey FROM range(1, 11) r(i)",
           "CREATE TABLE customer AS SELECT i AS c_custkey, 'Customer#' || i AS c_name, i % 5 AS c_nationkey, "
           "['BUILDING', 'MACHINERY', 'AUTOMOBILE'][i % 3 + 1] AS c_mktsegment FROM range(1, 31) r(i)",
           "CREATE TABLE part AS SELECT i AS p_partkey, 'Brand#' || (i % 3 + 1) AS p_brand, ['BRASS', 'STEEL', 'TIN'][i % 3 + 1] AS p_type FROM range(1, 21) r(i)",
           "CREATE TABLE partsupp AS SELECT p AS ps_partkey, q AS ps_suppkey, ((p + q) * 1.5)::DECIMAL(15, 2) AS ps_supplycost "
           "FROM range(1, 21) a(p), range(1, 11) b(q) WHERE (p + q) % 3 = 0",
           "CREATE TABLE orders AS SELECT i AS o_orderkey, i % 30 + 1 AS o_custkey, ['O', 'F', 'P'][i % 3 + 1] AS o_orderstatus, "
           "DATE '1995-01-01' + (i * 7)::INTEGER AS o_orderdate, (i * 111.11)::DECIMAL(15, 2) AS o_totalprice FROM range(1, 101) r(i)",
           "CREATE TABLE lineitem AS SELECT o AS l_orderkey, (o * l) % 20 + 1 AS l_partkey, (o + l) % 10 + 1 AS l_suppkey, "
           "(l * 5)::DECIMAL(15, 2) AS l_quantity, (o * l * 10.0)::DECIMAL(15, 2) AS l_extendedprice, ((l % 10) / 100.0)::DECIMAL(15, 2) AS l_discount, "
           "['R', 'A', 'N'][(o + l) % 3 + 1] AS l_returnflag, DATE '1995-01-01' + (o * 7 + l)::INTEGER AS l_shipdate FROM range(1, 101) a(o), range(1, 5) b(l)"}) {
    writer_.Run(sql);
  }
  Remember();
  for (const char* sql : {
           // TPC-H 1: one table, grouped and ordered
           "SELECT l_returnflag, sum(l_quantity), sum(l_extendedprice * (1 - l_discount)), avg(l_discount), count(*) FROM lineitem "
           "WHERE l_shipdate <= DATE '1996-12-01' - INTERVAL 90 DAY GROUP BY l_returnflag ORDER BY l_returnflag",
           // TPC-H 2: five tables and a correlated minimum over four
           "SELECT s_name, n_name, p_partkey FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey "
           "AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name <> 'AFRICA' AND ps_supplycost = (SELECT min(ps_supplycost) "
           "FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey "
           "AND n_regionkey = r_regionkey AND r_name <> 'AFRICA') ORDER BY n_name, s_name, p_partkey LIMIT 100",
           // TPC-H 5: six tables
           "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region "
           "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey "
           "AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey GROUP BY n_name ORDER BY revenue DESC, n_name",
           // TPC-H 8: eight tables under a derived table
           "SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) FROM (SELECT extract(year FROM o_orderdate) AS o_year, "
           "l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region "
           "WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey "
           "AND n1.n_regionkey = r_regionkey AND s_nationkey = n2.n_nationkey) all_nations GROUP BY o_year ORDER BY o_year",
           // TPC-H 13: an outer join under two aggregations
           "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) AS c_count FROM customer LEFT OUTER JOIN orders "
           "ON c_custkey = o_custkey AND o_orderstatus <> 'P' GROUP BY c_custkey) c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC",
           // TPC-H 15: a CTE used twice
           "WITH revenue AS (SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total FROM lineitem GROUP BY l_suppkey) "
           "SELECT s_suppkey, s_name, total FROM supplier, revenue WHERE s_suppkey = supplier_no AND total = (SELECT max(total) FROM revenue)",
           // TPC-H 18: IN over a HAVING
           "SELECT c_name, o_orderkey, sum(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem "
           "GROUP BY l_orderkey HAVING sum(l_quantity) > 40) AND c_custkey = o_custkey AND o_orderkey = l_orderkey "
           "GROUP BY c_name, o_orderkey ORDER BY o_orderkey LIMIT 100",
           // TPC-H 21: EXISTS and NOT EXISTS over the same table
           "SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey "
           "AND o_orderstatus = 'F' AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) "
           "AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_quantity > 100) "
           "AND s_nationkey = n_nationkey GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100",
           // TPC-DS 1: a CTE correlated with itself
           "WITH customer_total AS (SELECT o_custkey AS ctr_customer, c_nationkey AS ctr_nation, sum(o_totalprice) AS ctr_total FROM orders, customer "
           "WHERE o_custkey = c_custkey GROUP BY o_custkey, c_nationkey) SELECT c_name FROM customer_total ctr1, customer, nation "
           "WHERE ctr1.ctr_total > (SELECT avg(ctr_total) * 1.2 FROM customer_total ctr2 WHERE ctr1.ctr_nation = ctr2.ctr_nation) "
           "AND n_nationkey = ctr1.ctr_nation AND ctr1.ctr_customer = c_custkey ORDER BY c_name LIMIT 100",
           // TPC-DS 5: channels united under a ROLLUP
           "SELECT channel, bucket, sum(sales) FROM (SELECT n_name AS channel, o_orderkey % 3 AS bucket, o_totalprice AS sales FROM orders, customer, nation "
           "WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey AND o_orderstatus = 'O' UNION ALL SELECT n_name, o_orderkey % 3, o_totalprice "
           "FROM orders, customer, nation WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey AND o_orderstatus = 'F') x "
           "GROUP BY ROLLUP (channel, bucket) ORDER BY channel NULLS LAST, bucket NULLS LAST",
           // TPC-DS 12 and 36: windows over aggregates, ranked within grouping sets
           "SELECT p_brand, p_type, sum(l_extendedprice) * 100 / sum(sum(l_extendedprice)) OVER (PARTITION BY p_brand), "
           "rank() OVER (PARTITION BY grouping(p_brand) + grouping(p_type) ORDER BY sum(l_extendedprice) DESC) "
           "FROM lineitem, part WHERE l_partkey = p_partkey GROUP BY ROLLUP (p_brand, p_type) ORDER BY p_brand NULLS LAST, p_type NULLS LAST",
           // TPC-DS 38, 87 and 97: INTERSECT, EXCEPT and a FULL OUTER JOIN of two aggregates
           "SELECT count(*) FROM ((SELECT DISTINCT c_name, o_orderdate FROM orders, customer WHERE o_custkey = c_custkey AND o_orderstatus = 'O' "
           "INTERSECT SELECT DISTINCT c_name, o_orderdate FROM orders, customer WHERE o_custkey = c_custkey AND o_totalprice > 100) "
           "EXCEPT SELECT DISTINCT c_name, o_orderdate FROM orders, customer WHERE o_custkey = c_custkey AND o_totalprice > 9000) hot",
           "WITH a AS (SELECT l_suppkey AS k, l_partkey AS item FROM lineitem GROUP BY l_suppkey, l_partkey), "
           "b AS (SELECT ps_suppkey AS k, ps_partkey AS item FROM partsupp GROUP BY ps_suppkey, ps_partkey) "
           "SELECT sum(CASE WHEN a.k IS NOT NULL AND b.k IS NULL THEN 1 ELSE 0 END), sum(CASE WHEN a.k IS NULL AND b.k IS NOT NULL THEN 1 ELSE 0 END) "
           "FROM a FULL OUTER JOIN b ON a.k = b.k AND a.item = b.item"}) {
    EXPECT_EQ(Wire::StateOf(Submit(writer_, sql).Json()), "succeeded") << sql;  // the twin: a statement that works
    Reads(sql);
    Reads(std::string("EXPLAIN ANALYZE ") + sql);
    EXPECT_EQ(Execute(reader_, Prepared(reader_, sql)).status, 201) << sql;
  }
  ExpectUnchanged("the warehouse queries");
}

// ---- A server that embeds the adapter decides the lists ----

TEST_F(DuckDBReadOnly, AnEmbeddingServerExtendsTheLists) {
  Refused("SELECT * FROM checkpoint()");
  server_->Stop();
  Start(std::make_shared<PolicyDuckDB>([](ReadOnlyPolicy& policy) { policy.table_functions.insert("CHECKPOINT"); }));
  Reads("SELECT * FROM checkpoint()");  // listed by this server's policy
  Reads("EXPLAIN ANALYZE SELECT * FROM checkpoint()");
  Refused("CHECKPOINT");                // the kind is still not one that runs
  Refused("SELECT * FROM enable_logging()");
}

// A table function called once for each row of its input (DuckDB's in-out functions, the
// plan's INOUT_FUNCTION operator) is held to the list by its own name, as a scanned one is:
// an earlier server listed the operator and never looked at the function. DuckDB ships none
// that changes anything, so the check is shown with a server that does not list two that read.
TEST_F(DuckDBReadOnly, ATableFunctionCalledForEachRowIsHeldToTheListToo) {
  const std::vector<std::string> in_out = {
      "SELECT * FROM (SELECT '[1, 2]' AS x) s, LATERAL json_each(s.x)", "SELECT * FROM (SELECT '[1, 2]' AS x) s, json_each(s.x)",
      "SELECT count(*) FROM (SELECT * FROM (SELECT '[1, 2]' AS x) s, LATERAL json_each(s.x))",
      "SELECT * FROM t, LATERAL generate_series(1, t.id)", "EXPLAIN ANALYZE SELECT * FROM t, LATERAL generate_series(1, t.id)"};
  const std::vector<std::string> scanned = {"SELECT * FROM json_each('[1, 2]')", "SELECT * FROM generate_series(1, 3)"};
  for (const std::string& sql : in_out) Reads(sql);
  for (const std::string& sql : scanned) Reads(sql);
  server_->Stop();
  Start(std::make_shared<PolicyDuckDB>([](ReadOnlyPolicy& policy) {
    policy.table_functions.erase("JSON_EACH");
    policy.table_functions.erase("GENERATE_SERIES");
  }));
  for (const std::string& sql : in_out) {
    const Answer refused = Refused(sql);
    const bool names_it = refused.body.find("JSON_EACH") != std::string::npos || refused.body.find("GENERATE_SERIES") != std::string::npos;
    EXPECT_TRUE(names_it) << sql << ": " << refused.body;
  }
  for (const std::string& sql : scanned) Refused(sql);
  // What this server still lists still runs, called for each row or scanned.
  Reads("SELECT * FROM t, LATERAL range(t.id)");
  Reads("SELECT * FROM (SELECT '{\"a\": [1]}' AS x) s, LATERAL json_tree(s.x)");
  Reads("SELECT * FROM range(3)");
}

// The lists themselves. A name added to one is a decision that something only reads: this
// test is where it is made a second time, by whoever adds it. (No statement can show that a
// writing operator's name is missing from the list: the statement's kind and the catalogs
// its plan modifies refuse it first.)
TEST(DuckDBReadOnlyPolicy, TheListsAreTheseAndNoOther) {
  const ReadOnlyPolicy policy = ReadOnlyPolicy::Default();
  using Names = std::set<std::string>;
  EXPECT_EQ(policy.statement_kinds, (std::set<DUCKDB_V2_STATEMENT_TYPE>{DUCKDB_V2_STATEMENT_TYPE_SELECT, DUCKDB_V2_STATEMENT_TYPE_EXPLAIN,
                                                                         DUCKDB_V2_STATEMENT_TYPE_TRANSACTION, DUCKDB_V2_STATEMENT_TYPE_SET,
                                                                         DUCKDB_V2_STATEMENT_TYPE_PRAGMA}));
  EXPECT_EQ(Names(policy.pragmas.begin(), policy.pragmas.end()),
            (Names{"table_info", "show", "show_tables", "show_tables_expanded", "show_databases", "database_list", "database_size", "storage_info",
                   "metadata_info", "version", "platform", "functions", "collations", "user_agent", "extension_versions"}));
  EXPECT_EQ(Names(policy.plan_operators.begin(), policy.plan_operators.end()),
            (Names{"SEQ_SCAN", "DUMMY_SCAN", "CHUNK_SCAN", "COLUMN_DATA_SCAN", "DELIM_SCAN", "EXPRESSION_SCAN", "POSITIONAL_SCAN", "EMPTY_RESULT",
                   "ORDER_BY", "LIMIT", "LIMIT_PERCENT", "STREAMING_LIMIT", "RESERVOIR_SAMPLE", "STREAMING_SAMPLE", "TOP_N", "WINDOW",
                   "STREAMING_WINDOW", "UNNEST", "UNGROUPED_AGGREGATE", "HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY", "PARTITIONED_AGGREGATE",
                   "LIMITED_DISTINCT", "FILTER", "PROJECTION", "LEFT_DELIM_JOIN", "RIGHT_DELIM_JOIN", "BLOCKWISE_NL_JOIN", "NESTED_LOOP_JOIN",
                   "HASH_JOIN", "PIECEWISE_MERGE_JOIN", "IE_JOIN", "ASOF_JOIN", "CROSS_PRODUCT", "POSITIONAL_JOIN", "UNION", "REC_CTE",
                   "REC_KEY_CTE", "CTE", "REC_CTE_SCAN", "REC_REC_CTE_SCAN", "CTE_SCAN", "RECURSIVE_KEY_JOIN", "PIVOT", "EXPLAIN",
                   "EXPLAIN_ANALYZE", "RESULT_COLLECTOR"}));
  EXPECT_EQ(Names(policy.table_functions.begin(), policy.table_functions.end()),
            (Names{"RANGE", "GENERATE_SERIES", "UNNEST", "REPEAT", "REPEAT_ROW", "READ_CSV", "READ_CSV_AUTO", "SNIFF_CSV", "READ_PARQUET",
                   "PARQUET_SCAN", "READ_JSON", "READ_JSON_AUTO", "READ_JSON_OBJECTS", "READ_JSON_OBJECTS_AUTO", "READ_NDJSON", "READ_NDJSON_AUTO",
                   "READ_NDJSON_OBJECTS", "READ_TEXT", "READ_BLOB", "GLOB", "READ_DUCKDB", "READ_AVRO", "READ_XLSX", "PARQUET_METADATA",
                   "PARQUET_FILE_METADATA", "PARQUET_FULL_METADATA", "PARQUET_KV_METADATA", "PARQUET_SCHEMA", "PARQUET_BLOOM_PROBE", "JSON_EACH",
                   "JSON_TREE", "DUCKDB_COLUMNS", "DUCKDB_CONSTRAINTS", "DUCKDB_DATABASES", "DUCKDB_DEPENDENCIES", "DUCKDB_EXTENSIONS",
                   "DUCKDB_FUNCTIONS", "DUCKDB_INDEXES", "DUCKDB_KEYWORDS", "DUCKDB_OPTIMIZERS", "DUCKDB_SCHEMAS", "DUCKDB_SEQUENCES",
                   "DUCKDB_SETTINGS", "DUCKDB_TABLES", "DUCKDB_TRIGGERS", "DUCKDB_TYPES", "DUCKDB_VARIABLES", "DUCKDB_VIEWS",
                   "DUCKDB_COORDINATE_SYSTEMS", "DUCKDB_DIALECTS", "DUCKDB_SECRET_TYPES", "DUCKDB_SECRET_TYPE_PARAMETERS",
                   "DUCKDB_PREPARED_STATEMENTS", "DUCKDB_TEMPORARY_FILES", "DUCKDB_MEMORY", "DUCKDB_SECRETS", "WHICH_SECRET", "DUCKDB_LOGS",
                   "DUCKDB_LOG_CONTEXTS", "DUCKDB_EXTENSION_REPOSITORIES", "DUCKDB_CONNECTION_COUNT", "DUCKDB_EXTERNAL_FILE_CACHE",
                   "DUCKDB_AVAILABLE_METRICS", "DUCKDB_GRAMMAR_EXTENSIONS", "DUCKDB_TABLE_SAMPLE", "DUCKDB_EVICTION_QUEUES",
                   "DUCKDB_APPROX_DATABASE_COUNT", "DUCKDB_EXTERNAL_RESOURCES", "DUCKDB_EXTERNAL_RESOURCE_TYPES", "SQL_TOKENIZE", "TEST_ALL_TYPES",
                   "READ_SINGLE_CSV_FILE", "READ_SINGLE_JSON_FILE", "READ_SINGLE_PARQUET_FILE", "PRAGMA_COLLATIONS", "PRAGMA_DATABASE_SIZE",
                   "PRAGMA_METADATA_INFO", "PRAGMA_PLATFORM", "PRAGMA_SHOW", "PRAGMA_STORAGE_INFO", "PRAGMA_TABLE_INFO", "PRAGMA_USER_AGENT",
                   "PRAGMA_VERSION", "SUMMARY", "SQL_AUTO_COMPLETE", "ICU_CALENDAR_NAMES", "PG_TIMEZONE_NAMES", "POSTGRES_SCAN",
                   "POSTGRES_SCAN_PUSHDOWN", "SQLITE_SCAN", "MYSQL_SCAN", "DUCKLAKE_SCAN", "DUCKLAKE_SNAPSHOTS", "DUCKLAKE_TABLE_INFO",
                   "DUCKLAKE_LIST_FILES", "DUCKLAKE_TABLE_INSERTIONS", "DUCKLAKE_TABLE_DELETIONS", "DUCKLAKE_TABLE_CHANGES",
                   "DUCKLAKE_CURRENT_SNAPSHOT", "DUCKLAKE_LAST_COMMITTED_SNAPSHOT", "DUCKLAKE_OPTIONS", "DUCKLAKE_SETTINGS", "ICEBERG_SCAN",
                   "ICEBERG_METADATA", "ICEBERG_SNAPSHOTS", "DELTA_SCAN", "ST_READ", "ST_READ_META", "ST_READOSM", "ST_READSHP", "SHAPEFILE_META",
                   "ST_DRIVERS", "ST_GENERATEPOINTS",
                   // the names a statement holds for functions above: a lake's own through its catalog, DuckDB's table macros
                   "SNAPSHOTS", "TABLE_INFO", "LIST_FILES", "TABLE_INSERTIONS", "TABLE_DELETIONS", "TABLE_CHANGES", "CURRENT_SNAPSHOT",
                   "LAST_COMMITTED_SNAPSHOT", "OPTIONS", "SETTINGS", "HISTOGRAM", "HISTOGRAM_VALUES", "DUCKDB_LOGS_PARSED",
                   "DUCKDB_PROFILING_SETTINGS"}));
  // Bound, so that a plain EXPLAIN shows their plan, and never run: a table function on neither list is not even bound.
  EXPECT_EQ(Names(policy.explained_table_functions.begin(), policy.explained_table_functions.end()),
            (Names{"CHECKPOINT", "FORCE_CHECKPOINT", "ENABLE_LOGGING", "DISABLE_LOGGING", "TRUNCATE_DUCKDB_LOGS", "ENABLE_PROFILING",
                   "DISABLE_PROFILING", "QUERY_TABLE"}));
}

// ---- Errors: which are read_only, and what they say ----

// DuckDB's own refusal of a write (spec §11) is read_only: in an explicit READ ONLY
// transaction of a read-write session here, on a database attached read-only below. The
// answer is the adapter's words: DuckDB's message can quote the statement.
TEST_F(DuckDBReadOnly, DuckDBsOwnRefusalIsReadOnlyAndOtherErrorsAreNot) {
  Remember(Own::kNoSessions);
  writer_.Run("BEGIN TRANSACTION READ ONLY");
  const Answer refused = ExpectReadOnly(Submit(writer_, "INSERT INTO t VALUES (99, 'S3CR3T-literal')"), "a write in a READ ONLY transaction");
  EXPECT_EQ(refused.body.find("S3CR3T"), std::string::npos) << refused.body;
  writer_.Run("ROLLBACK");
  ExpectServerUnchanged("a write in a READ ONLY transaction of a read-write session");
  // Statements that fail for a reason of their own although their text spells DuckDB's
  // refusals: an earlier server went by those words wherever in the message they stood.
  const std::vector<std::pair<std::string, int>> not_read_only = {
      {"SELECT * FROM \"database attached in read-only mode S3CR3T-in-identifier\"", 400},
      {"SELECT * FROM \"TransactionContext Error: Cannot write to database S3CR3T - transaction is launched in read-only mode\"", 400},
      {"SELECT * FROM read_csv('/nonexistent/transaction is launched in read-only mode/S3CR3T-token-123.csv')", 422},
      {"SELECT * FROM read_csv('/nonexistent/which is attached in read-only mode!')", 422},
      {"SELECT error('TransactionContext Error: Cannot write to database \"x\" - transaction is launched in read-only mode')", 422},
      {"SELECT error('Invalid Input Error: Cannot execute statement of type \"INSERT\" on database \"x\" which is attached in read-only mode!')", 422}};
  for (const auto& [sql, status] : not_read_only) {
    for (const Wire* wire : {&writer_, &reader_}) {
      const Answer answer = Submit(*wire, sql);
      EXPECT_EQ(answer.status, status) << sql << ": " << answer.body;
      EXPECT_EQ(CodeOf(answer), status == 400 ? "sql_error" : "execution_error") << sql << ": " << answer.body;
      EXPECT_FALSE(answer.Json().contains("sqlstate") && answer.Json().at("sqlstate") == json::value("25006")) << sql << ": " << answer.body;
    }
  }
  // The refusals the adapter and its guard word quote no literal of the statement.
  writer_.Run("CREATE SEQUENCE seq");
  Remember();
  for (const char* sql : {"SELECT nextval('seq'), 'S3CR3T-literal'", "SET memory_limit = 'S3CR3T-literal'", "SET GLOBAL TimeZone = 'S3CR3T-literal'",
                          "PRAGMA add_parquet_key('S3CR3T-literal', '0123456789112345')", "CREATE SECRET (TYPE s3, KEY_ID 'k', SECRET 'S3CR3T-literal')",
                          "ATTACH 'postgres://u:S3CR3T-literal@127.0.0.1:1/db' AS pg", "SELECT * FROM query('SELECT ''S3CR3T-literal'' FROM checkpoint()')",
                          "COPY t TO '/tmp/S3CR3T-literal.csv'", "SET VARIABLE x = (SELECT 'S3CR3T-literal' FROM enable_logging())",
                          "EXPLAIN INSERT INTO t VALUES (0, 'S3CR3T-literal')", "PIVOT t ON v IN (SELECT 'S3CR3T-literal') USING count(*)"}) {
    const Answer answer = Refused(sql);
    EXPECT_EQ(answer.body.find("S3CR3T"), std::string::npos) << sql << ": " << answer.body;
  }
}

// A read-only server (the database opened read-only) answers read_only too (spec §11).
TEST(DuckDBReadOnlyServer, AWriteIsRefusedWithReadOnly) {
  const auto file = ScratchHome::Directory() / "read_only_server_test.duckdb";
  std::filesystem::remove(file);
  std::filesystem::remove(file.string() + ".wal");
  auto serve = [&](bool read_only, auto&& with) {
    chargewire_duckdb::DuckDBBackend::Options backend;
    backend.database = file.string();
    backend.read_only = read_only;
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    chargewire::Server server(options, std::make_shared<chargewire_duckdb::DuckDBBackend>(backend),
                              std::make_shared<update_outcome::AnyPassword>());
    server.Start();
    with(Wire::Open(server.port()));
    server.Stop();
  };
  serve(false, [](const Wire& wire) { wire.Run("CREATE TABLE t AS SELECT 1 AS i"); });
  serve(true, [](const Wire& wire) {
    for (const char* sql : {"INSERT INTO t VALUES (2)", "UPDATE t SET i = 3", "DELETE FROM t", "CREATE TABLE n AS SELECT 'S3CR3T-literal' AS v",
                            "INSERT INTO t SELECT length('S3CR3T-literal')"}) {
      const Answer answer = wire.Post(wire.session + "/statements", json::serialize(json::object{{"sql", sql}}), {{"Prefer", "wait=30"}});
      EXPECT_EQ(answer.status, 403) << sql << ": " << answer.body;
      EXPECT_EQ(answer.Json().at("code").as_string(), "read_only") << sql << ": " << answer.body;
      EXPECT_EQ(answer.body.find("S3CR3T"), std::string::npos) << sql << ": " << answer.body;
    }
    // An error of another kind keeps its code, whatever its text spells.
    const Answer missing = wire.Post(wire.session + "/statements", R"j({"sql": "SELECT * FROM \"which is attached in read-only mode!\""})j",
                                     {{"Prefer", "wait=30"}});
    EXPECT_EQ(missing.status, 400) << missing.body;
    EXPECT_EQ(missing.Json().at("code").as_string(), "sql_error") << missing.body;
    EXPECT_EQ(Wire::StateOf(wire.Run("SELECT CASE WHEN (SELECT sum(i) FROM t) = 1 THEN 1 ELSE error('written') END")), "succeeded");
  });
  std::filesystem::remove(file);
  std::filesystem::remove(file.string() + ".wal");
}

}  // namespace
