// Read-only sessions (spec §5.8) on DuckDB, against a server started in the test process:
// what the adapter's three layers hold (duckdb_read_only.hpp), one case for each way
// through a single layer that was found, the doors back to read-write, and that a server
// embedding the adapter can extend the lists or replace the plan check. The conformance
// suite (conformance/tests/test_read_only*.py) has the longer lists; these run in CI.
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
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

// A session whose policy also lists CHECKPOINT (what an embedding server does for the
// functions it knows), or whose plan check is its own.
class ExtendedSession : public chargewire_duckdb::DuckDBSession {
 public:
  ExtendedSession(duckdb_v2_instance_handle instance, bool own_check) : DuckDBSession(instance, 0), own_check_(own_check) {
    auto policy = std::make_shared<ReadOnlyPolicy>(ReadOnlyPolicy::Default());
    policy->table_functions.insert("CHECKPOINT");
    set_read_only_policy(std::move(policy));
  }

 protected:
  void CheckReadOnlyPlan(duckdb_v2_sql_statement_handle statement, const chargewire_duckdb::BoundValues& bound) override {
    if (own_check_) throw chargewire_duckdb::ReadOnlyRefusal("anything, says this server's own check,");
    DuckDBSession::CheckReadOnlyPlan(statement, bound);
  }

 private:
  bool own_check_;
};

class ExtendedDuckDB : public chargewire_duckdb::DuckDBBackend {
 public:
  explicit ExtendedDuckDB(bool own_check) : DuckDBBackend(Options{}), own_check_(own_check) {}

 protected:
  std::unique_ptr<chargewire::EngineSession> NewSession(const chargewire::Principal&, const std::string&) override {
    return std::make_unique<ExtendedSession>(instance(), own_check_);
  }

 private:
  bool own_check_;
};

class DuckDBReadOnly : public ::testing::Test {
 protected:
  void Start(std::shared_ptr<chargewire::Backend> backend) {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::move(backend),
                                                   std::make_shared<update_outcome::AnyPassword>());
    server_->Start();
    writer_ = Wire::Open(server_->port());
    reader_ = writer_;
    const Answer created = writer_.Post("/chargewire/v1/sessions", R"({"options": {"read_only": true}})");
    ASSERT_EQ(created.status, 201) << created.body;
    reader_.session = "/chargewire/v1/sessions/" + std::string(created.Json().at("session_id").as_string());
  }
  void SetUp() override {
    Start(std::make_shared<chargewire_duckdb::DuckDBBackend>(chargewire_duckdb::DuckDBBackend::Options{}));
    writer_.Run("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'b')) v(id, v)");
  }
  void TearDown() override {
    if (server_) server_->Stop();
  }

  static Answer Submit(const Wire& wire, const std::string& sql, const json::value& parameters = nullptr) {
    json::object body{{"sql", sql}};
    if (!parameters.is_null()) body["parameters"] = parameters;
    return wire.Post(wire.session + "/statements", json::serialize(body), {{"Prefer", "wait=30"}});
  }
  // Refused with 403 read_only in the read-only session.
  void Refused(const std::string& sql, const Wire* wire = nullptr) {
    const Answer answer = Submit(wire ? *wire : reader_, sql);
    EXPECT_EQ(answer.status, 403) << sql << ": " << answer.body;
    const json::object problem = answer.Json();
    EXPECT_EQ(problem.contains("code") ? json::serialize(problem.at("code")) : "", "\"read_only\"") << sql << ": " << answer.body;
    EXPECT_EQ(problem.contains("sqlstate") ? json::serialize(problem.at("sqlstate")) : "", "\"25006\"") << sql;
  }
  // Runs to a succeeded statement in the read-only session.
  void Reads(const std::string& sql, const json::value& parameters = nullptr) {
    const Answer answer = Submit(reader_, sql, parameters);
    EXPECT_EQ(answer.status, 201) << sql << ": " << answer.body;
    EXPECT_EQ(Wire::StateOf(answer.Json()), "succeeded") << sql << ": " << answer.body;
  }
  // One value, read by the read-write session, as text.
  std::string Value(const std::string& sql, const Wire* wire = nullptr) {
    const Wire& w = wire ? *wire : writer_;
    // An UPDATE's count carries a number without reading Arrow: SET VARIABLE and compare
    // would need rows. The query's value is put into a one-row table's update count.
    const Answer answer = w.Post(w.session + "/statements",
                                 json::serialize(json::object{{"sql", "CREATE OR REPLACE TEMP TABLE value_of AS SELECT * FROM range(CAST((" + sql + ") AS BIGINT))"}}),
                                 {{"Prefer", "wait=30"}});
    EXPECT_EQ(answer.status, 201) << sql << ": " << answer.body;
    const json::object doc = answer.Json();
    if (!doc.contains("result") || !doc.at("result").is_object()) return "?";
    return json::serialize(doc.at("result").as_object().at("rows_affected"));
  }
  std::string Rows() { return Value("SELECT count(*) FROM t"); }
  Answer Patch(const Wire& wire, const std::string& options) {
    return wire.connection->Exchange(http::verb::patch, wire.session, wire.With({}), R"({"options": )" + options + "}");
  }
  std::string Transaction(const Wire& wire) { return std::string(wire.Session().at("transaction").as_string()); }

  std::unique_ptr<chargewire::Server> server_;
  Wire writer_;
  Wire reader_;
};

TEST(ReadOnlyPlan, OnlyListedOperatorsAndFunctionsPass) {
  const ReadOnlyPolicy policy = ReadOnlyPolicy::Default();
  EXPECT_TRUE(chargewire_duckdb::PlanNamesNotAllowed(
                  R"([{"name": "PROJECTION", "children": [{"name": "SEQ_SCAN ", "children": [], "extra_info": {"Table": "t"}}]}])", policy)
                  .empty());
  // A node is refused by its own name, and by the function it names.
  EXPECT_EQ(chargewire_duckdb::PlanNamesNotAllowed(
                R"([{"name": "HASH_JOIN", "children": [{"name": "SOMETHING_NEW", "children": []},
                    {"name": "INOUT_FUNCTION", "extra_info": {"Function": "an_extension_function"}, "children": []}]}])", policy),
            (std::vector<std::string>{"SOMETHING_NEW", "AN_EXTENSION_FUNCTION"}));
  // A writing operator is not a reading one, and a plan that cannot be read is refused.
  for (const char* writer : {"INSERT", "BATCH_COPY_TO_FILE", "CREATE_TABLE_AS", "ATTACH", "CREATE_SECRET", "TABLE_SCAN"}) {
    EXPECT_FALSE(policy.plan_operators.contains(writer)) << writer;
  }
  EXPECT_THROW(chargewire_duckdb::PlanNamesNotAllowed("not json", policy), chargewire::Error);
  EXPECT_THROW(chargewire_duckdb::PlanNamesNotAllowed(R"([{"children": []}])", policy), chargewire::Error);
}

// Layer 2: the kinds of statement that never run, temporary objects and file writes among them.
TEST_F(DuckDBReadOnly, StatementsOfARefusedKind) {
  const std::string file = (std::filesystem::temp_directory_path() / "chargewire_read_only_test_out.csv").string();
  std::filesystem::remove(file);
  for (const std::string& sql : std::vector<std::string>{
           "INSERT INTO t VALUES (4, 'c')", "UPDATE t SET v = 'z'", "DELETE FROM t", "TRUNCATE t",
           "CREATE TABLE n (i INTEGER)", "CREATE TEMP TABLE n AS SELECT 1 AS i", "CREATE TEMP MACRO m(a) AS a + 1",
           "DROP TABLE t", "ALTER TABLE t ADD COLUMN x INTEGER", "COMMENT ON TABLE t IS 'x'",
           "COPY t TO '" + file + "'", "EXPLAIN ANALYZE COPY t TO '" + file + "'", "EXPORT DATABASE '" + file + ".d'",
           "ATTACH ':memory:' AS other", "DETACH memory", "LOAD tpch", "INSTALL tpch", "CHECKPOINT", "CALL checkpoint()",
           "VACUUM", "ANALYZE", "CREATE SECRET s (TYPE s3, KEY_ID 'not-a-key', SECRET 'not-a-secret')", "DROP SECRET s",
           "PREPARE p AS SELECT 1", "EXECUTE p", "DEALLOCATE p", "SELECT 1; INSERT INTO t VALUES (4, 'c')"}) {
    Refused(sql);
  }
  EXPECT_FALSE(std::filesystem::exists(file));
  EXPECT_EQ(Rows(), "3");
  EXPECT_EQ(Value("SELECT count(*) FROM duckdb_databases() WHERE database_name = 'other'"), "0");
}

// Layer 1: what DuckDB's READ ONLY transaction refuses inside a statement whose kind runs.
TEST_F(DuckDBReadOnly, WritesInsideAQuery) {
  writer_.Run("CREATE SEQUENCE seq");
  writer_.Run("CREATE VIEW seq_view AS SELECT nextval('seq') AS n");
  for (const char* sql : {"WITH w AS (INSERT INTO t VALUES (4, 'c') RETURNING id) SELECT count(*) FROM w",
                          "WITH w AS (DELETE FROM t RETURNING id) SELECT count(*) FROM w", "SELECT nextval('seq')",
                          "SELECT * FROM seq_view", "SET VARIABLE x = (SELECT nextval('seq'))",
                          "EXPLAIN ANALYZE WITH w AS (INSERT INTO t VALUES (4, 'c') RETURNING id) SELECT count(*) FROM w"}) {
    Refused(sql);
  }
  EXPECT_EQ(Rows(), "3");
  EXPECT_EQ(Value("SELECT nextval('seq')"), "1");
}

// Layer 3: functions that run SQL text of their own or do the server's housekeeping write
// through a READ ONLY transaction; the plan names them however they are called.
TEST_F(DuckDBReadOnly, FunctionsThatChangeSomethingHoweverTheyAreCalled) {
  const std::string write = "WITH w AS (INSERT INTO t VALUES (9, ''z'') RETURNING id) SELECT count(*) FROM w";
  const std::string run = "json_execute_serialized_sql(json_serialize_sql('" + write + "'))";
  writer_.Run("CREATE MACRO run_it(q) AS TABLE SELECT * FROM json_execute_serialized_sql(json_serialize_sql(q))");
  writer_.Run("CREATE MACRO checkpoint_it() AS TABLE SELECT * FROM checkpoint()");
  for (const std::string& sql : std::vector<std::string>{"SELECT * FROM " + run, "SELECT * FROM system.main." + run, "SELECT * FROM run_it('" + write + "')",
                                "SELECT (SELECT count(*) FROM " + run + ")", "EXPLAIN ANALYZE SELECT * FROM " + run,
                                "SET VARIABLE x = (SELECT count(*) FROM " + run + ")", std::string("SELECT * FROM checkpoint()"),
                                std::string("SELECT * FROM query('SELECT * FROM checkpoint()')"), std::string("SELECT * FROM checkpoint_it()"),
                                std::string("SELECT * FROM enable_logging()"), std::string("PRAGMA json_execute_serialized_sql('{}')")}) {
    Refused(sql);
  }
  EXPECT_EQ(Rows(), "3");
  // The twin: in a read-write session the same call does write.
  EXPECT_EQ(Wire::StateOf(Submit(writer_, "SELECT * FROM " + run).Json()), "succeeded");
  EXPECT_EQ(Rows(), "4");
}

// The doors back to read-write: each got a write out of a READ ONLY transaction that a
// client had started itself.
TEST_F(DuckDBReadOnly, NothingMakesItReadWriteAgain) {
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
  EXPECT_EQ(Transaction(reader_), "none");
  EXPECT_EQ(Patch(reader_, R"({"read_only": false})").Json().at("errors").as_object().at("read_only").as_object().at("code").as_string(),
            "read_only");
  // Manual commit: every transaction the commit and rollback calls start is READ ONLY.
  EXPECT_EQ(Patch(reader_, R"({"autocommit": false})").Json().at("errors").as_object().size(), 0u);
  for (const char* call : {"/commit", "/rollback"}) {
    Reads("SELECT count(*) FROM t");
    EXPECT_EQ(reader_.Post(reader_.session + call, "").status, 204);
    EXPECT_EQ(Transaction(reader_), "open");
    Refused(insert);
    Reads("COMMIT");
    Refused("WITH w AS (" + insert + " RETURNING id) SELECT count(*) FROM w");
    EXPECT_EQ(Transaction(reader_), "aborted");  // found by DuckDB while planning: it aborts the transaction
    Reads("ROLLBACK");
  }
  EXPECT_EQ(Patch(reader_, R"({"autocommit": true})").Json().at("errors").as_object().size(), 0u);
  EXPECT_EQ(Transaction(reader_), "none");
  Refused(insert);
  // A failed statement and an engine refusal leave no transaction behind in autocommit.
  EXPECT_EQ(Submit(reader_, "SELECT * FROM no_such_table").status, 400);
  Refused("WITH w AS (" + insert + " RETURNING id) SELECT count(*) FROM w");
  Reads("SELECT count(*) FROM t");
  Refused(insert);
  EXPECT_EQ(Rows(), "3");
  EXPECT_EQ(reader_.Session().at("options").as_object().at("read_only"), json::value(true));
}

TEST_F(DuckDBReadOnly, ATransactionWithWorkInItRefusesTheSwitch) {
  Wire session = writer_;
  const Answer created = writer_.Post("/chargewire/v1/sessions", R"({"options": {}})");
  session.session = "/chargewire/v1/sessions/" + std::string(created.Json().at("session_id").as_string());
  session.Run("BEGIN");
  session.Run("INSERT INTO t VALUES (4, 'c')");
  const Answer refused = Patch(session, R"({"read_only": true})");
  ASSERT_EQ(refused.status, 200) << refused.body;
  ASSERT_TRUE(refused.Json().at("errors").as_object().contains("read_only")) << refused.body;
  EXPECT_EQ(refused.Json().at("errors").as_object().at("read_only").as_object().at("code").as_string(), "invalid_option_value")
      << refused.body;
  EXPECT_EQ(Transaction(session), "open");
  session.Run("COMMIT");  // the work was left for the client to end
  EXPECT_EQ(Rows(), "4");
  // A transaction in which a statement failed counts as one with work in it.
  session.Run("BEGIN");
  EXPECT_EQ(Submit(session, "SELECT * FROM no_such_table").status, 400);
  Patch(session, R"({"read_only": true})");  // (DuckDB answers no option request in an aborted transaction)
  EXPECT_EQ(session.Session().at("options").as_object().at("read_only"), json::value(false));
  session.Run("ROLLBACK");
  // One in which nothing ran is replaced: manual commit and read-only in either order.
  EXPECT_EQ(Patch(session, R"({"autocommit": false})").Json().at("errors").as_object().size(), 0u);
  const Answer accepted = Patch(session, R"({"read_only": true})");
  EXPECT_EQ(accepted.Json().at("errors").as_object().size(), 0u) << accepted.body;
  EXPECT_EQ(Transaction(session), "open");
  Refused("INSERT INTO t VALUES (5, 'd')", &session);
  EXPECT_EQ(Rows(), "4");
}

TEST_F(DuckDBReadOnly, AWritePreparedBeforeTheSwitchIsRefusedAtItsExecution) {
  Wire session = writer_;
  const Answer created = writer_.Post("/chargewire/v1/sessions", R"({"options": {}})");
  session.session = "/chargewire/v1/sessions/" + std::string(created.Json().at("session_id").as_string());
  const Answer prepared = session.Post(session.session + "/prepared", R"j({"sql": "INSERT INTO t VALUES (?, 'p')"})j");
  ASSERT_EQ(prepared.status, 201) << prepared.body;
  const std::string execute = session.session + "/prepared/" + std::string(prepared.Json().at("prepared_id").as_string()) + "/execute";
  EXPECT_EQ(session.Post(execute, R"({"parameters": [4]})", {{"Prefer", "wait=30"}}).status, 201);
  EXPECT_EQ(Patch(session, R"({"read_only": true})").Json().at("errors").as_object().size(), 0u);
  const Answer refused = session.Post(execute, R"({"parameters": [5]})", {{"Prefer", "wait=30"}});
  EXPECT_EQ(refused.status, 403) << refused.body;
  EXPECT_EQ(Rows(), "4");
  // Preparing a write in a read-only session is refused there; a prepared read runs.
  EXPECT_EQ(reader_.Post(reader_.session + "/prepared", R"j({"sql": "INSERT INTO t VALUES (?, 'p')"})j").status, 403);
  const Answer read = reader_.Post(reader_.session + "/prepared", R"j({"sql": "SELECT count(*) FROM t WHERE id >= ?"})j");
  ASSERT_EQ(read.status, 201) << read.body;
  const Answer ran = reader_.Post(reader_.session + "/prepared/" + std::string(read.Json().at("prepared_id").as_string()) + "/execute",
                                  R"({"parameters": [2]})", {{"Prefer", "wait=30"}});
  EXPECT_EQ(ran.status, 201) << ran.body;
  EXPECT_EQ(Wire::StateOf(ran.Json()), "succeeded") << ran.body;
}

TEST_F(DuckDBReadOnly, SettingsStayInTheSession) {
  for (const char* sql : {"SET GLOBAL default_order = 'desc'", "SET memory_limit = '1GB'", "PRAGMA threads = 1",
                          "SET log_query_path = '/tmp/chargewire_read_only_test.log'", "SET enable_profiling = 'json'",
                          "PRAGMA enable_profiling", "SET explain_output = 'all'", "SET default_null_order = (SELECT 'nulls_first')",
                          "RESET GLOBAL default_order", "SET \"default_null_order\" = 'nulls_first'"}) {
    Refused(sql);
  }
  // DuckDB's plain SET changes default_order for every session; here it is the session's.
  Reads("SET default_order = 'desc'");
  Reads("SET default_null_order TO 'nulls_first'");
  Reads("SET SESSION search_path = 'main'");
  Reads("SET VARIABLE answer = 42");
  Reads("SET VARIABLE most = (SELECT max(id) FROM t)");
  Reads("USE memory.main");
  EXPECT_EQ(Value("SELECT count(*) FROM duckdb_settings() WHERE name = 'default_order' AND value ILIKE 'asc%'"), "1");
  EXPECT_EQ(Value("SELECT count(*) FROM duckdb_settings() WHERE name = 'default_null_order' AND value ILIKE '%first%'"), "0");
  Reads("RESET default_order");
  Reads("RESET VARIABLE answer");
}

TEST_F(DuckDBReadOnly, WhatAReaderUsesStillRuns) {
  for (const char* sql : {
           "SELECT * FROM t", "SELECT v, count(*), sum(id) FROM t GROUP BY v ORDER BY v", "SELECT DISTINCT v FROM t",
           "SELECT * FROM t a JOIN t b USING (id)", "SELECT * FROM t a LEFT JOIN t b ON a.id < b.id", "SELECT * FROM t a, t b",
           "SELECT * FROM t a ASOF JOIN t b ON a.id >= b.id", "SELECT * FROM t a POSITIONAL JOIN t b",
           "SELECT count(*) FROM range(60000) a(i) JOIN range(60000) b(j) ON a.i < b.j AND a.i > b.j - 3",
           "SELECT id, row_number() OVER (ORDER BY id) FROM t", "SELECT * FROM t UNION SELECT * FROM t",
           "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT sum(n) FROM r",
           "WITH c AS MATERIALIZED (SELECT * FROM t) SELECT * FROM c, c c2", "SELECT * FROM t WHERE id IN (SELECT id FROM t)",
           "SELECT (SELECT max(id) FROM t b WHERE b.id <= t.id) FROM t", "SELECT * FROM t ORDER BY id LIMIT 1 OFFSET 1",
           "SELECT * FROM t USING SAMPLE 50%", "VALUES (1), (2)", "SELECT * FROM range(10)", "SELECT unnest([1, 2, 3])",
           "PIVOT t ON v USING count(*)", "PIVOT t ON v IN ('a', 'b') USING count(*)", "DESCRIBE t", "SHOW TABLES",
           "SHOW ALL TABLES", "SUMMARIZE t", "EXPLAIN SELECT * FROM t", "EXPLAIN ANALYZE SELECT count(*) FROM t",
           "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM t", "SELECT * FROM information_schema.tables",
           "SELECT * FROM duckdb_tables()", "SELECT * FROM duckdb_columns()", "SELECT * FROM duckdb_settings()",
           "SELECT * FROM pragma_table_info('t')", "PRAGMA table_info('t')", "PRAGMA show_tables", "PRAGMA version",
           "SELECT * FROM query_table('t')", "SELECT * FROM query('SELECT 1')"}) {
    Reads(sql);
  }
  Reads("SELECT * FROM t WHERE id = ? AND v = ?", json::array{2, "b"});
  Reads("SELECT * FROM t WHERE id = $id", json::object{{"id", 2}});
  Reads("EXPLAIN ANALYZE SELECT * FROM t WHERE id = ?", json::array{2});
  // A read-only session in autocommit holds no snapshot between statements.
  writer_.Run("INSERT INTO t VALUES (4, 'c')");
  Reads("SELECT count(*) FROM t");
  const Answer tables = reader_.Get(reader_.session + "/metadata/tables");
  EXPECT_EQ(tables.status, 200) << tables.body;
}

// A server that embeds the adapter adds what it knows to read, or brings its own check.
TEST_F(DuckDBReadOnly, AnEmbeddingServerExtendsTheListsOrReplacesThePlanCheck) {
  Refused("SELECT * FROM checkpoint()");
  server_->Stop();
  Start(std::make_shared<ExtendedDuckDB>(false));
  Reads("SELECT * FROM checkpoint()");  // listed by this server's policy
  Refused("CHECKPOINT");                // the kind is still not one that runs
  Refused("SELECT * FROM enable_logging()");
  server_->Stop();
  Start(std::make_shared<ExtendedDuckDB>(true));
  Refused("SELECT 1");                  // its own check decides
  Reads("BEGIN");                       // what has no plan is not its to decide
  Reads("COMMIT");
}

// A read-only server (the database opened read-only) answers read_only too (spec §11).
TEST(DuckDBReadOnlyServer, AWriteIsRefusedWithReadOnly) {
  const auto file = std::filesystem::temp_directory_path() / "chargewire_read_only_server_test.duckdb";
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
    const Answer answer = wire.Post(wire.session + "/statements", R"j({"sql": "INSERT INTO t VALUES (2)"})j", {{"Prefer", "wait=30"}});
    EXPECT_EQ(answer.status, 403) << answer.body;
    EXPECT_EQ(answer.Json().at("code").as_string(), "read_only") << answer.body;
    EXPECT_EQ(Wire::StateOf(wire.Run("SELECT * FROM t")), "succeeded");
  });
  std::filesystem::remove(file);
  std::filesystem::remove(file.string() + ".wal");
}

}  // namespace
