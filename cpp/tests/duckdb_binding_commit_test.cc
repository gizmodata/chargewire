// Failed internal binding transactions must not invalidate the shared database.
// Ordinary sequence writes, including prepared defaults, must remain supported.
#include <gtest/gtest.h>

#include <arrow/api.h>

#include <memory>
#include <stdexcept>
#include <string>

#include "chargewire/errors.hpp"
#include "duckdb_backend.hpp"
#include "duckdb_session.hpp"

namespace {
using chargewire_duckdb::DuckDBBackend;
using chargewire_duckdb::DuckDBSession;

std::shared_ptr<arrow::Table> Read(chargewire::StatementOutcome outcome) {
  if (!outcome.rows) return nullptr;
  auto result = outcome.rows->ToTable();
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return *result;
}

void Execute(DuckDBSession& session, const std::string& sql) {
  Read(session.Execute(sql, nullptr));
}

int64_t Integer(DuckDBSession& session, const std::string& sql) {
  auto rows = Read(session.Execute(sql, nullptr));
  if (!rows || rows->num_rows() != 1) throw std::runtime_error("expected one row");
  auto scalar = rows->column(0)->GetScalar(0);
  if (!scalar.ok()) throw std::runtime_error(scalar.status().ToString());
  return std::stoll((*scalar)->ToString());
}

class BindingCommit : public ::testing::TestWithParam<bool> {};

// A statement that is only prepared is bound and never executed, and binding it can advance
// or set a sequence. DuckDB (scripts/patch-duckdb-read-only.cmake) registers the sequence's
// use as a write where it happens, so the transaction commits like any other: an unpatched
// DuckDB left a "read-only transaction that has made changes" for the commit to fail on.
TEST_P(BindingCommit, AStatementThatIsOnlyPreparedMayMoveASequenceAndItsTransactionCommits) {
  for (const auto* function : {"nextval('guard_seq')", "setval('guard_seq', 7)"}) {
    SCOPED_TRACE(function);
    DuckDBBackend backend(DuckDBBackend::Options{});
    DuckDBSession writer(backend.instance()), observer(backend.instance());
    Execute(writer, "CREATE SEQUENCE guard_seq");
    Execute(writer, "CREATE TABLE guard_rows (n INTEGER)");
    if (GetParam()) {
      Execute(writer, "BEGIN");
      Execute(writer, "INSERT INTO guard_rows VALUES (1)");
    }
    auto prepared = writer.Prepare(std::string("SELECT * FROM range(") + function + ")");
    if (GetParam()) {
      EXPECT_EQ(writer.Transaction(), chargewire::TransactionState::kOpen);
      Execute(writer, "COMMIT");
      EXPECT_EQ(Integer(observer, "SELECT count(*) FROM guard_rows"), 1);
    }
    EXPECT_GE(Integer(observer, "SELECT last_value FROM duckdb_sequences() WHERE sequence_name = 'guard_seq'"), 1);
    EXPECT_EQ(Integer(observer, "SELECT 42"), 42);
    EXPECT_GE(Integer(writer, "SELECT nextval('guard_seq')"), 1);
    DuckDBSession fresh(backend.instance());
    Execute(fresh, "CREATE TABLE after_bind (n INTEGER)");
    Execute(fresh, "INSERT INTO after_bind VALUES (43)");
    EXPECT_EQ(Integer(observer, "SELECT n FROM after_bind"), 43);
  }
}

// Two attached databases: DuckDB writes one per transaction, and a sequence's use in the
// second is refused where it happens, before the value moves.
TEST(BindingCommitTwoDatabases, ABindingThatWouldWriteASecondDatabaseIsRefusedBeforeTheSequenceMoves) {
  DuckDBBackend backend(DuckDBBackend::Options{});
  DuckDBSession writer(backend.instance()), observer(backend.instance());
  Execute(writer, "ATTACH ':memory:' AS a");
  Execute(writer, "ATTACH ':memory:' AS b");
  Execute(writer, "CREATE TABLE a.main.t (i INTEGER)");
  Execute(writer, "CREATE SEQUENCE b.main.s");
  Execute(writer, "BEGIN");
  Execute(writer, "INSERT INTO a.main.t VALUES (1)");
  try {
    auto prepared = writer.Prepare("SELECT * FROM range(nextval('b.main.s'))");
    ADD_FAILURE() << "the prepare was accepted";
  } catch (const chargewire::Error& error) {
    EXPECT_NE(std::string(error.what()).find("single transaction can only write to a single attached database"), std::string::npos) << error.what();
  }
  writer.Rollback();
  EXPECT_EQ(Integer(observer, "SELECT count(*) FROM a.main.t"), 0);
  EXPECT_EQ(Integer(observer, "SELECT nextval('b.main.s')"), 1);  // it had not moved
  // The twin: the same transaction without the prepare is stored.
  Execute(writer, "BEGIN");
  Execute(writer, "INSERT INTO a.main.t VALUES (2)");
  Execute(writer, "COMMIT");
  EXPECT_EQ(Integer(observer, "SELECT count(*) FROM a.main.t"), 1);
}

TEST_P(BindingCommit, RegisteredSequenceWritesAndPreparedDefaultsStillWork) {
  for (const auto* scope : {"", "TEMP "}) {
    SCOPED_TRACE(scope);
    DuckDBBackend backend(DuckDBBackend::Options{});
    DuckDBSession writer(backend.instance());
    Execute(writer, std::string("CREATE ") + scope + "SEQUENCE normal_seq");
    Execute(writer, std::string("CREATE ") + scope + "TABLE generated_ids (id BIGINT DEFAULT nextval('normal_seq'))");
    if (GetParam()) Execute(writer, "BEGIN");
    auto next = writer.Prepare("SELECT nextval('normal_seq')");
    Read(writer.ExecutePrepared(*next, chargewire::Parameters{}));
    EXPECT_EQ(Integer(writer, "SELECT currval('normal_seq')"), 1);
    EXPECT_EQ(Integer(writer, "SELECT setval('normal_seq', 10)"), 10);
    auto insert = writer.Prepare("INSERT INTO generated_ids DEFAULT VALUES");
    Read(writer.ExecutePrepared(*insert, chargewire::Parameters{}));
    EXPECT_EQ(Integer(writer, "SELECT id FROM generated_ids"), 11);
    if (GetParam()) Execute(writer, "COMMIT");
    EXPECT_EQ(Integer(writer, "SELECT nextval('normal_seq')"), 12);
  }
}

INSTANTIATE_TEST_SUITE_P(Transactions, BindingCommit, ::testing::Bool(),
                        [](const ::testing::TestParamInfo<bool>& info) { return info.param ? "Explicit" : "Autocommit"; });
}  // namespace
