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

TEST_P(BindingCommit, FailedBindingLeavesExistingAndFreshSessionsUsable) {
  for (const auto* function : {"nextval('guard_seq')", "setval('guard_seq', 7)"}) {
    SCOPED_TRACE(function);
    DuckDBBackend backend(DuckDBBackend::Options{});
    DuckDBSession writer(backend.instance()), observer(backend.instance());
    Execute(writer, "CREATE SEQUENCE guard_seq");
    if (GetParam()) Execute(writer, "BEGIN");
    bool refused = false;
    try {
      auto prepared = writer.Prepare(std::string("SELECT * FROM range(") + function + ")");
      if (GetParam()) Execute(writer, "COMMIT");
    } catch (const chargewire::Error& error) {
      refused = true;
      EXPECT_EQ(error.code(), chargewire::ErrorCode::kExecutionError);
      EXPECT_NE(std::string(error.what()).find("Cannot commit changes made by statement binding"), std::string::npos);
      // A failure during Prepare leaves an explicit transaction to roll back;
      // a failed COMMIT has already cleaned it up.
      if (GetParam() && writer.Transaction() != chargewire::TransactionState::kNone) writer.Rollback();
    }
    EXPECT_TRUE(refused);
    EXPECT_EQ(Integer(observer, "SELECT 42"), 42);
    // Sequence gaps after rollback are allowed; only continued availability is required.
    EXPECT_GE(Integer(writer, "SELECT nextval('guard_seq')"), 1);
    DuckDBSession fresh(backend.instance());
    Execute(fresh, "CREATE TABLE after_failed_bind (n INTEGER)");
    Execute(fresh, "INSERT INTO after_failed_bind VALUES (43)");
    EXPECT_EQ(Integer(observer, "SELECT n FROM after_failed_bind"), 43);
  }
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
