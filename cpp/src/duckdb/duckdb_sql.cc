#include "duckdb_sql.hpp"

#include <arrow/c/bridge.h>
#include <arrow/scalar.h>
#include <arrow/table.h>

namespace warpduckdb {

using warpsql::Error;
using warpsql::ErrorCode;

namespace {

// Rows from DuckDB, holding whatever they depend on (statement, prepared statement).
class KeptReader : public arrow::RecordBatchReader {
 public:
  KeptReader(std::shared_ptr<arrow::RecordBatchReader> rows, std::shared_ptr<void> keep_alive)
      : rows_(std::move(rows)), keep_alive_(std::move(keep_alive)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return rows_->schema(); }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    return rows_->ReadNext(batch);
  }
  arrow::Status Close() override { return rows_->Close(); }

 private:
  std::shared_ptr<arrow::RecordBatchReader> rows_;
  std::shared_ptr<void> keep_alive_;
};

using QName = Owned<duckdb_v2_qname_handle, duckdb_v2_qname_destroy>;

}  // namespace

std::vector<SqlStatement> Parse(duckdb_v2_connection_handle connection, std::string_view sql) {
  const std::string text(sql);
  StatementIterator iterator;
  Call([&](auto* err) { return duckdb_v2_parse_sql(connection, text.c_str(), iterator.out(), err); });
  std::vector<SqlStatement> statements;
  for (;;) {
    SqlStatement statement;
    Call([&](auto* err) {
      return duckdb_v2_statement_iterator_next(iterator.get(), statement.out(), err);
    });
    if (!statement) {
      return statements;
    }
    statements.push_back(std::move(statement));
  }
}

idx_t RunToEnd(duckdb_v2_connection_handle connection, duckdb_v2_sql_statement_handle statement) {
  Result result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute(connection, statement, nullptr, nullptr, 0, result.out(),
                                       err);
  });
  idx_t changed = 0;
  Call([&](auto* err) { return duckdb_v2_result_drain(result.get(), &changed, err); });
  return changed;
}

void RunSql(duckdb_v2_connection_handle connection, std::string_view sql) {
  for (auto& statement : Parse(connection, sql)) {
    RunToEnd(connection, statement.get());
  }
}

warpsql::StatementOutcome Outcome(ArrowResult& result, std::shared_ptr<void> keep_alive) {
  DUCKDB_V2_RESULT_TYPE type{};
  Call([&](auto* err) { return duckdb_v2_arrow_result_get_result_type(result.get(), &type, err); });
  warpsql::StatementOutcome outcome;
  if (type == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT) {
    ArrowArrayStream stream{};
    duckdb_v2_arrow_result_handle handle = result.get();
    Call([&](auto* err) { return duckdb_v2_arrow_result_to_arrow_c_stream(&handle, &stream, err); });
    result.release();  // the stream owns it now
    auto reader = arrow::ImportRecordBatchReader(&stream);
    if (!reader.ok()) {
      throw Error(ErrorCode::kExecutionError, reader.status().message());
    }
    outcome.rows = std::make_shared<KeptReader>(std::move(*reader), std::move(keep_alive));
  } else {
    idx_t changed = 0;
    Call([&](auto* err) { return duckdb_v2_arrow_result_drain(result.get(), &changed, err); });
    if (type == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS) {
      outcome.rows_affected = static_cast<int64_t>(changed);
    }
  }
  return outcome;
}

std::shared_ptr<arrow::RecordBatchReader> QueryArrow(
    duckdb_v2_connection_handle connection, std::string_view sql,
    const std::vector<std::string>& names, const std::vector<duckdb_v2_value_handle>& values) {
  auto statements = Parse(connection, sql);
  if (statements.size() != 1) {
    throw Error(ErrorCode::kExecutionError, "internal query must be one statement");
  }
  auto statement = std::make_shared<SqlStatement>(std::move(statements[0]));
  std::vector<duckdb_v2_identifier_t> name_handles;
  for (const auto& name : names) name_handles.push_back(Str(name));
  ArrowResult result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute_arrow(
        connection, statement->get(), name_handles.empty() ? nullptr : name_handles.data(),
        values.empty() ? nullptr : values.data(), values.size(), 0, result.out(), err);
  });
  auto outcome = Outcome(result, statement);
  if (!outcome.rows) {
    throw Error(ErrorCode::kExecutionError, "internal query returned no rows");
  }
  return outcome.rows;
}

std::string QueryText(duckdb_v2_connection_handle connection, std::string_view sql) {
  auto table = QueryArrow(connection, sql)->ToTable();
  if (!table.ok() || (*table)->num_rows() == 0) {
    return {};
  }
  auto scalar = (*table)->column(0)->GetScalar(0);
  return scalar.ok() && (*scalar)->is_valid ? (*scalar)->ToString() : std::string();
}

std::string QualifiedName(const std::vector<std::string>& parts) {
  std::vector<duckdb_v2_identifier_t> ids;
  for (const auto& part : parts) {
    if (!part.empty()) ids.push_back(Str(part));
  }
  QName name;
  Call([&](auto* err) { return duckdb_v2_qname_create(ids.data(), ids.size(), name.out(), err); });
  idx_t length = 0;
  Call([&](auto* err) { return duckdb_v2_qname_render(name.get(), nullptr, 0, &length, err); });
  std::string text(length + 1, '\0');
  Call([&](auto* err) {
    return duckdb_v2_qname_render(name.get(), text.data(), text.size(), &length, err);
  });
  text.resize(length);
  return text;
}

}  // namespace warpduckdb
