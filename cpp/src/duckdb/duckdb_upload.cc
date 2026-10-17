#include "duckdb_upload.hpp"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include <mutex>
#include <optional>

#include "duckdb_sql.hpp"

namespace warpduckdb {

using warpsql::Error;
using warpsql::ErrorCode;

namespace {

// The upload in progress.
struct Slot {
  ArrowArrayStream* rows = nullptr;
  int64_t rows_read = 0;
};

// DuckDB binds a statement on the thread that executes it, and Ingest() runs its one
// statement on its own thread: this is the upload that statement may read. Any other
// statement, on any other thread, finds none.
thread_local Slot* current_upload = nullptr;

constexpr std::string_view kFunction = "warp_upload";
constexpr idx_t kChunkRows = 2048;  // a table function returns at most this many rows

using ArrowImporter = Owned<duckdb_v2_arrow_importer_handle, duckdb_v2_arrow_importer_destroy>;
using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
using TableFunction = Owned<duckdb_v2_table_function_handle, duckdb_v2_table_function_destroy>;
using QName = Owned<duckdb_v2_qname_handle, duckdb_v2_qname_destroy>;
using TableDescription =
    Owned<duckdb_v2_table_description_handle, duckdb_v2_table_description_destroy>;

// Callbacks run inside DuckDB: report errors through the slot DuckDB hands over, never
// by letting an exception escape.
template <typename Fn>
void Guarded(duckdb_v2_error_info_handle* err, Fn&& fn) {
  try {
    fn();
  } catch (const std::exception& e) {
    duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
    const duckdb_v2_str text = Str(e.what());
    duckdb_v2_error_info_set_text(*err, &text);
  }
}

std::string StreamError(ArrowArrayStream* stream, int code) {
  const char* message = stream->get_last_error(stream);
  return message != nullptr ? message : "Arrow stream error " + std::to_string(code);
}

// warp_upload()'s state for one statement.
struct Scan {
  Slot* slot = nullptr;
  ArrowImporter importer;
  bool input_finished = false;
};

void Bind(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
          duckdb_v2_context_handle context, duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    Slot* slot = current_upload;
    if (slot == nullptr || slot->rows == nullptr) {
      throw Error(ErrorCode::kSqlError, "warp_upload() only reads the rows of a bulk ingest");
    }
    ArrowSchema schema{};
    const int code = slot->rows->get_schema(slot->rows, &schema);
    if (code != 0) {
      throw Error(ErrorCode::kExecutionError, StreamError(slot->rows, code));
    }
    auto scan = std::make_unique<Scan>();
    scan->slot = slot;
    try {
      Call([&](auto* e) {
        return duckdb_v2_arrow_importer_create(context, &schema, kChunkRows, scan->importer.out(), e);
      });
    } catch (...) {
      schema.release(&schema);
      throw;
    }
    schema.release(&schema);

    Schema columns;
    Call([&](auto* e) {
      return duckdb_v2_arrow_importer_get_schema(scan->importer.get(), columns.out(), e);
    });
    idx_t count = 0;
    Call([&](auto* e) { return duckdb_v2_schema_get_count(columns.get(), &count, e); });
    for (idx_t i = 0; i < count; i++) {
      duckdb_v2_identifier_t name{};
      duckdb_v2_logical_type_handle type = nullptr;  // borrowed from the schema: never destroy it
      Call([&](auto* e) { return duckdb_v2_schema_get_field(columns.get(), i, &name, &type, e); });
      Call([&](auto* e) {
        return duckdb_v2_table_function_bind_add_result_column(result, &name, type, e);
      });
    }
    duckdb_v2_opaque bind_data{scan.release(), [](void* p) { delete static_cast<Scan*>(p); },
                               nullptr};
    Call([&](auto* e) { return duckdb_v2_function_bind_set_bind_data(info, &bind_data, e); });
  });
}

void Exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
          duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* bind_data = nullptr;
    Call([&](auto* e) { return duckdb_v2_table_function_exec_get_bind_data(info, &bind_data, e); });
    auto& scan = *static_cast<Scan*>(bind_data);
    duckdb_v2_data_chunk_handle output = nullptr;
    Call([&](auto* e) { return duckdb_v2_table_function_exec_get_output_chunk(info, &output, e); });

    for (;;) {
      DataChunk chunk;
      Call([&](auto* e) {
        return duckdb_v2_arrow_importer_next_chunk(scan.importer.get(), chunk.out(), e);
      });
      duckdb_v2_vector_handle first = nullptr;
      Call([&](auto* e) { return duckdb_v2_data_chunk_get_vector(output, 0, &first, e); });
      if (chunk) {
        idx_t vectors = 0;
        idx_t rows = 0;
        Call([&](auto* e) { return duckdb_v2_data_chunk_get_vector_count(chunk.get(), &vectors, e); });
        Call([&](auto* e) { return duckdb_v2_data_chunk_get_size(chunk.get(), &rows, e); });
        for (idx_t i = 0; i < vectors; i++) {
          duckdb_v2_vector_handle source = nullptr;
          duckdb_v2_vector_handle target = nullptr;
          Call([&](auto* e) { return duckdb_v2_data_chunk_get_vector(chunk.get(), i, &source, e); });
          Call([&](auto* e) { return duckdb_v2_data_chunk_get_vector(output, i, &target, e); });
          Call([&](auto* e) { return duckdb_v2_vector_reference(target, source, e); });
        }
        Call([&](auto* e) { return duckdb_v2_vector_set_size(first, rows, e); });
        scan.slot->rows_read += static_cast<int64_t>(rows);
        return;
      }
      if (scan.input_finished) {
        Call([&](auto* e) { return duckdb_v2_vector_set_size(first, 0, e); });
        return;
      }
      ArrowArrayStream* rows = scan.slot->rows;
      ArrowArray array{};
      const int code = rows->get_next(rows, &array);
      if (code != 0) {
        throw Error(ErrorCode::kExecutionError, StreamError(rows, code));
      }
      if (array.release == nullptr) {
        scan.input_finished = true;  // release rows held back for a full chunk
        Call([&](auto* e) {
          return duckdb_v2_arrow_importer_append(scan.importer.get(), nullptr, false, true, e);
        });
      } else {
        // Handed over: the chunks reference the Arrow buffers without copying.
        Call([&](auto* e) {
          return duckdb_v2_arrow_importer_append(scan.importer.get(), &array, true, false, e);
        });
        if (array.release != nullptr) array.release(&array);
      }
    }
  });
}

// DuckDB stores intervals and durations in microseconds. Rather than silently drop
// nanoseconds, refuse uploads that carry them (top-level columns).
class PrecisionGuard : public arrow::RecordBatchReader {
 public:
  explicit PrecisionGuard(std::shared_ptr<arrow::RecordBatchReader> source)
      : source_(std::move(source)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return source_->schema(); }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    ARROW_RETURN_NOT_OK(source_->ReadNext(batch));
    if (*batch == nullptr) {
      return arrow::Status::OK();
    }
    for (int c = 0; c < (*batch)->num_columns(); c++) {
      const auto& column = *(*batch)->column(c);
      const auto& name = (*batch)->schema()->field(c)->name();
      if (column.type_id() == arrow::Type::DURATION &&
          static_cast<const arrow::DurationType&>(*column.type()).unit() == arrow::TimeUnit::NANO) {
        const auto& values = static_cast<const arrow::DurationArray&>(column);
        for (int64_t i = 0; i < values.length(); i++) {
          if (!values.IsNull(i) && values.Value(i) % 1000 != 0) return Refuse(name);
        }
      } else if (column.type_id() == arrow::Type::INTERVAL_MONTH_DAY_NANO) {
        const auto& values = static_cast<const arrow::MonthDayNanoIntervalArray&>(column);
        for (int64_t i = 0; i < values.length(); i++) {
          if (!values.IsNull(i) && values.GetValue(i).nanoseconds % 1000 != 0) return Refuse(name);
        }
      }
    }
    return arrow::Status::OK();
  }

  std::optional<std::string> failure() const {
    std::lock_guard lock(mutex_);
    return failure_;
  }

 private:
  arrow::Status Refuse(const std::string& column) {
    std::lock_guard lock(mutex_);
    failure_ = "column '" + column +
               "' has sub-microsecond values; DuckDB keeps intervals and durations in "
               "microseconds, so they would be truncated";
    return arrow::Status::Invalid(*failure_);
  }

  std::shared_ptr<arrow::RecordBatchReader> source_;
  mutable std::mutex mutex_;
  std::optional<std::string> failure_;
};

// DuckDB misreads Arrow day_time intervals, so top-level ones are sent on as
// month_day_nano intervals, which carry the same days and milliseconds exactly.
class DayTimeAsMonthDayNano : public arrow::RecordBatchReader {
 public:
  explicit DayTimeAsMonthDayNano(std::shared_ptr<arrow::RecordBatchReader> source) : source_(std::move(source)) {
    arrow::FieldVector fields;
    for (const auto& field : source_->schema()->fields()) {
      fields.push_back(field->type()->id() == arrow::Type::INTERVAL_DAY_TIME
                           ? field->WithType(arrow::month_day_nano_interval())
                           : field);
    }
    schema_ = arrow::schema(fields, source_->schema()->metadata());
  }

  static bool Needed(const arrow::Schema& schema) {
    for (const auto& field : schema.fields()) {
      if (field->type()->id() == arrow::Type::INTERVAL_DAY_TIME) return true;
    }
    return false;
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    std::shared_ptr<arrow::RecordBatch> in;
    ARROW_RETURN_NOT_OK(source_->ReadNext(&in));
    if (in == nullptr) {
      *batch = nullptr;
      return arrow::Status::OK();
    }
    auto columns = in->columns();
    for (auto& column : columns) {
      if (column->type_id() != arrow::Type::INTERVAL_DAY_TIME) continue;
      const auto& values = static_cast<const arrow::DayTimeIntervalArray&>(*column);
      arrow::MonthDayNanoIntervalBuilder builder;
      ARROW_RETURN_NOT_OK(builder.Reserve(values.length()));
      for (int64_t i = 0; i < values.length(); i++) {
        if (values.IsNull(i)) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
          const auto v = values.GetValue(i);
          ARROW_RETURN_NOT_OK(builder.Append({0, v.days, int64_t{v.milliseconds} * 1000000}));
        }
      }
      ARROW_ASSIGN_OR_RAISE(column, builder.Finish());
    }
    *batch = arrow::RecordBatch::Make(schema_, in->num_rows(), columns);
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<arrow::RecordBatchReader> source_;
  std::shared_ptr<arrow::Schema> schema_;
};

// Arrow types DuckDB can't store faithfully: refused with the reason, before any table
// is touched, rather than failing inside DuckDB or arriving changed.
void RefuseUnsupported(const arrow::DataType& type, bool nested) {
  if (type.id() == arrow::Type::HALF_FLOAT) {
    throw Error(ErrorCode::kUnsupportedArrowType,
                "Arrow type halffloat has no DuckDB equivalent for bulk ingest; send float32 instead");
  }
  if (type.id() == arrow::Type::INTERVAL_DAY_TIME && nested) {
    throw Error(ErrorCode::kUnsupportedArrowType,
                "a day_time interval nested in another type cannot be ingested (DuckDB misreads it); "
                "send month_day_nano intervals instead");
  }
  for (const auto& child : type.fields()) RefuseUnsupported(*child->type(), true);
}

bool TableExists(duckdb_v2_connection_handle connection, const std::string& rendered) {
  const duckdb_v2_str text = Str(rendered);
  QName name;
  Call([&](auto* err) { return duckdb_v2_qname_parse(&text, name.out(), err); });
  TableDescription description;
  duckdb_v2_error_info_handle err = nullptr;
  const auto code =
      duckdb_v2_connection_describe_table(connection, name.get(), description.out(), &err);
  if (err != nullptr) duckdb_v2_error_info_destroy(&err);
  return code == DUCKDB_V2_ERROR_NONE;
}

}  // namespace

void RegisterUploadFunction(duckdb_v2_connection_handle connection) {
  duckdb_v2_factory_handle factory = nullptr;
  Call([&](auto* err) { return duckdb_v2_connection_get_factory(connection, &factory, err); });
  TableFunction function;
  Call([&](auto* err) { return duckdb_v2_table_function_create(factory, function.out(), err); });
  const duckdb_v2_str name = Str(kFunction);
  Call([&](auto* err) { return duckdb_v2_table_function_set_name(function.get(), &name, err); });
  Call([&](auto* err) { return duckdb_v2_table_function_set_bind_callback(function.get(), Bind, err); });
  Call([&](auto* err) { return duckdb_v2_table_function_set_exec_callback(function.get(), Exec, err); });
  Call([&](auto* err) {
    return duckdb_v2_connection_register_table_function(connection, function.get(), err);
  });
}

int64_t Ingest(duckdb_v2_connection_handle connection, const warpsql::IngestTarget& target,
               std::shared_ptr<arrow::RecordBatchReader> rows) {
  if (target.temporary && (!target.catalog.empty() || !target.schema.empty())) {
    throw Error(ErrorCode::kBadRequest, "a temporary table takes no catalog or schema");
  }
  for (const auto& field : rows->schema()->fields()) RefuseUnsupported(*field->type(), false);
  const std::string name = QualifiedName({target.catalog, target.schema, target.table});
  // A column of Arrow type null (all values missing, so no type to infer) becomes
  // VARCHAR rather than DuckDB's NULL type, which nothing could be appended to later.
  std::string replace;
  for (const auto& field : rows->schema()->fields()) {
    if (field->type()->id() == arrow::Type::NA) {
      const std::string column = QualifiedName({field->name()});
      replace += (replace.empty() ? "" : ", ") + ("CAST(" + column + " AS VARCHAR) AS " + column);
    }
  }
  const std::string source = "SELECT *" + (replace.empty() ? std::string() : " REPLACE (" + replace + ")") +
                             " FROM " + std::string(kFunction) + "()";
  auto mode = target.mode;
  if (mode == warpsql::IngestMode::kCreateAppend) {
    mode = TableExists(connection, name) ? warpsql::IngestMode::kAppend
                                          : warpsql::IngestMode::kCreate;
  }
  const std::string create = target.temporary ? "CREATE TEMPORARY TABLE " : "CREATE TABLE ";
  std::string sql;
  switch (mode) {
    case warpsql::IngestMode::kCreate: sql = create + name + " AS " + source; break;
    case warpsql::IngestMode::kReplace:
      sql = (target.temporary ? "CREATE OR REPLACE TEMPORARY TABLE " : "CREATE OR REPLACE TABLE ") +
            name + " AS " + source;
      break;
    case warpsql::IngestMode::kAppend: sql = "INSERT INTO " + name + " " + source; break;
    case warpsql::IngestMode::kCreateAppend: break;  // resolved above
  }

  if (DayTimeAsMonthDayNano::Needed(*rows->schema())) {
    rows = std::make_shared<DayTimeAsMonthDayNano>(std::move(rows));
  }
  auto guard = std::make_shared<PrecisionGuard>(std::move(rows));
  ArrowArrayStream stream{};
  auto exported = arrow::ExportRecordBatchReader(guard, &stream);
  if (!exported.ok()) {
    throw Error(ErrorCode::kExecutionError, exported.message());
  }
  Slot slot{&stream, 0};
  current_upload = &slot;
  try {
    RunSql(connection, sql);
  } catch (const Error&) {
    current_upload = nullptr;
    stream.release(&stream);
    if (auto failure = guard->failure()) {
      throw Error(ErrorCode::kUnsupportedArrowType, *failure);
    }
    throw;
  }
  current_upload = nullptr;
  stream.release(&stream);
  return slot.rows_read;
}

}  // namespace warpduckdb
