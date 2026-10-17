#include "duckdb_upload.hpp"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "duckdb_sql.hpp"

namespace warpduckdb {

using warpsql::Error;
using warpsql::ErrorCode;

namespace {

// DuckDB stores intervals and durations in microseconds. Rather than silently drop
// nanoseconds, uploads that carry them (top-level columns) are refused.
std::optional<std::string> SubMicrosecondColumn(const arrow::RecordBatch& batch) {
  for (int c = 0; c < batch.num_columns(); c++) {
    const auto& column = *batch.column(c);
    bool lossy = false;
    if (column.type_id() == arrow::Type::DURATION &&
        static_cast<const arrow::DurationType&>(*column.type()).unit() == arrow::TimeUnit::NANO) {
      const auto& values = static_cast<const arrow::DurationArray&>(column);
      for (int64_t i = 0; i < values.length() && !lossy; i++) {
        lossy = !values.IsNull(i) && values.Value(i) % 1000 != 0;
      }
    } else if (column.type_id() == arrow::Type::INTERVAL_MONTH_DAY_NANO) {
      const auto& values = static_cast<const arrow::MonthDayNanoIntervalArray&>(column);
      for (int64_t i = 0; i < values.length() && !lossy; i++) {
        lossy = !values.IsNull(i) && values.GetValue(i).nanoseconds % 1000 != 0;
      }
    }
    if (lossy) {
      return "column '" + batch.schema()->field(c)->name() +
             "' has sub-microsecond values; DuckDB keeps intervals and durations in "
             "microseconds, so they would be truncated";
    }
  }
  return std::nullopt;
}

// DuckDB misreads Arrow day_time intervals, so top-level ones are sent on as
// month_day_nano intervals, which carry the same days and milliseconds exactly.
bool HasDayTime(const arrow::Schema& schema) {
  for (const auto& field : schema.fields()) {
    if (field->type()->id() == arrow::Type::INTERVAL_DAY_TIME) return true;
  }
  return false;
}

std::shared_ptr<arrow::Schema> WithoutDayTime(const arrow::Schema& schema) {
  arrow::FieldVector fields;
  for (const auto& field : schema.fields()) {
    fields.push_back(field->type()->id() == arrow::Type::INTERVAL_DAY_TIME
                         ? field->WithType(arrow::month_day_nano_interval())
                         : field);
  }
  return arrow::schema(fields, schema.metadata());
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DayTimeAsMonthDayNano(
    const std::shared_ptr<arrow::RecordBatch>& in, const std::shared_ptr<arrow::Schema>& schema) {
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
  return arrow::RecordBatch::Make(schema, in->num_rows(), columns);
}

// Any RecordBatchReader as IngestBatches: read one batch at a time, under a lock.
class SerialBatches : public warpsql::IngestBatches {
 public:
  explicit SerialBatches(std::shared_ptr<arrow::RecordBatchReader> source) : source_(std::move(source)) {}
  std::shared_ptr<arrow::Schema> schema() const override { return source_->schema(); }
  arrow::Status NextIndexed(std::shared_ptr<arrow::RecordBatch>* batch, int64_t* index) override {
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(source_->ReadNext(batch));
    *index = next_++;
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<arrow::RecordBatchReader> source_;
  std::mutex mutex_;
  int64_t next_ = 0;
};

// The upload in progress: its rows, and what reading them found.
struct Slot {
  std::shared_ptr<warpsql::IngestBatches> rows;
  std::shared_ptr<arrow::Schema> schema;  // what DuckDB is given (day_time intervals converted)
  bool convert_day_time = false;
  std::atomic<int64_t> rows_read = 0;
  std::mutex mutex;
  std::optional<std::string> refusal;  // a value DuckDB can't keep (unsupported_arrow_type)
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

// The C schema DuckDB resolves the upload's columns from.
struct CSchema {
  ArrowSchema schema{};
  explicit CSchema(const arrow::Schema& from) {
    auto status = arrow::ExportSchema(from, &schema);
    if (!status.ok()) throw Error(ErrorCode::kExecutionError, status.message());
  }
  ~CSchema() {
    if (schema.release != nullptr) schema.release(&schema);
  }
};

// warp_upload() reads the upload on several DuckDB threads at once (parallel insert):
// each thread takes the next uploaded batch (IngestBatches::NextIndexed decodes and
// validates it in that thread) and turns it into DuckDB chunks with its own importer.
// Every chunk carries its batch's position as its batch index, so DuckDB keeps the
// upload's row order while it inserts in parallel.
struct Scan {
  Slot* slot = nullptr;
  std::size_t max_threads = 1;
  std::mutex mutex;
  std::vector<ArrowImporter> importers;  // one per thread, created in init_global
};

struct LocalScan {
  duckdb_v2_arrow_importer_handle importer = nullptr;  // borrowed from the Scan
  idx_t batch_index = 0;
  bool converting = false;  // the importer holds rows of the current batch
  bool finished = false;
};

std::size_t ScanThreads() {
  return std::clamp<std::size_t>(std::thread::hardware_concurrency(), 1, 32);
}

void Bind(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
          duckdb_v2_context_handle context, duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    Slot* slot = current_upload;
    if (slot == nullptr || slot->rows == nullptr) {
      throw Error(ErrorCode::kSqlError, "warp_upload() only reads the rows of a bulk ingest");
    }
    auto scan = std::make_unique<Scan>();
    scan->slot = slot;
    scan->max_threads = ScanThreads();
    CSchema schema(*slot->schema);
    ArrowImporter importer;
    Call([&](auto* e) {
      return duckdb_v2_arrow_importer_create(context, &schema.schema, kChunkRows, importer.out(), e);
    });
    Schema columns;
    Call([&](auto* e) { return duckdb_v2_arrow_importer_get_schema(importer.get(), columns.out(), e); });
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
    scan->importers.push_back(std::move(importer));
    duckdb_v2_opaque bind_data{scan.release(), [](void* p) { delete static_cast<Scan*>(p); },
                               nullptr};
    Call([&](auto* e) { return duckdb_v2_function_bind_set_bind_data(info, &bind_data, e); });
  });
}

Scan& BoundScan(void* bind_data) { return *static_cast<Scan*>(bind_data); }

void InitGlobal(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle context,
                duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* bind_data = nullptr;
    Call([&](auto* e) { return duckdb_v2_table_function_init_global_get_bind_data(info, &bind_data, e); });
    Scan& scan = BoundScan(bind_data);
    // The importers are made here, on one thread, and each scanning thread takes one.
    CSchema schema(*scan.slot->schema);
    while (scan.importers.size() < scan.max_threads) {
      ArrowImporter importer;
      Call([&](auto* e) {
        return duckdb_v2_arrow_importer_create(context, &schema.schema, kChunkRows, importer.out(), e);
      });
      scan.importers.push_back(std::move(importer));
    }
    Call([&](auto* e) {
      return duckdb_v2_table_function_init_global_set_max_threads(info, scan.max_threads, e);
    });
  });
}

void InitLocal(duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_context_handle,
               duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* bind_data = nullptr;
    Call([&](auto* e) { return duckdb_v2_table_function_init_local_get_bind_data(info, &bind_data, e); });
    Scan& scan = BoundScan(bind_data);
    auto local = std::make_unique<LocalScan>();
    {
      std::lock_guard lock(scan.mutex);
      if (scan.importers.empty()) {
        throw Error(ErrorCode::kExecutionError, "warp_upload(): more scanning threads than importers");
      }
      // Kept by the Scan; the local state only borrows it.
      local->importer = scan.importers.back().get();
      scan.importers.back().release();
      scan.importers.pop_back();
    }
    duckdb_v2_opaque state{local.release(), [](void* p) {
                             auto* local = static_cast<LocalScan*>(p);
                             duckdb_v2_arrow_importer_destroy(&local->importer);
                             delete local;
                           },
                           nullptr};
    Call([&](auto* e) { return duckdb_v2_table_function_init_local_set_local_state(info, &state, e); });
  });
}

void Exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
          duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* bind_data = nullptr;
    void* local_data = nullptr;
    Call([&](auto* e) { return duckdb_v2_table_function_exec_get_bind_data(info, &bind_data, e); });
    Call([&](auto* e) { return duckdb_v2_table_function_exec_get_local_state(info, &local_data, e); });
    Slot& slot = *BoundScan(bind_data).slot;
    auto& local = *static_cast<LocalScan*>(local_data);
    duckdb_v2_data_chunk_handle output = nullptr;
    Call([&](auto* e) { return duckdb_v2_table_function_exec_get_output_chunk(info, &output, e); });
    duckdb_v2_vector_handle first = nullptr;
    Call([&](auto* e) { return duckdb_v2_data_chunk_get_vector(output, 0, &first, e); });

    for (;;) {
      if (local.converting) {
        DataChunk chunk;
        Call([&](auto* e) {
          return duckdb_v2_arrow_importer_next_chunk(local.importer, chunk.out(), e);
        });
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
          slot.rows_read += static_cast<int64_t>(rows);
          return;
        }
        local.converting = false;
      }
      if (local.finished) {
        Call([&](auto* e) { return duckdb_v2_vector_set_size(first, 0, e); });
        return;
      }
      std::shared_ptr<arrow::RecordBatch> batch;
      int64_t index = 0;
      if (auto status = slot.rows->NextIndexed(&batch, &index); !status.ok()) {
        throw Error(ErrorCode::kExecutionError, status.message());
      }
      if (batch == nullptr) {
        local.finished = true;
        continue;
      }
      if (auto refusal = SubMicrosecondColumn(*batch)) {
        {
          std::lock_guard lock(slot.mutex);
          slot.refusal = *refusal;
        }
        throw Error(ErrorCode::kUnsupportedArrowType, *refusal);
      }
      if (slot.convert_day_time) {
        auto converted = DayTimeAsMonthDayNano(batch, slot.schema);
        if (!converted.ok()) throw Error(ErrorCode::kExecutionError, converted.status().message());
        batch = std::move(converted).ValueUnsafe();
      }
      ArrowArray array{};
      if (auto status = arrow::ExportRecordBatch(*batch, &array); !status.ok()) {
        throw Error(ErrorCode::kExecutionError, status.message());
      }
      batch.reset();
      local.batch_index = static_cast<idx_t>(index);
      // Each batch is flushed on its own: its rows carry its batch index, none may be held
      // back into the next batch's chunks. The chunks reference the Arrow buffers.
      try {
        Call([&](auto* e) {
          return duckdb_v2_arrow_importer_append(local.importer, &array, true, true, e);
        });
      } catch (...) {
        if (array.release != nullptr) array.release(&array);
        throw;
      }
      if (array.release != nullptr) array.release(&array);
      local.converting = true;
    }
  });
}

void PartitionData(duckdb_v2_table_function_partition_data_info_handle info, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* local_data = nullptr;
    Call([&](auto* e) {
      return duckdb_v2_table_function_partition_data_get_local_state(info, &local_data, e);
    });
    const idx_t index = static_cast<LocalScan*>(local_data)->batch_index;
    Call([&](auto* e) { return duckdb_v2_table_function_partition_data_set_batch_index(info, index, e); });
  });
}

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
  Call([&](auto* err) {
    return duckdb_v2_table_function_set_init_global_callback(function.get(), InitGlobal, err);
  });
  Call([&](auto* err) {
    return duckdb_v2_table_function_set_init_local_callback(function.get(), InitLocal, err);
  });
  Call([&](auto* err) { return duckdb_v2_table_function_set_exec_callback(function.get(), Exec, err); });
  Call([&](auto* err) {
    return duckdb_v2_table_function_set_partition_data_callback(function.get(), PartitionData, err);
  });
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

  Slot slot;
  slot.rows = std::dynamic_pointer_cast<warpsql::IngestBatches>(rows);
  if (!slot.rows) {
    slot.rows = std::make_shared<SerialBatches>(std::move(rows));  // not from the SDK: one at a time
  }
  slot.convert_day_time = HasDayTime(*slot.rows->schema());
  slot.schema = slot.convert_day_time ? WithoutDayTime(*slot.rows->schema()) : slot.rows->schema();
  current_upload = &slot;
  try {
    RunSql(connection, sql);
  } catch (const Error&) {
    current_upload = nullptr;
    std::lock_guard lock(slot.mutex);
    if (slot.refusal) {
      throw Error(ErrorCode::kUnsupportedArrowType, *slot.refusal);
    }
    throw;
  }
  current_upload = nullptr;
  return slot.rows_read;
}

}  // namespace warpduckdb
