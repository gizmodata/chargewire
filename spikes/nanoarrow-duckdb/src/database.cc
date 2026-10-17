#include "database.hpp"

#include <string>
#include <vector>

namespace warp {

namespace {

using QualifiedName = Owned<duckdb_v2_qname_handle, duckdb_v2_qname_destroy>;
using TableDescription =
    Owned<duckdb_v2_table_description_handle, duckdb_v2_table_description_destroy>;

// The name SQL uses to read the rows of the ingest in progress. Outside an ingest
// it reads nothing and fails.
constexpr std::string_view kUploadFunction = "warp_upload";

// DuckDB's own maximum rows per chunk; the importer must not produce more than a
// table function may return at once.
constexpr idx_t kChunkRows = 2048;

// Reports `message` through the error slot a callback is handed. Callbacks run
// inside DuckDB and must never let an exception escape.
void Fail(duckdb_v2_error_info_handle* err, DUCKDB_V2_ERROR code, std::string_view message) {
  duckdb_v2_error_info_set_code(*err, code);
  const duckdb_v2_str text = Str(message);
  duckdb_v2_error_info_set_text(*err, &text);
}

template <typename Fn>
void Guarded(duckdb_v2_error_info_handle* err, Fn&& fn) {
  try {
    fn();
  } catch (const std::exception& e) {
    Fail(err, DUCKDB_V2_ERROR_INPUT_INVALID, e.what());
  }
}

QualifiedName ParseName(std::string_view text) {
  QualifiedName name;
  const duckdb_v2_str str = Str(text);
  Call("parse table name", [&](auto* err) { return duckdb_v2_qname_parse(&str, name.out(), err); });
  return name;
}

std::string RenderName(const QualifiedName& name) {
  idx_t length = 0;
  Call("render table name", [&](auto* err) {
    return duckdb_v2_qname_render(name.get(), nullptr, 0, &length, err);
  });
  std::string text(length + 1, '\0');
  Call("render table name", [&](auto* err) {
    return duckdb_v2_qname_render(name.get(), text.data(), text.size(), &length, err);
  });
  text.resize(length);
  return text;
}

std::string ArrowStreamError(ArrowArrayStream* stream, int code) {
  const char* message = stream->get_last_error(stream);
  return message != nullptr ? message : "Arrow stream error " + std::to_string(code);
}

}  // namespace

// Set only while Ingest() runs, so warp_upload() in any other statement finds
// nothing to read.
struct Upload {
  ArrowArrayStream* rows = nullptr;
  int64_t rows_read = 0;
};

namespace {

// warp_upload()'s state for one statement: the importer resolved from the upload's
// schema, and where the stream is.
struct UploadScan {
  Upload* upload = nullptr;  // not owned
  ArrowImporter importer;
  bool input_finished = false;
};

void UploadBind(duckdb_v2_function_bind_info_handle info,
                duckdb_v2_table_function_bind_info_handle result,
                duckdb_v2_context_handle context, duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* user_data = nullptr;
    Call("upload user data",
         [&](auto* e) { return duckdb_v2_function_bind_get_user_data(info, &user_data, e); });
    auto* upload = static_cast<Upload*>(user_data);
    if (upload->rows == nullptr) {
      throw Error("warp_upload() only reads the rows of a bulk ingest in progress");
    }

    nanoarrow::UniqueSchema schema;
    const int code = upload->rows->get_schema(upload->rows, schema.get());
    if (code != 0) {
      throw Error("reading the upload's schema: " + ArrowStreamError(upload->rows, code));
    }

    auto scan = std::make_unique<UploadScan>();
    scan->upload = upload;
    Call("resolve the upload's Arrow schema", [&](auto* e) {
      return duckdb_v2_arrow_importer_create(context, schema.get(), kChunkRows,
                                             scan->importer.out(), e);
    });

    Schema columns;
    Call("upload columns", [&](auto* e) {
      return duckdb_v2_arrow_importer_get_schema(scan->importer.get(), columns.out(), e);
    });
    idx_t count = 0;
    Call("upload column count",
         [&](auto* e) { return duckdb_v2_schema_get_count(columns.get(), &count, e); });
    for (idx_t i = 0; i < count; i++) {
      duckdb_v2_identifier_t name{};
      LogicalType type;
      Call("upload column", [&](auto* e) {
        return duckdb_v2_schema_get_field(columns.get(), i, &name, type.out(), e);
      });
      Call("declare upload column", [&](auto* e) {
        return duckdb_v2_table_function_bind_add_result_column(result, &name, type.get(), e);
      });
    }

    duckdb_v2_opaque bind_data{scan.release(),
                               [](void* p) { delete static_cast<UploadScan*>(p); }, nullptr};
    Call("store upload state",
         [&](auto* e) { return duckdb_v2_function_bind_set_bind_data(info, &bind_data, e); });
  });
}

void UploadExec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    void* bind_data = nullptr;
    Call("upload state",
         [&](auto* e) { return duckdb_v2_table_function_exec_get_bind_data(info, &bind_data, e); });
    auto& scan = *static_cast<UploadScan*>(bind_data);
    duckdb_v2_data_chunk_handle output = nullptr;
    Call("output chunk",
         [&](auto* e) { return duckdb_v2_table_function_exec_get_output_chunk(info, &output, e); });

    for (;;) {
      DataChunk chunk;
      Call("convert upload rows", [&](auto* e) {
        return duckdb_v2_arrow_importer_next_chunk(scan.importer.get(), chunk.out(), e);
      });
      if (chunk) {
        idx_t vectors = 0;
        idx_t rows = 0;
        Call("chunk shape", [&](auto* e) {
          return duckdb_v2_data_chunk_get_vector_count(chunk.get(), &vectors, e);
        });
        Call("chunk size",
             [&](auto* e) { return duckdb_v2_data_chunk_get_size(chunk.get(), &rows, e); });
        duckdb_v2_vector_handle first_output = nullptr;
        for (idx_t i = 0; i < vectors; i++) {
          duckdb_v2_vector_handle source = nullptr;
          duckdb_v2_vector_handle target = nullptr;
          Call("upload vector",
               [&](auto* e) { return duckdb_v2_data_chunk_get_vector(chunk.get(), i, &source, e); });
          Call("output vector",
               [&](auto* e) { return duckdb_v2_data_chunk_get_vector(output, i, &target, e); });
          Call("hand over upload vector",
               [&](auto* e) { return duckdb_v2_vector_reference(target, source, e); });
          if (i == 0) {
            first_output = target;
          }
        }
        Call("output size",
             [&](auto* e) { return duckdb_v2_vector_set_size(first_output, rows, e); });
        scan.upload->rows_read += static_cast<int64_t>(rows);
        return;
      }

      if (scan.input_finished) {
        duckdb_v2_vector_handle first_output = nullptr;
        Call("output vector",
             [&](auto* e) { return duckdb_v2_data_chunk_get_vector(output, 0, &first_output, e); });
        Call("end of upload",
             [&](auto* e) { return duckdb_v2_vector_set_size(first_output, 0, e); });
        return;
      }

      ArrowArrayStream* rows = scan.upload->rows;
      nanoarrow::UniqueArray array;
      const int code = rows->get_next(rows, array.get());
      if (code != 0) {
        throw Error("reading the upload: " + ArrowStreamError(rows, code));
      }
      if (array->release == nullptr) {
        // End of the stream: release any rows the importer held back for a full chunk.
        scan.input_finished = true;
        Call("finish upload", [&](auto* e) {
          return duckdb_v2_arrow_importer_append(scan.importer.get(), nullptr, false, true, e);
        });
      } else {
        // Hand the array over; the chunks reference its buffers without copying.
        Call("convert upload batch", [&](auto* e) {
          return duckdb_v2_arrow_importer_append(scan.importer.get(), array.get(), true, false, e);
        });
      }
    }
  });
}

}  // namespace

Database::Database(const std::string& path) : upload_(std::make_unique<Upload>()) {
  Call("create DuckDB environment",
       [&](auto* err) { return duckdb_v2_environment_create(environment_.out(), err); });
  Call("create DuckDB instance", [&](auto* err) {
    return duckdb_v2_instance_create(environment_.get(), instance_.out(), err);
  });
  const duckdb_v2_identifier_t name = Str("db");
  const duckdb_v2_str location = Str(path);
  Call("attach " + path, [&](auto* err) {
    return duckdb_v2_instance_attach(instance_.get(), &name, &location, nullptr, true, err);
  });
  Call("connect", [&](auto* err) {
    return duckdb_v2_connection_create(instance_.get(), connection_.out(), err);
  });
  RegisterUploadFunction();
}

Database::~Database() {
  // Handles must go child-first: connection, then instance, then environment.
  connection_.reset();
  instance_.reset();
  environment_.reset();
}

void Database::RegisterUploadFunction() {
  duckdb_v2_factory_handle factory = nullptr;
  Call("factory", [&](auto* err) {
    return duckdb_v2_connection_get_factory(connection_.get(), &factory, err);
  });
  TableFunction function;
  Call("create warp_upload()",
       [&](auto* err) { return duckdb_v2_table_function_create(factory, function.out(), err); });
  const duckdb_v2_str name = Str(kUploadFunction);
  Call("name warp_upload()",
       [&](auto* err) { return duckdb_v2_table_function_set_name(function.get(), &name, err); });
  duckdb_v2_opaque user_data{upload_.get(), nullptr, nullptr};
  Call("warp_upload() state", [&](auto* err) {
    return duckdb_v2_table_function_set_user_data(function.get(), &user_data, err);
  });
  Call("warp_upload() bind", [&](auto* err) {
    return duckdb_v2_table_function_set_bind_callback(function.get(), UploadBind, err);
  });
  Call("warp_upload() exec", [&](auto* err) {
    return duckdb_v2_table_function_set_exec_callback(function.get(), UploadExec, err);
  });
  Call("register warp_upload()", [&](auto* err) {
    return duckdb_v2_connection_register_table_function(connection_.get(), function.get(), err);
  });
}

void Database::Execute(std::string_view sql) {
  const std::string text(sql);
  StatementIterator statements;
  Call("parse", [&](auto* err) {
    return duckdb_v2_parse_sql(connection_.get(), text.c_str(), statements.out(), err);
  });
  for (;;) {
    SqlStatement statement;
    Call("parse", [&](auto* err) {
      return duckdb_v2_statement_iterator_next(statements.get(), statement.out(), err);
    });
    if (!statement) {
      return;
    }
    Result result;
    Call("execute", [&](auto* err) {
      return duckdb_v2_statement_execute(connection_.get(), statement.get(), nullptr, nullptr, 0,
                                         result.out(), err);
    });
    idx_t changed = 0;
    Call("execute", [&](auto* err) { return duckdb_v2_result_drain(result.get(), &changed, err); });
  }
}

SqlStatement Database::ParseOne(std::string_view sql) {
  const std::string text(sql);
  StatementIterator statements;
  Call("parse", [&](auto* err) {
    return duckdb_v2_parse_sql(connection_.get(), text.c_str(), statements.out(), err);
  });
  SqlStatement statement;
  Call("parse", [&](auto* err) {
    return duckdb_v2_statement_iterator_next(statements.get(), statement.out(), err);
  });
  if (!statement) {
    throw Error("no SQL statement given");
  }
  SqlStatement extra;
  Call("parse", [&](auto* err) {
    return duckdb_v2_statement_iterator_next(statements.get(), extra.out(), err);
  });
  if (extra) {
    throw Error("expected one SQL statement, got several");
  }
  return statement;
}

namespace {

// DuckDB's result stream, kept together with the statement it came from so the
// statement outlives every read of the result.
class StatementStream {
 public:
  StatementStream(SqlStatement statement, nanoarrow::UniqueArrayStream rows)
      : statement_(std::move(statement)), rows_(std::move(rows)) {}

  void ToArrayStream(ArrowArrayStream* out) && {
    nanoarrow::ArrayStreamFactory<StatementStream>::InitArrayStream(
        new StatementStream(std::move(*this)), out);
  }

 private:
  friend class nanoarrow::ArrayStreamFactory<StatementStream>;

  int GetSchema(ArrowSchema* schema) { return rows_->get_schema(rows_.get(), schema); }
  int GetNext(ArrowArray* array) { return rows_->get_next(rows_.get(), array); }
  const char* GetLastError() { return rows_->get_last_error(rows_.get()); }

  SqlStatement statement_;
  nanoarrow::UniqueArrayStream rows_;
};

}  // namespace

nanoarrow::UniqueArrayStream Database::Query(std::string_view sql, int64_t batch_rows) {
  SqlStatement statement = ParseOne(sql);
  ArrowResult result;
  Call("execute", [&](auto* err) {
    return duckdb_v2_statement_execute_arrow(connection_.get(), statement.get(), nullptr, nullptr,
                                             0, static_cast<idx_t>(batch_rows), result.out(), err);
  });
  nanoarrow::UniqueArrayStream rows;
  duckdb_v2_arrow_result_handle handle = result.get();
  Call("stream result", [&](auto* err) {
    return duckdb_v2_arrow_result_to_arrow_c_stream(&handle, rows.get(), err);
  });
  result.release();  // the stream owns the result now

  nanoarrow::UniqueArrayStream stream;
  StatementStream(std::move(statement), std::move(rows)).ToArrayStream(stream.get());
  return stream;
}

bool Database::TableExists(std::string_view table) {
  QualifiedName name = ParseName(table);
  TableDescription description;
  duckdb_v2_error_info_handle err = nullptr;
  const DUCKDB_V2_ERROR code = duckdb_v2_connection_describe_table(connection_.get(), name.get(),
                                                                   description.out(), &err);
  if (err != nullptr) {
    duckdb_v2_error_info_destroy(&err);
  }
  return code == DUCKDB_V2_ERROR_NONE;
}

int64_t Database::Ingest(std::string_view table, IngestMode mode, ArrowArrayStream* rows) {
  const std::string target = RenderName(ParseName(table));
  const std::string source = "SELECT * FROM " + std::string(kUploadFunction) + "()";

  if (mode == IngestMode::kCreateAppend) {
    mode = TableExists(table) ? IngestMode::kAppend : IngestMode::kCreate;
  }
  std::string sql;
  switch (mode) {
    case IngestMode::kCreate:
      sql = "CREATE TABLE " + target + " AS " + source;
      break;
    case IngestMode::kReplace:
      sql = "CREATE OR REPLACE TABLE " + target + " AS " + source;
      break;
    case IngestMode::kAppend:
      sql = "INSERT INTO " + target + " " + source;
      break;
    case IngestMode::kCreateAppend:
      break;  // resolved above
  }

  upload_->rows = rows;
  upload_->rows_read = 0;
  try {
    Execute(sql);
  } catch (...) {
    upload_->rows = nullptr;
    throw;
  }
  upload_->rows = nullptr;
  return upload_->rows_read;
}

}  // namespace warp
