// C++ conveniences over DuckDB's v2 C API (duckdb_v2.h): owned handles, and calls
// that turn DuckDB's errors into Chargewire errors. No DuckDB internal header.
#pragma once

#include <duckdb_v2.h>

#include <string>
#include <string_view>
#include <utility>

#include "chargewire/errors.hpp"

namespace chargewire_duckdb {

inline duckdb_v2_str Str(std::string_view text) { return {text.data(), text.size()}; }

inline std::string ToString(duckdb_v2_str text) {
  return text.ptr == nullptr ? std::string() : std::string(text.ptr, text.len);
}

// The Chargewire error (and SQLSTATE, where the SQL standard has one) for a DuckDB error.
inline chargewire::Error Translate(DUCKDB_V2_ERROR code, std::string message) {
  using chargewire::ErrorCode;
  switch (code) {
    case DUCKDB_V2_ERROR_QUERY_PARSER:
    case DUCKDB_V2_ERROR_QUERY_SYNTAX:
      return chargewire::Error(ErrorCode::kSqlError, std::move(message), "42601");
    case DUCKDB_V2_ERROR_DATABASE_CATALOG:
      // DuckDB has one code for every catalog error; the two a client acts on most
      // (create an existing table, write to a missing one) get the standard SQLSTATEs.
      if (message.find("Table with name") != std::string::npos) {
        if (message.find("already exists") != std::string::npos) {
          return chargewire::Error(ErrorCode::kSqlError, std::move(message), "42P07");
        }
        if (message.find("does not exist") != std::string::npos) {
          return chargewire::Error(ErrorCode::kSqlError, std::move(message), "42P01");
        }
      }
      return chargewire::Error(ErrorCode::kSqlError, std::move(message));
    case DUCKDB_V2_ERROR_QUERY_BINDER:
    case DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED:
    case DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED:
    case DUCKDB_V2_ERROR_INPUT_INVALID:
    case DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID:
    case DUCKDB_V2_ERROR_TYPE_MISMATCH:
    case DUCKDB_V2_ERROR_TYPE_INVALID:
    case DUCKDB_V2_ERROR_TYPE_UNKNOWN:
      return chargewire::Error(ErrorCode::kSqlError, std::move(message));
    case DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO:
      return chargewire::Error(ErrorCode::kExecutionError, std::move(message), "22012");
    case DUCKDB_V2_ERROR_DATABASE_CONSTRAINT:
      return chargewire::Error(ErrorCode::kExecutionError, std::move(message), "23000");
    case DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION:
      return chargewire::Error(ErrorCode::kPermissionDenied, std::move(message), "42501");
    case DUCKDB_V2_ERROR_RUNTIME_INTERRUPT:
      return chargewire::Error(ErrorCode::kStatementCancelled, std::move(message), "57014");
    case DUCKDB_V2_ERROR_RUNTIME_INTERNAL:
    case DUCKDB_V2_ERROR_RUNTIME_FATAL:
      // The engine's own fault, not the statement's (spec §11).
      return chargewire::Error(ErrorCode::kInternalError, std::move(message), "XX000");
    default:
      return chargewire::Error(ErrorCode::kExecutionError, std::move(message));
  }
}

// Runs one v2 call that reports failure through an error-info out parameter; throws
// the translated chargewire::Error if it fails.
template <typename Fn>
void Call(Fn&& fn) {
  duckdb_v2_error_info_handle err = nullptr;
  const DUCKDB_V2_ERROR code = fn(&err);
  if (code == DUCKDB_V2_ERROR_NONE) {
    return;
  }
  std::string message = "DuckDB error " + std::to_string(static_cast<int>(code));
  DUCKDB_V2_ERROR detailed = code;
  if (err != nullptr) {
    duckdb_v2_str text{};
    duckdb_v2_error_info_get_text(err, &text);
    message = ToString(text);
    duckdb_v2_error_info_get_code(err, &detailed);
    duckdb_v2_error_info_destroy(&err);
  }
  throw Translate(detailed, std::move(message));
}

template <typename Handle, DUCKDB_V2_ERROR (*Destroy)(Handle*)>
class Owned {
 public:
  Owned() = default;
  explicit Owned(Handle handle) : handle_(handle) {}
  ~Owned() { reset(); }
  Owned(Owned&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}
  Owned& operator=(Owned&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
  }
  Owned(const Owned&) = delete;
  Owned& operator=(const Owned&) = delete;

  Handle get() const { return handle_; }
  Handle* out() {
    reset();
    return &handle_;
  }
  Handle release() { return std::exchange(handle_, nullptr); }
  explicit operator bool() const { return handle_ != nullptr; }
  void reset() {
    if (handle_ != nullptr) {
      Destroy(&handle_);
      handle_ = nullptr;
    }
  }

 private:
  Handle handle_ = nullptr;
};

using Environment = Owned<duckdb_v2_environment_handle, duckdb_v2_environment_destroy>;
using Instance = Owned<duckdb_v2_instance_handle, duckdb_v2_instance_destroy>;
using Connection = Owned<duckdb_v2_connection_handle, duckdb_v2_connection_destroy>;
using StatementIterator =
    Owned<duckdb_v2_statement_iterator_handle, duckdb_v2_statement_iterator_destroy>;
using SqlStatement = Owned<duckdb_v2_sql_statement_handle, duckdb_v2_sql_statement_destroy>;
using Result = Owned<duckdb_v2_result_handle, duckdb_v2_result_destroy>;
using ArrowResult = Owned<duckdb_v2_arrow_result_handle, duckdb_v2_arrow_result_destroy>;
using Value = Owned<duckdb_v2_value_handle, duckdb_v2_value_destroy>;
using LogicalType = Owned<duckdb_v2_logical_type_handle, duckdb_v2_logical_type_destroy>;
using DataChunk = Owned<duckdb_v2_data_chunk_handle, duckdb_v2_data_chunk_destroy>;

}  // namespace chargewire_duckdb
