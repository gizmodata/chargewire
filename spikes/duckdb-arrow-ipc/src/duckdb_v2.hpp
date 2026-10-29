// Small C++ conveniences over DuckDB's v2 C API (duckdb_v2.h): owned handles and
// error checking. Nothing here includes a DuckDB internal header.
#pragma once

#include <duckdb_v2.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace chargewire {

class Error : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

inline duckdb_v2_str Str(std::string_view text) { return {text.data(), text.size()}; }

inline std::string ToString(duckdb_v2_str text) {
  return text.ptr == nullptr ? std::string() : std::string(text.ptr, text.len);
}

// Runs one v2 call that reports failure through an error-info out parameter, and
// throws Error with DuckDB's message if it fails:
//   Call("connect", [&](auto* err) { return duckdb_v2_connection_create(instance, &conn, err); });
template <typename Fn>
void Call(std::string_view what, Fn&& fn) {
  duckdb_v2_error_info_handle err = nullptr;
  const DUCKDB_V2_ERROR code = fn(&err);
  if (code == DUCKDB_V2_ERROR_NONE) {
    return;
  }
  std::string message(what);
  if (err != nullptr) {
    duckdb_v2_str text{};
    duckdb_v2_error_info_get_text(err, &text);
    message += ": " + ToString(text);
    duckdb_v2_error_info_destroy(&err);
  } else {
    message += ": DuckDB error " + std::to_string(static_cast<int>(code));
  }
  throw Error(message);
}

// Unique ownership of a v2 handle, released with its destroy function.
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
  // For APIs that write a new handle: releases the current one first.
  Handle* out() {
    reset();
    return &handle_;
  }
  // Gives up ownership without destroying the handle.
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
using ArrowImporter = Owned<duckdb_v2_arrow_importer_handle, duckdb_v2_arrow_importer_destroy>;
using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
using LogicalType = Owned<duckdb_v2_logical_type_handle, duckdb_v2_logical_type_destroy>;
using DataChunk = Owned<duckdb_v2_data_chunk_handle, duckdb_v2_data_chunk_destroy>;
using TableFunction = Owned<duckdb_v2_table_function_handle, duckdb_v2_table_function_destroy>;

}  // namespace chargewire
