// Bind parameters as DuckDB values: JSON scalars (spec §6.1) and Arrow rows (spec §7.2).
#pragma once

#include <arrow/array.h>
#include <arrow/type.h>
#include <boost/json.hpp>

#include <string>
#include <vector>

#include "duckdb_v2.hpp"

namespace warpduckdb {

// The types DuckDB infers for a statement's parameters, by position and name.
class ParameterTypes {
 public:
  ParameterTypes() = default;
  static ParameterTypes Load(duckdb_v2_connection_handle connection,
                             duckdb_v2_sql_statement_handle statement);

  // The type of parameter `index` (or the one named `name`, when given); null when DuckDB
  // can't tell.
  duckdb_v2_logical_type_handle For(std::size_t index, std::string_view name) const;
  std::size_t size() const { return types_.size(); }
  const std::string& name(std::size_t index) const { return names_[index]; }
  bool named() const;
  // An Arrow schema describing the parameters, for clients building Arrow parameters.
  std::shared_ptr<arrow::Schema> ArrowSchema() const;

 private:
  std::vector<std::string> names_;
  std::vector<LogicalType> types_;
};

// Values to bind, with the names to bind them by when the statement uses named parameters.
struct BoundValues {
  std::vector<Value> values;
  std::vector<std::string> names;  // empty: positional

  std::vector<duckdb_v2_value_handle> Handles() const;
  std::vector<duckdb_v2_identifier_t> NameHandles() const;
};

class ValueFactory {
 public:
  explicit ValueFactory(duckdb_v2_factory_handle factory) : factory_(factory) {}

  // JSON: an array binds positionally, an object by name. A JSON null takes `types`'
  // type for its parameter (VARCHAR when DuckDB can't tell).
  BoundValues FromJson(const boost::json::value& parameters, const ParameterTypes& types);

  // One Arrow row: column i binds parameter i (or the parameter named like the column,
  // when the statement uses named parameters). Throws unsupported_arrow_type for types
  // that can't bind exactly.
  BoundValues FromArrowRow(const arrow::RecordBatch& batch, int64_t row,
                           const ParameterTypes& types);

 private:
  Value Null(duckdb_v2_logical_type_handle type);
  // The DuckDB type an Arrow type binds as; empty when there is none.
  LogicalType TypeFor(const arrow::DataType& type);
  Value FromJsonScalar(const boost::json::value& item);
  Value FromArrow(const arrow::Array& array, int64_t row, duckdb_v2_logical_type_handle null_type);

  duckdb_v2_factory_handle factory_;
};

}  // namespace warpduckdb
