#include "duckdb_values.hpp"

#include "duckdb_sql.hpp"

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/util/decimal.h>

#include <format>

namespace warpduckdb {

namespace json = boost::json;
using warpsql::Error;
using warpsql::ErrorCode;

namespace {

DUCKDB_V2_LOGICAL_TYPE_ID TypeId(duckdb_v2_logical_type_handle type) {
  DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
  if (type != nullptr) {
    duckdb_v2_logical_type_get_id(type, &id, nullptr);
  }
  return id;
}

// Parameter `index` of a parameterized type (DECIMAL width/scale, LIST element type).
Value TypeParam(duckdb_v2_logical_type_handle type, idx_t index) {
  duckdb_v2_identifier_t name{};  // borrowed; unused
  Value value;
  Call([&](auto* e) { return duckdb_v2_logical_type_get_param(type, index, &name, value.out(), e); });
  return value;
}

// An Arrow type for a parameter's DuckDB type, for the parameter schema clients read.
// Types without an Arrow counterpart the parameters accept are described as null
// ("unknown").
std::shared_ptr<arrow::DataType> ArrowTypeFor(duckdb_v2_logical_type_handle type) {
  switch (TypeId(type)) {
    case DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL: {
      uint8_t width = 0, scale = 0;
      Call([&](auto* e) { return duckdb_v2_value_get_utinyint(TypeParam(type, 0).get(), &width, e); });
      Call([&](auto* e) { return duckdb_v2_value_get_utinyint(TypeParam(type, 1).get(), &scale, e); });
      return arrow::decimal128(width, scale);
    }
    case DUCKDB_V2_LOGICAL_TYPE_ID_UUID: return arrow::utf8();  // as results carry it
    case DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL: return arrow::month_day_nano_interval();
    case DUCKDB_V2_LOGICAL_TYPE_ID_LIST: {
      LogicalType child;
      Call([&](auto* e) { return duckdb_v2_value_get_type(TypeParam(type, 0).get(), child.out(), e); });
      auto element = ArrowTypeFor(child.get());
      return element->id() == arrow::Type::NA ? arrow::null() : arrow::list(std::move(element));
    }
    default:
      break;
  }
  switch (TypeId(type)) {
    case DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN: return arrow::boolean();
    case DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT: return arrow::int8();
    case DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT: return arrow::int16();
    case DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER: return arrow::int32();
    case DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT: return arrow::int64();
    case DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT: return arrow::uint8();
    case DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT: return arrow::uint16();
    case DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER: return arrow::uint32();
    case DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT: return arrow::uint64();
    case DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT: return arrow::float32();
    case DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE: return arrow::float64();
    case DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR: return arrow::utf8();
    case DUCKDB_V2_LOGICAL_TYPE_ID_BLOB: return arrow::binary();
    case DUCKDB_V2_LOGICAL_TYPE_ID_DATE: return arrow::date32();
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIME: return arrow::time64(arrow::TimeUnit::MICRO);
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS: return arrow::time64(arrow::TimeUnit::NANO);
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC: return arrow::timestamp(arrow::TimeUnit::SECOND);
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS: return arrow::timestamp(arrow::TimeUnit::MILLI);
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP: return arrow::timestamp(arrow::TimeUnit::MICRO);
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS: return arrow::timestamp(arrow::TimeUnit::NANO);
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
      return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    case DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
      return arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
    default: return arrow::null();
  }
}

}  // namespace

ParameterTypes ParameterTypes::Load(duckdb_v2_connection_handle connection,
                                    duckdb_v2_sql_statement_handle statement) {
  using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
  ParameterTypes out;
  Schema result;
  Schema parameters;
  duckdb_v2_error_info_handle err = nullptr;
  if (duckdb_v2_statement_bind(connection, statement, result.out(), parameters.out(), &err) !=
      DUCKDB_V2_ERROR_NONE) {
    if (err != nullptr) duckdb_v2_error_info_destroy(&err);
    return out;  // executing reports the real error
  }
  idx_t count = 0;
  Call([&](auto* e) { return duckdb_v2_schema_get_count(parameters.get(), &count, e); });
  for (idx_t i = 0; i < count; i++) {
    duckdb_v2_identifier_t name{};
    duckdb_v2_logical_type_handle borrowed = nullptr;  // the schema's: never destroy it
    Call([&](auto* e) {
      return duckdb_v2_schema_get_field(parameters.get(), i, &name, &borrowed, e);
    });
    LogicalType type;  // our own copy outlives the schema
    Call([&](auto* e) { return duckdb_v2_logical_type_copy(borrowed, type.out(), e); });
    const auto id = TypeId(type.get());
    if (id == DUCKDB_V2_LOGICAL_TYPE_ID_INVALID || id == DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL ||
        id == DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN || id == DUCKDB_V2_LOGICAL_TYPE_ID_ANY) {
      type.reset();
    }
    out.names_.push_back(ToString(name));
    out.types_.push_back(std::move(type));
  }
  return out;
}

duckdb_v2_logical_type_handle ParameterTypes::For(std::size_t index, std::string_view name) const {
  for (std::size_t i = 0; i < types_.size(); i++) {
    if (name.empty() ? i == index : names_[i] == name) {
      return types_[i] ? types_[i].get() : nullptr;
    }
  }
  return nullptr;
}

bool ParameterTypes::named() const {
  for (const auto& name : names_) {
    // DuckDB names positional parameters by number ("1", "2", …).
    if (!name.empty() && !std::isdigit(static_cast<unsigned char>(name[0]))) {
      return true;
    }
  }
  return false;
}

std::shared_ptr<arrow::Schema> ParameterTypes::ArrowSchema() const {
  arrow::FieldVector fields;
  for (std::size_t i = 0; i < types_.size(); i++) {
    std::string name = names_[i].empty() ? std::to_string(i + 1) : names_[i];
    fields.push_back(arrow::field(std::move(name), ArrowTypeFor(types_[i].get())));
  }
  return arrow::schema(std::move(fields));
}

std::shared_ptr<arrow::Schema> ParameterTypes::ArrowSchema(duckdb_v2_connection_handle connection) const {
  auto fallback = ArrowSchema();
  if (types_.empty()) return fallback;
  std::string sql = "SELECT ";
  for (std::size_t i = 0; i < types_.size(); i++) {
    if (i > 0) sql += ", ";
    if (!types_[i]) {
      sql += "NULL";
      continue;
    }
    idx_t length = 0;
    if (duckdb_v2_logical_type_to_text(types_[i].get(), nullptr, 0, &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
      return fallback;
    }
    std::string text(length + 1, '\0');
    if (duckdb_v2_logical_type_to_text(types_[i].get(), text.data(), text.size(), &length, nullptr) !=
        DUCKDB_V2_ERROR_NONE) {
      return fallback;
    }
    text.resize(length);
    sql += "CAST(NULL AS " + text + ")";
  }
  try {
    auto exported = QueryArrow(connection, sql + " LIMIT 0")->schema();
    arrow::FieldVector fields;
    for (int i = 0; i < exported->num_fields(); i++) {
      fields.push_back(exported->field(i)->WithName(fallback->field(i)->name()));
    }
    return arrow::schema(std::move(fields));
  } catch (const std::exception&) {
    return fallback;
  }
}

std::vector<duckdb_v2_value_handle> BoundValues::Handles() const {
  std::vector<duckdb_v2_value_handle> out;
  for (const auto& value : values) out.push_back(value.get());
  return out;
}

std::vector<duckdb_v2_identifier_t> BoundValues::NameHandles() const {
  std::vector<duckdb_v2_identifier_t> out;
  for (const auto& name : names) out.push_back(Str(name));
  return out;
}

Value ValueFactory::Null(duckdb_v2_logical_type_handle type) {
  // DuckDB's untyped NULL exists only inside its binder; a NULL takes its parameter's
  // type, or VARCHAR when DuckDB can't tell (spec §6.1).
  LogicalType varchar;
  if (type == nullptr) {
    Call([&](auto* err) {
      return duckdb_v2_factory_create_type_from_id(factory_, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
                                                   nullptr, nullptr, 0, varchar.out(), err);
    });
    type = varchar.get();
  }
  Value value;
  Call([&](auto* err) { return duckdb_v2_value_create_null(factory_, type, value.out(), err); });
  return value;
}

Value ValueFactory::FromJsonScalar(const json::value& item) {
  Value value;
  switch (item.kind()) {
    case json::kind::bool_:
      Call([&](auto* e) { return duckdb_v2_value_create_bool(factory_, item.as_bool(), value.out(), e); });
      break;
    case json::kind::int64:
      Call([&](auto* e) { return duckdb_v2_value_create_bigint(factory_, item.as_int64(), value.out(), e); });
      break;
    case json::kind::uint64:
      Call([&](auto* e) { return duckdb_v2_value_create_ubigint(factory_, item.as_uint64(), value.out(), e); });
      break;
    case json::kind::double_:
      Call([&](auto* e) { return duckdb_v2_value_create_double(factory_, item.as_double(), value.out(), e); });
      break;
    case json::kind::string: {
      const duckdb_v2_str text = Str(item.as_string());
      Call([&](auto* e) { return duckdb_v2_value_create_varchar(factory_, &text, value.out(), e); });
      break;
    }
    default:
      throw Error(ErrorCode::kBadRequest,
                  "JSON bind parameters must be scalars; use a prepared statement with Arrow "
                  "parameters for nested values");
  }
  return value;
}

BoundValues ValueFactory::FromJson(const json::value& parameters, const ParameterTypes& types) {
  BoundValues out;
  if (parameters.is_array()) {
    std::size_t index = 0;
    for (const auto& item : parameters.as_array()) {
      out.values.push_back(item.is_null() ? Null(types.For(index, {})) : FromJsonScalar(item));
      index++;
    }
  } else if (parameters.is_object()) {
    for (const auto& item : parameters.as_object()) {
      out.names.emplace_back(item.key());
      out.values.push_back(item.value().is_null() ? Null(types.For(0, item.key()))
                                                  : FromJsonScalar(item.value()));
    }
  }
  return out;
}

BoundValues ValueFactory::FromArrowRow(const arrow::RecordBatch& batch, int64_t row,
                                       const ParameterTypes& types) {
  BoundValues out;
  const bool by_name = types.named();
  for (int i = 0; i < batch.num_columns(); i++) {
    const std::string& name = batch.schema()->field(i)->name();
    auto null_type = by_name ? types.For(0, name) : types.For(static_cast<std::size_t>(i), {});
    if (by_name) {
      out.names.push_back(name);
    }
    out.values.push_back(FromArrow(*batch.column(i), row, null_type));
  }
  return out;
}

namespace {

// DuckDB's name for the type an Arrow type binds as ("INTEGER[]", "DECIMAL(10, 2)"); empty
// when there is none.
std::string TypeText(const arrow::DataType& type) {
  std::string text;
  switch (type.id()) {
    case arrow::Type::BOOL: text = "BOOLEAN"; break;
    case arrow::Type::INT8: text = "TINYINT"; break;
    case arrow::Type::INT16: text = "SMALLINT"; break;
    case arrow::Type::INT32: text = "INTEGER"; break;
    case arrow::Type::INT64: text = "BIGINT"; break;
    case arrow::Type::UINT8: text = "UTINYINT"; break;
    case arrow::Type::UINT16: text = "USMALLINT"; break;
    case arrow::Type::UINT32: text = "UINTEGER"; break;
    case arrow::Type::UINT64: text = "UBIGINT"; break;
    case arrow::Type::FLOAT: text = "FLOAT"; break;
    case arrow::Type::DOUBLE: text = "DOUBLE"; break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::STRING_VIEW: text = "VARCHAR"; break;
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::BINARY_VIEW:
    case arrow::Type::FIXED_SIZE_BINARY: text = "BLOB"; break;
    case arrow::Type::DATE32:
    case arrow::Type::DATE64: text = "DATE"; break;
    case arrow::Type::TIME32: text = "TIME"; break;
    case arrow::Type::TIME64:
      text = static_cast<const arrow::Time64Type&>(type).unit() == arrow::TimeUnit::NANO ? "TIME_NS" : "TIME";
      break;
    case arrow::Type::TIMESTAMP: {
      const auto& ts = static_cast<const arrow::TimestampType&>(type);
      if (!ts.timezone().empty()) {
        text = ts.unit() == arrow::TimeUnit::NANO ? "TIMESTAMP_NS WITH TIME ZONE" : "TIMESTAMPTZ";
      } else {
        switch (ts.unit()) {
          case arrow::TimeUnit::SECOND: text = "TIMESTAMP_S"; break;
          case arrow::TimeUnit::MILLI: text = "TIMESTAMP_MS"; break;
          case arrow::TimeUnit::MICRO: text = "TIMESTAMP"; break;
          case arrow::TimeUnit::NANO: text = "TIMESTAMP_NS"; break;
        }
      }
      break;
    }
    case arrow::Type::DECIMAL128: {
      const auto& decimal = static_cast<const arrow::Decimal128Type&>(type);
      text = std::format("DECIMAL({}, {})", decimal.precision(), decimal.scale());
      break;
    }
    case arrow::Type::INTERVAL_MONTH_DAY_NANO: text = "INTERVAL"; break;
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      const std::string element = TypeText(*static_cast<const arrow::BaseListType&>(type).value_type());
      if (!element.empty()) text = element + "[]";
      break;
    }
    case arrow::Type::EXTENSION:
      if (static_cast<const arrow::ExtensionType&>(type).extension_name() == "arrow.uuid") {
        text = "UUID";
      }
      break;
    default:
      break;
  }
  return text;
}

}  // namespace

LogicalType ValueFactory::TypeFor(const arrow::DataType& type) {
  const std::string text = TypeText(type);
  if (text.empty()) return {};
  LogicalType out;
  const duckdb_v2_str str = Str(text);
  Call([&](auto* e) { return duckdb_v2_factory_create_type_from_text(factory_, &str, out.out(), e); });
  return out;
}

Value ValueFactory::FromArrow(const arrow::Array& array, int64_t row,
                              duckdb_v2_logical_type_handle null_type) {
  if (array.IsNull(row)) {
    // Without a type from the statement, the NULL takes its Arrow column's type.
    LogicalType from_arrow;
    if (null_type == nullptr) {
      from_arrow = TypeFor(*array.type());
      null_type = from_arrow ? from_arrow.get() : nullptr;
    }
    return Null(null_type);
  }
  // A dictionary-encoded parameter (pandas categoricals, Arrow's string dictionaries)
  // binds as the value its index names.
  if (array.type_id() == arrow::Type::DICTIONARY) {
    const auto& dictionary = static_cast<const arrow::DictionaryArray&>(array);
    return FromArrow(*dictionary.dictionary(), dictionary.GetValueIndex(row), null_type);
  }
  Value value;
  auto make = [&](auto&& fn) { Call([&](auto* e) { return fn(value.out(), e); }); };
  const auto& type = *array.type();
  switch (type.id()) {
    case arrow::Type::BOOL:
      make([&](auto* out, auto* e) {
        return duckdb_v2_value_create_bool(factory_, static_cast<const arrow::BooleanArray&>(array).Value(row), out, e);
      });
      break;
    case arrow::Type::INT8:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_tinyint(factory_, static_cast<const arrow::Int8Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::INT16:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_smallint(factory_, static_cast<const arrow::Int16Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::INT32:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_int(factory_, static_cast<const arrow::Int32Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::INT64:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_bigint(factory_, static_cast<const arrow::Int64Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::UINT8:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_utinyint(factory_, static_cast<const arrow::UInt8Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::UINT16:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_usmallint(factory_, static_cast<const arrow::UInt16Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::UINT32:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_uint(factory_, static_cast<const arrow::UInt32Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::UINT64:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_ubigint(factory_, static_cast<const arrow::UInt64Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::FLOAT:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_float(factory_, static_cast<const arrow::FloatArray&>(array).Value(row), out, e); });
      break;
    case arrow::Type::DOUBLE:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_double(factory_, static_cast<const arrow::DoubleArray&>(array).Value(row), out, e); });
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::STRING_VIEW: {
      std::string text;
      if (type.id() == arrow::Type::STRING) text = static_cast<const arrow::StringArray&>(array).GetString(row);
      else if (type.id() == arrow::Type::LARGE_STRING) text = static_cast<const arrow::LargeStringArray&>(array).GetString(row);
      else text = std::string(static_cast<const arrow::StringViewArray&>(array).GetView(row));
      const duckdb_v2_str str = Str(text);
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_varchar(factory_, &str, out, e); });
      break;
    }
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::BINARY_VIEW:
    case arrow::Type::FIXED_SIZE_BINARY: {
      std::string bytes;
      if (type.id() == arrow::Type::BINARY) bytes = static_cast<const arrow::BinaryArray&>(array).GetString(row);
      else if (type.id() == arrow::Type::LARGE_BINARY) bytes = static_cast<const arrow::LargeBinaryArray&>(array).GetString(row);
      else if (type.id() == arrow::Type::BINARY_VIEW) bytes = std::string(static_cast<const arrow::BinaryViewArray&>(array).GetView(row));
      else bytes = static_cast<const arrow::FixedSizeBinaryArray&>(array).GetString(row);
      const duckdb_v2_str str = Str(bytes);
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_blob(factory_, &str, out, e); });
      break;
    }
    case arrow::Type::DATE32:
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_date(factory_, static_cast<const arrow::Date32Array&>(array).Value(row), out, e); });
      break;
    case arrow::Type::DATE64: {
      const int64_t ms = static_cast<const arrow::Date64Array&>(array).Value(row);
      if (ms % 86'400'000 != 0) {
        throw Error(ErrorCode::kUnsupportedArrowType, "a date64 parameter is not a whole day");
      }
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_date(factory_, static_cast<int32_t>(ms / 86'400'000), out, e); });
      break;
    }
    case arrow::Type::TIME32: {
      const auto& time = static_cast<const arrow::Time32Type&>(type);
      const int64_t v = static_cast<const arrow::Time32Array&>(array).Value(row);
      const int64_t micros = time.unit() == arrow::TimeUnit::SECOND ? v * 1'000'000 : v * 1'000;
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_time(factory_, micros, out, e); });
      break;
    }
    case arrow::Type::TIME64: {
      const auto& time = static_cast<const arrow::Time64Type&>(type);
      const int64_t v = static_cast<const arrow::Time64Array&>(array).Value(row);
      if (time.unit() == arrow::TimeUnit::NANO) {
        make([&](auto* out, auto* e) { return duckdb_v2_value_create_time_ns(factory_, v, out, e); });
      } else {
        make([&](auto* out, auto* e) { return duckdb_v2_value_create_time(factory_, v, out, e); });
      }
      break;
    }
    case arrow::Type::TIMESTAMP: {
      const auto& ts = static_cast<const arrow::TimestampType&>(type);
      const int64_t v = static_cast<const arrow::TimestampArray&>(array).Value(row);
      const bool zoned = !ts.timezone().empty();
      switch (ts.unit()) {
        case arrow::TimeUnit::SECOND:
          if (zoned) make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_tz(factory_, v * 1'000'000, out, e); });
          else make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_sec(factory_, v, out, e); });
          break;
        case arrow::TimeUnit::MILLI:
          if (zoned) make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_tz(factory_, v * 1'000, out, e); });
          else make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_ms(factory_, v, out, e); });
          break;
        case arrow::TimeUnit::MICRO:
          if (zoned) make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_tz(factory_, v, out, e); });
          else make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp(factory_, v, out, e); });
          break;
        case arrow::TimeUnit::NANO:
          if (zoned) make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_tz_ns(factory_, v, out, e); });
          else make([&](auto* out, auto* e) { return duckdb_v2_value_create_timestamp_ns(factory_, v, out, e); });
          break;
      }
      break;
    }
    case arrow::Type::DECIMAL128: {
      const auto& decimal = static_cast<const arrow::Decimal128Type&>(type);
      if (decimal.precision() > 38) {
        throw Error(ErrorCode::kUnsupportedArrowType, "decimal parameters hold at most 38 digits");
      }
      const arrow::Decimal128 v(static_cast<const arrow::Decimal128Array&>(array).GetValue(row));
      const duckdb_v2_hugeint_t huge{v.low_bits(), v.high_bits()};
      make([&](auto* out, auto* e) {
        return duckdb_v2_value_create_decimal(factory_, &huge, static_cast<uint8_t>(decimal.precision()),
                                              static_cast<uint8_t>(decimal.scale()), out, e);
      });
      break;
    }
    case arrow::Type::EXTENSION: {
      const auto& extension = static_cast<const arrow::ExtensionType&>(type);
      if (extension.extension_name() == "arrow.uuid") {
        // Bound as text; DuckDB casts it to the parameter's UUID type.
        const auto& storage = *static_cast<const arrow::ExtensionArray&>(array).storage();
        const auto raw = static_cast<const arrow::FixedSizeBinaryArray&>(storage).GetString(row);
        std::string text;
        static constexpr char kHex[] = "0123456789abcdef";
        for (std::size_t i = 0; i < raw.size(); i++) {
          if (i == 4 || i == 6 || i == 8 || i == 10) text += '-';
          text += kHex[static_cast<unsigned char>(raw[i]) >> 4];
          text += kHex[static_cast<unsigned char>(raw[i]) & 15];
        }
        const duckdb_v2_str str = Str(text);
        make([&](auto* out, auto* e) { return duckdb_v2_value_create_varchar(factory_, &str, out, e); });
        break;
      }
      throw Error(ErrorCode::kUnsupportedArrowType,
                  std::format("Arrow parameters of type {} are not supported", type.ToString()));
    }
    case arrow::Type::INTERVAL_MONTH_DAY_NANO: {
      const auto v = static_cast<const arrow::MonthDayNanoIntervalArray&>(array).Value(row);
      if (v.nanoseconds % 1000 != 0) {
        throw Error(ErrorCode::kUnsupportedArrowType,
                    "an interval parameter has sub-microsecond precision, which DuckDB doesn't keep");
      }
      const duckdb_v2_interval_t interval{v.months, v.days, v.nanoseconds / 1000};
      make([&](auto* out, auto* e) { return duckdb_v2_value_create_interval(factory_, &interval, out, e); });
      break;
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      const auto& list = static_cast<const arrow::BaseListType&>(type);
      LogicalType element = TypeFor(*list.value_type());
      if (!element) {
        throw Error(ErrorCode::kUnsupportedArrowType,
                    std::format("Arrow parameters of type {} are not supported", type.ToString()));
      }
      std::shared_ptr<arrow::Array> values;
      int64_t offset = 0, length = 0;
      if (type.id() == arrow::Type::LIST) {
        const auto& lists = static_cast<const arrow::ListArray&>(array);
        values = lists.values(); offset = lists.value_offset(row); length = lists.value_length(row);
      } else {
        const auto& lists = static_cast<const arrow::LargeListArray&>(array);
        values = lists.values(); offset = lists.value_offset(row); length = lists.value_length(row);
      }
      std::vector<Value> children;
      for (int64_t i = 0; i < length; i++) {
        children.push_back(FromArrow(*values, offset + i, element.get()));
      }
      std::vector<duckdb_v2_value_handle> handles;
      for (const auto& child : children) handles.push_back(child.get());
      make([&](auto* out, auto* e) {
        return duckdb_v2_value_create_list(factory_, element.get(), handles.empty() ? nullptr : handles.data(),
                                           handles.size(), out, e);
      });
      break;
    }
    case arrow::Type::NA:
      return Null(null_type);
    default:
      throw Error(ErrorCode::kUnsupportedArrowType,
                  std::format("Arrow parameters of type {} are not supported", type.ToString()));
  }
  return value;
}

}  // namespace warpduckdb
