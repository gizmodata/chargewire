// Metadata result schemas (spec §10): exactly Arrow Flight SQL's, so driver code that
// reads Flight SQL metadata reads these unchanged.
#include "metadata.hpp"

#include <arrow/api.h>

namespace warpsql {

namespace {

std::shared_ptr<arrow::Field> Text(std::string name, bool nullable = true) {
  return arrow::field(std::move(name), arrow::utf8(), nullable);
}

std::shared_ptr<arrow::Schema> KeysSchema() {
  return arrow::schema({
      Text("pk_catalog_name"),
      Text("pk_db_schema_name"),
      Text("pk_table_name", false),
      Text("pk_column_name", false),
      Text("fk_catalog_name"),
      Text("fk_db_schema_name"),
      Text("fk_table_name", false),
      Text("fk_column_name", false),
      arrow::field("key_sequence", arrow::int32(), false),
      Text("fk_key_name"),
      Text("pk_key_name"),
      arrow::field("update_rule", arrow::uint8(), false),
      arrow::field("delete_rule", arrow::uint8(), false),
  });
}

}  // namespace

std::shared_ptr<arrow::Schema> MetadataSchema(MetadataKind kind, bool include_schema) {
  switch (kind) {
    case MetadataKind::kCatalogs:
      return arrow::schema({Text("catalog_name", false)});
    case MetadataKind::kDbSchemas:
      return arrow::schema({Text("catalog_name"), Text("db_schema_name", false)});
    case MetadataKind::kTables: {
      arrow::FieldVector fields{Text("catalog_name"), Text("db_schema_name"),
                                Text("table_name", false), Text("table_type", false)};
      if (include_schema) {
        fields.push_back(arrow::field("table_schema", arrow::binary(), false));
      }
      return arrow::schema(std::move(fields));
    }
    case MetadataKind::kTableTypes:
      return arrow::schema({Text("table_type", false)});
    case MetadataKind::kPrimaryKeys:
      return arrow::schema({Text("catalog_name"), Text("db_schema_name"),
                            Text("table_name", false), Text("column_name", false),
                            Text("key_name"), arrow::field("key_sequence", arrow::int32(), false)});
    case MetadataKind::kImportedKeys:
    case MetadataKind::kExportedKeys:
    case MetadataKind::kCrossReference:
      return KeysSchema();
    case MetadataKind::kTypeInfo: {
      auto i32 = [](std::string name, bool nullable = true) {
        return arrow::field(std::move(name), arrow::int32(), nullable);
      };
      auto flag = [](std::string name, bool nullable = true) {
        return arrow::field(std::move(name), arrow::boolean(), nullable);
      };
      return arrow::schema({
          Text("type_name", false), i32("data_type", false), i32("column_size"),
          Text("literal_prefix"), Text("literal_suffix"),
          arrow::field("create_params", arrow::list(arrow::field("item", arrow::utf8(), false))),
          i32("nullable", false), flag("case_sensitive", false), i32("searchable", false),
          flag("unsigned_attribute"), flag("fixed_prec_scale", false), flag("auto_increment"),
          Text("local_type_name"), i32("minimum_scale"), i32("maximum_scale"),
          i32("sql_data_type", false), i32("datetime_subcode"), i32("num_prec_radix"),
          i32("interval_precision"),
      });
    }
  }
  return nullptr;
}

}  // namespace warpsql

namespace warpsql::detail {

std::shared_ptr<arrow::Table> ConformMetadata(MetadataKind kind, bool include_schema,
                                              arrow::RecordBatchReader& rows) {
  const auto schema = MetadataSchema(kind, include_schema);
  arrow::RecordBatchVector batches;
  for (;;) {
    auto next = rows.Next();
    if (!next.ok()) {
      throw Error(ErrorCode::kExecutionError, next.status().message());
    }
    auto batch = std::move(next).ValueUnsafe();
    if (batch == nullptr) {
      break;
    }
    if (batch->num_columns() != schema->num_fields()) {
      throw Error(ErrorCode::kExecutionError,
                  "the server's metadata has the wrong number of columns");
    }
    for (int i = 0; i < schema->num_fields(); i++) {
      if (!batch->column(i)->type()->Equals(*schema->field(i)->type())) {
        throw Error(ErrorCode::kExecutionError,
                    "the server's metadata column '" + schema->field(i)->name() +
                        "' has type " + batch->column(i)->type()->ToString());
      }
    }
    // Same columns under the canonical names and nullability.
    batches.push_back(arrow::RecordBatch::Make(schema, batch->num_rows(), batch->columns()));
  }
  auto table = arrow::Table::FromRecordBatches(schema, batches);
  if (!table.ok()) {
    throw Error(ErrorCode::kExecutionError, table.status().message());
  }
  return *table;
}

}  // namespace warpsql::detail
