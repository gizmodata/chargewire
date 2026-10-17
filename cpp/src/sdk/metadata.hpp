#pragma once

#include <arrow/record_batch.h>
#include <arrow/table.h>

#include <memory>

#include "warpsql/backend.hpp"

namespace warpsql::detail {

// Reads a backend's metadata rows and returns them under the canonical schema, or
// throws if the backend returned other column types.
std::shared_ptr<arrow::Table> ConformMetadata(MetadataKind kind, bool include_schema,
                                              arrow::RecordBatchReader& rows);

}  // namespace warpsql::detail
