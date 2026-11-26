// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "format_v2/column_data.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format {

// Unwrap a thrift TFieldPtr, returning nullptr when the pointer is unset.
inline const schema::external::TField* get_field_ptr(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
}

const schema::external::TSchema* find_history_schema(const TFileScanRangeParams* params,
                                                     int64_t schema_id);

// The schema named by current_schema_id, or the first one when FE sends no id; nullptr when the
// plan carries no schema dictionary.
const schema::external::TSchema* find_current_schema(const TFileScanRangeParams* params);

// Build the identity of a table field and its nested fields: field id, name, name mapping and
// timestamp marker, but no type. Array items are named `element` and map children `key`/`value`.
ColumnDefinition build_schema_identity_from_external_field(const schema::external::TField& field);

bool can_map_by_history_schema(const TFileScanRangeParams* params, int64_t split_schema_id);

// Annotate a file-local schema with the field ids and name mappings from the historical table
// schema that describes the current split. TableReader has already annotated projected table
// columns from current_schema_id; this function performs the symmetric annotation for the file
// schema so TableColumnMapper can match evolved Hudi/Paimon files by field id.
Status annotate_file_schema_from_history(const TFileScanRangeParams* params,
                                         int64_t split_schema_id,
                                         std::vector<ColumnDefinition>* file_schema);

// Parquet INT96 carries no instant annotation, so the same physical column can hold a wall clock
// (Paimon TIMESTAMP) or an instant (Paimon TIMESTAMP_LTZ, an Iceberg timestamptz written by Spark
// or Hive). Rewrite each DATETIMEV2/TIMESTAMPTZ leaf that carries the table schema's marker
// (`timestamp_is_adjusted_to_utc`) to TIMESTAMPTZ or DATETIMEV2, then rebuild its containers so a
// parent type agrees with its children. Precision and nullability are preserved, and a leaf
// without a marker keeps its file type. For example, an INT96 leaf read as DATETIMEV2(6) and marked
// as an instant becomes TIMESTAMPTZ(6), and an ARRAY<DATETIMEV2(6)> around it becomes
// ARRAY<TIMESTAMPTZ(6)>.
Status apply_file_timestamp_semantics(ColumnDefinition* column);

} // namespace doris::format
