// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format_v2/parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;
class SchemaDescriptor;
} // namespace parquet

namespace doris::format::parquet {

class NativeFieldDescriptor;

enum class ParquetColumnSchemaKind {
    PRIMITIVE, // physical primitive leaf
    STRUCT,    // Parquet group with STRUCT semantics
    LIST,      // Parquet group with LIST semantics
    MAP,       // Parquet group with MAP semantics
};

// ============================================================================
// ============================================================================
// ============================================================================
struct ParquetColumnSchema {
    int local_id = -1;

    int parquet_field_id = -1;

    std::string name;

    DataTypePtr type = nullptr;

    int leaf_column_id = -1;

    ParquetTypeDescriptor type_descriptor {};

    ParquetColumnSchemaKind kind = ParquetColumnSchemaKind::PRIMITIVE;

    const ::parquet::ColumnDescriptor* descriptor = nullptr;

    // ======== Dremel Levels ========

    int16_t max_definition_level = 0;
    int16_t max_repetition_level = 0;

    int16_t nullable_definition_level = 0;

    int16_t definition_level = 0;
    int16_t repetition_level = 0;

    int16_t repeated_ancestor_definition_level = 0;

    int16_t repeated_repetition_level = 0;

    std::vector<std::unique_ptr<ParquetColumnSchema>> children {};
};

Status build_parquet_column_schema(const ::parquet::SchemaDescriptor& schema,
                                   std::vector<std::unique_ptr<ParquetColumnSchema>>* fields);

Status build_parquet_column_schema(const NativeFieldDescriptor& schema,
                                   std::vector<std::unique_ptr<ParquetColumnSchema>>* fields);

} // namespace doris::format::parquet
