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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format/new_parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;
class SchemaDescriptor;

namespace schema {
class Node;
} // namespace schema
} // namespace parquet

namespace doris::parquet {

enum class ParquetColumnSchemaKind {
    PRIMITIVE,
    STRUCT,
    LIST,
    MAP,
};

// 新 Parquet reader 的 file-local schema tree。
// 它描述 Parquet 逻辑字段到 leaf column ordinal 的关系，不包含 table/global schema 语义。
struct ParquetColumnSchema {
    int field_id = -1;
    // Parquet schema 中的 primitive leaf column ordinal。
    // 该 id 用于访问 ColumnDescriptor、RowGroupReader::RecordReader、ColumnChunk
    // metadata 和 statistics。复杂类型节点本身没有单一 leaf column，因此为 -1。
    int leaf_column_id = -1;
    std::string name;
    DataTypePtr type;
    ParquetTypeDescriptor type_descriptor;
    ParquetColumnSchemaKind kind = ParquetColumnSchemaKind::PRIMITIVE;
    const ::parquet::schema::Node* node = nullptr;
    const ::parquet::ColumnDescriptor* descriptor = nullptr;
    std::vector<std::unique_ptr<ParquetColumnSchema>> children;
};

// 从 Arrow Parquet core schema 构造 file-local schema tree。
Status build_parquet_column_schema(const ::parquet::SchemaDescriptor& schema,
                                   std::vector<std::unique_ptr<ParquetColumnSchema>>* fields);

} // namespace doris::parquet
