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
#include <limits>
#include <memory>
#include <string>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "format_v2/parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace doris::parquet {

struct NestedScalarBatch;

struct ArrowLeafReaderContext {
    const ::parquet::ColumnDescriptor* descriptor = nullptr;
    ParquetTypeDescriptor type_descriptor;
    DataTypePtr type;
    std::string name;
    std::shared_ptr<::parquet::internal::RecordReader> record_reader;

    const std::string& column_name() const { return name; }
    const DataTypePtr& data_type() const { return type; }
};

Status read_leaf_records(const ArrowLeafReaderContext& context, int64_t batch_rows,
                         ::parquet::internal::RecordReader** record_reader, int64_t* rows_read);

Status build_leaf_null_map(const ArrowLeafReaderContext& context,
                           ::parquet::internal::RecordReader& record_reader, int64_t records_read,
                           NullMap* null_map);

Status append_leaf_values(const ArrowLeafReaderContext& context,
                          ::parquet::internal::RecordReader& record_reader, int64_t row_count,
                          const NullMap* null_map, MutableColumnPtr& column);

Status read_nested_leaf_batch(
        const ArrowLeafReaderContext& context, int64_t batch_rows,
        int16_t value_slot_definition_level, NestedScalarBatch* batch,
        int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max());

} // namespace doris::parquet
