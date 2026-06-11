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
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace arrow {
class Array;
} // namespace arrow

namespace doris::parquet {

// Nested scalar leaf state is intentionally split into shape and value.
//
// Shape is the Dremel row/level stream plus row/value membership. Value is the primitive payload
// attached to level slots that actually contain a value. Complex readers build Doris ColumnStruct,
// ColumnArray, and ColumnMap from shape first, then append values into that layout.
struct ParquetNestedScalarBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    int16_t value_slot_definition_level = 0;
    int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max();
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    std::vector<int64_t> value_indices;
    MutableColumnPtr values_column;

    bool empty() const { return levels_written == 0; }
};

// Normalized view of one already-read Arrow RecordReader batch.
//
// The Arrow reader exposes fixed-width values through RecordReader::values(), but binary values
// through BinaryRecordReader::GetBuilderChunks(). Keeping that distinction here prevents nested
// Dremel layout code from depending on Arrow's physical storage and one-shot transfer semantics.
class ParquetLeafBatch {
public:
    int64_t consumed_level_count() const { return _consumed_level_count; }
    int64_t decoded_level_count() const { return _decoded_level_count; }
    int64_t values_written() const { return _values_written; }
    bool read_dense_for_nullable() const { return _read_dense_for_nullable; }
    const int16_t* def_levels() const { return _def_levels; }
    const int16_t* rep_levels() const { return _rep_levels; }

private:
    friend class ParquetLeafReader;

    bool is_binary_value() const;

    DecodedValueKind _value_kind = DecodedValueKind::INT32;
    int64_t _consumed_level_count = 0;
    int64_t _decoded_level_count = 0;
    int64_t _values_written = 0;
    const int16_t* _def_levels = nullptr;
    const int16_t* _rep_levels = nullptr;
    const uint8_t* _fixed_values = nullptr;
    bool _read_dense_for_nullable = false;
    std::vector<std::shared_ptr<::arrow::Array>> _binary_chunks;
};

class ParquetLeafReader {
public:
    ParquetLeafReader(const ::parquet::ColumnDescriptor* descriptor,
                      ParquetTypeDescriptor type_descriptor, DataTypePtr type, std::string name,
                      std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                      ParquetColumnReaderProfile profile = {},
                      const cctz::time_zone* timezone = nullptr, bool enable_strict_mode = false);

    Status read_batch(int64_t batch_rows, ParquetLeafBatch* batch, int64_t* rows_read) const;
    Status build_null_map(const ParquetLeafBatch& batch, int64_t records_read,
                          NullMap* null_map) const;
    Status append_values(const ParquetLeafBatch& batch, int64_t row_count, const NullMap* null_map,
                         MutableColumnPtr& column) const;

    Status read_nested_batch(
            int64_t batch_rows, int16_t value_slot_definition_level,
            ParquetNestedScalarBatch* batch,
            int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max()) const;

private:
    Status collect_batch(::parquet::internal::RecordReader& record_reader,
                         ParquetLeafBatch* batch) const;
    Status build_spaced_fixed_values(const ParquetLeafBatch& batch, int64_t row_count,
                                     const NullMap* null_map,
                                     std::vector<uint8_t>* spaced_values) const;

    const ::parquet::ColumnDescriptor* _descriptor = nullptr;
    ParquetTypeDescriptor _type_descriptor;
    DataTypePtr _type;
    std::string _name;
    std::shared_ptr<::parquet::internal::RecordReader> _record_reader;
    ParquetColumnReaderProfile _profile;
    const cctz::time_zone* _timezone = nullptr;
    bool _enable_strict_mode = false;
};

} // namespace doris::parquet
