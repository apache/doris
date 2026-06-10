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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::parquet {

class ScalarColumnReader;

constexpr int64_t NESTED_READ_BATCH_ROWS = 4096;

// Nested reader state is intentionally split into shape and value.
//
// Shape is the Dremel row/level stream plus the derived parent layout information: parent nulls,
// repeated-entry counts, and row/value membership. Value is the primitive payload attached to the
// level slots that actually contain a value. Complex readers must build Doris ColumnStruct,
// ColumnArray, and ColumnMap from shape first, then append values into that layout. This mirrors
// Arrow's nested reader contract and prevents LIST/MAP/STRUCT from inferring offsets or parent
// validity from payload counts.
struct NestedScalarBatch {
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

class NestedScalarValueCursor {
public:
    NestedScalarValueCursor() = default;

    explicit NestedScalarValueCursor(const NestedScalarBatch* batch) { reset(batch); }

    void reset(const NestedScalarBatch* batch) {
        DORIS_CHECK(batch != nullptr);
        _batch = batch;
    }

    Status value_index(const std::string& column_name, int64_t level_idx, int64_t* value_idx) {
        DORIS_CHECK(_batch != nullptr);
        DORIS_CHECK(value_idx != nullptr);
        DORIS_CHECK(level_idx < _batch->levels_written);
        DORIS_CHECK(level_idx >= 0);
        DORIS_CHECK(static_cast<size_t>(level_idx) < _batch->value_indices.size());
        const int64_t computed_value_idx = _batch->value_indices[static_cast<size_t>(level_idx)];
        if (computed_value_idx < 0) {
            return Status::Corruption("Nested parquet value is absent for column {}", column_name);
        }
        DORIS_CHECK(_batch->values_column.get() != nullptr);
        if (computed_value_idx >= _batch->values_column->size()) {
            return Status::Corruption("Nested parquet value index is out of range for column {}",
                                      column_name);
        }
        *value_idx = computed_value_idx;
        return Status::OK();
    }

    bool has_value_slot(int64_t level_idx) const {
        DORIS_CHECK(_batch != nullptr);
        DORIS_CHECK(level_idx >= 0);
        DORIS_CHECK(static_cast<size_t>(level_idx) < _batch->value_indices.size());
        return _batch->value_indices[static_cast<size_t>(level_idx)] >= 0;
    }

private:
    const NestedScalarBatch* _batch = nullptr;
};

Status read_nested_scalar_batch(
        ScalarColumnReader& column_reader, int64_t batch_rows, int16_t value_slot_definition_level,
        NestedScalarBatch* batch,
        int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max());

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 NestedScalarValueCursor* value_cursor, MutableColumnPtr& column);

ColumnArray* array_column_from_output(MutableColumnPtr& column);
ColumnMap* map_column_from_output(MutableColumnPtr& column);
ColumnStruct* struct_column_from_output(MutableColumnPtr& column);
NullMap* null_map_from_nullable_output(MutableColumnPtr& column);

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts);
void append_parent_nulls(NullMap* dst, const NullMap& src);

Status append_nullable_scalar_child(const std::string& column_name, std::string_view parent_kind,
                                    std::string_view child_kind,
                                    const ScalarColumnReader& child_reader,
                                    const NestedScalarBatch& batch, int64_t level_idx,
                                    int16_t max_definition_level,
                                    NestedScalarValueCursor* value_cursor,
                                    MutableColumnPtr& column);

} // namespace doris::parquet
