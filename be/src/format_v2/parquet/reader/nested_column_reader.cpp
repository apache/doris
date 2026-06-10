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

#include "format_v2/parquet/reader/nested_column_reader.h"

#include <cstdint>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "format_v2/parquet/reader/arrow_leaf_reader_adapter.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"

namespace doris::parquet {

Status read_nested_scalar_batch(ScalarColumnReader& column_reader, int64_t batch_rows,
                                int16_t value_slot_definition_level, NestedScalarBatch* batch,
                                int16_t value_slot_repetition_level) {
    RETURN_IF_ERROR(read_nested_leaf_batch(column_reader.leaf_context(), batch_rows,
                                           value_slot_definition_level, batch,
                                           value_slot_repetition_level));
    column_reader.advance_rows_read(batch->records_read);
    if (column_reader.profile().reader_read_rows != nullptr) {
        COUNTER_UPDATE(column_reader.profile().reader_read_rows, batch->records_read);
    }
    return Status::OK();
}

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 NestedScalarValueCursor* value_cursor, MutableColumnPtr& column) {
    DORIS_CHECK(value_cursor != nullptr);
    int64_t value_idx = -1;
    RETURN_IF_ERROR(value_cursor->value_index(column_reader.name(), level_idx, &value_idx));
    auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
    if (nullable_column != nullptr) {
        nullable_column->get_nested_column().insert_from(*batch.values_column,
                                                         static_cast<size_t>(value_idx));
        nullable_column->get_null_map_data().push_back(0);
        return Status::OK();
    }
    column->insert_from(*batch.values_column, static_cast<size_t>(value_idx));
    return Status::OK();
}

ColumnArray* array_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnArray*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnArray*>(column.get());
}

ColumnMap* map_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnMap*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnMap*>(column.get());
}

ColumnStruct* struct_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnStruct*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnStruct*>(column.get());
}

NullMap* null_map_from_nullable_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return &nullable_column->get_null_map_data();
    }
    return nullptr;
}

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts) {
    offsets.reserve(offsets.size() + entry_counts.size());
    uint64_t current_offset = offsets.empty() ? 0 : offsets.back();
    for (const auto entry_count : entry_counts) {
        current_offset += entry_count;
        offsets.push_back(current_offset);
    }
}

void append_parent_nulls(NullMap* dst, const NullMap& src) {
    if (dst == nullptr) {
        return;
    }
    dst->insert(src.begin(), src.end());
}

Status append_nullable_scalar_child(const std::string& column_name, std::string_view parent_kind,
                                    std::string_view child_kind,
                                    const ScalarColumnReader& child_reader,
                                    const NestedScalarBatch& batch, int64_t level_idx,
                                    int16_t max_definition_level,
                                    NestedScalarValueCursor* value_cursor,
                                    MutableColumnPtr& column) {
    const int16_t def_level = batch.def_levels[level_idx];
    if (def_level == max_definition_level) {
        return append_scalar_batch_value(child_reader, batch, level_idx, value_cursor, column);
    }
    if (!child_reader.type()->is_nullable()) {
        return Status::Corruption("Parquet {} column {} contains null for non-nullable {}",
                                  parent_kind, column_name, child_kind);
    }
    column->insert_default();
    return Status::OK();
}

} // namespace doris::parquet
