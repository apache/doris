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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "format/new_parquet/complex_column_reader.h"
#include "format/new_parquet/nested_level_assembler.h"
#include "format/new_parquet/scalar_column_reader.h"

namespace doris::parquet {

Status read_nested_scalar_batch(
        ScalarColumnReader& column_reader, int64_t batch_rows, int16_t value_slot_definition_level,
        NestedScalarBatch* batch,
        int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max());

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 MutableColumnPtr& column);

Status read_nested_scalar_batch_from_overflow(ScalarColumnReader& reader, int64_t batch_rows,
                                              int16_t value_slot_definition_level,
                                              NestedScalarOverflow* overflow,
                                              NestedScalarBatch* batch);

Status validate_nested_scalar_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedScalarBatch& candidate_batch,
                                        std::string_view candidate_name, std::string_view action);

Status validate_nested_struct_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedStructBatch& candidate_batch,
                                        std::string_view action);

Status advance_non_scalar_struct_children(StructColumnReader& struct_reader, bool parent_is_null,
                                          std::vector<MutableColumnPtr>& child_columns);

Status read_nested_struct_batch(StructColumnReader& struct_reader, int64_t batch_rows,
                                int16_t value_slot_definition_level, NestedStructBatch* batch);

Status read_nested_struct_batch_from_overflow(StructColumnReader& reader, int64_t batch_rows,
                                              int16_t value_slot_definition_level,
                                              NestedStructOverflow* overflow,
                                              NestedStructBatch* batch);

Status append_struct_batch_value(StructColumnReader& struct_reader, const NestedStructBatch& batch,
                                 int64_t level_idx, MutableColumnPtr& column);

ColumnArray* array_column_from_output(MutableColumnPtr& column);
ColumnMap* map_column_from_output(MutableColumnPtr& column);
ColumnStruct* struct_column_from_output(MutableColumnPtr& column);
NullMap* null_map_from_nullable_output(MutableColumnPtr& column);

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts);
void append_parent_nulls(NullMap* dst, const NullMap& src);

struct RepeatedParentSinkState {
    std::vector<uint64_t>* entry_counts = nullptr;
    NullMap* parent_nulls = nullptr;

    Status append_null_parent(const std::string& column_name, std::string_view parent_kind,
                              const DataTypePtr& type) const;
    void append_present_parent() const;
    Status add_entry(const std::string& column_name) const;
};

template <typename Sink>
Status assemble_repeated_levels(ScalarColumnReader& driver_reader, int16_t repeated_level,
                                int16_t value_slot_definition_level, int64_t rows,
                                NestedScalarOverflow* overflow, Sink& sink, int64_t* rows_read) {
    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(driver_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    return doris::parquet::assemble_repeated_levels<NestedScalarBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_scalar_tail, sink, rows_read);
}

template <typename Sink>
Status assemble_repeated_struct_levels(StructColumnReader& driver_reader, int16_t repeated_level,
                                       int16_t value_slot_definition_level, int64_t rows,
                                       NestedStructOverflow* overflow, Sink& sink,
                                       int64_t* rows_read) {
    auto read_batch = [&](int64_t batch_rows, NestedStructBatch* batch) {
        return read_nested_struct_batch(driver_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    return doris::parquet::assemble_repeated_levels<NestedStructBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_struct_tail, sink, rows_read);
}

} // namespace doris::parquet
