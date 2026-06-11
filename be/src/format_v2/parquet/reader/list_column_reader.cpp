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

#include "format_v2/parquet/reader/list_column_reader.h"

#include <parquet/api/schema.h>

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/parquet/reader/map_column_reader.h"
#include "format_v2/parquet/reader/nested_column_materializer.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"
#include "format_v2/parquet/reader/struct_column_reader.h"

namespace doris::parquet {
namespace {

void remove_nullable_wrapper_if_not_expected(const DataTypePtr& output_type,
                                             MutableColumnPtr* column) {
    DORIS_CHECK(column != nullptr);
    if (output_type->is_nullable()) {
        return;
    }
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(**column)) {
        *column = nullable_column->get_nested_column_ptr();
    }
}

} // namespace

Status ListColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    RETURN_IF_ERROR(load_nested_batch(rows));
    return build_nested_column(rows, column, rows_read);
}

Status ListColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    auto scratch_column = _type->create_column();
    RETURN_IF_ERROR(load_nested_batch(rows));
    int64_t rows_read = 0;
    RETURN_IF_ERROR(build_nested_column(rows, scratch_column, &rows_read));
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet LIST column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    update_reader_skip_rows(rows);
    return Status::OK();
}

Status ListColumnReader::load_nested_batch(int64_t rows) {
    DORIS_CHECK(_element_reader != nullptr);
    reset_nested_build_level_cursor();
    return _element_reader->load_nested_batch(rows);
}

Status ListColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                             int64_t* values_read) {
    if (column.get() == nullptr || values_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet list build result pointer for column {}",
                                       _name);
    }
    DORIS_CHECK(_element_reader != nullptr);
    auto* array_column = array_column_from_output(column);
    DORIS_CHECK(array_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto nested_column = array_column->get_data_ptr()->assert_mutable();
    const auto& element_output_type =
            assert_cast<const DataTypeArray&>(*remove_nullable(_type)).get_nested_type();
    remove_nullable_wrapper_if_not_expected(element_output_type, &nested_column);

    const auto& def_levels = _element_reader->nested_definition_levels();
    const auto& rep_levels = _element_reader->nested_repetition_levels();
    const int64_t levels_written = _element_reader->nested_levels_written();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    *values_read = 0;
    int64_t level_idx = nested_build_level_cursor();
    while (level_idx < levels_written) {
        const int16_t def_level = def_levels[level_idx];
        const int16_t rep_level = rep_levels[level_idx];
        const bool starts_parent = rep_level < _repetition_level;
        if (starts_parent && *values_read >= length_upper_bound) {
            break;
        }
        ++level_idx;
        if (def_level < _repeated_ancestor_definition_level || rep_level > _repetition_level) {
            continue;
        }
        if (rep_level == _repetition_level) {
            if (entry_counts.empty()) {
                return Status::Corruption("Invalid repeated level for parquet LIST column {}",
                                          _name);
            }
            if (def_level >= _definition_level) {
                ++entry_counts.back();
            }
            continue;
        }

        const bool parent_is_null = def_level < _definition_level - 1;
        if (parent_is_null && parent_null_map == nullptr) {
            return Status::Corruption("Parquet LIST column {} contains null for non-nullable LIST",
                                      _name);
        }
        parent_nulls.push_back(parent_is_null);
        entry_counts.push_back(def_level >= _definition_level ? 1 : 0);
        ++*values_read;
    }
    set_nested_build_level_cursor(level_idx);

    int64_t child_value_count = 0;
    uint64_t total_entries = 0;
    for (const auto entry_count : entry_counts) {
        total_entries += entry_count;
    }
    RETURN_IF_ERROR(_element_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                         nested_column, &child_value_count));
    if (child_value_count != static_cast<int64_t>(total_entries)) {
        return Status::Corruption("Parquet LIST column {} built {} child values, expected {}",
                                  _name, child_value_count, total_entries);
    }
    array_column->get_data_ptr() = std::move(nested_column);
    append_offsets(array_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

const std::vector<int16_t>& ListColumnReader::nested_definition_levels() const {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->nested_definition_levels();
}

const std::vector<int16_t>& ListColumnReader::nested_repetition_levels() const {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->nested_repetition_levels();
}

int64_t ListColumnReader::nested_levels_written() const {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->nested_levels_written();
}

bool ListColumnReader::is_or_has_repeated_child() const {
    return true;
}

} // namespace doris::parquet
