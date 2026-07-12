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

#include "format_v2/parquet/reader/list_column_reader.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/parquet/reader/nested_column_materializer.h"

namespace doris::format::parquet {
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
    return skip_nested_rows(rows);
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
    const int16_t min_parent_definition_level =
            static_cast<int16_t>(_definition_level - 1 - (_type->is_nullable() ? 1 : 0));
    while (level_idx < levels_written) {
        const int16_t def_level = def_levels[level_idx];
        const int16_t rep_level = rep_levels[level_idx];
        const bool starts_parent = rep_level < _repetition_level;
        if (starts_parent && *values_read >= length_upper_bound) {
            break;
        }
        ++level_idx;
        if (rep_level > _repetition_level || def_level < min_parent_definition_level ||
            (!starts_parent && def_level < _repeated_ancestor_definition_level)) {
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

    uint64_t total_entries = 0;
    int64_t child_value_count = 0;
    if (!_element_reader->is_or_has_repeated_child()) {
        for (const auto entry_count : entry_counts) {
            total_entries += entry_count;
        }
        RETURN_IF_ERROR(_element_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                             nested_column, &child_value_count));
    } else {
        uint64_t pending_entries = 0;
        auto flush_pending_entries = [&]() -> Status {
            if (pending_entries == 0) {
                return Status::OK();
            }
            int64_t span_child_value_count = 0;
            RETURN_IF_ERROR(_element_reader->build_nested_column(
                    static_cast<int64_t>(pending_entries), nested_column, &span_child_value_count));
            if (span_child_value_count != static_cast<int64_t>(pending_entries)) {
                return Status::Corruption(
                        "Parquet LIST column {} built {} child values, expected {}", _name,
                        span_child_value_count, pending_entries);
            }
            child_value_count += span_child_value_count;
            pending_entries = 0;
            return Status::OK();
        };

        for (const auto entry_count : entry_counts) {
            total_entries += entry_count;
            if (entry_count > 0) {
                pending_entries += entry_count;
                continue;
            }
            RETURN_IF_ERROR(flush_pending_entries());
            _element_reader->advance_nested_build_level_cursor_past_parent(_repetition_level);
        }
        RETURN_IF_ERROR(flush_pending_entries());
    }
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

void ListColumnReader::advance_nested_build_level_cursor_past_parent(
        int16_t parent_repetition_level) {
    DORIS_CHECK(_element_reader != nullptr);
    ParquetColumnReader::advance_nested_build_level_cursor_past_parent(parent_repetition_level);
    _element_reader->advance_nested_build_level_cursor_past_parent(parent_repetition_level);
}

} // namespace doris::format::parquet
