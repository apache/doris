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

#include "format_v2/parquet/reader/map_column_reader.h"

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/parquet/reader/nested_column_materializer.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"

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

Status MapColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    RETURN_IF_ERROR(load_nested_batch(rows));
    return build_nested_column(rows, column, rows_read);
}

Status MapColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    auto scratch_column = _type->create_column();
    RETURN_IF_ERROR(load_nested_batch(rows));
    int64_t rows_read = 0;
    RETURN_IF_ERROR(build_nested_column(rows, scratch_column, &rows_read));
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet MAP column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    update_reader_skip_rows(rows);
    return Status::OK();
}

Status MapColumnReader::load_nested_batch(int64_t rows) {
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    reset_nested_build_level_cursor();
    RETURN_IF_ERROR(_key_reader->load_nested_batch(rows));
    return _value_reader->load_nested_batch(rows);
}

// MAP 的嵌套构建核心逻辑：
//
// 从 key reader 的 def/rep levels 重建 ColumnMap：
//
// 1. 遍历 key reader 的 def/rep levels，解析 entry 结构（同 LIST，key stream 提供 shape）。
// 2. 委托 key reader 的 build_nested_column() 填充所有 key 值。
// 3. key null 校验：检查 key 列中是否存在 NULL，有则报错（MAP key 不允许 NULL）。
// 4. value 填充分两条路径：
//    a. ScalarColumnReader 路径：value 与 key 在 level 流中一一对应（same rep level），
//       通过 append_nested_value() 逐 entry 填充 value。
//    b. 复杂 value 路径（如 MAP<INT, ARRAY<INT>>）：value 拥有自己的嵌套 shape，
//       直接 build_nested_column(total_entries) 递归填充。
// 5. append_offsets() + append_parent_nulls() 写入 ColumnMap 结构。
Status MapColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                            int64_t* values_read) {
    if (column.get() == nullptr || values_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet map build result pointer for column {}",
                                       _name);
    }
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    auto* map_column = map_column_from_output(column);
    DORIS_CHECK(map_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto key_column = map_column->get_keys_ptr()->assert_mutable();
    auto value_column = map_column->get_values_ptr()->assert_mutable();
    const auto& map_output_type = assert_cast<const DataTypeMap&>(*remove_nullable(_type));
    remove_nullable_wrapper_if_not_expected(map_output_type.get_key_type(), &key_column);
    remove_nullable_wrapper_if_not_expected(map_output_type.get_value_type(), &value_column);

    const auto& def_levels = _key_reader->nested_definition_levels();
    const auto& rep_levels = _key_reader->nested_repetition_levels();
    const int64_t levels_written = _key_reader->nested_levels_written();

    std::vector<uint64_t> entry_counts;
    std::vector<int64_t> map_level_indices;
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
        const int64_t current_level_idx = level_idx;
        ++level_idx;
        if (def_level < _repeated_ancestor_definition_level || rep_level > _repetition_level) {
            continue;
        }
        map_level_indices.push_back(current_level_idx);
        if (rep_level == _repetition_level) {
            if (entry_counts.empty()) {
                return Status::Corruption("Invalid repeated level for parquet MAP column {}",
                                          _name);
            }
            if (def_level >= _definition_level) {
                ++entry_counts.back();
            }
            continue;
        }

        const bool parent_is_null = def_level < _definition_level - 1;
        if (parent_is_null && parent_null_map == nullptr) {
            return Status::Corruption("Parquet MAP column {} contains null for non-nullable MAP",
                                      _name);
        }
        parent_nulls.push_back(parent_is_null);
        entry_counts.push_back(def_level >= _definition_level ? 1 : 0);
        ++*values_read;
    }
    set_nested_build_level_cursor(level_idx);

    uint64_t total_entries = 0;
    for (const auto entry_count : entry_counts) {
        total_entries += entry_count;
    }
    const size_t key_start = key_column->size();
    int64_t key_value_count = 0;
    RETURN_IF_ERROR(_key_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                     key_column, &key_value_count));
    if (key_value_count != static_cast<int64_t>(total_entries)) {
        return Status::Corruption("Parquet MAP column {} built {} keys, expected {}", _name,
                                  key_value_count, total_entries);
    }
    if (const auto* nullable_key_column = check_and_get_column<ColumnNullable>(*key_column);
        nullable_key_column != nullptr &&
        nullable_key_column->has_null(key_start, nullable_key_column->size())) {
        return Status::Corruption("Parquet MAP column {} contains null key", _name);
    }
    int64_t value_count = 0;
    if (auto* scalar_value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get())) {
        const auto& value_def_levels = scalar_value_reader->nested_definition_levels();
        const auto& value_rep_levels = scalar_value_reader->nested_repetition_levels();
        const int64_t value_levels_written = scalar_value_reader->nested_levels_written();
        int64_t value_level_idx = scalar_value_reader->nested_build_level_cursor();
        for (const int64_t key_level_idx : map_level_indices) {
            while (value_level_idx < value_levels_written &&
                   (value_def_levels[value_level_idx] < _repeated_ancestor_definition_level ||
                    value_rep_levels[value_level_idx] > _repetition_level)) {
                ++value_level_idx;
            }
            if (value_level_idx >= value_levels_written) {
                return Status::Corruption(
                        "Parquet MAP column {} value stream ended before key stream", _name);
            }
            // MAP is encoded as a repeated key/value struct. The key stream owns entry existence,
            // but the value stream still has one shape slot for every consumed MAP slot. Consume
            // value slots in lockstep with key slots so shape-only slots from empty/null maps do
            // not become scalar values.
            if (value_rep_levels[value_level_idx] != rep_levels[key_level_idx]) {
                return Status::Corruption(
                        "Parquet MAP column {} value repetition level is not aligned with key "
                        "stream",
                        _name);
            }
            if (def_levels[key_level_idx] >= _definition_level) {
                RETURN_IF_ERROR(
                        scalar_value_reader->append_nested_value(value_level_idx, value_column));
                ++value_count;
            }
            ++value_level_idx;
        }
        scalar_value_reader->set_nested_build_level_cursor(value_level_idx);
    } else {
        // Complex MAP values own their nested shape below the entry slot, so they can recursively
        // materialize exactly one child value for each MAP entry.
        RETURN_IF_ERROR(_value_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                           value_column, &value_count));
    }
    if (value_count != static_cast<int64_t>(total_entries)) {
        return Status::Corruption("Parquet MAP column {} built {} values, expected {}", _name,
                                  value_count, total_entries);
    }

    map_column->get_keys_ptr() = std::move(key_column);
    map_column->get_values_ptr() = std::move(value_column);
    append_offsets(map_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

const std::vector<int16_t>& MapColumnReader::nested_definition_levels() const {
    DORIS_CHECK(_key_reader != nullptr);
    return _key_reader->nested_definition_levels();
}

const std::vector<int16_t>& MapColumnReader::nested_repetition_levels() const {
    DORIS_CHECK(_key_reader != nullptr);
    return _key_reader->nested_repetition_levels();
}

int64_t MapColumnReader::nested_levels_written() const {
    DORIS_CHECK(_key_reader != nullptr);
    return _key_reader->nested_levels_written();
}

bool MapColumnReader::is_or_has_repeated_child() const {
    return true;
}

} // namespace doris::format::parquet
