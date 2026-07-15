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
    return skip_nested_rows(rows);
}

Status MapColumnReader::load_nested_batch(int64_t rows) {
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    reset_nested_build_level_cursor();
    RETURN_IF_ERROR(_key_reader->load_nested_batch(rows));
    return _value_reader->load_nested_batch(rows);
}

Status MapColumnReader::load_nested_levels_batch(int64_t rows) {
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    reset_nested_build_level_cursor();
    RETURN_IF_ERROR(_key_reader->load_nested_levels_batch(rows));
    return _value_reader->load_nested_levels_batch(rows);
}

Status MapColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                            int64_t* values_read) {
    if (column.get() == nullptr) {
        return Status::InvalidArgument("Invalid parquet map build result pointer for column {}",
                                       _name);
    }
    return _consume_or_build_nested_column(length_upper_bound, &column, values_read);
}

Status MapColumnReader::consume_nested_column(int64_t length_upper_bound,
                                              int64_t* values_consumed) {
    return _consume_or_build_nested_column(length_upper_bound, nullptr, values_consumed);
}

Status MapColumnReader::_consume_or_build_nested_column(int64_t length_upper_bound,
                                                        MutableColumnPtr* column,
                                                        int64_t* values_processed) {
    if (values_processed == nullptr) {
        return Status::InvalidArgument("Invalid parquet map process result pointer for column {}",
                                       _name);
    }
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    ColumnMap* map_column = nullptr;
    NullMap* parent_null_map = nullptr;
    MutableColumnPtr key_column;
    MutableColumnPtr value_column;
    if (column != nullptr) {
        map_column = map_column_from_output(*column);
        DORIS_CHECK(map_column != nullptr);
        parent_null_map = null_map_from_nullable_output(*column);
        key_column = map_column->get_keys_ptr()->assert_mutable();
        value_column = map_column->get_values_ptr()->assert_mutable();
        const auto& map_output_type = assert_cast<const DataTypeMap&>(*remove_nullable(_type));
        remove_nullable_wrapper_if_not_expected(map_output_type.get_key_type(), &key_column);
        remove_nullable_wrapper_if_not_expected(map_output_type.get_value_type(), &value_column);
    }

    const auto& def_levels = _key_reader->nested_definition_levels();
    const auto& rep_levels = _key_reader->nested_repetition_levels();
    const int64_t levels_written = _key_reader->nested_levels_written();

    _entry_counts.clear();
    _map_level_indices.clear();
    _parent_nulls.clear();
    *values_processed = 0;
    int64_t level_idx = nested_build_level_cursor();
    const int16_t min_parent_definition_level =
            static_cast<int16_t>(_definition_level - 1 - (_type->is_nullable() ? 1 : 0));
    while (level_idx < levels_written) {
        const int16_t def_level = def_levels[level_idx];
        const int16_t rep_level = rep_levels[level_idx];
        const bool starts_parent = rep_level < _repetition_level;
        if (starts_parent && *values_processed >= length_upper_bound) {
            break;
        }
        const int64_t current_level_idx = level_idx;
        ++level_idx;
        if (rep_level > _repetition_level || def_level < min_parent_definition_level ||
            (!starts_parent && def_level < _repeated_ancestor_definition_level)) {
            continue;
        }
        _map_level_indices.push_back(current_level_idx);
        if (rep_level == _repetition_level) {
            if (_entry_counts.empty()) {
                return Status::Corruption("Invalid repeated level for parquet MAP column {}",
                                          _name);
            }
            if (def_level >= _definition_level) {
                ++_entry_counts.back();
            }
            continue;
        }

        const bool parent_is_null = def_level < _definition_level - 1;
        if (parent_is_null && !_type->is_nullable()) {
            return Status::Corruption("Parquet MAP column {} contains null for non-nullable MAP",
                                      _name);
        }
        _parent_nulls.push_back(parent_is_null);
        _entry_counts.push_back(def_level >= _definition_level ? 1 : 0);
        ++*values_processed;
    }
    set_nested_build_level_cursor(level_idx);

    uint64_t total_entries = 0;
    for (const auto entry_count : _entry_counts) {
        total_entries += entry_count;
    }
    int64_t key_value_count = 0;
    size_t key_start = 0;
    if (column != nullptr) {
        key_start = key_column->size();
        RETURN_IF_ERROR(_key_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                         key_column, &key_value_count));
    } else if (auto* scalar_key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get())) {
        // MAP keys are required even if a projected Doris key type is nullable. Validate each
        // actual entry directly from the key level stream while advancing past empty/null maps.
        for (const int64_t key_level_idx : _map_level_indices) {
            if (def_levels[key_level_idx] >= _definition_level) {
                RETURN_IF_ERROR(scalar_key_reader->validate_nested_value(key_level_idx, true));
                ++key_value_count;
            }
        }
        scalar_key_reader->set_nested_build_level_cursor(level_idx);
    } else {
        RETURN_IF_ERROR(_key_reader->consume_nested_column(static_cast<int64_t>(total_entries),
                                                           &key_value_count));
    }
    if (key_value_count != static_cast<int64_t>(total_entries)) {
        return Status::Corruption("Parquet MAP column {} built {} keys, expected {}", _name,
                                  key_value_count, total_entries);
    }
    if (column != nullptr) {
        if (const auto* nullable_key_column = check_and_get_column<ColumnNullable>(*key_column);
            nullable_key_column != nullptr &&
            nullable_key_column->has_null(key_start, nullable_key_column->size())) {
            return Status::Corruption("Parquet MAP column {} contains null key", _name);
        }
    }
    int64_t value_count = 0;
    if (auto* scalar_value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get())) {
        const auto& value_def_levels = scalar_value_reader->nested_definition_levels();
        const auto& value_rep_levels = scalar_value_reader->nested_repetition_levels();
        const int64_t value_levels_written = scalar_value_reader->nested_levels_written();
        int64_t value_level_idx = scalar_value_reader->nested_build_level_cursor();
        for (const int64_t key_level_idx : _map_level_indices) {
            while (value_level_idx < value_levels_written &&
                   (value_rep_levels[value_level_idx] > _repetition_level ||
                    value_def_levels[value_level_idx] < min_parent_definition_level ||
                    (value_rep_levels[value_level_idx] >= _repetition_level &&
                     value_def_levels[value_level_idx] < _repeated_ancestor_definition_level))) {
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
                if (column != nullptr) {
                    RETURN_IF_ERROR(scalar_value_reader->append_nested_value(value_level_idx,
                                                                             value_column));
                } else {
                    RETURN_IF_ERROR(
                            scalar_value_reader->validate_nested_value(value_level_idx, false));
                }
                ++value_count;
            }
            ++value_level_idx;
        }
        scalar_value_reader->set_nested_build_level_cursor(value_level_idx);
    } else {
        // Complex MAP values own their nested shape below the entry slot, so they recursively
        // process exactly one child value for each MAP entry.
        if (column != nullptr) {
            RETURN_IF_ERROR(_value_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                               value_column, &value_count));
        } else {
            RETURN_IF_ERROR(_value_reader->consume_nested_column(
                    static_cast<int64_t>(total_entries), &value_count));
        }
    }
    if (value_count != static_cast<int64_t>(total_entries)) {
        return Status::Corruption("Parquet MAP column {} built {} values, expected {}", _name,
                                  value_count, total_entries);
    }

    if (column != nullptr) {
        map_column->get_keys_ptr() = std::move(key_column);
        map_column->get_values_ptr() = std::move(value_column);
        append_offsets(map_column->get_offsets(), _entry_counts);
        append_parent_nulls(parent_null_map, _parent_nulls);
    }
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

void MapColumnReader::advance_nested_build_level_cursor_past_parent(
        int16_t parent_repetition_level) {
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    ParquetColumnReader::advance_nested_build_level_cursor_past_parent(parent_repetition_level);
    _key_reader->advance_nested_build_level_cursor_past_parent(parent_repetition_level);
    _value_reader->advance_nested_build_level_cursor_past_parent(parent_repetition_level);
}

} // namespace doris::format::parquet
