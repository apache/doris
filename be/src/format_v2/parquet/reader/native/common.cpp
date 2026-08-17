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

#include "format_v2/parquet/reader/native/common.h"

#include <algorithm>
#include <cstring>

#include "core/types.h"
#include "util/simd/bits.h"

namespace doris::format::parquet::native {

Status FilterMap::init(const uint8_t* filter_map_data, size_t filter_map_size, bool filter_all) {
    _filter_all = filter_all;
    _filter_map_data = filter_map_data;
    _filter_map_size = filter_map_size;
    if (filter_all) {
        _has_filter = true;
        _filter_ratio = 1;
    } else if (filter_map_data == nullptr) {
        _has_filter = false;
        _filter_ratio = 0;
    } else {
        const size_t filter_count = simd::count_zero_num(
                reinterpret_cast<const int8_t*>(filter_map_data), filter_map_size);
        if (filter_count == filter_map_size) {
            _has_filter = true;
            _filter_all = true;
            _filter_ratio = 1;
        } else if (filter_count > 0 && filter_map_size > 0) {
            _has_filter = true;
            _filter_ratio = static_cast<double>(filter_count) / filter_map_size;
        } else {
            _has_filter = false;
            _filter_ratio = 0;
        }
    }
    return Status::OK();
}

bool FilterMap::can_filter_all(size_t remaining_num_values, size_t filter_map_index) {
    if (!_has_filter) {
        return false;
    }
    if (_filter_all) {
        DCHECK_LE(remaining_num_values + filter_map_index, _filter_map_size);
        return true;
    }
    if (remaining_num_values + filter_map_index > _filter_map_size) {
        return false;
    }
    return simd::count_zero_num(
                   reinterpret_cast<const int8_t*>(_filter_map_data + filter_map_index),
                   remaining_num_values) == remaining_num_values;
}

bool should_use_fused_nullable_selection(size_t num_values, size_t num_nulls,
                                         size_t num_null_runs) {
    constexpr size_t MIN_BATCH_VALUES = 1024;
    constexpr size_t MIN_NULL_RUNS = 32;
    constexpr size_t MAX_AVERAGE_NULL_RUN = 64;
    constexpr size_t MIN_NULL_RATIO_DENOMINATOR = 10;
    if (num_values < MIN_BATCH_VALUES || num_nulls < num_values / MIN_NULL_RATIO_DENOMINATOR) {
        return false;
    }
    return num_null_runs >= std::max(MIN_NULL_RUNS, num_values / MAX_AVERAGE_NULL_RUN);
}

Status build_filtered_nullable_selection(const std::vector<uint16_t>& run_length_null_map,
                                         size_t num_values, size_t num_nulls,
                                         NullMap* output_null_map, FilterMap* filter_map,
                                         size_t filter_map_index, ParquetSelection* selection,
                                         NullMap* selected_nulls, size_t* num_filtered) {
    if (output_null_map == nullptr || filter_map == nullptr || selection == nullptr ||
        selected_nulls == nullptr || num_filtered == nullptr) {
        return Status::InvalidArgument(
                "Nullable selection planning requires non-null output state");
    }
    if (!filter_map->has_filter()) {
        return Status::InvalidArgument("Nullable selection planning requires a row filter");
    }
    if (!filter_map->filter_all() &&
        (filter_map->filter_map_data() == nullptr ||
         filter_map_index + num_values > filter_map->filter_map_size())) {
        return Status::InvalidArgument("Nullable selection filter range [{}, {}) exceeds size {}",
                                       filter_map_index, filter_map_index + num_values,
                                       filter_map->filter_map_size());
    }
    if (num_nulls > num_values) {
        return Status::InvalidArgument("Nullable selection has {} nulls for {} values", num_nulls,
                                       num_values);
    }

    selection->ranges.clear();
    selection->total_values = num_values - num_nulls;
    selection->selected_values = 0;
    selected_nulls->clear();
    *num_filtered = 0;
    if (filter_map->filter_all()) {
        *num_filtered = num_values;
        return Status::OK();
    }

    selected_nulls->reserve(num_values);
    const uint8_t* filter = filter_map->filter_map_data() + filter_map_index;
    const auto select_physical_values = [&](size_t physical_index, size_t count) {
        if (!selection->ranges.empty() &&
            selection->ranges.back().first + selection->ranges.back().count == physical_index) {
            selection->ranges.back().count += count;
        } else {
            selection->ranges.push_back({.first = physical_index, .count = count});
        }
        selection->selected_values += count;
    };

    if (num_nulls == 0) {
        size_t row = 0;
        while (row < num_values) {
            const bool selected = filter[row] != 0;
            const size_t run_start = row++;
            while (row < num_values && (filter[row] != 0) == selected) {
                ++row;
            }
            const size_t run_length = row - run_start;
            if (selected) {
                select_physical_values(run_start, run_length);
            } else {
                *num_filtered += run_length;
            }
        }
        selected_nulls->resize_fill(selection->selected_values, 0);
    } else {
        size_t logical_index = 0;
        size_t physical_index = 0;
        size_t observed_nulls = 0;
        bool is_null = false;
        for (const size_t run_length : run_length_null_map) {
            if (logical_index + run_length > num_values) {
                return Status::InvalidArgument("Nullable selection run lengths exceed {} values",
                                               num_values);
            }
            const size_t run_end = logical_index + run_length;
            while (logical_index < run_end) {
                const bool selected = filter[logical_index] != 0;
                const size_t filter_run_start = logical_index++;
                while (logical_index < run_end && (filter[logical_index] != 0) == selected) {
                    ++logical_index;
                }
                const size_t filter_run_length = logical_index - filter_run_start;
                if (selected) {
                    selected_nulls->resize_fill(selected_nulls->size() + filter_run_length,
                                                static_cast<UInt8>(is_null));
                    if (!is_null) {
                        select_physical_values(physical_index, filter_run_length);
                    }
                } else {
                    *num_filtered += filter_run_length;
                }
                if (!is_null) {
                    physical_index += filter_run_length;
                } else {
                    observed_nulls += filter_run_length;
                }
            }
            is_null = !is_null;
        }
        if (logical_index != num_values || observed_nulls != num_nulls ||
            physical_index != selection->total_values) {
            return Status::InvalidArgument(
                    "Nullable selection level plan is inconsistent: values={}, nulls={}",
                    logical_index, observed_nulls);
        }
    }

    const size_t old_null_size = output_null_map->size();
    output_null_map->resize(old_null_size + selected_nulls->size());
    if (!selected_nulls->empty()) {
        memcpy(output_null_map->data() + old_null_size, selected_nulls->data(),
               selected_nulls->size());
    }
    return Status::OK();
}

Status FilterMap::generate_nested_filter_map(const std::vector<level_t>& rep_levels,
                                             std::vector<uint8_t>& nested_filter_map_data,
                                             std::unique_ptr<FilterMap>* nested_filter_map,
                                             size_t* current_row_ptr, size_t start_index) const {
    if (!has_filter() || filter_all()) {
        return Status::InternalError(
                "Native FilterMap requires a partial filter: has_filter={}, filter_all={}",
                has_filter(), filter_all());
    }
    if (rep_levels.empty()) {
        return Status::OK();
    }

    nested_filter_map_data.resize(rep_levels.size());
    size_t current_row = current_row_ptr != nullptr ? *current_row_ptr : 0;
    for (size_t i = start_index; i < rep_levels.size(); ++i) {
        if (i != start_index && rep_levels[i] == 0) {
            ++current_row;
            if (current_row >= _filter_map_size) {
                return Status::InvalidArgument("Nested filter row {} exceeds filter map size {}",
                                               current_row, _filter_map_size);
            }
        }
        nested_filter_map_data[i] = _filter_map_data[current_row];
    }
    if (current_row_ptr != nullptr) {
        *current_row_ptr = current_row;
    }

    auto new_filter = std::make_unique<FilterMap>();
    RETURN_IF_ERROR(
            new_filter->init(nested_filter_map_data.data(), nested_filter_map_data.size(), false));
    *nested_filter_map = std::move(new_filter);
    return Status::OK();
}

Status ColumnSelectVector::init(const std::vector<uint16_t>& run_length_null_map, size_t num_values,
                                NullMap* null_map, FilterMap* filter_map, size_t filter_map_index,
                                const std::unordered_set<size_t>* skipped_indices) {
    _num_values = num_values;
    _num_nulls = 0;
    _read_index = 0;
    size_t map_index = 0;
    bool is_null = false;
    _has_filter = filter_map->has_filter();

    if (_has_filter) {
        _data_map.resize(num_values);
        for (const auto run_length : run_length_null_map) {
            if (is_null) {
                _num_nulls += run_length;
                for (size_t i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_NULL;
                }
            } else {
                for (size_t i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_CONTENT;
                }
            }
            is_null = !is_null;
        }

        size_t num_read = 0;
        size_t source_index = 0;
        size_t valid_count = 0;
        while (valid_count < num_values) {
            DCHECK_LT(filter_map_index + source_index, filter_map->filter_map_size());
            if (skipped_indices != nullptr &&
                skipped_indices->contains(filter_map_index + source_index)) {
                ++source_index;
                continue;
            }
            if (filter_map->filter_map_data()[filter_map_index + source_index] != 0) {
                _data_map[valid_count] =
                        _data_map[valid_count] == FILTERED_NULL ? NULL_DATA : CONTENT;
                ++num_read;
            }
            ++valid_count;
            ++source_index;
        }
        _num_filtered = num_values - num_read;

        if (null_map != nullptr && num_read > 0) {
            size_t null_map_index = null_map->size();
            null_map->resize(null_map_index + num_read);
            if (_num_nulls == 0) {
                memset(null_map->data() + null_map_index, 0, num_read);
            } else if (_num_nulls == num_values) {
                memset(null_map->data() + null_map_index, 1, num_read);
            } else {
                for (size_t i = 0; i < num_values; ++i) {
                    if (_data_map[i] == CONTENT) {
                        (*null_map)[null_map_index++] = static_cast<UInt8>(false);
                    } else if (_data_map[i] == NULL_DATA) {
                        (*null_map)[null_map_index++] = static_cast<UInt8>(true);
                    }
                }
            }
        }
    } else {
        _num_filtered = 0;
        _run_length_null_map = &run_length_null_map;
        if (null_map != nullptr) {
            size_t null_map_index = null_map->size();
            null_map->resize(null_map_index + num_values);
            for (const auto run_length : run_length_null_map) {
                memset(null_map->data() + null_map_index, is_null ? 1 : 0, run_length);
                null_map_index += run_length;
                if (is_null) {
                    _num_nulls += run_length;
                }
                is_null = !is_null;
            }
        } else {
            for (const auto run_length : run_length_null_map) {
                if (is_null) {
                    _num_nulls += run_length;
                }
                is_null = !is_null;
            }
        }
    }
    return Status::OK();
}

void ColumnSelectVector::reset(bool has_filter) {
    _data_map.clear();
    _run_length_null_map = nullptr;
    _has_filter = has_filter;
    _num_values = 0;
    _num_nulls = 0;
    _num_filtered = 0;
    _read_index = 0;
}

Status ColumnSelectVector::init_nested(std::vector<level_t>* repetition_levels,
                                       std::vector<level_t>* definition_levels,
                                       size_t level_start_index, level_t repeated_parent_def_level,
                                       level_t definition_level, NullMap* null_map,
                                       FilterMap* parent_filter, size_t filter_map_index,
                                       size_t* ancestor_null_count) {
    if (repetition_levels == nullptr || definition_levels == nullptr || parent_filter == nullptr ||
        ancestor_null_count == nullptr) {
        return Status::InvalidArgument("Nested selection requires non-null input state");
    }
    if (repetition_levels->size() != definition_levels->size() ||
        level_start_index > repetition_levels->size()) {
        return Status::InvalidArgument(
                "Nested selection has invalid level bounds: repetition={}, definition={}, start={}",
                repetition_levels->size(), definition_levels->size(), level_start_index);
    }
    if (!parent_filter->has_filter() || parent_filter->filter_all()) {
        return Status::InvalidArgument("Nested selection requires a partial parent filter");
    }
    if (level_start_index == repetition_levels->size()) {
        reset(true);
        *ancestor_null_count = 0;
        return Status::OK();
    }
    if (filter_map_index >= parent_filter->filter_map_size()) {
        return Status::InvalidArgument("Nested filter row {} exceeds filter map size {}",
                                       filter_map_index, parent_filter->filter_map_size());
    }
    reset(true);
    _data_map.reserve(repetition_levels->size() - level_start_index);
    *ancestor_null_count = 0;
    size_t current_parent = filter_map_index;
    size_t output_level = level_start_index;
    for (size_t input_level = level_start_index; input_level < repetition_levels->size();
         ++input_level) {
        if (input_level != level_start_index && (*repetition_levels)[input_level] == 0) {
            ++current_parent;
            if (current_parent >= parent_filter->filter_map_size()) {
                return Status::InvalidArgument("Nested filter row {} exceeds filter map size {}",
                                               current_parent, parent_filter->filter_map_size());
            }
        }
        const bool selected = parent_filter->filter_map_data()[current_parent] != 0;
        if (selected) {
            (*repetition_levels)[output_level] = (*repetition_levels)[input_level];
            (*definition_levels)[output_level] = (*definition_levels)[input_level];
            ++output_level;
        }
        // An ancestor-null placeholder belongs to the surviving parent's level shape, but it must
        // never consume a leaf selection entry because no physical leaf value exists for it.
        const level_t def_level = (*definition_levels)[input_level];
        if (def_level < repeated_parent_def_level) {
            ++*ancestor_null_count;
            continue;
        }
        const bool is_null = def_level < definition_level;
        ++_num_values;
        _num_nulls += is_null;
        if (!selected) {
            ++_num_filtered;
        } else if (null_map != nullptr) {
            null_map->push_back(static_cast<UInt8>(is_null));
        }
        DataReadType read_type = is_null ? FILTERED_NULL : FILTERED_CONTENT;
        if (selected) {
            read_type = is_null ? NULL_DATA : CONTENT;
        }
        _data_map.push_back(read_type);
    }
    repetition_levels->resize(output_level);
    definition_levels->resize(output_level);
    return Status::OK();
}

} // namespace doris::format::parquet::native
