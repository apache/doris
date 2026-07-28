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

} // namespace doris::format::parquet::native
