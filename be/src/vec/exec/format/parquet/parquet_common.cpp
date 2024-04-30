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

#include "parquet_common.h"

#include <glog/logging.h>

#include "util/simd/bits.h"
#include "vec/core/types.h"

namespace doris::vectorized {

const int32_t ParquetInt96::JULIAN_EPOCH_OFFSET_DAYS = 2440588;
const int64_t ParquetInt96::MICROS_IN_DAY = 86400000000;
const int64_t ParquetInt96::NANOS_PER_MICROSECOND = 1000;

ColumnSelectVector::ColumnSelectVector(const uint8_t* filter_map, size_t filter_map_size,
                                       bool filter_all) {
    build(filter_map, filter_map_size, filter_all);
}

void ColumnSelectVector::build(const uint8_t* filter_map, size_t filter_map_size, bool filter_all) {
    _filter_all = filter_all;
    _filter_map = filter_map;
    _filter_map_size = filter_map_size;
    if (filter_all) {
        _has_filter = true;
        _filter_ratio = 1;
    } else if (filter_map == nullptr) {
        _has_filter = false;
        _filter_ratio = 0;
    } else {
        size_t filter_count =
                simd::count_zero_num(reinterpret_cast<const int8_t*>(filter_map), filter_map_size);
        if (filter_count == filter_map_size) {
            _has_filter = true;
            _filter_all = true;
            _filter_ratio = 1;
        } else if (filter_count > 0 && filter_map_size > 0) {
            _has_filter = true;
            _filter_ratio = (double)filter_count / filter_map_size;
        } else {
            _has_filter = false;
            _filter_ratio = 0;
        }
    }
}

void ColumnSelectVector::set_run_length_null_map(const std::vector<uint16_t>& run_length_null_map,
                                                 size_t num_values, NullMap* null_map) {
    _num_values = num_values;
    _num_nulls = 0;
    _read_index = 0;
    size_t map_index = 0;
    bool is_null = false;
    if (_has_filter) {
        // No run length null map is generated when _filter_all = true
        DCHECK(!_filter_all);
        _data_map.resize(num_values);
        for (auto& run_length : run_length_null_map) {
            if (is_null) {
                _num_nulls += run_length;
                for (int i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_NULL;
                }
            } else {
                for (int i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_CONTENT;
                }
            }
            is_null = !is_null;
        }
        size_t num_read = 0;
        DCHECK_LE(_filter_map_index + num_values, _filter_map_size);
        for (size_t i = 0; i < num_values; ++i) {
            if (_filter_map[_filter_map_index++]) {
                _data_map[i] = _data_map[i] == FILTERED_NULL ? NULL_DATA : CONTENT;
                num_read++;
            }
        }
        _num_filtered = num_values - num_read;
        if (null_map != nullptr && num_read > 0) {
            NullMap& map_data_column = *null_map;
            auto null_map_index = map_data_column.size();
            map_data_column.resize(null_map_index + num_read);
            if (_num_nulls == 0) {
                memset(map_data_column.data() + null_map_index, 0, num_read);
            } else if (_num_nulls == num_values) {
                memset(map_data_column.data() + null_map_index, 1, num_read);
            } else {
                for (size_t i = 0; i < num_values; ++i) {
                    if (_data_map[i] == CONTENT) {
                        map_data_column[null_map_index++] = (UInt8) false;
                    } else if (_data_map[i] == NULL_DATA) {
                        map_data_column[null_map_index++] = (UInt8) true;
                    }
                }
            }
        }
    } else {
        _num_filtered = 0;
        _run_length_null_map = &run_length_null_map;
        if (null_map != nullptr) {
            NullMap& map_data_column = *null_map;
            auto null_map_index = map_data_column.size();
            map_data_column.resize(null_map_index + num_values);

            for (auto& run_length : run_length_null_map) {
                if (is_null) {
                    memset(map_data_column.data() + null_map_index, 1, run_length);
                    null_map_index += run_length;
                    _num_nulls += run_length;
                } else {
                    memset(map_data_column.data() + null_map_index, 0, run_length);
                    null_map_index += run_length;
                }
                is_null = !is_null;
            }
        } else {
            for (auto& run_length : run_length_null_map) {
                if (is_null) {
                    _num_nulls += run_length;
                }
                is_null = !is_null;
            }
        }
    }
}

bool ColumnSelectVector::can_filter_all(size_t remaining_num_values) {
    if (!_has_filter) {
        return false;
    }
    if (_filter_all) {
        // all data in normal columns can be skipped when _filter_all = true,
        // so the remaining_num_values should be less than the remaining filter map size.
        DCHECK_LE(remaining_num_values + _filter_map_index, _filter_map_size);
        // return true always, to make sure that the data in normal columns can be skipped.
        return true;
    }
    if (remaining_num_values + _filter_map_index > _filter_map_size) {
        return false;
    }
    return simd::count_zero_num(reinterpret_cast<const int8_t*>(_filter_map + _filter_map_index),
                                remaining_num_values) == remaining_num_values;
}

void ColumnSelectVector::skip(size_t num_values) {
    _filter_map_index += num_values;
}
} // namespace doris::vectorized
