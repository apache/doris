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

#include <stddef.h>

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "vec/columns/column_nullable.h"

namespace doris::vectorized {

using level_t = int16_t;

struct RowRange {
    RowRange() = default;
    RowRange(int64_t first, int64_t last) : first_row(first), last_row(last) {}

    int64_t first_row;
    int64_t last_row;

    bool operator<(const RowRange& range) const { return first_row < range.first_row; }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "[" << first_row << "," << last_row << ")";
        return ss.str();
    }
};

#pragma pack(1)
struct ParquetInt96 {
    int64_t lo; // time of nanoseconds in a day
    int32_t hi; // days from julian epoch

    inline int64_t to_timestamp_micros() const {
        return (hi - JULIAN_EPOCH_OFFSET_DAYS) * MICROS_IN_DAY + lo / NANOS_PER_MICROSECOND;
    }
    inline __int128 to_int128() const {
        __int128 ans = 0;
        ans = (((__int128)hi) << 64) + lo;
        return ans;
    }

    static const int32_t JULIAN_EPOCH_OFFSET_DAYS;
    static const int64_t MICROS_IN_DAY;
    static const int64_t NANOS_PER_MICROSECOND;
};
#pragma pack()
static_assert(sizeof(ParquetInt96) == 12, "The size of ParquetInt96 is not 12.");

class ColumnSelectVector {
public:
    enum DataReadType : uint8_t { CONTENT = 0, NULL_DATA, FILTERED_CONTENT, FILTERED_NULL };

    ColumnSelectVector(const uint8_t* filter_map, size_t filter_map_size, bool filter_all);

    ColumnSelectVector() = default;

    void build(const uint8_t* filter_map, size_t filter_map_size, bool filter_all);

    const uint8_t* filter_map() { return _filter_map; }

    size_t num_values() const { return _num_values; }

    size_t num_nulls() const { return _num_nulls; }

    size_t num_filtered() const { return _num_filtered; }

    double filter_ratio() const { return _has_filter ? _filter_ratio : 0; }

    void fallback_filter() { _has_filter = false; }

    bool has_filter() const { return _has_filter; }

    bool can_filter_all(size_t remaining_num_values);

    bool filter_all() const { return _filter_all; }

    void skip(size_t num_values);

    void reset() {
        if (_has_filter) {
            _filter_map_index = 0;
        }
    }

    template <bool has_filter>
    size_t get_next_run(DataReadType* data_read_type) {
        DCHECK_EQ(_has_filter, has_filter);
        if constexpr (has_filter) {
            if (_read_index == _num_values) {
                return 0;
            }
            const DataReadType& type = _data_map[_read_index++];
            size_t run_length = 1;
            while (_read_index < _num_values) {
                if (_data_map[_read_index] == type) {
                    run_length++;
                    _read_index++;
                } else {
                    break;
                }
            }
            *data_read_type = type;
            return run_length;
        } else {
            size_t run_length = 0;
            while (run_length == 0) {
                if (_read_index == (*_run_length_null_map).size()) {
                    return 0;
                }
                *data_read_type = _read_index % 2 == 0 ? CONTENT : NULL_DATA;
                run_length = (*_run_length_null_map)[_read_index++];
            }
            return run_length;
        }
    }

    void set_run_length_null_map(const std::vector<uint16_t>& run_length_null_map,
                                 size_t num_values, NullMap* null_map = nullptr);

private:
    std::vector<DataReadType> _data_map;
    // the length of non-null values and null values are arranged in turn.
    const std::vector<uint16_t>* _run_length_null_map;
    bool _has_filter = false;
    // only used when the whole batch is skipped
    bool _filter_all = false;
    const uint8_t* _filter_map = nullptr;
    size_t _filter_map_size = 0;
    double _filter_ratio = 0;
    size_t _filter_map_index = 0;

    // generated in set_run_length_null_map
    size_t _num_values;
    size_t _num_nulls;
    size_t _num_filtered;
    size_t _read_index;
};
} // namespace doris::vectorized