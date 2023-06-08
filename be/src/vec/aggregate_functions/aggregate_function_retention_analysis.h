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

#include <set>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/io/io_helper.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

static const int kRetentionEventCount = 101;
static const uint8_t kRowColShift = 8;
static const uint8_t kRowColMask = (1 << kRowColShift) - 1;

static const int32_t UTC16 = 16 * 60 * 60;
static const int32_t UTC8 = 8 * 60 * 60;
static const int32_t DAY_TIME_SEC = 24 * 60 * 60;
static const int32_t WEEK_TIME_SEC = 24 * 60 * 60 * 7;

struct RetentionAnalysisData {
    uint32_t first_event[kRetentionEventCount] = {0};
    uint32_t retention_event[kRetentionEventCount] = {0};
    bool same_event = false;

    RetentionAnalysisData() { reset(); }

    void reset() {
        for (int64_t i = 0; i < kRetentionEventCount; i++) {
            first_event[i] = 0;
            retention_event[i] = 0;
        }
        same_event = false;
    }

    void add(const int64_t start_time, const std::string unit, const int64_t event_time,
             const int32_t event_type) {
        if (event_type == 0 || unit.empty() || event_time < start_time) {
            return;
        }

        uint32_t st = start_time / 1000; // uint32_t is enough to hold a timestamp in seconds
        uint32_t et = event_time / 1000;
        int16_t index = get_delta_periods(st, et, unit);
        if (index < 0 || index >= kRetentionEventCount) {
            return;
        }

        switch (event_type) {
        case 1:
            add_first_event(index, et);
            break;
        case 2:
            add_retention_event(index, et);
            break;
        case 3:
            add_first_event(index, et);
            add_retention_event(index, et);
            same_event = true;
            break;
        default:
            break;
        }
    }

    void merge(const RetentionAnalysisData& other) {
        for (int16_t i = 0; i < kRetentionEventCount; i++) {
            // make value smaller
            if (first_event[i] == 0 || (first_event[i] > 0 && other.first_event[i] > 0 &&
                                        first_event[i] > other.first_event[i])) {
                first_event[i] = other.first_event[i];
            }

            // make value larger
            if (retention_event[i] == 0 ||
                (retention_event[i] > 0 && other.retention_event[i] > 0 &&
                 retention_event[i] < other.retention_event[i])) {
                retention_event[i] = other.retention_event[i];
            }
        }
        if (other.same_event) {
            same_event = other.same_event;
        }
    }

    void write(BufferWritable& out) const {
        for (int i = 0; i < kRetentionEventCount; i++) {
            uint64_t tmp = (((uint64_t)first_event[i] << 32) | (uint64_t)retention_event[i]);
            write_var_uint(tmp, out);
        }
        write_binary(same_event, out);
    }

    void serialize(BufferWritable& out) const {
        const size_t buffer_size = kRetentionEventCount * 8 + 1;
        std::string serialized_events;
        serialized_events.resize(buffer_size);
        uint8_t* start_ptr = (uint8_t*)serialized_events.c_str();
        memset(start_ptr, 0, buffer_size);
        uint64_t* ptr = reinterpret_cast<uint64_t*>(start_ptr);
        for (int i = 0; i < kRetentionEventCount; i++) {
            uint64_t tmp_1 = (uint64_t)first_event[i];
            uint64_t tmp_2 = (uint64_t)retention_event[i];
            uint64_t tmp = (uint64_t)((tmp_1 << 32) | tmp_2);
            *ptr = tmp;
            ++ptr;
        }
        bool* p = reinterpret_cast<bool*>(ptr);
        *p = same_event;

        write_string_binary(serialized_events, out);
    }

    void read(BufferReadable& in) {
        uint64_t tmp;
        for (int i = 0; i < kRetentionEventCount; i++) {
            read_var_uint(tmp, in);
            first_event[i] = (uint32_t)(tmp >> 32);
            retention_event[i] = (uint32_t)(tmp & (((uint64_t)1 << 32) - 1));
        }
        read_binary(same_event, in);
    }

    void deserialize(BufferReadable& in) {
        std::string serialized_events;
        read_string_binary(serialized_events, in);

        uint64_t* ptr = (uint64_t*)serialized_events.c_str();
        for (int i = 0; i < kRetentionEventCount; i++) {
            uint64_t tmp = *ptr;
            first_event[i] = (uint32_t)(tmp >> 32);
            retention_event[i] = (uint32_t)(tmp & (((uint64_t)1 << 32) - 1));
            ++ptr;
        }
        bool* p = reinterpret_cast<bool*>(ptr);
        same_event = *p;
    }

    std::string get() const {
        int8_t row_max_index = kRetentionEventCount - 1;
        for (; row_max_index >= 0; row_max_index--) {
            if (first_event[row_max_index] != 0 || row_max_index == 0) {
                break;
            }
        }

        int8_t column_max_index = kRetentionEventCount - 1;
        for (; column_max_index >= 0; column_max_index--) {
            if (retention_event[column_max_index] != 0 || column_max_index == 0) {
                break;
            }
        }

        std::set<uint16_t> local_set;
        for (uint8_t row = 0; row <= row_max_index; row++) {
            uint32_t first_ts = first_event[row];
            if (first_ts == 0) {
                continue;
            }

            local_set.insert(encode_row_col(row, 0));
            if (same_event) {
                local_set.insert(encode_row_col(row, 1));
            }

            for (uint8_t column = row; column <= column_max_index; column++) {
                uint32_t retention_ts = retention_event[column];
                if (retention_ts > first_ts) {
                    local_set.insert(encode_row_col(row, column - row + 1));
                }
            }
        }

        const size_t buffer_size = local_set.size() * 2;
        std::string result;
        result.resize(buffer_size);
        uint8_t* start_ptr = (uint8_t*)result.c_str();
        memset(start_ptr, 0, buffer_size);
        uint16_t* ptr = reinterpret_cast<uint16_t*>(start_ptr);
        for (const auto& iter : local_set) {
            *ptr = iter;
            ++ptr;
        }

        return result;
    }

    int16_t get_delta_periods(uint32_t start_time, uint32_t event_time, const std::string& unit) {
        switch (*(char*)unit.c_str()) {
        case 'd':
            return ((int32_t)get_start_of_day(event_time) - (int32_t)get_start_of_day(start_time)) /
                   DAY_TIME_SEC;
        case 'w':
            return ((int32_t)get_start_of_week(event_time) -
                    (int32_t)get_start_of_week(start_time)) /
                   WEEK_TIME_SEC;
        case 'm': {
            time_t s = start_time;
            time_t e = event_time;
            std::tm start_tm = *localtime(&s);
            std::tm event_tm = *localtime(&e);
            return event_tm.tm_mon - start_tm.tm_mon;
        }
        default:
            return -1;
        }
    }

    uint32_t get_start_of_day(uint32_t ts) {
        uint32_t mod = (ts - UTC16) % DAY_TIME_SEC;
        return ts - mod;
    }

    uint32_t get_start_of_week(uint32_t ts) {
        // 1970-01-01 is Thursday
        const static uint32_t week_offset = 4 * 86400;
        uint32_t mod = (ts + UTC8 - week_offset) % WEEK_TIME_SEC;
        return ts - mod;
    }

    void add_first_event(int16_t index, uint32_t event_time) {
        DCHECK_LT(index, kRetentionEventCount);
        if (first_event[index] == 0 || first_event[index] > event_time) {
            first_event[index] = event_time;
        }
    }

    void add_retention_event(int16_t index, uint32_t event_time) {
        DCHECK_LT(index, kRetentionEventCount);
        if (retention_event[index] == 0 || retention_event[index] < event_time) {
            retention_event[index] = event_time;
        }
    }

    uint16_t encode_row_col(uint8_t row, uint8_t column) const {
        return (row << kRowColShift) | column;
    }
};

class AggregateFunctionRetentionAnalysis
        : public IAggregateFunctionDataHelper<RetentionAnalysisData,
                                              AggregateFunctionRetentionAnalysis> {
public:
    AggregateFunctionRetentionAnalysis(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<RetentionAnalysisData,
                                           AggregateFunctionRetentionAnalysis>(argument_types_) {}

    String get_name() const override { return "retention_info"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
    void add(AggregateDataPtr __restrict place, const IColumn** columns, const size_t row_num,
             Arena*) const override {
        DCHECK_EQ(get_argument_types().size(), 4);

        Int64 start_time;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[0])) {
            if (nullable->is_null_at(row_num)) {
                return;
            }
            auto column = nullable->get_nested_column_ptr();
            start_time = assert_cast<const ColumnVector<Int64>*>(column.get())->get_data()[row_num];
        } else {
            start_time = assert_cast<const ColumnVector<Int64>*>(columns[0])->get_data()[row_num];
        }

        std::string unit;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[1])) {
            auto column = nullable->get_nested_column_ptr();
            if (nullable->is_null_at(row_num)) {
                return;
            }
            auto str_col = assert_cast<const ColumnString*>(column.get());
            const auto& offsets = str_col->get_offsets();
            if ((offsets[row_num] - offsets[row_num - 1]) == 0) {
                return;
            }
            unit = str_col->get_data_at(row_num).to_string();
        } else {
            unit = assert_cast<const ColumnString*>(columns[1])->get_data_at(row_num).to_string();
        }

        Int64 event_time;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[2])) {
            if (nullable->is_null_at(row_num)) {
                return;
            }
            auto column = nullable->get_nested_column_ptr();
            event_time = assert_cast<const ColumnVector<Int64>*>(column.get())->get_data()[row_num];
        } else {
            event_time = assert_cast<const ColumnVector<Int64>*>(columns[2])->get_data()[row_num];
        }

        Int32 event_type;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[3])) {
            if (nullable->is_null_at(row_num)) {
                return;
            }
            auto column = nullable->get_nested_column_ptr();
            event_type = assert_cast<const ColumnVector<Int32>*>(column.get())->get_data()[row_num];
        } else {
            event_type = assert_cast<const ColumnVector<Int32>*>(columns[3])->get_data()[row_num];
        }

        this->data(place).add(start_time, unit, event_time, event_type);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        std::string result = this->data(place).get();
        if (to.is_nullable()) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(0);
            assert_cast<ColumnString&>(null_column.get_nested_column())
                    .insert_data(result.c_str(), result.length());
        } else {
            assert_cast<ColumnString&>(to).insert_data(result.c_str(), result.length());
        }
    }
};
} // namespace doris::vectorized
