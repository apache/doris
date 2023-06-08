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
#include <sstream>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/io/io_helper.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

static const uint8_t kRowColShift = 8;
static const uint8_t kRowColMask = (1 << kRowColShift) - 1;

static const uint8_t kRetentionRowCount = 101;
static const uint8_t kRetentionColumnCount = kRetentionRowCount + 1;

struct RetentionAnalysisCountData {
    uint32_t retention_info[kRetentionRowCount][kRetentionColumnCount];

    RetentionAnalysisCountData() { reset(); }

    void reset() {
        for (int i = 0; i < kRetentionRowCount; i++) {
            for (int j = 0; j < kRetentionColumnCount; j++) {
                retention_info[i][j] = 0;
            }
        }
    }

    void add(const std::string& retention) {
        if (retention.empty()) {
            return;
        }
        std::set<uint16_t> src_row_column;
        parse_from_info(retention, &src_row_column);
        uint8_t row = 0;
        uint8_t column = 0;
        for (const auto& iter : src_row_column) {
            decode_row_col(iter, &row, &column);
            if (UNLIKELY(row >= kRetentionRowCount || column >= kRetentionColumnCount)) {
                continue;
            }
            retention_info[row][column]++;
        }
    }

    void merge(const RetentionAnalysisCountData& other) {
        for (int i = 0; i < kRetentionRowCount; i++) {
            for (int j = 0; j < kRetentionColumnCount; j++) {
                retention_info[i][j] += other.retention_info[i][j];
            }
        }
    }

    void write(BufferWritable& out) const {
        for (int i = 0; i < kRetentionRowCount; i++) {
            for (int j = 0; j < kRetentionColumnCount; j++) {
                write_var_uint(retention_info[i][j], out);
            }
        }
    }

    void serialize(BufferWritable& out) const {
        const size_t buffer_size = kRetentionRowCount * kRetentionColumnCount * 4;
        std::string serialized_events;
        serialized_events.resize(buffer_size);
        uint8_t* start_ptr = (uint8_t*)serialized_events.c_str();
        memset(start_ptr, 0, buffer_size);
        uint32_t* ptr = reinterpret_cast<uint32_t*>(start_ptr);
        for (int i = 0; i < kRetentionRowCount; i++) {
            for (int j = 0; j < kRetentionColumnCount; j++) {
                *ptr = retention_info[i][j];
                ++ptr;
            }
        }

        write_string_binary(serialized_events, out);
    }

    void read(BufferReadable& in) {
        uint64_t tmp;
        for (int i = 0; i < kRetentionRowCount; i++) {
            for (int j = 0; j < kRetentionColumnCount; j++) {
                read_var_uint(tmp, in);
                retention_info[i][j] = (uint32_t)tmp;
            }
        }
    }

    void deserialize(BufferReadable& in) {
        std::string serialized_events;
        read_string_binary(serialized_events, in);

        uint32_t* ptr = (uint32_t*)serialized_events.c_str();
        for (int i = 0; i < kRetentionRowCount; i++) {
            for (int j = 0; j < kRetentionColumnCount; j++) {
                retention_info[i][j] = *ptr;
                ++ptr;
            }
        }
    }

    std::string get() const {
        std::ostringstream oss;
        for (int32_t i = 0; i < kRetentionRowCount; i++) {
            for (int32_t j = 0; j < kRetentionColumnCount; j++) {
                uint32_t count = retention_info[i][j];
                if (count == 0) {
                    continue;
                }
                oss << i << ":" << j - 1 << ":" << count << ";";
            }
        }
        return oss.str();
    }

    void parse_from_info(const std::string str, std::set<uint16_t>* local) {
        DCHECK(local);
        uint16_t* index = (uint16_t*)(str.c_str());
        uint16_t* end_index = (uint16_t*)((uint8_t*)str.c_str() + str.size());
        while (index < end_index) {
            local->insert(*index);
            ++index;
        }
    }

    void decode_row_col(uint16_t v, uint8_t* row, uint8_t* column) {
        *row = v >> kRowColShift;
        *column = v & kRowColMask;
    }
};

class AggregateFunctionRetentionAnalysisCount
        : public IAggregateFunctionDataHelper<RetentionAnalysisCountData,
                                              AggregateFunctionRetentionAnalysisCount> {
public:
    AggregateFunctionRetentionAnalysisCount(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<RetentionAnalysisCountData,
                                           AggregateFunctionRetentionAnalysisCount>(
                      argument_types_) {}

    String get_name() const override { return "retention_count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
    void add(AggregateDataPtr __restrict place, const IColumn** columns, const size_t row_num,
             Arena*) const override {
        DCHECK_EQ(get_argument_types().size(), 1);

        std::string retention;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*columns[0])) {
            if (nullable->is_null_at(row_num)) {
                return;
            }
            auto column = nullable->get_nested_column_ptr();
            auto str_col = assert_cast<const ColumnString*>(column.get());
            const auto& offsets = str_col->get_offsets();
            if ((offsets[row_num] - offsets[row_num - 1]) == 0) {
                return;
            }
            retention = str_col->get_data_at(row_num).to_string();
        } else {
            retention =
                    assert_cast<const ColumnString*>(columns[0])->get_data_at(row_num).to_string();
        }

        this->data(place).add(retention);
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
