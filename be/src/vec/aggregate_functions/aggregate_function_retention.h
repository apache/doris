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

// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/AggregateFunctionRetention.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/var_int.h"

namespace doris::vectorized {
struct RetentionState {
    static constexpr size_t MAX_EVENTS = 32;
    uint8_t events[MAX_EVENTS] = {0};

    RetentionState() {}

    void reset() {
        for (int64_t i = 0; i < MAX_EVENTS; i++) {
            events[i] = 0;
        }
    }

    void set(int event) { events[event] = 1; }

    void merge(const RetentionState& other) {
        for (int64_t i = 0; i < MAX_EVENTS; i++) {
            events[i] |= other.events[i];
        }
    }

    void write(BufferWritable& out) const {
        int64_t serialized_events = 0;
        for (int64_t i = 0; i < MAX_EVENTS; i++) {
            serialized_events |= events[i];
            serialized_events <<= 1;
        }
        write_var_int(serialized_events, out);
    }

    void read(BufferReadable& in) {
        int64_t serialized_events = 0;
        uint64_t u_serialized_events = 0;
        read_var_int(serialized_events, in);
        u_serialized_events = serialized_events;

        u_serialized_events >>= 1;
        for (int64_t i = MAX_EVENTS - 1; i >= 0; i--) {
            events[i] = (uint8)(1 & u_serialized_events);
            u_serialized_events >>= 1;
        }
    }

    void insert_result_into(IColumn& to, size_t events_size, const uint8_t* events) const {
        auto& data_to = assert_cast<ColumnUInt8&>(to).get_data();

        ColumnArray::Offset64 current_offset = data_to.size();
        data_to.resize(current_offset + events_size);

        bool first_flag = events[0];
        data_to[current_offset] = first_flag;
        ++current_offset;

        for (size_t i = 1; i < events_size; ++i) {
            data_to[current_offset] = (first_flag && events[i]);
            ++current_offset;
        }
    }
};

class AggregateFunctionRetention
        : public IAggregateFunctionDataHelper<RetentionState, AggregateFunctionRetention> {
public:
    AggregateFunctionRetention(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<RetentionState, AggregateFunctionRetention>(
                      argument_types_, {}) {}

    String get_name() const override { return "retention"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeUInt8>()));
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
    void add(AggregateDataPtr __restrict place, const IColumn** columns, const size_t row_num,
             Arena*) const override {
        for (int i = 0; i < get_argument_types().size(); i++) {
            auto event = assert_cast<const ColumnVector<UInt8>*>(columns[i])->get_data()[row_num];
            if (event) {
                this->data(place).set(i);
            }
        }
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
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column(),
                                                 get_argument_types().size(),
                                                 this->data(place).events);
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col, get_argument_types().size(),
                                                 this->data(place).events);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }
};
} // namespace doris::vectorized