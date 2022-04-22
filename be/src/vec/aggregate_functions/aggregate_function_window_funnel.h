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
// https://github.com/ClickHouse/ClickHouse/blob/master/AggregateFunctionWindowFunnel.h
// and modified by Doris

#pragma once

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/var_int.h"

namespace doris::vectorized {

struct WindowFunnelState {
    std::vector<std::pair<VecDateTimeValue, int>> events;
    int max_event_level;
    bool sorted;
    int64_t window;

    WindowFunnelState() {
        sorted = true;
        max_event_level = 0;
        window = 0;
    }

    void reset() {
        sorted = true;
        max_event_level = 0;
        window = 0;
        events.shrink_to_fit();
    }

    void add(const VecDateTimeValue& timestamp, int event_idx, int event_num, int64_t win) {
        window = win;
        max_event_level = event_num;
        if (sorted && events.size() > 0) {
            if (events.back().first == timestamp) {
                sorted = events.back().second <= event_idx;
            } else {
                sorted = events.back().first < timestamp;
            }
        }
        events.emplace_back(timestamp, event_idx);
    }

    void sort() {
        if (sorted) {
            return;
        }
        std::stable_sort(events.begin(), events.end());
    }

    int get() const {
        std::vector<std::optional<VecDateTimeValue>> events_timestamp(max_event_level);
        for (int64_t i = 0; i < events.size(); i++) {
            const int& event_idx = events[i].second;
            const VecDateTimeValue& timestamp = events[i].first;
            if (event_idx == 0) {
                events_timestamp[0] = timestamp;
                continue;
            }
            if (events_timestamp[event_idx - 1].has_value()) {
                const VecDateTimeValue& first_timestamp = events_timestamp[event_idx - 1].value();
                VecDateTimeValue last_timestamp = first_timestamp;
                TimeInterval interval(SECOND, window, false);
                last_timestamp.date_add_interval(interval, SECOND);

                if (timestamp <= last_timestamp) {
                    events_timestamp[event_idx] = first_timestamp;
                    if (event_idx + 1 == max_event_level) {
                        // Usually, max event level is small.
                        return max_event_level;
                    }
                }
            }
        }

        for (int64_t i = events_timestamp.size() - 1; i >= 0; i--) {
            if (events_timestamp[i].has_value()) {
                return i + 1;
            }
        }

        return 0;
    }

    void merge(const WindowFunnelState& other) {
        if (other.events.empty()) {
            return;
        }

        int64_t orig_size = events.size();
        events.insert(std::end(events), std::begin(other.events), std::end(other.events));
        const auto begin = std::begin(events);
        const auto middle = std::next(events.begin(), orig_size);
        const auto end = std::end(events);
        if (!other.sorted) {
            std::stable_sort(middle, end);
        }

        if (!sorted) {
            std::stable_sort(begin, middle);
        }
        std::inplace_merge(begin, middle, end);
        max_event_level = max_event_level > 0 ? max_event_level : other.max_event_level;
        window = window > 0 ? window : other.window;

        sorted = true;
    }

    void write(BufferWritable &out) const {
        write_var_int(max_event_level, out);
        write_var_int(window, out);
        write_var_int(events.size(), out);

        for (int64_t i = 0; i < events.size(); i++) {
            int64_t timestamp =
                    binary_cast<vectorized::VecDateTimeValue, vectorized::Int64>(events[i].first);
            int event_idx = events[i].second;
            write_var_int(timestamp, out);
            write_var_int(event_idx, out);
        }
    }

    void read(BufferReadable& in) {
        int64_t event_level;
        read_var_int(event_level, in);
        max_event_level = (int)event_level;
        read_var_int(window, in);
        int64_t size = 0;
        read_var_int(size, in);
        for (int64_t i = 0; i < size; i++) {
            int64_t timestamp;
            int64_t event_idx;

            read_var_int(timestamp, in);
            read_var_int(event_idx, in);
            VecDateTimeValue time_value =
                    binary_cast<vectorized::Int64, vectorized::VecDateTimeValue>(timestamp);
            add(time_value, (int)event_idx, max_event_level, window);
        }
    }
};

class AggregateFunctionWindowFunnel
        : public IAggregateFunctionDataHelper<WindowFunnelState,
                                              AggregateFunctionWindowFunnel> {
public:
    AggregateFunctionWindowFunnel(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<WindowFunnelState,
                                           AggregateFunctionWindowFunnel>(argument_types_, {}) {
    }

    String get_name() const override { return "window_funnel"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeInt32>();
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& window = static_cast<const ColumnVector<Int64>&>(*columns[0]).get_data()[row_num];
        // TODO: handle mode in the future.
        // be/src/olap/row_block2.cpp copy_data_to_column
        const auto& timestamp = static_cast<const ColumnVector<VecDateTimeValue>&>(*columns[2]).get_data()[row_num];
        const int NON_EVENT_NUM = 3;
        for (int i = NON_EVENT_NUM; i < get_argument_types().size(); i++) {
            const auto& is_set = static_cast<const ColumnVector<UInt8>&>(*columns[i]).get_data()[row_num];
            if (is_set) {
                this->data(place).add(timestamp, i - NON_EVENT_NUM,
                                      get_argument_types().size() - NON_EVENT_NUM, window);
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
        this->data(const_cast<AggregateDataPtr>(place)).sort();
        assert_cast<ColumnInt32&>(to).get_data().push_back(data(place).get());
    }
};

} // namespace doris::vectorized
