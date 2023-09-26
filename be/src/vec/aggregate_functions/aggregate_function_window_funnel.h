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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/compiler_util.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/var_int.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

enum class WindowFunnelMode : Int64 { INVALID, DEFAULT, DEDUPLICATION, FIXED, INCREASE };

WindowFunnelMode string_to_window_funnel_mode(const String& string) {
    if (string == "default") {
        return WindowFunnelMode::DEFAULT;
    } else if (string == "deduplication") {
        return WindowFunnelMode::DEDUPLICATION;
    } else if (string == "fixed") {
        return WindowFunnelMode::FIXED;
    } else if (string == "increase") {
        return WindowFunnelMode::INCREASE;
    } else {
        return WindowFunnelMode::INVALID;
    }
}

template <typename DateValueType, typename NativeType>
struct WindowFunnelState {
    std::vector<std::pair<DateValueType, int>> events;
    int max_event_level;
    bool sorted;
    int64_t window;
    WindowFunnelMode window_funnel_mode;
    bool enable_mode;

    WindowFunnelState() {
        sorted = true;
        max_event_level = 0;
        window = 0;
        window_funnel_mode = WindowFunnelMode::INVALID;
    }

    void reset() {
        sorted = true;
        max_event_level = 0;
        window = 0;
        events.shrink_to_fit();
    }

    void add(const DateValueType& timestamp, int event_idx, int event_num, int64_t win,
             WindowFunnelMode mode) {
        window = win;
        max_event_level = event_num;
        window_funnel_mode = enable_mode ? mode : WindowFunnelMode::DEFAULT;

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
        if (max_event_level == 0) {
            return 0;
        }
        std::vector<std::optional<std::pair<DateValueType, DateValueType>>> events_timestamp(
                max_event_level);
        bool is_first_set = false;
        for (int64_t i = 0; i < events.size(); i++) {
            const int& event_idx = events[i].second;
            const DateValueType& timestamp = events[i].first;
            if (event_idx == 0) {
                events_timestamp[0] = {timestamp, timestamp};
                is_first_set = true;
                continue;
            }
            if (window_funnel_mode == WindowFunnelMode::DEDUPLICATION &&
                events_timestamp[event_idx].has_value()) {
                break;
            }
            if (events_timestamp[event_idx - 1].has_value()) {
                const DateValueType& first_timestamp =
                        events_timestamp[event_idx - 1].value().first;
                DateValueType last_timestamp = first_timestamp;
                TimeInterval interval(SECOND, window, false);
                last_timestamp.template date_add_interval<SECOND>(interval);

                if (window_funnel_mode != WindowFunnelMode::INCREASE) {
                    if (timestamp <= last_timestamp) {
                        events_timestamp[event_idx] = {first_timestamp, timestamp};
                        if (event_idx + 1 == max_event_level) {
                            // Usually, max event level is small.
                            return max_event_level;
                        }
                    }
                } else {
                    if (timestamp <= last_timestamp &&
                        events_timestamp[event_idx - 1].value().second < timestamp) {
                        if (!events_timestamp[event_idx].has_value() ||
                            events_timestamp[event_idx].value().second > timestamp) {
                            events_timestamp[event_idx] = {first_timestamp, timestamp};
                        }
                        if (event_idx + 1 == max_event_level) {
                            // Usually, max event level is small.
                            return max_event_level;
                        }
                    }
                }
            } else {
                if (is_first_set && window_funnel_mode == WindowFunnelMode::FIXED) {
                    for (size_t i = 0; i < events_timestamp.size(); i++) {
                        if (!events_timestamp[i].has_value()) {
                            return i;
                        }
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
        if (enable_mode) {
            window_funnel_mode = window_funnel_mode == WindowFunnelMode::INVALID
                                         ? other.window_funnel_mode
                                         : window_funnel_mode;
        } else {
            window_funnel_mode = WindowFunnelMode::DEFAULT;
        }
        sorted = true;
    }

    void write(BufferWritable& out) const {
        write_var_int(max_event_level, out);
        write_var_int(window, out);
        if (enable_mode) {
            write_var_int(static_cast<std::underlying_type_t<WindowFunnelMode>>(window_funnel_mode),
                          out);
        }
        write_var_int(events.size(), out);

        for (int64_t i = 0; i < events.size(); i++) {
            int64_t timestamp = binary_cast<DateValueType, NativeType>(events[i].first);
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
        window_funnel_mode = WindowFunnelMode::DEFAULT;
        if (enable_mode) {
            int64_t mode;
            read_var_int(mode, in);
            window_funnel_mode = static_cast<WindowFunnelMode>(mode);
        }
        int64_t size = 0;
        read_var_int(size, in);
        for (int64_t i = 0; i < size; i++) {
            int64_t timestamp;
            int64_t event_idx;

            read_var_int(timestamp, in);
            read_var_int(event_idx, in);
            DateValueType time_value = binary_cast<NativeType, DateValueType>(timestamp);
            add(time_value, (int)event_idx, max_event_level, window, window_funnel_mode);
        }
    }
};

template <typename DateValueType, typename NativeType>
class AggregateFunctionWindowFunnel
        : public IAggregateFunctionDataHelper<
                  WindowFunnelState<DateValueType, NativeType>,
                  AggregateFunctionWindowFunnel<DateValueType, NativeType>> {
public:
    AggregateFunctionWindowFunnel(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      WindowFunnelState<DateValueType, NativeType>,
                      AggregateFunctionWindowFunnel<DateValueType, NativeType>>(argument_types_) {}

    void create(AggregateDataPtr __restrict place) const override {
        auto data = new (place) WindowFunnelState<DateValueType, NativeType>();
        /// support window funnel mode from 2.0. See `BeExecVersionManager::max_be_exec_version`
        data->enable_mode = version >= 3;
    }

    String get_name() const override { return "window_funnel"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt32>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& window =
                assert_cast<const ColumnVector<Int64>&>(*columns[0]).get_data()[row_num];
        StringRef mode = columns[1]->get_data_at(row_num);
        const auto& timestamp =
                assert_cast<const ColumnVector<NativeType>&>(*columns[2]).get_data()[row_num];
        const int NON_EVENT_NUM = 3;
        for (int i = NON_EVENT_NUM; i < IAggregateFunction::get_argument_types().size(); i++) {
            const auto& is_set =
                    assert_cast<const ColumnVector<UInt8>&>(*columns[i]).get_data()[row_num];
            if (is_set) {
                this->data(place).add(
                        binary_cast<NativeType, DateValueType>(timestamp), i - NON_EVENT_NUM,
                        IAggregateFunction::get_argument_types().size() - NON_EVENT_NUM, window,
                        string_to_window_funnel_mode(mode.to_string()));
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
        assert_cast<ColumnInt32&>(to).get_data().push_back(
                IAggregateFunctionDataHelper<
                        WindowFunnelState<DateValueType, NativeType>,
                        AggregateFunctionWindowFunnel<DateValueType, NativeType>>::data(place)
                        .get());
    }

protected:
    using IAggregateFunction::version;
};

} // namespace doris::vectorized
