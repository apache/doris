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

#include <gen_cpp/data.pb.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "util/binary_cast.hpp"
#include "util/simd/bits.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/sort_block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/var_int.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
#include "common/compile_check_begin.h"
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

struct DataValue {
    using TimestampEvent = std::vector<ColumnUInt8::Container>;
    std::vector<UInt64> dt;
    TimestampEvent event_columns_data;
    bool operator<(const DataValue& other) const { return dt < other.dt; }
    void clear() {
        dt.clear();
        for (auto& data : event_columns_data) {
            data.clear();
        }
    }
    auto size() const { return dt.size(); }
    bool empty() const { return dt.empty(); }
    std::string debug_string() const {
        std::string result = "\n" + std::to_string(dt.size()) + " " +
                             std::to_string(event_columns_data[0].size()) + "\n";
        for (size_t i = 0; i < dt.size(); ++i) {
            result += std::to_string(dt[i]) + " VS " +
                      binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(dt[i]).debug_string() +
                      " ,";
            for (const auto& event : event_columns_data) {
                result += std::to_string(event[i]) + ",";
            }
            result += "\n";
        }
        return result;
    }
};

template <PrimitiveType PrimitiveType, typename NativeType>
struct WindowFunnelState {
    static_assert(PrimitiveType == PrimitiveType::TYPE_DATETIMEV2);
    using DateValueType = DateV2Value<DateTimeV2ValueType>;
    int event_count = 0;
    int64_t window;
    bool enable_mode;
    WindowFunnelMode window_funnel_mode;
    DataValue events_list;

    WindowFunnelState() {
        event_count = 0;
        window = 0;
        window_funnel_mode = WindowFunnelMode::INVALID;
    }
    WindowFunnelState(int arg_event_count) : WindowFunnelState() {
        event_count = arg_event_count;
        events_list.event_columns_data.resize(event_count);
    }

    void reset() { events_list.clear(); }

    void add(const IColumn** arg_columns, ssize_t row_num, int64_t win, WindowFunnelMode mode) {
        window = win;
        window_funnel_mode = enable_mode ? mode : WindowFunnelMode::DEFAULT;
        events_list.dt.emplace_back(assert_cast<const ColumnVector<PrimitiveType>&>(*arg_columns[2])
                                            .get_data()[row_num]);
        for (int i = 0; i < event_count; i++) {
            events_list.event_columns_data[i].emplace_back(
                    assert_cast<const ColumnUInt8&>(*arg_columns[3 + i]).get_data()[row_num]);
        }
    }

    void sort() {
        auto num = events_list.size();
        std::vector<size_t> indices(num);
        std::iota(indices.begin(), indices.end(), 0);
        std::sort(indices.begin(), indices.end(),
                  [this](size_t i1, size_t i2) { return events_list.dt[i1] < events_list.dt[i2]; });

        auto reorder = [&indices, &num](auto& vec) {
            std::decay_t<decltype(vec)> temp;
            temp.resize(num);
            for (auto i = 0; i < num; i++) {
                temp[i] = vec[indices[i]];
            }
            std::swap(vec, temp);
        };

        reorder(events_list.dt);
        for (auto& inner_vec : events_list.event_columns_data) {
            reorder(inner_vec);
        }
    }

    template <WindowFunnelMode WINDOW_FUNNEL_MODE>
    int _match_event_list(size_t& start_row, size_t row_count) const {
        int matched_count = 0;
        DateValueType start_timestamp;
        DateValueType end_timestamp;
        TimeInterval interval(SECOND, window, false);
        int column_idx = 0;
        const auto& timestamp_data = events_list.dt;
        const auto& first_event_data = events_list.event_columns_data[column_idx].data();
        auto match_row = simd::find_one(first_event_data, start_row, row_count);
        start_row = match_row + 1;
        if (match_row < row_count) {
            auto prev_timestamp = binary_cast<NativeType, DateValueType>(timestamp_data[match_row]);
            end_timestamp = prev_timestamp;
            end_timestamp.template date_add_interval<SECOND>(interval);

            matched_count++;
            column_idx++;
            auto last_match_row = match_row;
            ++match_row;
            for (; column_idx < event_count && match_row < row_count; column_idx++, match_row++) {
                const auto& event_data = events_list.event_columns_data[column_idx];
                if constexpr (WINDOW_FUNNEL_MODE == WindowFunnelMode::FIXED) {
                    if (event_data[match_row] == 1) {
                        auto current_timestamp =
                                binary_cast<NativeType, DateValueType>(timestamp_data[match_row]);
                        if (current_timestamp <= end_timestamp) {
                            matched_count++;
                            continue;
                        }
                    }
                    break;
                }
                match_row = simd::find_one(event_data.data(), match_row, row_count);
                if (match_row < row_count) {
                    auto current_timestamp =
                            binary_cast<NativeType, DateValueType>(timestamp_data[match_row]);
                    bool is_matched = current_timestamp <= end_timestamp;
                    if (is_matched) {
                        if constexpr (WINDOW_FUNNEL_MODE == WindowFunnelMode::INCREASE) {
                            is_matched = current_timestamp > prev_timestamp;
                        }
                    }
                    if (!is_matched) {
                        break;
                    }
                    if constexpr (WINDOW_FUNNEL_MODE == WindowFunnelMode::INCREASE) {
                        prev_timestamp =
                                binary_cast<NativeType, DateValueType>(timestamp_data[match_row]);
                    }
                    if constexpr (WINDOW_FUNNEL_MODE == WindowFunnelMode::DEDUPLICATION) {
                        bool is_dup = false;
                        if (match_row != last_match_row + 1) {
                            for (int tmp_column_idx = 0; tmp_column_idx < column_idx;
                                 tmp_column_idx++) {
                                const auto& tmp_event_data =
                                        events_list.event_columns_data[tmp_column_idx].data();
                                auto dup_match_row = simd::find_one(tmp_event_data,
                                                                    last_match_row + 1, match_row);
                                if (dup_match_row < match_row) {
                                    is_dup = true;
                                    break;
                                }
                            }
                        }
                        if (is_dup) {
                            break;
                        }
                        last_match_row = match_row;
                    }
                    matched_count++;
                } else {
                    break;
                }
            }
        }
        return matched_count;
    }

    template <WindowFunnelMode WINDOW_FUNNEL_MODE>
    int _get_internal() const {
        size_t start_row = 0;
        int max_found_event_count = 0;
        auto row_count = events_list.size();
        while (start_row < row_count) {
            auto found_event_count = _match_event_list<WINDOW_FUNNEL_MODE>(start_row, row_count);
            if (found_event_count == event_count) {
                return found_event_count;
            }
            max_found_event_count = std::max(max_found_event_count, found_event_count);
        }
        return max_found_event_count;
    }
    int get() const {
        auto row_count = events_list.size();
        if (event_count == 0 || row_count == 0) {
            return 0;
        }
        switch (window_funnel_mode) {
        case WindowFunnelMode::DEFAULT:
            return _get_internal<WindowFunnelMode::DEFAULT>();
        case WindowFunnelMode::DEDUPLICATION:
            return _get_internal<WindowFunnelMode::DEDUPLICATION>();
        case WindowFunnelMode::FIXED:
            return _get_internal<WindowFunnelMode::FIXED>();
        case WindowFunnelMode::INCREASE:
            return _get_internal<WindowFunnelMode::INCREASE>();
        default:
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Invalid window_funnel mode");
            return 0;
        }
    }

    void merge(const WindowFunnelState& other) {
        if (other.events_list.empty()) {
            return;
        }
        events_list.dt.insert(std::end(events_list.dt), std::begin(other.events_list.dt),
                              std::end(other.events_list.dt));
        for (size_t i = 0; i < event_count; i++) {
            events_list.event_columns_data[i].insert(
                    std::end(events_list.event_columns_data[i]),
                    std::begin(other.events_list.event_columns_data[i]),
                    std::end(other.events_list.event_columns_data[i]));
        }
        event_count = event_count > 0 ? event_count : other.event_count;
        window = window > 0 ? window : other.window;
        if (enable_mode) {
            window_funnel_mode = window_funnel_mode == WindowFunnelMode::INVALID
                                         ? other.window_funnel_mode
                                         : window_funnel_mode;
        } else {
            window_funnel_mode = WindowFunnelMode::DEFAULT;
        }
    }

    void write(BufferWritable& out) const {
        write_var_int(event_count, out);
        write_var_int(window, out);
        if (enable_mode) {
            write_var_int(static_cast<std::underlying_type_t<WindowFunnelMode>>(window_funnel_mode),
                          out);
        }
        auto size = events_list.size();
        write_var_int(size, out);
        for (const auto& timestamp : events_list.dt) {
            write_var_int(timestamp, out);
        }
        for (int64_t i = 0; i < event_count; i++) {
            const auto& event_columns_data = events_list.event_columns_data[i];
            for (auto event : event_columns_data) {
                write_var_int(event, out);
            }
        }
    }

    void read(BufferReadable& in) {
        int64_t event_level;
        read_var_int(event_level, in);
        event_count = (int)event_level;
        read_var_int(window, in);
        window_funnel_mode = WindowFunnelMode::DEFAULT;
        if (enable_mode) {
            int64_t mode;
            read_var_int(mode, in);
            window_funnel_mode = static_cast<WindowFunnelMode>(mode);
        }
        int64_t size = 0;
        read_var_int(size, in);
        events_list.clear();
        events_list.dt.resize(size);
        for (auto i = 0; i < size; i++) {
            read_var_int(*reinterpret_cast<Int64*>(&events_list.dt[i]), in);
        }
        events_list.event_columns_data.resize(event_count);
        for (int64_t i = 0; i < event_count; i++) {
            auto& event_columns_data = events_list.event_columns_data[i];
            event_columns_data.resize(size);
            for (auto j = 0; j < size; j++) {
                Int64 temp_value;
                read_var_int(temp_value, in);
                event_columns_data[j] = static_cast<UInt8>(temp_value);
            }
        }
    }
};

template <PrimitiveType PrimitiveType, typename NativeType>
class AggregateFunctionWindowFunnel
        : public IAggregateFunctionDataHelper<
                  WindowFunnelState<PrimitiveType, NativeType>,
                  AggregateFunctionWindowFunnel<PrimitiveType, NativeType>>,
          MultiExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionWindowFunnel(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      WindowFunnelState<PrimitiveType, NativeType>,
                      AggregateFunctionWindowFunnel<PrimitiveType, NativeType>>(argument_types_) {}

    void create(AggregateDataPtr __restrict place) const override {
        auto data = new (place) WindowFunnelState<PrimitiveType, NativeType>(
                cast_set<int>(IAggregateFunction::get_argument_types().size() - 3));
        /// support window funnel mode from 2.0. See `BeExecVersionManager::max_be_exec_version`
        data->enable_mode = IAggregateFunction::version >= 3;
    }

    String get_name() const override { return "window_funnel"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt32>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& window = assert_cast<const ColumnInt64&>(*columns[0]).get_data()[row_num];
        StringRef mode = columns[1]->get_data_at(row_num);
        this->data(place).add(columns, row_num, window,
                              string_to_window_funnel_mode(mode.to_string()));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(const_cast<AggregateDataPtr>(place)).sort();
        assert_cast<ColumnInt32&>(to).get_data().push_back(
                IAggregateFunctionDataHelper<
                        WindowFunnelState<PrimitiveType, NativeType>,
                        AggregateFunctionWindowFunnel<PrimitiveType, NativeType>>::data(place)
                        .get());
    }

protected:
    using IAggregateFunction::version;
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"
