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
#include <optional>
#include <type_traits>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "common/compiler_util.h"
#include "common/exception.h"
#include "util/binary_cast.hpp"
#include "util/simd/bits.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"
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

template <TypeIndex TYPE_INDEX, typename NativeType>
struct WindowFunnelState {
    using DateValueType = std::conditional_t<TYPE_INDEX == TypeIndex::DateTimeV2,
                                             DateV2Value<DateTimeV2ValueType>, VecDateTimeValue>;
    int event_count = 0;
    int64_t window;
    bool enable_mode;
    WindowFunnelMode window_funnel_mode;
    mutable vectorized::MutableBlock mutable_block;
    ColumnVector<NativeType>::Container* timestamp_column_data;
    std::vector<ColumnVector<UInt8>::Container*> event_columns_datas;
    SortDescription sort_description {1};
    bool sorted;

    WindowFunnelState() {
        event_count = 0;
        window = 0;
        window_funnel_mode = WindowFunnelMode::INVALID;

        sort_description[0].column_number = 0;
        sort_description[0].direction = 1;
        sort_description[0].nulls_direction = -1;
        sorted = false;
    }
    WindowFunnelState(int arg_event_count) : WindowFunnelState() {
        event_count = arg_event_count;
        auto timestamp_column = ColumnVector<NativeType>::create();
        timestamp_column_data =
                &assert_cast<ColumnVector<NativeType>&>(*timestamp_column).get_data();

        MutableColumns event_columns;
        for (int i = 0; i < event_count; i++) {
            auto event_column = ColumnVector<UInt8>::create();
            event_columns_datas.emplace_back(
                    &assert_cast<ColumnVector<UInt8>&>(*event_column).get_data());
            event_columns.emplace_back(std::move(event_column));
        }
        Block tmp_block;
        tmp_block.insert({std::move(timestamp_column),
                          DataTypeFactory::instance().create_data_type(TYPE_INDEX), "timestamp"});
        for (int i = 0; i < event_count; i++) {
            tmp_block.insert({std::move(event_columns[i]),
                              DataTypeFactory::instance().create_data_type(TypeIndex::UInt8),
                              "event_" + std::to_string(i)});
        }

        mutable_block = MutableBlock(std::move(tmp_block));
    }

    void reset() {
        window = 0;
        mutable_block.clear();
        timestamp_column_data = nullptr;
        event_columns_datas.clear();
        sorted = false;
    }

    void add(const IColumn** arg_columns, ssize_t row_num, int64_t win, WindowFunnelMode mode) {
        window = win;
        window_funnel_mode = enable_mode ? mode : WindowFunnelMode::DEFAULT;

        timestamp_column_data->push_back(
                assert_cast<const ColumnVector<NativeType>&>(*arg_columns[2]).get_data()[row_num]);
        for (int i = 0; i < event_count; i++) {
            event_columns_datas[i]->push_back(
                    assert_cast<const ColumnVector<UInt8>&>(*arg_columns[3 + i])
                            .get_data()[row_num]);
        }
    }

    void sort() {
        if (sorted) {
            return;
        }

        Block tmp_block = mutable_block.to_block();
        auto block = tmp_block.clone_without_columns();
        sort_block(tmp_block, block, sort_description, 0);
        mutable_block = MutableBlock(std::move(block));
        sorted = true;
    }

    template <WindowFunnelMode WINDOW_FUNNEL_MODE>
    int _match_event_list(size_t& start_row, size_t row_count,
                          const NativeType* timestamp_data) const {
        int matched_count = 0;
        DateValueType start_timestamp;
        DateValueType end_timestamp;
        TimeInterval interval(SECOND, window, false);

        int column_idx = 1;
        const auto& first_event_column = mutable_block.get_column_by_position(column_idx);
        const auto& first_event_data =
                assert_cast<const ColumnVector<UInt8>&>(*first_event_column).get_data();
        auto match_row = simd::find_one(first_event_data.data(), start_row, row_count);
        start_row = match_row + 1;
        if (match_row < row_count) {
            auto prev_timestamp = binary_cast<NativeType, DateValueType>(timestamp_data[match_row]);
            end_timestamp = prev_timestamp;
            end_timestamp.template date_add_interval<SECOND>(interval);

            matched_count++;

            column_idx++;
            auto last_match_row = match_row;
            for (; column_idx < event_count + 1; column_idx++) {
                const auto& event_column = mutable_block.get_column_by_position(column_idx);
                const auto& event_data =
                        assert_cast<const ColumnVector<UInt8>&>(*event_column).get_data();
                if constexpr (WINDOW_FUNNEL_MODE == WindowFunnelMode::FIXED) {
                    ++match_row;
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
                match_row = simd::find_one(event_data.data(), match_row + 1, row_count);
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
                            for (int tmp_column_idx = 1; tmp_column_idx < column_idx;
                                 tmp_column_idx++) {
                                const auto& tmp_event_column =
                                        mutable_block.get_column_by_position(tmp_column_idx);
                                const auto& tmp_event_data =
                                        assert_cast<const ColumnVector<UInt8>&>(*tmp_event_column)
                                                .get_data();
                                auto dup_match_row = simd::find_one(tmp_event_data.data(),
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
        const auto& ts_column = mutable_block.get_column_by_position(0)->get_ptr();
        const auto& timestamp_data =
                assert_cast<const ColumnVector<NativeType>&>(*ts_column).get_data().data();

        auto row_count = mutable_block.rows();
        while (start_row < row_count) {
            auto found_event_count =
                    _match_event_list<WINDOW_FUNNEL_MODE>(start_row, row_count, timestamp_data);
            if (found_event_count == event_count) {
                return found_event_count;
            }
            max_found_event_count = std::max(max_found_event_count, found_event_count);
        }
        return max_found_event_count;
    }
    int get() const {
        auto row_count = mutable_block.rows();
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
        if (!other.mutable_block.empty()) {
            auto st = mutable_block.merge(other.mutable_block.to_block());
            if (!st.ok()) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
                return;
            }
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
        PBlock pblock;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        Status status;
        std::string buff;
        Block block = mutable_block.to_block();
        // as the branch-2.1 is used the new impl of window funnel, and the be_exec_version is 5
        // but in branch-3.0 this be_exec_version have update to 7, so when upgrade from branch-2.1 to branch-3.0
        // maybe have error send the branch-3.0 version of version 7 to branch-2.1([0---version--5])
        status = block.serialize(
                5, &pblock, &uncompressed_bytes, &compressed_bytes,
                segment_v2::CompressionTypePB::ZSTD); // ZSTD for better compression ratio
        if (!status.ok()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, status.to_string());
            return;
        }
        if (!pblock.SerializeToString(&buff)) {
            throw doris::Exception(ErrorCode::SERIALIZE_PROTOBUF_ERROR,
                                   "Serialize window_funnel data");
            return;
        }
        auto data_bytes = buff.size();
        write_var_uint(data_bytes, out);
        out.write(buff.data(), data_bytes);
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
        uint64_t data_bytes = 0;
        read_var_uint(data_bytes, in);
        std::string buff;
        buff.resize(data_bytes);
        in.read(buff.data(), data_bytes);

        PBlock pblock;
        if (!pblock.ParseFromArray(buff.data(), data_bytes)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Failed to parse window_funnel data to block");
        }
        Block block;
        auto status = block.deserialize(pblock);
        if (!status.ok()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, status.to_string());
        }
        mutable_block = MutableBlock(std::move(block));
    }
};

template <TypeIndex TYPE_INDEX, typename NativeType>
class AggregateFunctionWindowFunnel
        : public IAggregateFunctionDataHelper<
                  WindowFunnelState<TYPE_INDEX, NativeType>,
                  AggregateFunctionWindowFunnel<TYPE_INDEX, NativeType>> {
public:
    AggregateFunctionWindowFunnel(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<WindowFunnelState<TYPE_INDEX, NativeType>,
                                           AggregateFunctionWindowFunnel<TYPE_INDEX, NativeType>>(
                      argument_types_) {}

    void create(AggregateDataPtr __restrict place) const override {
        auto data = new (place) WindowFunnelState<TYPE_INDEX, NativeType>(
                IAggregateFunction::get_argument_types().size() - 3);
        /// support window funnel mode from 2.0. See `BeExecVersionManager::max_be_exec_version`
        data->enable_mode = version >= 3;
    }

    String get_name() const override { return "window_funnel"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt32>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& window =
                assert_cast<const ColumnVector<Int64>&>(*columns[0]).get_data()[row_num];
        StringRef mode = columns[1]->get_data_at(row_num);
        this->data(place).add(columns, row_num, window,
                              string_to_window_funnel_mode(mode.to_string()));
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
                        WindowFunnelState<TYPE_INDEX, NativeType>,
                        AggregateFunctionWindowFunnel<TYPE_INDEX, NativeType>>::data(place)
                        .get());
    }

protected:
    using IAggregateFunction::version;
};

template <typename DateValueType, typename NativeType>
struct WindowFunnelStateOld {
    std::vector<std::pair<DateValueType, int>> events;
    int max_event_level;
    bool sorted;
    int64_t window;
    WindowFunnelMode window_funnel_mode;
    bool enable_mode;

    WindowFunnelStateOld() {
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

    void merge(const WindowFunnelStateOld& other) {
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
class AggregateFunctionWindowFunnelOld
        : public IAggregateFunctionDataHelper<
                  WindowFunnelStateOld<DateValueType, NativeType>,
                  AggregateFunctionWindowFunnelOld<DateValueType, NativeType>> {
public:
    AggregateFunctionWindowFunnelOld(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      WindowFunnelStateOld<DateValueType, NativeType>,
                      AggregateFunctionWindowFunnelOld<DateValueType, NativeType>>(
                      argument_types_) {}

    void create(AggregateDataPtr __restrict place) const override {
        auto data = new (place) WindowFunnelStateOld<DateValueType, NativeType>();
        /// support window funnel mode from 2.0. See `BeExecVersionManager::max_be_exec_version`
        data->enable_mode = version >= 3;
    }

    String get_name() const override { return "window_funnel"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt32>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& window =
                assert_cast<const ColumnVector<Int64>&, TypeCheckOnRelease::DISABLE>(*columns[0])
                        .get_data()[row_num];
        StringRef mode = columns[1]->get_data_at(row_num);
        const auto& timestamp =
                assert_cast<const ColumnVector<NativeType>&, TypeCheckOnRelease::DISABLE>(
                        *columns[2])
                        .get_data()[row_num];
        const int NON_EVENT_NUM = 3;
        for (int i = NON_EVENT_NUM; i < IAggregateFunction::get_argument_types().size(); i++) {
            const auto& is_set =
                    assert_cast<const ColumnVector<UInt8>&, TypeCheckOnRelease::DISABLE>(
                            *columns[i])
                            .get_data()[row_num];
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
                        WindowFunnelStateOld<DateValueType, NativeType>,
                        AggregateFunctionWindowFunnelOld<DateValueType, NativeType>>::data(place)
                        .get());
    }

protected:
    using IAggregateFunction::version;
};

} // namespace doris::vectorized
