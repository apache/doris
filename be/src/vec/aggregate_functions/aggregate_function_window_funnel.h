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
    ColumnVector<NativeType>::Container* timestamp_column_data = nullptr;
    std::vector<ColumnVector<UInt8>::Container*> event_columns_datas;
    SortDescription sort_description {1};

    WindowFunnelState() {
        event_count = 0;
        window = 0;
        window_funnel_mode = WindowFunnelMode::INVALID;

        sort_description[0].column_number = 0;
        sort_description[0].direction = 1;
        sort_description[0].nulls_direction = -1;
    }
    WindowFunnelState(int arg_event_count) : WindowFunnelState() {
        event_count = arg_event_count;
        event_columns_datas.resize(event_count);
        auto timestamp_column = ColumnVector<NativeType>::create();

        MutableColumns event_columns;
        for (int i = 0; i < event_count; i++) {
            event_columns.emplace_back(ColumnVector<UInt8>::create());
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
        _reset_columns_ptr();
    }

    void _reset_columns_ptr() {
        auto& ts_column = mutable_block.get_column_by_position(0);
        timestamp_column_data = &assert_cast<ColumnVector<NativeType>&>(*ts_column).get_data();
        for (int i = 0; i != event_count; i++) {
            auto& event_column = mutable_block.get_column_by_position(i + 1);
            event_columns_datas[i] = &assert_cast<ColumnVector<UInt8>&>(*event_column).get_data();
        }
    }
    void reset() { mutable_block.clear_column_data(); }

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
        Block tmp_block = mutable_block.to_block();
        auto block = tmp_block.clone_without_columns();
        sort_block(tmp_block, block, sort_description, 0);
        mutable_block = std::move(block);
        _reset_columns_ptr();
    }

    template <WindowFunnelMode WINDOW_FUNNEL_MODE>
    int _match_event_list(size_t& start_row, size_t row_count) const {
        int matched_count = 0;
        DateValueType start_timestamp;
        DateValueType end_timestamp;
        TimeInterval interval(SECOND, window, false);

        int column_idx = 1;

        const NativeType* timestamp_data = timestamp_column_data->data();
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
            ++match_row;
            for (; column_idx < event_count + 1 && match_row < row_count;
                 column_idx++, match_row++) {
                const auto& event_column = mutable_block.get_column_by_position(column_idx);
                const auto& event_data =
                        assert_cast<const ColumnVector<UInt8>&>(*event_column).get_data();
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
        auto row_count = mutable_block.rows();
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
        block.clear_column_data();
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

        mutable_block = std::move(block);
        const_cast<WindowFunnelState<TYPE_INDEX, NativeType>*>(this)->_reset_columns_ptr();
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
        _reset_columns_ptr();
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

} // namespace doris::vectorized
