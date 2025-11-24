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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.h
// and modified by Doris

#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <memory>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_reader_first_last.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;

struct RowNumberData {
    int64_t count = 0;
};

class WindowFunctionRowNumber final
        : public IAggregateFunctionDataHelper<RowNumberData, WindowFunctionRowNumber> {
public:
    WindowFunctionRowNumber(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "row_number"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, ssize_t, Arena&) const override {
        ++data(place).count;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionRowNumber::data(place).count = 0;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                doris::vectorized::WindowFunctionRowNumber::data(place).count);
    }

    bool result_column_could_resize() const override { return true; }

    void insert_result_into_range(ConstAggregateDataPtr __restrict place, IColumn& to,
                                  const size_t start, const size_t end) const override {
        auto& column = assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to);
        for (size_t i = start; i < end; ++i) {
            column.get_data()[i] = (doris::vectorized::WindowFunctionRowNumber::data(place).count);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {}
};

struct RankData {
    int64_t rank = 0;
    int64_t count = 1;
    int64_t peer_group_start = -1;
};

class WindowFunctionRank final : public IAggregateFunctionDataHelper<RankData, WindowFunctionRank> {
public:
    WindowFunctionRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, ssize_t, Arena&) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        int64_t peer_group_count = frame_end - frame_start;
        if (WindowFunctionRank::data(place).peer_group_start != frame_start) {
            WindowFunctionRank::data(place).peer_group_start = frame_start;
            WindowFunctionRank::data(place).rank += WindowFunctionRank::data(place).count;
        }
        WindowFunctionRank::data(place).count = peer_group_count;
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionRank::data(place).rank = 0;
        WindowFunctionRank::data(place).count = 1;
        WindowFunctionRank::data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                data(place).rank);
    }

    bool result_column_could_resize() const override { return true; }

    void insert_result_into_range(ConstAggregateDataPtr __restrict place, IColumn& to,
                                  const size_t start, const size_t end) const override {
        auto& column = assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to);
        for (size_t i = start; i < end; ++i) {
            column.get_data()[i] = (doris::vectorized::WindowFunctionRank::data(place).rank);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {}
};

struct DenseRankData {
    int64_t rank = 0;
    int64_t peer_group_start = -1;
};

class WindowFunctionDenseRank final
        : public IAggregateFunctionDataHelper<DenseRankData, WindowFunctionDenseRank> {
public:
    WindowFunctionDenseRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "dense_rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, ssize_t, Arena&) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        if (WindowFunctionDenseRank::data(place).peer_group_start != frame_start) {
            WindowFunctionDenseRank::data(place).peer_group_start = frame_start;
            WindowFunctionDenseRank::data(place).rank++;
        }
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionDenseRank::data(place).rank = 0;
        WindowFunctionDenseRank::data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                data(place).rank);
    }

    bool result_column_could_resize() const override { return true; }

    void insert_result_into_range(ConstAggregateDataPtr __restrict place, IColumn& to,
                                  const size_t start, const size_t end) const override {
        auto& column = assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to);
        for (size_t i = start; i < end; ++i) {
            column.get_data()[i] = (doris::vectorized::WindowFunctionDenseRank::data(place).rank);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {}
};

struct PercentRankData {
    int64_t rank = 0;
    int64_t count = 1;
    int64_t peer_group_start = -1;
    int64_t partition_size = 0;
};

class WindowFunctionPercentRank final
        : public IAggregateFunctionDataHelper<PercentRankData, WindowFunctionPercentRank> {
private:
    static double _cal_percent(int64_t rank, int64_t total_rows) {
        return total_rows <= 1 ? 0.0 : double(rank - 1) * 1.0 / double(total_rows - 1);
    }

public:
    WindowFunctionPercentRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "percent_rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr place, const IColumn**, ssize_t, Arena&) const override {}

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        int64_t peer_group_count = frame_end - frame_start;
        if (WindowFunctionPercentRank::data(place).peer_group_start != frame_start) {
            WindowFunctionPercentRank::data(place).peer_group_start = frame_start;
            WindowFunctionPercentRank::data(place).rank +=
                    WindowFunctionPercentRank::data(place).count;
            // some variables are partition related, but there is no chance to init them
            // when the new partition arrives, so we calculate them every time now.
            WindowFunctionPercentRank::data(place).partition_size = partition_end - partition_start;
        }
        WindowFunctionPercentRank::data(place).count = peer_group_count;
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionPercentRank::data(place).rank = 0;
        WindowFunctionPercentRank::data(place).count = 1;
        WindowFunctionPercentRank::data(place).peer_group_start = -1;
        WindowFunctionPercentRank::data(place).partition_size = 0;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        auto percent_rank = _cal_percent(data(place).rank, data(place).partition_size);
        assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                percent_rank);
    }

    bool result_column_could_resize() const override { return true; }

    void insert_result_into_range(ConstAggregateDataPtr __restrict place, IColumn& to,
                                  const size_t start, const size_t end) const override {
        auto& column = assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to);
        auto percent_rank = _cal_percent(data(place).rank, data(place).partition_size);
        for (size_t i = start; i < end; ++i) {
            column.get_data()[i] = percent_rank;
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {}
};

struct CumeDistData {
    int64_t numerator = 0;
    int64_t denominator = 0;
    int64_t peer_group_start = -1;
};

class WindowFunctionCumeDist final
        : public IAggregateFunctionDataHelper<CumeDistData, WindowFunctionCumeDist> {
private:
    static void check_default(AggregateDataPtr place, int64_t partition_start,
                              int64_t partition_end) {
        if (data(place).denominator == 0) {
            data(place).denominator = partition_end - partition_start;
        }
    }

public:
    WindowFunctionCumeDist(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "cume_dist"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr place, const IColumn**, ssize_t, Arena&) const override {}

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        check_default(place, partition_start, partition_end);
        int64_t peer_group_count = frame_end - frame_start;
        if (WindowFunctionCumeDist::data(place).peer_group_start != frame_start) {
            WindowFunctionCumeDist::data(place).peer_group_start = frame_start;
            WindowFunctionCumeDist::data(place).numerator += peer_group_count;
        }
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionCumeDist::data(place).numerator = 0;
        WindowFunctionCumeDist::data(place).denominator = 0;
        WindowFunctionCumeDist::data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        auto cume_dist = (double)data(place).numerator * 1.0 / (double)data(place).denominator;
        assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                cume_dist);
    }

    bool result_column_could_resize() const override { return true; }

    void insert_result_into_range(ConstAggregateDataPtr __restrict place, IColumn& to,
                                  const size_t start, const size_t end) const override {
        auto& column = assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to);
        auto cume_dist = (double)data(place).numerator * 1.0 / (double)data(place).denominator;
        for (size_t i = start; i < end; ++i) {
            column.get_data()[i] = cume_dist;
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {}
};

struct NTileData {
    int64_t bucket_index = 0;
    int64_t rows = 0;
};

class WindowFunctionNTile final
        : public IAggregateFunctionDataHelper<NTileData, WindowFunctionNTile>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    WindowFunctionNTile(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "ntile"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, ssize_t, Arena&) const override {}

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        // some variables are partition related, but there is no chance to init them
        // when the new partition arrives, so we calculate them every time now.
        // Partition = big_bucket_num * big_bucket_size + small_bucket_num * small_bucket_size
        int64_t row_index = ++WindowFunctionNTile::data(place).rows - 1;
        int64_t bucket_num = columns[0]->get_int(0);
        int64_t partition_size = partition_end - partition_start;

        int64_t small_bucket_size = partition_size / bucket_num;
        int64_t big_bucket_num = partition_size % bucket_num;
        int64_t first_small_bucket_row_index = big_bucket_num * (small_bucket_size + 1);
        if (row_index >= first_small_bucket_row_index) {
            // small_bucket_size can't be zero
            WindowFunctionNTile::data(place).bucket_index =
                    big_bucket_num + 1 +
                    (row_index - first_small_bucket_row_index) / small_bucket_size;
        } else {
            WindowFunctionNTile::data(place).bucket_index = row_index / (small_bucket_size + 1) + 1;
        }
    }

    void reset(AggregateDataPtr place) const override { WindowFunctionNTile::data(place).rows = 0; }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                WindowFunctionNTile::data(place).bucket_index);
    }

    bool result_column_could_resize() const override { return true; }

    void insert_result_into_range(ConstAggregateDataPtr __restrict place, IColumn& to,
                                  const size_t start, const size_t end) const override {
        auto& column = assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to);
        for (size_t i = start; i < end; ++i) {
            column.get_data()[i] = WindowFunctionNTile::data(place).bucket_index;
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {}
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable>
struct FirstLastData
        : public ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable, false> {
public:
    void set_is_null() { this->_data_value.reset(); }
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable>
struct NthValueData : public FirstLastData<ColVecType, result_is_nullable, arg_is_nullable> {
public:
    void reset() {
        this->_data_value.reset();
        this->_has_value = false;
        this->_frame_start_pose = 0;
        this->_frame_total_rows = 0;
    }

    int64_t _frame_start_pose = 0;
    int64_t _frame_total_rows = 0;
};

template <typename ColVecType, bool arg_is_nullable>
struct BaseValue : public Value<ColVecType, arg_is_nullable> {
public:
    bool is_null() const { return this->_ptr == nullptr; }
    // because _ptr pointer to first_argument or third argument, so it's difficult to cast ptr
    // so here will call virtual function
    StringRef get_value() const { return this->_ptr->get_data_at(this->_offset); }
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable>
struct LeadLagData {
public:
    static constexpr bool result_nullable = result_is_nullable;
    void reset() {
        _data_value.reset();
        _is_inited = false;
        _offset_value = 0;
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (_data_value.is_null()) {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                StringRef value = _data_value.get_value();
                col.insert_data(value.data, value.size);
            }
        } else {
            StringRef value = _data_value.get_value();
            to.insert_data(value.data, value.size);
        }
    }

    void set_value(const IColumn** columns, size_t pos) {
        if constexpr (arg_is_nullable) {
            if (assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0])
                        ->is_null_at(pos)) {
                // ptr == nullptr means nullable
                _data_value.reset();
                return;
            }
        }
        // here ptr is pointer to nullable column or not null column from first
        _data_value.set_value(columns[0], pos);
    }

    void set_value_from_default(const IColumn* column, size_t pos) {
        DCHECK_GE(pos, 0);
        if (is_column_nullable(*column)) {
            const auto* nullable_column =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column);
            if (nullable_column->is_null_at(pos)) {
                this->_data_value.reset();
            } else {
                this->_data_value.set_value(nullable_column->get_nested_column_ptr().get(), pos);
            }
        } else {
            this->_data_value.set_value(column, pos);
        }
    }

    void set_offset_value(const IColumn* column) {
        if (!_is_inited) {
            const auto* column_number = assert_cast<const ColumnInt64*>(column);
            _offset_value = column_number->get_data()[0];
            _is_inited = true;
        }
    }

    int64_t get_offset_value() const { return _offset_value; }

private:
    BaseValue<ColVecType, arg_is_nullable> _data_value;
    bool _is_inited = false;
    int64_t _offset_value = 0;
};

template <typename Data, bool = false>
struct WindowFunctionLeadImpl : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if (frame_end > partition_end) { //output default value, win end is under partition
            this->set_offset_value(columns[1]);
            // eg: lead(column, 10, default_value), column size maybe 3 rows
            // offset value 10 is from second argument, pos: 11 is calculated as frame_end
            auto pos = frame_end - 1 - this->get_offset_value();
            this->set_value_from_default(columns[2], pos);
            return;
        }
        this->set_value(columns, frame_end - 1);
    }

    static const char* name() { return "lead"; }
};

template <typename Data, bool = false>
struct WindowFunctionLagImpl : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        // window start is beyond partition
        if (partition_start >= frame_end) { //[unbound preceding(0), offset preceding(-123)]
            this->set_offset_value(columns[1]);
            auto pos = frame_end - 1 + this->get_offset_value();
            this->set_value_from_default(columns[2], pos);
            return;
        }
        this->set_value(columns, frame_end - 1);
    }

    static const char* name() { return "lag"; }
};

template <typename Data, bool arg_ignore_null = false>
struct WindowFunctionFirstImpl : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        // case 1: (has_set_value() = true && arg_ignore_null = false)
        // case 2: (has_set_value() = true && arg_ignore_null = true && is_null() = false)
        if ((this->has_set_value()) &&
            (!arg_ignore_null || (arg_ignore_null && !this->is_null()))) {
            return;
        }
        DCHECK_LE(frame_start, frame_end);
        if (frame_start >= partition_end || frame_end <= partition_start) {
            this->set_is_null();
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);

        if constexpr (arg_ignore_null) {
            frame_end = std::min<int64_t>(frame_end, partition_end);
            if (columns[0]->is_nullable()) {
                const auto& arg_nullable = assert_cast<const ColumnNullable&>(*columns[0]);
                // the valid range is: [frame_start, frame_end)
                while (frame_start < frame_end - 1 && arg_nullable.is_null_at(frame_start)) {
                    frame_start++;
                }
            }
        }
        this->set_value(columns, frame_start);
    }

    static const char* name() { return "first_value"; }
};

template <typename Data, bool arg_ignore_null = false>
struct WindowFunctionLastImpl : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        DCHECK_LE(frame_start, frame_end);
        if ((frame_end <= partition_start) ||
            (frame_start >= partition_end)) { //beyond or under partition, set null
            if ((this->has_set_value()) &&
                (!arg_ignore_null || (arg_ignore_null && !this->is_null()))) {
                // have set value, do nothing, because like rows unbouned preceding and M following
                // it's caculated as the cumulative mode, so it's could reuse the previous
            } else {
                this->set_is_null();
            }
            return;
        }
        frame_end = std::min<int64_t>(frame_end, partition_end);

        if constexpr (arg_ignore_null) {
            frame_start = std::max<int64_t>(frame_start, partition_start);
            if (columns[0]->is_nullable()) {
                const auto& arg_nullable = assert_cast<const ColumnNullable&>(*columns[0]);
                // wants find a not null value in [frame_start, frame_end)
                // iff has find: set_value and return directly
                // iff not find: the while loop is finished
                //     case 1: iff has_set_value, means the previous window have value, could reuse it, so return directly
                //     case 2: iff not has_set_value, means there is none value, set it's to NULL
                while (frame_start < frame_end) {
                    if (arg_nullable.is_null_at(frame_end - 1)) {
                        frame_end--;
                    } else {
                        this->set_value(columns, frame_end - 1);
                        return;
                    }
                }
                if (!this->has_set_value()) {
                    this->set_is_null();
                }
                return;
            }
        }

        this->set_value(columns, frame_end - 1);
    }

    static const char* name() { return "last_value"; }
};

template <typename Data, bool = false>
struct WindowFunctionNthValueImpl : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        DCHECK_LE(frame_start, frame_end);
        int64_t real_frame_start = std::max<int64_t>(frame_start, partition_start);
        int64_t real_frame_end = std::min<int64_t>(frame_end, partition_end);
        this->_frame_start_pose =
                this->_frame_total_rows ? this->_frame_start_pose : real_frame_start;
        this->_frame_total_rows += real_frame_end - real_frame_start;
        int64_t offset = assert_cast<const ColumnInt64&, TypeCheckOnRelease::DISABLE>(*columns[1])
                                 .get_data()[0] -
                         1;
        if (offset >= this->_frame_total_rows) {
            // offset is beyond the frame, so set null
            this->set_is_null();
            return;
        }
        this->set_value(columns, offset + this->_frame_start_pose);
    }

    static const char* name() { return "nth_value"; }
};

template <typename Data>
class WindowFunctionData final
        : public IAggregateFunctionDataHelper<Data, WindowFunctionData<Data>> {
public:
    WindowFunctionData(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, WindowFunctionData<Data>>(argument_types_),
              _argument_type(argument_types_[0]) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override {
        if constexpr (Data::result_nullable) {
            return make_nullable(_argument_type);
        } else {
            return _argument_type;
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8*, UInt8*) const override {
        this->data(place).add_range_single_place(partition_start, partition_end, frame_start,
                                                 frame_end, columns);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        throw doris::Exception(Status::FatalError("WindowFunctionLeadLagData do not support add"));
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {
        throw doris::Exception(
                Status::FatalError("WindowFunctionLeadLagData do not support merge"));
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        throw doris::Exception(
                Status::FatalError("WindowFunctionLeadLagData do not support serialize"));
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {
        throw doris::Exception(
                Status::FatalError("WindowFunctionLeadLagData do not support deserialize"));
    }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
