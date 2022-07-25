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

#include "factory_helpers.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct RowNumberData {
    int64_t count;
};

class WindowFunctionRowNumber final
        : public IAggregateFunctionDataHelper<RowNumberData, WindowFunctionRowNumber> {
public:
    WindowFunctionRowNumber(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "row_number"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionRowNumber::data(place).count = 0;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct RankData {
    int64_t rank;
    int64_t count;
    int64_t peer_group_start;
};

class WindowFunctionRank final : public IAggregateFunctionDataHelper<RankData, WindowFunctionRank> {
public:
    WindowFunctionRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
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
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).rank);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct DenseRankData {
    int64_t rank;
    int64_t peer_group_start;
};
class WindowFunctionDenseRank final
        : public IAggregateFunctionDataHelper<DenseRankData, WindowFunctionDenseRank> {
public:
    WindowFunctionDenseRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "dense_rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
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
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).rank);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct NTileData {
    int64_t bucket_index;
    int64_t rows;
};

class WindowFunctionNTile final
        : public IAggregateFunctionDataHelper<NTileData, WindowFunctionNTile> {
public:
    WindowFunctionNTile(const DataTypes& argument_types_, const Array& parameters)
            : IAggregateFunctionDataHelper(argument_types_, parameters) {}

    String get_name() const override { return "ntile"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {}

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        // some variables are partition related, but there is no chance to init them
        // when the new partition arrives, so we calculate them evey time now.
        // Partition = big_bucket_num * big_bucket_size + small_bucket_num * small_bucket_size
        int64_t row_index = ++WindowFunctionNTile::data(place).rows - 1;
        int64_t bucket_num = columns[0]->get_int(0);
        int64_t partition_size = partition_end - partition_start;

        int64 small_bucket_size = partition_size / bucket_num;
        int64 big_bucket_num = partition_size % bucket_num;
        int64 first_small_bucket_row_index = big_bucket_num * (small_bucket_size + 1);
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
        assert_cast<ColumnInt64&>(to).get_data().push_back(
                WindowFunctionNTile::data(place).bucket_index);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct Value {
public:
    bool is_null() const { return _is_null; }
    void set_null(bool is_null) { _is_null = is_null; }
    StringRef get_value() const { return _ptr->get_data_at(_offset); }

    void set_value(const IColumn* column, size_t row) {
        _ptr = column;
        _offset = row;
    }
    void reset() {
        _is_null = false;
        _ptr = nullptr;
        _offset = 0;
    }

protected:
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;
    bool _is_null;
};

struct CopiedValue : public Value {
public:
    StringRef get_value() const { return _copied_value; }

    void set_value(const IColumn* column, size_t row) {
        _copied_value = column->get_data_at(row).to_string();
    }

private:
    std::string _copied_value;
};

template <typename T, bool result_is_nullable, bool is_string, typename StoreType = Value>
struct LeadAndLagData {
public:
    bool has_init() const { return _is_init; }

    static constexpr bool nullable = result_is_nullable;

    void set_null_if_need() {
        if (!_has_value) {
            this->set_is_null();
        }
    }

    void reset() {
        _data_value.reset();
        _default_value.reset();
        _is_init = false;
        _has_value = false;
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
        if (columns[0]->is_nullable() &&
            assert_cast<const ColumnNullable*>(columns[0])->is_null_at(pos)) {
            _data_value.set_null(true);
        } else {
            _data_value.set_value(columns[0], pos);
            _data_value.set_null(false);
        }
        _has_value = true;
    }

    bool defualt_is_null() { return _default_value.is_null(); }

    void set_is_null() { _data_value.set_null(true); }

    void set_value_from_default() { _data_value = _default_value; }

    bool has_set_value() { return _has_value; }

    void check_default(const IColumn* column) {
        if (!has_init()) {
            if (is_column_nullable(*column)) {
                const auto* nullable_column = assert_cast<const ColumnNullable*>(column);
                if (nullable_column->is_null_at(0)) {
                    _default_value.set_null(true);
                }
            } else {
                _default_value.set_value(column, 0);
            }
            _is_init = true;
        }
    }

private:
    StoreType _data_value;
    StoreType _default_value;
    bool _has_value = false;
    bool _is_init = false;
};

template <typename Data>
struct WindowFunctionLeadData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, size_t frame_start,
                                size_t frame_end, const IColumn** columns) {
        this->check_default(columns[2]);
        if (frame_end > partition_end) { //output default value, win end is under partition
            if (this->defualt_is_null()) {
                this->set_is_null();
            } else {
                this->set_value_from_default();
            }
            return;
        }
        this->set_value(columns, frame_end - 1);
    }
    void add(int64_t row, const IColumn** columns) {
        LOG(FATAL) << "WindowFunctionLeadData do not support add";
    }
    static const char* name() { return "lead"; }
};

template <typename Data>
struct WindowFunctionLagData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        this->check_default(columns[2]);
        if (partition_start >= frame_end) { //[unbound preceding(0), offset preceding(-123)]
            if (this->defualt_is_null()) {  // win start is beyond partition
                this->set_is_null();
            } else {
                this->set_value_from_default();
            }
            return;
        }
        this->set_value(columns, frame_end - 1);
    }
    void add(int64_t row, const IColumn** columns) {
        LOG(FATAL) << "WindowFunctionLagData do not support add";
    }
    static const char* name() { return "lag"; }
};

template <typename Data>
struct WindowFunctionFirstData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if (frame_start < frame_end &&
            frame_end <= partition_start) { //rewrite last_value when under partition
            this->set_is_null();            //so no need more judge
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);
        this->set_value(columns, frame_start);
    }
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_value"; }
};

template <typename Data>
struct WindowFunctionFirstNonNullData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if (frame_start < frame_end &&
            frame_end <= partition_start) { //rewrite last_value when under partition
            this->set_is_null();            //so no need more judge
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if constexpr (Data::nullable) {
            this->set_null_if_need();
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            for (int i = frame_start; i < frame_end; i++) {
                if (!nullable_column->is_null_at(i)) {
                    this->set_value(columns, i);
                    return;
                }
            }
        } else {
            this->set_value(columns, frame_start);
        }
    }

    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if constexpr (Data::nullable) {
            this->set_null_if_need();
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_non_null_value"; }
};

template <typename Data>
struct WindowFunctionLastData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if ((frame_start < frame_end) &&
            ((frame_end <= partition_start) ||
             (frame_start >= partition_end))) { //beyond or under partition, set null
            this->set_is_null();
            return;
        }
        frame_end = std::min<int64_t>(frame_end, partition_end);
        this->set_value(columns, frame_end - 1);
    }
    void add(int64_t row, const IColumn** columns) { this->set_value(columns, row); }
    static const char* name() { return "last_value"; }
};

template <typename Data>
struct WindowFunctionLastNonNullData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if ((frame_start < frame_end) &&
            ((frame_end <= partition_start) ||
             (frame_start >= partition_end))) { //beyond or under partition, set null
            this->set_is_null();
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if constexpr (Data::nullable) {
            this->set_null_if_need();
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            for (int i = frame_end - 1; i >= frame_start; i--) {
                if (!nullable_column->is_null_at(i)) {
                    this->set_value(columns, i);
                    return;
                }
            }
        } else {
            this->set_value(columns, frame_end - 1);
        }
    }

    void add(int64_t row, const IColumn** columns) {
        if constexpr (Data::nullable) {
            this->set_null_if_need();
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }

    static const char* name() { return "last_non_null_value"; }
};

template <typename Data>
class WindowFunctionData final
        : public IAggregateFunctionDataHelper<Data, WindowFunctionData<Data>> {
public:
    WindowFunctionData(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<Data, WindowFunctionData<Data>>(argument_types, {}),
              _argument_type(argument_types[0]) {}

    String get_name() const override { return Data::name(); }
    DataTypePtr get_return_type() const override { return _argument_type; }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        this->data(place).add_range_single_place(partition_start, partition_end, frame_start,
                                                 frame_end, columns);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(row_num, columns);
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        LOG(FATAL) << "WindowFunctionData do not support merge";
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        LOG(FATAL) << "WindowFunctionData do not support serialize";
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        LOG(FATAL) << "WindowFunctionData do not support deserialize";
    }

private:
    DataTypePtr _argument_type;
};

template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          bool result_is_nullable, bool is_copy = false>
static IAggregateFunction* create_function_single_value(const String& name,
                                                        const DataTypes& argument_types,
                                                        const Array& parameters) {
    using StoreType = std::conditional_t<is_copy, CopiedValue, Value>;

    assert_arity_at_most<3>(name, argument_types);

    auto type = remove_nullable(argument_types[0]);
    WhichDataType which(*type);

#define DISPATCH(TYPE)                        \
    if (which.idx == TypeIndex::TYPE)         \
        return new AggregateFunctionTemplate< \
                Data<LeadAndLagData<TYPE, result_is_nullable, false, StoreType>>>(argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.is_decimal()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<Int128, result_is_nullable, false, StoreType>>>(argument_types);
    }
    if (which.is_date_or_datetime()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<Int64, result_is_nullable, false, StoreType>>>(argument_types);
    }
    if (which.is_date_v2()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<UInt32, result_is_nullable, false, StoreType>>>(argument_types);
    }
    if (which.is_date_time_v2()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<UInt64, result_is_nullable, false, StoreType>>>(argument_types);
    }
    if (which.is_string_or_fixed_string()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<StringRef, result_is_nullable, true, StoreType>>>(
                argument_types);
    }
    DCHECK(false) << "with unknowed type, failed in  create_aggregate_function_" << name;
    return nullptr;
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_first(const std::string& name,
                                                     const DataTypes& argument_types,
                                                     const Array& parameters,
                                                     bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<WindowFunctionData, WindowFunctionFirstData, is_nullable,
                                         is_copy>(name, argument_types, parameters));
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_first_non_null_value(const std::string& name,
                                                                    const DataTypes& argument_types,
                                                                    const Array& parameters,
                                                                    bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<WindowFunctionData, WindowFunctionFirstNonNullData,
                                         is_nullable, is_copy>(name, argument_types, parameters));
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_last(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<WindowFunctionData, WindowFunctionLastData, is_nullable,
                                         is_copy>(name, argument_types, parameters));
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_last_non_null_value(const std::string& name,
                                                                   const DataTypes& argument_types,
                                                                   const Array& parameters,
                                                                   bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<WindowFunctionData, WindowFunctionLastNonNullData,
                                         is_nullable, is_copy>(name, argument_types, parameters));
}

} // namespace doris::vectorized
