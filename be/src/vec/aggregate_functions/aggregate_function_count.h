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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionCount.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;

struct AggregateFunctionCountData {
    UInt64 count = 0;
};

/// Simply count number of calls.
class AggregateFunctionCount final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount>,
          VarargsExpression,
          NotNullableAggregateFunction {
public:
    AggregateFunctionCount(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn**, ssize_t, Arena&) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        AggregateFunctionCount::data(place).count = 0;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        buf.write_var_uint(data(place).count);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        buf.read_var_uint(data(place).count);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena&,
                                 size_t num_rows) const override {
        auto data = assert_cast<const ColumnFixedLengthObject&>(column).get_data().data();
        memcpy(places, data, sizeof(Data) * num_rows);
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            *reinterpret_cast<Data*>(&data[sizeof(Data) * i]) =
                    *reinterpret_cast<Data*>(places[i] + offset);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena&) const override {
        auto& dst_col = assert_cast<ColumnFixedLengthObject&>(*dst);
        DCHECK(dst_col.item_size() == sizeof(Data))
                << "size is not equal: " << dst_col.item_size() << " " << sizeof(Data);
        dst_col.resize(num_rows);
        auto* data = dst_col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            auto& state = *reinterpret_cast<Data*>(&data[sizeof(Data) * i]);
            state.count = 1;
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena&) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        const size_t num_rows = column.size();
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            AggregateFunctionCount::data(place).count += data[i].count;
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena&) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = begin; i <= end; ++i) {
            doris::vectorized::AggregateFunctionCount::data(place).count += data[i].count;
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(to);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        size_t old_size = col.size();
        col.resize(old_size + 1);
        (reinterpret_cast<Data*>(col.get_data().data()) + old_size)->count =
                AggregateFunctionCount::data(place).count;
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(Data));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if (frame_start >= frame_end) {
            if (!*could_use_previous_result) {
                *use_null_result = true;
            }
        } else {
            AggregateFunctionCount::data(place).count += frame_end - frame_start;
            *use_null_result = false;
            *could_use_previous_result = true;
        }
    }
};

// TODO: Maybe AggregateFunctionCountNotNullUnary should be a subclass of AggregateFunctionCount
// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                              AggregateFunctionCountNotNullUnary> {
public:
    AggregateFunctionCountNotNullUnary(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        data(place).count +=
                !assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0])
                         .is_null_at(row_num);
    }

    void reset(AggregateDataPtr place) const override { data(place).count = 0; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        buf.write_var_uint(data(place).count);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        buf.read_var_uint(data(place).count);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if (to.is_nullable()) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(0);
            assert_cast<ColumnInt64&>(null_column.get_nested_column())
                    .get_data()
                    .push_back(data(place).count);
        } else {
            assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena&,
                                 size_t num_rows) const override {
        auto data = assert_cast<const ColumnFixedLengthObject&>(column).get_data().data();
        memcpy(places, data, sizeof(Data) * num_rows);
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            *reinterpret_cast<Data*>(&data[sizeof(Data) * i]) =
                    *reinterpret_cast<Data*>(places[i] + offset);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena&) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(num_rows);
        auto& data = col.get_data();
        const ColumnNullable& input_col = assert_cast<const ColumnNullable&>(*columns[0]);
        for (size_t i = 0; i < num_rows; i++) {
            auto& state = *reinterpret_cast<Data*>(&data[sizeof(Data) * i]);
            state.count = !input_col.is_null_at(i);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena&) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        const size_t num_rows = column.size();
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            AggregateFunctionCountNotNullUnary::data(place).count += data[i].count;
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena&) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = begin; i <= end; ++i) {
            doris::vectorized::AggregateFunctionCountNotNullUnary::data(place).count +=
                    data[i].count;
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(to);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(1);
        reinterpret_cast<Data*>(col.get_data().data())->count =
                AggregateFunctionCountNotNullUnary::data(place).count;
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(Data));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if (frame_start >= frame_end) {
            if (!*could_use_previous_result) {
                *use_null_result = true;
            }
        } else {
            const auto& nullable_column =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            size_t count = 0;
            if (nullable_column.has_null()) {
                for (int64_t i = frame_start; i < frame_end; ++i) {
                    if (!nullable_column.is_null_at(i)) {
                        ++count;
                    }
                }
            } else {
                count = frame_end - frame_start;
            }
            *use_null_result = false;
            *could_use_previous_result = true;
            AggregateFunctionCountNotNullUnary::data(place).count += count;
        }
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
