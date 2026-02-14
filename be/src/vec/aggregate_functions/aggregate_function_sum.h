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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionSum.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <memory>
#include <vector>

#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_fixed_length_object.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;
template <PrimitiveType T>
class ColumnDecimal;
template <PrimitiveType T>
class DataTypeNumber;
template <PrimitiveType T>
class ColumnVector;

template <PrimitiveType T>
struct AggregateFunctionSumData {
    typename PrimitiveTypeTraits<T>::CppType sum {};

    NO_SANITIZE_UNDEFINED void add(typename PrimitiveTypeTraits<T>::CppType value) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        sum += value;
    }

    void merge(const AggregateFunctionSumData& rhs) { sum += rhs.sum; }

    void write(BufferWritable& buf) const { buf.write_binary(sum); }

    void read(BufferReadable& buf) { buf.read_binary(sum); }

    typename PrimitiveTypeTraits<T>::CppType get() const { return sum; }
};

template <PrimitiveType T, PrimitiveType TResult, typename Data>
class AggregateFunctionSum;

template <PrimitiveType T, PrimitiveType TResult>
constexpr static bool is_valid_sum_types =
        (is_same_or_wider_decimalv3(T, TResult) || (is_decimalv2(T) && is_decimalv2(TResult)) ||
         (is_float_or_double(T) && is_float_or_double(TResult)) ||
         (is_int_or_bool(T) && is_int(TResult)));
/// Counts the sum of the numbers.
template <PrimitiveType T, PrimitiveType TResult, typename Data>
    requires(is_valid_sum_types<T, TResult>)
class AggregateFunctionSum<T, TResult, Data> final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    using ResultDataType = typename PrimitiveTypeTraits<TResult>::DataType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using ColVecResult = typename PrimitiveTypeTraits<TResult>::ColumnType;

    String get_name() const override { return "sum"; }

    AggregateFunctionSum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[0])) {}

    DataTypePtr get_return_type() const override {
        if constexpr (is_decimal(TResult)) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    bool is_trivial() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        this->data(place).add(
                typename PrimitiveTypeTraits<TResult>::CppType(column.get_data()[row_num]));
    }

    void reset(AggregateDataPtr place) const override { this->data(place).sum = {}; }

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
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
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
        auto& src = assert_cast<const ColVecType&>(*columns[0]);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(num_rows);
        auto* src_data = src.get_data().data();
        auto* dst_data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            auto& state = *reinterpret_cast<Data*>(&dst_data[sizeof(Data) * i]);
            state.sum = typename PrimitiveTypeTraits<TResult>::CppType(src_data[i]);
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
            this->data(place).sum += data[i].sum;
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnFixedLengthObject&>(*column);
        const auto* data = col.get_data().data();
        this->merge_vec(places, offset, AggregateDataPtr(data), arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnFixedLengthObject&>(*column);
        const auto* data = col.get_data().data();
        this->merge_vec_selected(places, offset, AggregateDataPtr(data), arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(to);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        size_t old_size = col.size();
        col.resize(old_size + 1);
        (reinterpret_cast<Data*>(col.get_data().data()) + old_size)->sum = this->data(place).sum;
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(Data));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }

    bool supported_incremental_mode() const override { return true; }

    NO_SANITIZE_UNDEFINED void execute_function_with_incremental(
            int64_t partition_start, int64_t partition_end, int64_t frame_start, int64_t frame_end,
            AggregateDataPtr place, const IColumn** columns, Arena& arena, bool previous_is_nul,
            bool end_is_nul, bool has_null, UInt8* use_null_result,
            UInt8* could_use_previous_result) const override {
        int64_t current_frame_start = std::max<int64_t>(frame_start, partition_start);
        int64_t current_frame_end = std::min<int64_t>(frame_end, partition_end);

        if (current_frame_start >= current_frame_end) {
            *use_null_result = true;
            return;
        }
        if (*could_use_previous_result) {
            const auto& column =
                    assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            const auto* data = column.get_data().data();
            auto outcoming_pos = frame_start - 1;
            auto incoming_pos = frame_end - 1;
            if (!previous_is_nul && outcoming_pos >= partition_start &&
                outcoming_pos < partition_end) {
                this->data(place).add(
                        typename PrimitiveTypeTraits<TResult>::CppType(-data[outcoming_pos]));
            }
            if (!end_is_nul && incoming_pos >= partition_start && incoming_pos < partition_end) {
                this->data(place).add(
                        typename PrimitiveTypeTraits<TResult>::CppType(data[incoming_pos]));
            }
        } else {
            this->add_range_single_place(partition_start, partition_end, frame_start, frame_end,
                                         place, columns, arena, use_null_result,
                                         could_use_previous_result);
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        auto current_frame_start = std::max<int64_t>(frame_start, partition_start);
        auto current_frame_end = std::min<int64_t>(frame_end, partition_end);

        if (current_frame_start >= current_frame_end) {
            if (!*could_use_previous_result) {
                *use_null_result = true;
            }
        } else {
            const auto& column =
                    assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            for (size_t row_num = current_frame_start; row_num < current_frame_end; ++row_num) {
                this->data(place).add(
                        typename PrimitiveTypeTraits<TResult>::CppType(column.get_data()[row_num]));
            }
            *use_null_result = false;
            *could_use_previous_result = true;
        }
    }

private:
    UInt32 scale;
};

constexpr PrimitiveType result_type(PrimitiveType T) {
    if (T == TYPE_LARGEINT) {
        return TYPE_LARGEINT;
    } else if (is_int_or_bool(T)) {
        return TYPE_BIGINT;
    } else if (is_float_or_double(T) || is_time_type(T)) {
        return TYPE_DOUBLE;
    } else {
        return T;
    }
}

// TODO: use result type from FE plan
template <PrimitiveType T>
struct SumSimple {
    static_assert(!is_decimalv3(T));
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    static constexpr PrimitiveType ResultType = result_type(T);
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <PrimitiveType T>
using AggregateFunctionSumSimple = typename SumSimple<T>::Function;

// use result type got from FE plan
template <PrimitiveType InputType, PrimitiveType ResultType>
struct SumDecimalV3 {
    static_assert(is_decimalv3(InputType) && is_decimalv3(ResultType));
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<InputType, ResultType, AggregateDataType>;
};
template <PrimitiveType InputType, PrimitiveType ResultType>
using AggregateFunctionSumDecimalV3 = typename SumDecimalV3<InputType, ResultType>::Function;

template <PrimitiveType T>
struct SumSimpleForAggReader {
    using AggregateDataType = AggregateFunctionSumData<T>;
    using Function = AggregateFunctionSum<T, T, AggregateDataType>;
};
// do not level up return type for agg reader
template <PrimitiveType T>
using AggregateFunctionSumSimpleReader = typename SumSimpleForAggReader<T>::Function;

} // namespace doris::vectorized

#include "common/compile_check_end.h"
