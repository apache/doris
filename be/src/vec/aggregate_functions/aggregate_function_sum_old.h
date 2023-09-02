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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionOldSum.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <memory>
#include <type_traits>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_sum.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"
namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
template <typename T>
class DataTypeNumber;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/*
 * this function is used to solve agg of sum/count is not compatibility during the upgrade process
 * in PR #20370 have changed the serialize type and serialize column
 * before is ColumnVector, now sum/count change to use ColumnFixedLengthObject
 * so during the upgrade process, will be not compatible if exist old BE and Newer BE
 */

template <typename T, typename TResult, typename Data>
class AggregateFunctionOldSum final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionOldSum<T, TResult, Data>> {
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>,
                                              DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String get_name() const override { return "sum"; }

    AggregateFunctionOldSum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionOldSum<T, TResult, Data>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[0])) {}

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = assert_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).sum = {}; }

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
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto data = assert_cast<const ColVecResult&>(column).get_data().data();
        auto dst_data = reinterpret_cast<Data*>(places);
        for (size_t i = 0; i != num_rows; ++i) {
            dst_data[i].sum = data[i];
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColVecResult&>(*dst);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = this->data(places[i] + offset).sum;
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& col = assert_cast<ColVecResult&>(*dst);
        auto& src = assert_cast<const ColVecType&>(*columns[0]);
        col.resize(num_rows);
        auto* src_data = src.get_data().data();
        auto* dst_data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            dst_data[i] = src_data[i];
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto data = assert_cast<const ColVecResult&>(column).get_data().data();
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).sum += data[i];
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColVecResult&>(to);
        col.resize(1);
        reinterpret_cast<Data*>(col.get_data().data())->sum = this->data(place).sum;
    }

    MutableColumnPtr create_serialize_column() const override {
        return get_return_type()->create_column();
    }

    DataTypePtr get_serialized_type() const override { return get_return_type(); }

private:
    UInt32 scale;
};

template <typename T, bool level_up>
struct OldSumSimple {
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = std::conditional_t<level_up, DisposeDecimal<T, NearestFieldType<T>>, T>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionOldSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimpleOld = typename OldSumSimple<T, true>::Function;

// do not level up return type for agg reader
template <typename T>
using AggregateFunctionSumSimpleReaderOld = typename OldSumSimple<T, false>::Function;

} // namespace doris::vectorized
