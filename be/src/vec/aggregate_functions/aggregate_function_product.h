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

#pragma once

#include <cstddef>
#include <type_traits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {

template <PrimitiveType T>
struct AggregateFunctionProductData {
    typename PrimitiveTypeTraits<T>::ColumnItemType product {};

    void add(typename PrimitiveTypeTraits<T>::ColumnItemType value,
             typename PrimitiveTypeTraits<T>::ColumnItemType) {
        product *= value;
    }

    void merge(const AggregateFunctionProductData& other,
               typename PrimitiveTypeTraits<T>::ColumnItemType) {
        product *= other.product;
    }

    void write(BufferWritable& buffer) const { buffer.write_binary(product); }

    void read(BufferReadable& buffer) { buffer.read_binary(product); }

    typename PrimitiveTypeTraits<T>::ColumnItemType get() const { return product; }

    void reset(typename PrimitiveTypeTraits<T>::ColumnItemType value) {
        product = std::move(value);
    }
};

template <>
struct AggregateFunctionProductData<TYPE_DECIMALV2> {
    Decimal128V2 product {};

    void add(Decimal128V2 value, Decimal128V2) {
        DecimalV2Value decimal_product(product);
        DecimalV2Value decimal_value(value);
        DecimalV2Value ret = decimal_product * decimal_value;
        memcpy(&product, &ret, sizeof(Decimal128V2));
    }

    void merge(const AggregateFunctionProductData& other, Decimal128V2) {
        DecimalV2Value decimal_product(product);
        DecimalV2Value decimal_value(other.product);
        DecimalV2Value ret = decimal_product * decimal_value;
        memcpy(&product, &ret, sizeof(Decimal128V2));
    }

    void write(BufferWritable& buffer) const { buffer.write_binary(product); }

    void read(BufferReadable& buffer) { buffer.read_binary(product); }

    Decimal128V2 get() const { return product; }

    void reset(Decimal128V2 value) { product = std::move(value); }
};

template <PrimitiveType T>
    requires(T == TYPE_DECIMAL128I || T == TYPE_DECIMAL256)
struct AggregateFunctionProductData<T> {
    typename PrimitiveTypeTraits<T>::ColumnItemType product {};

    template <typename NestedType>
    void add(Decimal<NestedType> value,
             typename PrimitiveTypeTraits<T>::ColumnItemType multiplier) {
        product *= value;
        product /= multiplier;
    }

    void merge(const AggregateFunctionProductData& other,
               typename PrimitiveTypeTraits<T>::ColumnItemType multiplier) {
        product *= other.product;
        product /= multiplier;
    }

    void write(BufferWritable& buffer) const { buffer.write_binary(product); }

    void read(BufferReadable& buffer) { buffer.read_binary(product); }

    typename PrimitiveTypeTraits<T>::ColumnItemType get() const { return product; }

    void reset(typename PrimitiveTypeTraits<T>::ColumnItemType value) {
        product = std::move(value);
    }
};

template <PrimitiveType T, PrimitiveType TResult, typename Data>
class AggregateFunctionProduct final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    using ResultDataType = typename PrimitiveTypeTraits<TResult>::DataType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using ColVecResult = typename PrimitiveTypeTraits<TResult>::ColumnType;

    std::string get_name() const override { return "product"; }

    AggregateFunctionProduct(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[0])) {
        if constexpr (is_decimal(T)) {
            multiplier.value =
                    ResultDataType::get_scale_multiplier(get_decimal_scale(*argument_types_[0]));
        }
    }

    DataTypePtr get_return_type() const override {
        if constexpr (is_decimal(T)) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        this->data(place).add(
                typename PrimitiveTypeTraits<TResult>::ColumnItemType(column.get_data()[row_num]),
                multiplier);
    }

    void reset(AggregateDataPtr place) const override {
        if constexpr (is_decimal(T)) {
            this->data(place).reset(multiplier);
        } else {
            this->data(place).reset(1);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs), multiplier);
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

private:
    UInt32 scale;
    typename PrimitiveTypeTraits<TResult>::ColumnItemType multiplier;
};

} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
