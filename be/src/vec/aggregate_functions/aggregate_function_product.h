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
namespace vectorized {

template <typename T>
struct AggregateFunctionProductData {
    T product {};

    void add(T value) { product *= value; }

    void merge(const AggregateFunctionProductData& other) { product *= other.product; }

    void write(BufferWritable& buffer) const { write_binary(product, buffer); }

    void read(BufferReadable& buffer) { read_binary(product, buffer); }

    T get() const { return product; }

    void reset(T value) { product = std::move(value); }
};

template <typename T, typename TResult, typename Data>
class AggregateFunctionProduct final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>> {
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>,
                                              DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    std::string get_name() const { return "product"; }

    AggregateFunctionProduct(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>(
                      argument_types_, {}),
              scale(0) {}

    AggregateFunctionProduct(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T, TResult, Data>>(
                      argument_types_, {}),
              scale(get_decimal_scale(data_type)) {}

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override {
        if constexpr (IsDecimalNumber<T>) {
            this->data(place).reset(T(1 * ResultDataType::get_scale_multiplier(scale)));
        } else {
            this->data(place).reset(1);
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
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }

private:
    UInt32 scale;
};

} // namespace vectorized
} // namespace doris
