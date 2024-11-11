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

#include <glog/logging.h>

#include <cstring>
#include <limits>
#include <ostream>
#include <type_traits>
#include <vector>

#include "runtime/decimalv2_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/// Calculates arithmetic mean of numbers.
template <bool is_regr_avgy, typename T, typename Data, bool y_nullable, bool x_nullable>
class AggregateFunctionRegrAvg final
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionRegrAvg<is_regr_avgy, T, Data, y_nullable, x_nullable>> {
public:
    using ResultType = std::conditional_t<
            IsDecimalV2<T>, Decimal128V2,
            std::conditional_t<IsDecimalNumber<T>, typename Data::ResultType, Float64>>;
    using ResultDataType = std::conditional_t<
            IsDecimalV2<T>, DataTypeDecimal<Decimal128V2>,
            std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<typename Data::ResultType>,
                               DataTypeNumber<Float64>>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<
            IsDecimalV2<T>, ColumnDecimal<Decimal128V2>,
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<typename Data::ResultType>,
                               ColumnFloat64>>;

    /// ctor for native types
    AggregateFunctionRegrAvg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionRegrAvg<is_regr_avgy, T, Data,
                                                                          y_nullable, x_nullable>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[1])) {}

    String get_name() const override {
        if constexpr (is_regr_avgy) {
            return "regr_avgy";
        } else {
            return "regr_avgx";
        }
    }

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        const ColVecType* column {};

        if constexpr (y_nullable) {
            const auto& y_column_nullable = assert_cast<const ColumnNullable&>(*columns[0]);
            if (y_column_nullable.is_null_at(row_num)) {
                return;
            }
            if constexpr (is_regr_avgy) {
                column = assert_cast<const ColVecType*>(
                        y_column_nullable.get_nested_column_ptr().get());
            }
        } else {
            if constexpr (is_regr_avgy) {
                column = assert_cast<const ColVecType*>((*columns[0]).get_ptr().get());
            }
        }

        if constexpr (x_nullable) {
            const auto& x_column_nullable = assert_cast<const ColumnNullable&>(*columns[1]);
            if (x_column_nullable.is_null_at(row_num)) {
                return;
            }
            if constexpr (!is_regr_avgy) {
                column = assert_cast<const ColVecType*>(
                        x_column_nullable.get_nested_column_ptr().get());
            }
        } else {
            if constexpr (!is_regr_avgy) {
                column = assert_cast<const ColVecType*>((*columns[0]).get_ptr().get());
            }
        }

        if constexpr (IsDecimalNumber<T>) {
            this->data(place).sum += column->get_element(row_num).value;
        } else {
            this->data(place).sum += column->get_element(row_num);
        }
        ++this->data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).sum = {};
        this->data(place).count = {};
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        if constexpr (IsDecimalNumber<T>) {
            this->data(place).sum += this->data(rhs).sum.value;
        } else {
            this->data(place).sum += this->data(rhs).sum;
        }
        this->data(place).count += this->data(rhs).count;
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
        column.get_data().push_back(this->data(place).template result<ResultType>());
    }

private:
    UInt32 scale;
};

} // namespace doris::vectorized
