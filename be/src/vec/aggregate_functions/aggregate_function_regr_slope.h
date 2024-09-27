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

#include <cmath>
#include <cstdint>
#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
namespace doris::vectorized {

template <typename T>
struct AggregateFunctionRegrSlopeData {
    UInt64 count = 0;
    double sum_x {};
    double sum_y {};
    double sum_of_x_mul_y {};
    double sum_of_x_squared {};

    void write(BufferWritable& buf) const {
        write_binary(sum_x, buf);
        write_binary(sum_y, buf);
        write_binary(sum_of_x_mul_y, buf);
        write_binary(sum_of_x_squared, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum_x, buf);
        read_binary(sum_y, buf);
        read_binary(sum_of_x_mul_y, buf);
        read_binary(sum_of_x_squared, buf);
        read_binary(count, buf);
    }

    void reset() {
        sum_x = {};
        sum_y = {};
        sum_of_x_mul_y = {};
        sum_of_x_squared = {};
        count = 0;
    }

    double get_slope_result() const {
        double denominator = count * sum_of_x_squared - sum_x * sum_x;
        if (count < 2 || denominator == 0.0) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return (count * sum_of_x_mul_y - sum_x * sum_y) / denominator;
    }

    void merge(const AggregateFunctionRegrSlopeData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_of_x_mul_y += rhs.sum_of_x_mul_y;
        sum_of_x_squared += rhs.sum_of_x_squared;
        count += rhs.count;
    }

    void add(T value_y, T value_x) { // 注意参数顺序变更
        sum_x += value_x;
        sum_y += value_y;
        sum_of_x_mul_y += value_x * value_y;
        sum_of_x_squared += value_x * value_x;
        count += 1;
    }
};

template <typename TX, typename TY>
struct RegrSlopeFuncTwoArg {
    using TypeX = TX;
    using TypeY = TY;
    using Data = AggregateFunctionRegrSlopeData<Float64>;
};

template <typename StatFunc, bool NullableInput>
class AggregateFunctionRegrSlopeSimple
        : public IAggregateFunctionDataHelper<
                  typename StatFunc::Data,
                  AggregateFunctionRegrSlopeSimple<StatFunc, NullableInput>> {
public:
    using TX = typename StatFunc::TypeX;
    using TY = typename StatFunc::TypeY;
    using XInputCol = ColumnVector<TX>;
    using YInputCol = ColumnVector<TY>;
    using ResultCol = ColumnVector<Float64>;

    explicit AggregateFunctionRegrSlopeSimple(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      typename StatFunc::Data,
                      AggregateFunctionRegrSlopeSimple<StatFunc, NullableInput>>(argument_types_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return "regr_slope"; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (NullableInput) {
            const ColumnNullable& y_column_nullable =
                    assert_cast<const ColumnNullable&>(*columns[0]);
            const ColumnNullable& x_column_nullable =
                    assert_cast<const ColumnNullable&>(*columns[1]);
            bool y_null = y_column_nullable.is_null_at(row_num);
            bool x_null = x_column_nullable.is_null_at(row_num);
            if (y_null || x_null) {
                return;
            } else {
                TY y_value = assert_cast<const YInputCol&>(y_column_nullable.get_nested_column())
                                     .get_data()[row_num];
                TX x_value = assert_cast<const XInputCol&>(x_column_nullable.get_nested_column())
                                     .get_data()[row_num];
                this->data(place).add(static_cast<Float64>(y_value), static_cast<Float64>(x_value));
            }
        } else {
            TY y_value = assert_cast<const YInputCol&>(*columns[0]).get_data()[row_num];
            TX x_value = assert_cast<const XInputCol&>(*columns[1]).get_data()[row_num];
            this->data(place).add(static_cast<Float64>(y_value), static_cast<Float64>(x_value));
        }
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

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
        const auto& data = this->data(place);
        auto& dst_column_with_nullable = assert_cast<ColumnNullable&>(to);
        auto& dst_column = assert_cast<ResultCol&>(dst_column_with_nullable.get_nested_column());
        Float64 slope = data.get_slope_result();
        if (std::isnan(slope)) {
            dst_column_with_nullable.get_null_map_data().push_back(1);
            dst_column.insert_default();
        } else {
            dst_column_with_nullable.get_null_map_data().push_back(0);
            dst_column.get_data().push_back(slope);
        }
    }
};

} // namespace doris::vectorized