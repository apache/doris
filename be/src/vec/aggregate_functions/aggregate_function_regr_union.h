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
#include "common/compile_check_begin.h"

template <typename T>
struct AggregateFunctionRegrData {
    using Type = T;
    UInt64 count = 0;
    Float64 sum_x {};
    Float64 sum_y {};
    Float64 sum_of_x_mul_y {};
    Float64 sum_of_x_squared {};

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

    void merge(const AggregateFunctionRegrData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_of_x_mul_y += rhs.sum_of_x_mul_y;
        sum_of_x_squared += rhs.sum_of_x_squared;
        count += rhs.count;
    }

    void add(T value_y, T value_x) {
        sum_x += (double)value_x;
        sum_y += (double)value_y;
        sum_of_x_mul_y += (double)value_x * (double)value_y;
        sum_of_x_squared += (double)value_x * (double)value_x;
        count += 1;
    }

    Float64 get_slope() const {
        Float64 denominator = (double)count * sum_of_x_squared - sum_x * sum_x;
        if (count < 2 || denominator == 0.0) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        Float64 slope = ((double)count * sum_of_x_mul_y - sum_x * sum_y) / denominator;
        return slope;
    }
};

template <typename T>
struct RegrSlopeFunc : AggregateFunctionRegrData<T> {
    static constexpr const char* name = "regr_slope";

    Float64 get_result() const { return this->get_slope(); }
};

template <typename T>
struct RegrInterceptFunc : AggregateFunctionRegrData<T> {
    static constexpr const char* name = "regr_intercept";

    Float64 get_result() const {
        auto slope = this->get_slope();
        if (std::isnan(slope)) {
            return slope;
        } else {
            Float64 intercept = (this->sum_y - slope * this->sum_x) / (double)this->count;
            return intercept;
        }
    }
};

template <typename RegrFunc, bool y_nullable, bool x_nullable>
class AggregateFunctionRegrSimple
        : public IAggregateFunctionDataHelper<
                  RegrFunc, AggregateFunctionRegrSimple<RegrFunc, y_nullable, x_nullable>> {
public:
    using Type = typename RegrFunc::Type;
    using XInputCol = ColumnVector<Type>;
    using YInputCol = ColumnVector<Type>;
    using ResultCol = ColumnVector<Float64>;

    explicit AggregateFunctionRegrSimple(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      RegrFunc, AggregateFunctionRegrSimple<RegrFunc, y_nullable, x_nullable>>(
                      argument_types_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return RegrFunc::name; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        bool y_null = false;
        bool x_null = false;
        const YInputCol* y_nested_column = nullptr;
        const XInputCol* x_nested_column = nullptr;

        if constexpr (y_nullable) {
            const ColumnNullable& y_column_nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            y_null = y_column_nullable.is_null_at(row_num);
            y_nested_column = assert_cast<const YInputCol*, TypeCheckOnRelease::DISABLE>(
                    y_column_nullable.get_nested_column_ptr().get());
        } else {
            y_nested_column = assert_cast<const YInputCol*, TypeCheckOnRelease::DISABLE>(
                    (*columns[0]).get_ptr().get());
        }

        if constexpr (x_nullable) {
            const ColumnNullable& x_column_nullable =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[1]);
            x_null = x_column_nullable.is_null_at(row_num);
            x_nested_column = assert_cast<const XInputCol*, TypeCheckOnRelease::DISABLE>(
                    x_column_nullable.get_nested_column_ptr().get());
        } else {
            x_nested_column = assert_cast<const XInputCol*, TypeCheckOnRelease::DISABLE>(
                    (*columns[1]).get_ptr().get());
        }

        if (x_null || y_null) {
            return;
        }

        Type y_value = y_nested_column->get_data()[row_num];
        Type x_value = x_nested_column->get_data()[row_num];

        this->data(place).add(y_value, x_value);
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
        Float64 result = data.get_result();
        if (std::isnan(result)) {
            dst_column_with_nullable.get_null_map_data().push_back(1);
            dst_column.insert_default();
        } else {
            dst_column_with_nullable.get_null_map_data().push_back(0);
            dst_column.get_data().push_back(result);
        }
    }
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"
