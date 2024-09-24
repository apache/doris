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
#include <cstddef>
#include <memory>

#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct AggregateFunctionRegrInterceptData {
    UInt64 count = 0;
    double sum_x = 0.0;
    double sum_y = 0.0;
    double sum_xx = 0.0;
    double sum_xy = 0.0;
};

class AggregateFunctionRegrIntercept final
        : public IAggregateFunctionDataHelper<AggregateFunctionRegrInterceptData, AggregateFunctionRegrIntercept> {
public:
    AggregateFunctionRegrIntercept(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionRegrInterceptData, AggregateFunctionRegrIntercept>(argument_types_) {
    }

    String get_name() const override { return "regr_intercept"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        bool y_valid = true;
        bool x_valid = true;
        const IColumn* y_column = columns[0];
        const IColumn* x_column = columns[1];

        if (y_column->is_nullable()) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(*y_column);
            y_valid = !nullable_col.is_null_at(row_num);
            y_column = &nullable_col.get_nested_column();
        }

        if (x_column->is_nullable()) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(*x_column);
            x_valid = !nullable_col.is_null_at(row_num);
            x_column = &nullable_col.get_nested_column();
        }


        if (y_valid && x_valid) {
            double y_value = 0.0;
            double x_value = 0.0;

            // Handle different numeric types for y
            if (const auto* float64_col = check_and_get_column<ColumnFloat64>(y_column)) {
                y_value = float64_col->get_data()[row_num];
            } else if (const auto* float32_col = check_and_get_column<ColumnFloat32>(y_column)) {
                y_value = static_cast<double>(float32_col->get_data()[row_num]);
            } else if (const auto* int64_col = check_and_get_column<ColumnInt64>(y_column)) {
                y_value = static_cast<double>(int64_col->get_data()[row_num]);
            } else if (const auto* int32_col = check_and_get_column<ColumnInt32>(y_column)) {
                y_value = static_cast<double>(int32_col->get_data()[row_num]);
            } else {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Unexpected column type for y in regr_intercept");
            }

            // Handle different numeric types for x
            if (const auto* float64_col = check_and_get_column<ColumnFloat64>(x_column)) {
                x_value = float64_col->get_data()[row_num];
            } else if (const auto* float32_col = check_and_get_column<ColumnFloat32>(x_column)) {
                x_value = static_cast<double>(float32_col->get_data()[row_num]);
            } else if (const auto* int64_col = check_and_get_column<ColumnInt64>(x_column)) {
                x_value = static_cast<double>(int64_col->get_data()[row_num]);
            } else if (const auto* int32_col = check_and_get_column<ColumnInt32>(x_column)) {
                x_value = static_cast<double>(int32_col->get_data()[row_num]);
            } else {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Unexpected column type for x in regr_intercept");
            }

            data(place).count += 1;
            data(place).sum_x += x_value;
            data(place).sum_y += y_value;
            data(place).sum_xx += x_value * x_value;
            data(place).sum_xy += x_value * y_value;
        }
    }

    void reset(AggregateDataPtr __restrict place) const override {
        data(place).count = 0;
        data(place).sum_x = 0.0;
        data(place).sum_y = 0.0;
        data(place).sum_xx = 0.0;
        data(place).sum_xy = 0.0;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        data(place).count += data(rhs).count;
        data(place).sum_x += data(rhs).sum_x;
        data(place).sum_y += data(rhs).sum_y;
        data(place).sum_xx += data(rhs).sum_xx;
        data(place).sum_xy += data(rhs).sum_xy;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
        write_float_binary(data(place).sum_x, buf);
        write_float_binary(data(place).sum_y, buf);
        write_float_binary(data(place).sum_xx, buf);
        write_float_binary(data(place).sum_xy, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(data(place).count, buf);
        read_float_binary(data(place).sum_x, buf);
        read_float_binary(data(place).sum_y, buf);
        read_float_binary(data(place).sum_xx, buf);
        read_float_binary(data(place).sum_xy, buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        UInt64 n = data(place).count;
        double sum_x = data(place).sum_x;
        double sum_y = data(place).sum_y;
        double sum_xx = data(place).sum_xx;
        double sum_xy = data(place).sum_xy;

        double denominator = n * sum_xx - sum_x * sum_x;
        bool result_is_null = (n == 0 || denominator == 0.0);

        double intercept = 0.0;
        if (!result_is_null) {
            double slope = (n * sum_xy - sum_x * sum_y) / denominator;
            intercept = (sum_y - slope * sum_x) / n;
        }

        if (to.is_nullable()) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(result_is_null);
            auto& nested_column = assert_cast<ColumnFloat64&>(null_column.get_nested_column());
            nested_column.get_data().push_back(intercept);
        } else {
            if (result_is_null) {
                assert_cast<ColumnFloat64&>(to).get_data().push_back(
                    std::numeric_limits<double>::quiet_NaN());
            } else {
                assert_cast<ColumnFloat64&>(to).get_data().push_back(intercept);
            }
        }
    }

};

} // namespace doris::vectorized