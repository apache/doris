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

#include <math.h>

#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Arena;
class BufferReadable;
class BufferWritable;

/**
 * SEM = sqrt(variance / count) = sqrt(m2 / (count * (count - 1)))
 * It uses Welford’s method for numerically stable one-pass computation of the mean and variance.
 */
struct AggregateFunctionSemData {
    double mean {};
    double m2 {}; // Cumulative sum of squares
    UInt64 count = 0;

    //  Let the old mean be mean_{n-1} and we receive a new value x_n.
    //  The new mean should be: mean_n = mean_{n-1} + (x_n - mean_{n-1}) / n
    //  The new M2 should capture total squared deviations from the new mean:
    //      M2_n = M2_{n-1} + (x_n - mean_{n-1}) * (x_n - mean_n)
    void add(const double& value) {
        count++;
        double delta = value - mean;
        mean += delta / static_cast<double>(count);
        double delta2 = value - mean;
        m2 += delta * delta2;
    }

    // Suppose we have dataset A (count_a, mean_a, M2_a) and B (count_b, mean_b, M2_b).
    // When merging:
    //   - The total count is count_a + count_b
    //   - The new mean is the weighted average of the two means
    //   - The new M2 accumulates both variances and an adjustment term to account for
    //     the difference in means between A and B:
    //        M2 = M2_a + M2_b + delta^2 * count_a * count_b / total_count
    // where delta = mean_b - mean_a
    void merge(const AggregateFunctionSemData& rhs) {
        UInt64 total_count = count + rhs.count;
        double delta = rhs.mean - mean;
        mean = (mean * static_cast<double>(count) + rhs.mean * static_cast<double>(rhs.count)) /
               static_cast<double>(total_count);
        m2 += rhs.m2 + delta * delta * static_cast<double>(count) * static_cast<double>(rhs.count) /
                               static_cast<double>(total_count);
        count = total_count;
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(mean);
        buf.write_binary(m2);
        buf.write_binary(count);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(mean);
        buf.read_binary(m2);
        buf.read_binary(count);
    }

    void reset() {
        mean = {};
        m2 = {};
        count = 0;
    }

    double result() const {
        if (count < 2) {
            return 0;
        }
        double dCount = static_cast<double>(count);
        double result = std::sqrt(m2 / (dCount * (dCount - 1.0)));
        return result;
    }
};

template <typename Data>
class AggregateFunctionSem final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSem<Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionSem(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSem<Data>>(argument_types_) {}

    String get_name() const override { return "sem"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        this->data(place).add((double)column.get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

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
        auto& column = assert_cast<ColumnFloat64&>(to);
        column.get_data().push_back(this->data(place).result());
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized