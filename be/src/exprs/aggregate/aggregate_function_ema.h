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

// This file is adapted from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionExponentialMovingAverage.cpp

#pragma once

#include <array>
#include <cmath>
#include <memory>

#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vexpr_context.h"

namespace doris {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;

/**
 * Exponentially smoothed moving average over time.
 *
 * Each value corresponds to a timeunit index. The half_decay parameter is the
 * time lag at which exponential weights decay by one-half.
 *
 * State is a (value, time) pair representing the exponentially accumulated sum
 * at a reference time. To get the average, divide by sumWeights(half_decay).
 *
 * Formula:
 *   scale(dt, x) = 2^(-dt/x)
 *   sumWeights(x) = 1 / (1 - 2^(-1/x))
 *   add(v, t): merge current state with point (v, t)
 *   merge(a, b): move both to the later time, then sum values
 *   get():  value / sumWeights(half_decay)
 *
 * Usage: exponential_moving_average(half_decay, value, timeunit)
 *   - half_decay: constant double, the half-life period in timeunit units
 *   - value:      numeric column to average
 *   - timeunit:   numeric time index (not raw timestamp; use intDiv if needed)
 * Returns DOUBLE.
 */
struct ExponentialMovingAverageData {
    double value = 0.0;
    double time = 0.0;
    double half_decay = 0.0;

    static double scale(double time_passed, double hd) { return std::exp2(-time_passed / hd); }

    static double sum_weights(double hd) { return 1.0 / (1.0 - std::exp2(-1.0 / hd)); }

    void add(double new_value, double current_time, double hd) {
        half_decay = hd;
        ExponentialMovingAverageData other;
        other.value = new_value;
        other.time = current_time;
        merge_point(other, hd);
    }

    void merge_point(const ExponentialMovingAverageData& other, double hd) {
        if (time > other.time) {
            value = value + other.value * scale(time - other.time, hd);
        } else if (time < other.time) {
            value = other.value + value * scale(other.time - time, hd);
            time = other.time;
        } else {
            value = value + other.value;
        }
    }

    void merge(const ExponentialMovingAverageData& rhs) {
        double hd = half_decay != 0.0 ? half_decay : rhs.half_decay;
        if (hd == 0.0) {
            return;
        }
        half_decay = hd;
        merge_point(rhs, hd);
    }

    double get() const {
        if (half_decay == 0.0) {
            return 0.0;
        }
        return value / sum_weights(half_decay);
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(value);
        buf.write_binary(time);
        buf.write_binary(half_decay);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(value);
        buf.read_binary(time);
        buf.read_binary(half_decay);
    }

    void reset() {
        value = 0.0;
        time = 0.0;
        half_decay = 0.0;
    }
};

class AggregateFunctionExponentialMovingAverage final
        : public IAggregateFunctionDataHelper<ExponentialMovingAverageData,
                                              AggregateFunctionExponentialMovingAverage>,
          MultiExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionExponentialMovingAverage(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<ExponentialMovingAverageData,
                                           AggregateFunctionExponentialMovingAverage>(
                      argument_types_) {}

    String get_name() const override { return "exponential_moving_average"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    const std::vector<size_t>& get_const_argument_indexes() const override {
        static const std::vector<size_t> indexes {0};
        return indexes;
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const double new_value =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1])
                        .get_data()[row_num];
        const double current_time =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[2])
                        .get_data()[row_num];
        const auto& half_decay_column =
                *check_and_get_column_with_const<ColumnFloat64>(*columns[0]);
        this->data(place).add(new_value, current_time, half_decay_column.get_data()[0]);
    }

    void check_input_columns_type(const IColumn** columns) const override {
        this->template check_const_argument_column_type<ColumnFloat64>(columns[0]);
        this->template check_argument_column_type<ColumnFloat64>(columns[1]);
        this->template check_argument_column_type<ColumnFloat64>(columns[2]);
    }

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
        assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                this->data(place).get());
    }
};

} // namespace doris
