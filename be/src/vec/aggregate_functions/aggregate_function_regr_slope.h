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

#include <boost/iterator/iterator_facade.hpp>
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
namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

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
        if (count == 0 || denominator == 0.0) {
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

    void add(const IColumn* column_y, const IColumn* column_x, size_t row_num) {
        const auto& sources_x = assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column_x);
        const auto& sources_y = assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column_y);
        const auto& value_x = sources_x.get_data()[row_num];
        const auto& value_y = sources_y.get_data()[row_num];
        sum_x += value_x;
        sum_y += value_y;
        sum_of_x_mul_y += value_x * value_y;
        sum_of_x_squared += value_x * value_x;
        count += 1;
    }
};

template <typename T, typename Data>
struct RegrSlopeData : AggregateFunctionRegrSlopeData<T> {
    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to);
        col.get_data().push_back(this->get_slope_result());
    }
};

template <typename Data>
struct RegrSlopeName : Data {
    static const char* name() { return "regr_slope"; }
};

template <typename T, typename Data>
class AggregateFunctionRegrFunc
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionRegrFunc<T, Data>> {
public:
    AggregateFunctionRegrFunc(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionRegrFunc<T, Data>>(argument_types_) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        this->data(place).add(columns[0], columns[1], row_num);
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
        this->data(place).insert_result_into(to);
    }
};

} // namespace doris::vectorized