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
struct AggregateFunctionRegrSxxData {
    UInt64 count = 0;
    double square_of_sum_x {};
    double sum_x {};
    void write(BufferWritable& buf) const {
        write_binary(square_of_sum_x, buf);
        write_binary(sum_x, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(square_of_sum_x, buf);
        read_binary(sum_x, buf);
        read_binary(count, buf);
    }

    void reset() {
        square_of_sum_x = {};
        sum_x = {};
        count = 0;
    }

    double get_regr_sxx_result() const { return square_of_sum_x - (sum_x * sum_x) / count; }

    void merge(const AggregateFunctionRegrSxxData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        square_of_sum_x += rhs.square_of_sum_x;
        sum_x += rhs.sum_x;
        count += rhs.count;
    }

    void add(const IColumn* column_y, const IColumn* column_x, size_t row_num) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        const auto& sources_x = assert_cast<const ColumnVector<T>&>(*column_x);
        const auto& value = sources_x.get_data()[row_num];
        sum_x += value;
        square_of_sum_x += value * value;
        count += 1;
    }
};
template <typename T>
struct AggregateFunctionRegrSxyData {
    using ResultType = T;
    UInt64 count = 0;
    double sum_x {};
    double sum_y {};
    double sum_of_x_mul_y {};
    void write(BufferWritable& buf) const {
        write_binary(sum_x, buf);
        write_binary(sum_y, buf);
        write_binary(sum_of_x_mul_y, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum_x, buf);
        read_binary(sum_y, buf);
        read_binary(sum_of_x_mul_y, buf);
        read_binary(count, buf);
    }

    void reset() {
        count = 0;
        sum_x = {};
        sum_y = {};
        sum_of_x_mul_y = {};
    }

    double get_regr_sxy_result() const { return sum_of_x_mul_y - (sum_x * sum_y) / count; }

    void merge(const AggregateFunctionRegrSxyData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_of_x_mul_y += rhs.sum_of_x_mul_y;
        count += rhs.count;
    }

    void add(const IColumn* column_y, const IColumn* column_x, size_t row_num) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        const auto& sources_x = assert_cast<const ColumnVector<T>&>(*column_x);
        const auto& sources_y = assert_cast<const ColumnVector<T>&>(*column_y);
        const auto& value_x = sources_x.get_data()[row_num];
        const auto& value_y = sources_y.get_data()[row_num];
        sum_x += value_x;
        sum_y += value_y;
        sum_of_x_mul_y += value_x * value_y;
        count += 1;
    }
};
template <typename T>
struct AggregateFunctionRegrSyyData {
    using ResultType = T;
    UInt64 count = 0;
    double square_of_sum_y {};
    double sum_y {};
    void write(BufferWritable& buf) const {
        write_binary(square_of_sum_y, buf);
        write_binary(sum_y, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(square_of_sum_y, buf);
        read_binary(sum_y, buf);
        read_binary(count, buf);
    }

    void reset() {
        square_of_sum_y = {};
        sum_y = {};
        count = 0;
    }
    double get_regr_syy_result() const { return square_of_sum_y - (sum_y * sum_y) / count; }
    void merge(const AggregateFunctionRegrSyyData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        square_of_sum_y += rhs.square_of_sum_y;
        sum_y += rhs.sum_y;
        count += rhs.count;
    }

    void add(const IColumn* column_y, const IColumn* column_x, size_t row_num) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        const auto& sources_y = assert_cast<const ColumnVector<T>&>(*column_y);
        const auto& value = sources_y.get_data()[row_num];
        square_of_sum_y += value * value;
        sum_y += value;
        count += 1;
    }
};

template <typename T, typename Data>
struct RegrSxxData : AggregateFunctionRegrSxxData<T> {
    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&>(to);
        col.get_data().push_back(this->get_regr_sxx_result());
    }
};

template <typename T, typename Data>
struct RegrSxyData : AggregateFunctionRegrSxyData<T> {
    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&>(to);
        col.get_data().push_back(this->get_regr_sxy_result());
    }
};
template <typename T, typename Data>
struct RegrSyyData : AggregateFunctionRegrSyyData<T> {
    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&>(to);
        col.get_data().push_back(this->get_regr_syy_result());
    }
};
template <typename Data>
struct RegrSxxName : Data {
    static const char* name() { return "regr_sxx"; }
};

template <typename Data>
struct RegrSxyName : Data {
    static const char* name() { return "regr_sxy"; }
};

template <typename Data>
struct RegrSyyName : Data {
    static const char* name() { return "regr_syy"; }
};

template <typename T, typename Data>
class AggregateFunctionRegrFunc
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionRegrFunc<T, Data>> {
public:
    AggregateFunctionRegrFunc(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionRegrFunc<T, Data>>(
                      argument_types_) {}

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