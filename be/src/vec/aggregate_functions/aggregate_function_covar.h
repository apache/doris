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

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
template <typename>
class ColumnVector;

template <typename T>
struct BaseData {
    BaseData() = default;
    virtual ~BaseData() = default;
    static DataTypePtr get_return_type() { return std::make_shared<DataTypeFloat64>(); }

    void write(BufferWritable& buf) const {
        write_binary(sum_x, buf);
        write_binary(sum_y, buf);
        write_binary(sum_xy, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum_x, buf);
        read_binary(sum_y, buf);
        read_binary(sum_xy, buf);
        read_binary(count, buf);
    }

    void reset() {
        sum_x = 0.0;
        sum_y = 0.0;
        sum_xy = 0.0;
        count = 0;
    }

    // Cov(X, Y) = E(XY) - E(X)E(Y)
    double get_pop_result() const {
        if (count == 1) {
            return 0.0;
        }
        return sum_xy / (double)count - sum_x * sum_y / ((double)count * (double)count);
    }

    double get_samp_result() const {
        return sum_xy / double(count - 1) -
               sum_x * sum_y / ((double)(count) * ((double)(count - 1)));
    }

    void merge(const BaseData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_xy += rhs.sum_xy;
        count += rhs.count;
    }

    void add(const IColumn* column_x, const IColumn* column_y, size_t row_num) {
        const auto& sources_x =
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column_x);
        double source_data_x = double(sources_x.get_data()[row_num]);
        const auto& sources_y =
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column_y);
        double source_data_y = double(sources_y.get_data()[row_num]);

        sum_x += source_data_x;
        sum_y += source_data_y;
        sum_xy += source_data_x * source_data_y;
        count += 1;
    }

    double sum_x {};
    double sum_y {};
    double sum_xy {};
    int64_t count {};
};

template <typename T>
struct PopData : BaseData<T> {
    static const char* name() { return "covar"; }

    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&>(to);
        col.get_data().push_back(this->get_pop_result());
    }
};

template <typename T>
struct SampData : BaseData<T> {
    static const char* name() { return "covar_samp"; }

    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&>(to);
        if (this->count == 1 || this->count == 0) {
            col.insert_default();
        } else {
            col.get_data().push_back(this->get_samp_result());
        }
    }
};

template <typename Data>
class AggregateFunctionSampCovariance
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSampCovariance<Data>> {
public:
    AggregateFunctionSampCovariance(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSampCovariance<Data>>(
                      argument_types_) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return Data::get_return_type(); }

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
