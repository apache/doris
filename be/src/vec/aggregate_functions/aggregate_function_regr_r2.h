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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace doris::vectorized

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionRegrR2Data {
    UInt64 count = 0;
    double sum_x = 0.0;
    double sum_y = 0.0;
    double sum_xx = 0.0;
    double sum_yy = 0.0;
    double sum_xy = 0.0;

    double get_regr_r2_result() const {
        double sxx = sum_xx - (sum_x * sum_x) / count;
        double syy = sum_yy - (sum_y * sum_y) / count;
        double sxy = sum_xy - (sum_x * sum_y) / count;

        return (sxx != 0 && syy != 0) ? (sxy * sxy) / (sxx * syy) : 0.0;
    }

    void add(const IColumn* column_y, const IColumn* column_x, size_t row_num) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        if (column_y->is_nullable() || column_x->is_nullable()) {
            return;
        }

        const auto& sources_x =
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column_x);
        const auto& sources_y =
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*column_y);
        const double x = sources_x.get_data()[row_num];
        const double y = sources_y.get_data()[row_num];

        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_yy += y * y;
        sum_xy += x * y;
        count += 1;
    }

    void reset() {
        sum_x = 0.0;
        sum_y = 0.0;
        sum_xx = 0.0;
        sum_yy = 0.0;
        sum_xy = 0.0;
        count = 0;
    }

    void merge(const AggregateFunctionRegrR2Data& rhs) {
        if (rhs.count == 0) {
            return;
        }
        sum_x += rhs.sum_x;
        sum_y += rhs.sum_y;
        sum_xx += rhs.sum_xx;
        sum_yy += rhs.sum_yy;
        sum_xy += rhs.sum_xy;
        count += rhs.count;
    }

    void write(BufferWritable& buf) const {
        write_binary(sum_x, buf);
        write_binary(sum_y, buf);
        write_binary(sum_xx, buf);
        write_binary(sum_yy, buf);
        write_binary(sum_xy, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum_x, buf);
        read_binary(sum_y, buf);
        read_binary(sum_xx, buf);
        read_binary(sum_yy, buf);
        read_binary(sum_xy, buf);
        read_binary(count, buf);
    }

    void insert_result_into(IColumn& to) const {
        auto& col = assert_cast<ColumnFloat64&>(to);
        if (this->count < 2) {
            col.insert_default();
        } else {
            col.get_data().push_back(this->get_regr_r2_result());
        }
    }
};

template <typename Data>
class AggregateFunctionRegrR2
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionRegrR2<Data>> {
public:
    AggregateFunctionRegrR2(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionRegrR2<Data>>(argument_types_) {}

    String get_name() const override { return "regr_r2"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

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