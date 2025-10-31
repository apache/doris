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

struct AggregateFunctionSemData {
    double sum {};
    double sumOfSquares {};
    UInt64 count = 0;

    void add(const double& value) {
        sum += value;
        sumOfSquares += value * value;
        count++;
    }

    void merge(const AggregateFunctionSemData& rhs) {
        sum += rhs.sum;
        sumOfSquares += rhs.sumOfSquares;
        count += rhs.count;
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(sum);
        buf.write_binary(sumOfSquares);
        buf.write_binary(count);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(sum);
        buf.read_binary(sumOfSquares);
        buf.read_binary(count);
    }

    void reset() {
        sum = {};
        sumOfSquares = {};
        count = 0;
    }

    double result() const {
        if (count <= 1) {
            return 0;
        }
        double dCount = static_cast<double>(count);
        double result =
                std::sqrt((sumOfSquares - (sum * sum) / dCount) / ((dCount) * (dCount - 1.0)));
        return result;
    }
};

template <PrimitiveType T, typename Data>
class AggregateFunctionSem final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSem<T, Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;

    AggregateFunctionSem(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSem<T, Data>>(argument_types_) {}

    String get_name() const override { return "sem"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
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