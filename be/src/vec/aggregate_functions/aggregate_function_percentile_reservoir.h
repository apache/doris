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

#include <memory>

#include "runtime/primitive_type.h"
#include "util/reservoir_sampler.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class Arena;
class BufferReadable;

struct QuantileReservoirSampler {
    void add(const double x, const double level) {
        this->level = level;
        data.insert(x);
    }

    void merge(const QuantileReservoirSampler& rhs) {
        level = rhs.level;
        data.merge(rhs.data);
    }

    void reset() {
        level = 0.0;
        data.clear();
    }

    void serialize(BufferWritable& buf) const {
        buf.write_binary(level);
        data.write(buf);
    }

    void deserialize(BufferReadable& buf) {
        buf.read_binary(level);
        data.read(buf);
    }

    double get() {
        return data.quantileInterpolated(this->level);
    }

private:
    double level = 0.0;
    ReservoirSampler data;
};

template <typename Data>
class AggregateFunctionPercentileReservoir final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionPercentileReservoir<Data>>,
          MultiExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionPercentileReservoir(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionPercentileReservoir<Data>>(
                      argument_types_) {}

    String get_name() const override { return "percentile_reservoir"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        auto value = assert_cast<const ColumnFloat64&>(*columns[0]).get_data()[row_num];
        auto level = assert_cast<const ColumnFloat64&>(*columns[1]).get_data()[0];
        this->data(place).add(value, level);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, AggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(AggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(AggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnFloat64&>(to).get_data().push_back(this->data(place).get());
    }
};

} // namespace doris::vectorized