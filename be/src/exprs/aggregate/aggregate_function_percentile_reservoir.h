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

#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "util/reservoir_sampler.h"

namespace doris {

class Arena;
class BufferReadable;

struct QuantileReservoirSampler {
    void add(const double x, const double input_level) {
        this->level = input_level;
        data.insert(x);
    }

    void add_batch(const double* values, size_t size, const double input_level) {
        this->level = input_level;
        data.insert_many(values, size);
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

    double get() const {
        // The caller is a ConstAggregateDataPtr, but it itself is an AggregateDataPtr.
        // To call a non-const method here, a const_cast is required.
        return const_cast<ReservoirSampler&>(data).quantileInterpolated(this->level);
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
        auto value = assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0])
                             .get_data()[row_num];
        auto level = assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1])
                             .get_data()[0];
        this->data(place).add(value, level);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena&) const override {
        const auto& sources =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& levels =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        this->data(place).add_batch(sources.get_data().data(), batch_size, levels.get_data()[0]);
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena&, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if (frame_start < frame_end) {
            const auto& sources =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            const auto& levels =
                    assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1]);
            this->data(place).add_batch(sources.get_data().data() + frame_start,
                                        frame_end - frame_start, levels.get_data()[0]);
            *use_null_result = false;
            *could_use_previous_result = true;
        } else if (!*could_use_previous_result) {
            *use_null_result = true;
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnFloat64&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(
                this->data(place).get());
    }
};

} // namespace doris