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

#include <array>
#include <memory>

#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vexpr_context.h"
#include "util/percentile_util.h"
#include "util/reservoir_sampler.h"

namespace doris {

class Arena;
class BufferReadable;

struct QuantileReservoirSampler {
    void init(double input_level) {
        if (!level_initialized) {
            check_quantile(input_level);
            level = input_level;
            level_initialized = true;
        }
    }

    bool is_level_initialized() const { return level_initialized; }

    void add(const double x) { data.insert(x); }

    void add_batch(const double* values, size_t size) { data.insert_many(values, size); }

    void merge(const QuantileReservoirSampler& rhs) {
        if (!rhs.level_initialized) {
            return;
        }
        if (!level_initialized) {
            level = rhs.level;
            level_initialized = true;
        }
        data.merge(rhs.data);
    }

    void reset() {
        // The level is a semantic constant for the aggregate expression, so keep the validated
        // value when analytic execution resets only the samples for the next window frame.
        data.clear();
    }

    void serialize(BufferWritable& buf) const {
        buf.write_binary(level_initialized);
        if (!level_initialized) {
            return;
        }
        buf.write_binary(level);
        data.write(buf);
    }

    void deserialize(BufferReadable& buf) {
        level = 0.0;
        data.clear();
        buf.read_binary(level_initialized);
        if (!level_initialized) {
            return;
        }
        buf.read_binary(level);
        check_quantile(level);
        data.read(buf);
    }

    double get() const {
        // The caller is a ConstAggregateDataPtr, but it itself is an AggregateDataPtr.
        // To call a non-const method here, a const_cast is required.
        return const_cast<ReservoirSampler&>(data).quantileInterpolated(this->level);
    }

private:
    double level = 0.0;
    bool level_initialized = false;
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

    const std::vector<size_t>& get_const_argument_indexes() const override {
        static const std::vector<size_t> indexes {1};
        return indexes;
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        auto value = assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0])
                             .get_data()[row_num];
        auto& state = this->data(place);
        if (!state.is_level_initialized()) {
            const auto& level_column = *check_and_get_column_with_const<ColumnFloat64>(*columns[1]);
            state.init(level_column.get_data()[0]);
        }
        state.add(value);
    }

    void check_input_columns_type(const IColumn** columns) const override {
        this->template check_argument_column_type<ColumnFloat64>(columns[0]);
        this->template check_const_argument_column_type<ColumnFloat64>(columns[1]);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena&) const override {
        const auto& sources =
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        auto& state = this->data(place);
        if (!state.is_level_initialized()) {
            const auto& level_column = *check_and_get_column_with_const<ColumnFloat64>(*columns[1]);
            state.init(level_column.get_data()[0]);
        }
        state.add_batch(sources.get_data().data(), batch_size);
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
            auto& state = this->data(place);
            if (!state.is_level_initialized()) {
                const auto& level_column =
                        *check_and_get_column_with_const<ColumnFloat64>(*columns[1]);
                state.init(level_column.get_data()[0]);
            }
            state.add_batch(sources.get_data().data() + frame_start, frame_end - frame_start);
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
