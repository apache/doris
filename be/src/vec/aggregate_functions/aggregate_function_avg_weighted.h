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

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <memory>
#include <type_traits>

#include "runtime/decimalv2_value.h"
#include "util/binary_cast.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
template <typename T>
class ColumnDecimal;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionAvgWeightedData {
    void add(const T& data_val, double weight_val) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        if constexpr (IsDecimalV2<T>) {
            DecimalV2Value value = binary_cast<Int128, DecimalV2Value>(data_val);
            data_sum = data_sum + (double(value) * weight_val);
        } else {
            data_sum = data_sum + (data_val * weight_val);
        }
        weight_sum = weight_sum + weight_val;
    }

    void write(BufferWritable& buf) const {
        write_binary(data_sum, buf);
        write_binary(weight_sum, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(data_sum, buf);
        read_binary(weight_sum, buf);
    }

    void merge(const AggregateFunctionAvgWeightedData& rhs) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        data_sum = data_sum + rhs.data_sum;
        weight_sum = weight_sum + rhs.weight_sum;
    }

    void reset() {
        data_sum = 0.0;
        weight_sum = 0.0;
    }

    double get() const { return weight_sum ? data_sum / weight_sum : std::nan(""); }

    double data_sum = 0.0;
    double weight_sum = 0.0;
};

template <typename T>
class AggregateFunctionAvgWeight final
        : public IAggregateFunctionDataHelper<AggregateFunctionAvgWeightedData<T>,
                                              AggregateFunctionAvgWeight<T>> {
public:
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

    AggregateFunctionAvgWeight(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionAvgWeightedData<T>,
                                           AggregateFunctionAvgWeight<T>>(argument_types_) {}

    String get_name() const override { return "avg_weighted"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = assert_cast<const ColVecType&>(*columns[0]);
        const auto& weight = assert_cast<const ColumnVector<Float64>&>(*columns[1]);
        this->data(place).add(column.get_data()[row_num], weight.get_element(row_num));
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

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
        auto& column = assert_cast<ColumnVector<Float64>&>(to);
        column.get_data().push_back(this->data(place).get());
    }
};

} // namespace doris::vectorized
