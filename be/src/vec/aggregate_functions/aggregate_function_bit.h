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

#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionGroupBitOrData {
    static constexpr auto name = "groupBitOr";

    T bit = 0;

    void add(T value) { bit |= value; }

    void merge(const AggregateFunctionGroupBitOrData& rhs) { bit |= rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(bit, buf); }

    void read(BufferReadable& buf) { read_binary(bit, buf); }

    T get() const { return bit; }
};

template <typename T>
struct AggregateFunctionGroupBitAndData {
    static constexpr auto name = "groupBitAnd";

    T bit = -1;

    void add(T value) { bit &= value; }

    void merge(const AggregateFunctionGroupBitAndData& rhs) { bit &= rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(bit, buf); }

    void read(BufferReadable& buf) { read_binary(bit, buf); }

    T get() const { return bit; }
};

template <typename T>
struct AggregateFunctionGroupBitXorData {
    static constexpr auto name = "groupBitXor";

    T bit = 0;

    void add(T value) { bit ^= value; }

    void merge(const AggregateFunctionGroupBitXorData& rhs) { bit ^= rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(bit, buf); }

    void read(BufferReadable& buf) { read_binary(bit, buf); }

    T get() const { return bit; }
};

/// Counts bitwise operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitwise final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>> {
public:
    AggregateFunctionBitwise(const DataTypePtr& type)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>({type}, {}) {}

    using ResultType = std::conditional_t<IsNumber<T>, Int64, Int32>;
    using ResultDataType =
            std::conditional_t<IsNumber<T>, DataTypeNumber<Int64>, DataTypeNumber<Int32>>;
    using ColVecType = std::conditional_t<IsNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult =
            std::conditional_t<IsNumber<T>, ColumnVector<Int64>, ColumnVector<Int32>>;

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeNumber<T>>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).sum = {}; }

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
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }
};

} // namespace doris::vectorized