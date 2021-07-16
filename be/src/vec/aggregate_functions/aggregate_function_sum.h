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

#include <istream>
#include <ostream>
#include <type_traits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionSumData {
    T sum{};

    void add(T value) { sum += value; }

    void merge(const AggregateFunctionSumData& rhs) { sum += rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(sum, buf); }

    void read(BufferReadable& buf) { read_binary(sum, buf); }

    T get() const { return sum; }
};

template <typename T>
struct AggregateFunctionSumKahanData {
    static_assert(
            std::is_floating_point_v<T>,
            "It doesn't make sense to use Kahan Summation algorithm for non floating point types");

    T sum{};
    T compensation{};

    void add(T value) {
        auto compensated_value = value - compensation;
        auto new_sum = sum + compensated_value;
        compensation = (new_sum - sum) - compensated_value;
        sum = new_sum;
    }

    void merge(const AggregateFunctionSumKahanData& rhs) {
        auto raw_sum = sum + rhs.sum;
        auto rhs_compensated = raw_sum - sum;
        auto compensations = ((rhs.sum - rhs_compensated) + (sum - (raw_sum - rhs_compensated))) +
                             compensation + rhs.compensation;
        sum = raw_sum + compensations;
        compensation = compensations - (sum - raw_sum);
    }

    void write(WriteBuffer& buf) const {
        write_binary(sum, buf);
        write_binary(compensation, buf);
    }

    void read(ReadBuffer& buf) {
        read_binary(sum, buf);
        read_binary(compensation, buf);
    }

    T get() const { return sum; }
};

/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data>
class AggregateFunctionSum final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>> {
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>,
                                              DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String get_name() const override { return "sum"; }

    AggregateFunctionSum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(
                      argument_types_, {}),
              scale(0) {}

    AggregateFunctionSum(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(
                      argument_types_, {}),
              scale(get_decimal_scale(data_type)) {}

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>)
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }

    const char* get_header_file_path() const override { return __FILE__; }

private:
    UInt32 scale;
};

} // namespace doris::vectorized
