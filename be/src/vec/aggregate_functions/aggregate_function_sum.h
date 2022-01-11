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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionSum.h
// and modified by Doris

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
    T sum {};

    void add(T value) { sum += value; }

    void merge(const AggregateFunctionSumData& rhs) { sum += rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(sum, buf); }

    void read(BufferReadable& buf) { read_binary(sum, buf); }

    T get() const { return sum; }
};

template <typename Int32> inline
void simd_sum_int32(const Int32* columns, size_t begin, size_t end, Int32& sum)
{
    sum = 0;
    int d = (end - begin + 1);
    int n = d / 4;
    int m = d % 4;

    __m128i* addr = (__m128i*)(&columns[begin]);
    __m128i sse_sum = _mm_setzero_si128();

    for (int i = 0; i < n; ++i) {
        sse_sum = _mm_add_epi32(sse_sum, _mm_load_si128(addr++));
    }

    for (int i = 0; i < m; ++i) {
        sum += ((Int32*)addr)[i];
    }

    sse_sum = _mm_hadd_epi32(sse_sum, sse_sum);
    sse_sum = _mm_hadd_epi32(sse_sum, sse_sum);
    sum += _mm_cvtsi128_si32(sse_sum); // [SSE2] 返回低32位
}

template <typename Int64> inline
void simd_sum_int64(const Int64* columns, size_t begin, size_t end, Int64& sum)
{
    sum = 0;
    int d = (end - begin + 1);
    int n = d / 4;
    int m = d % 4;

    __m256i* addr = (__m256i*)(&columns[begin]);
    __m256i sse_sum = _mm256_setzero_si256();
    for (int i = 0; i < n; ++i) {
        sse_sum = _mm256_add_epi64(sse_sum, _mm256_lddqu_si256(addr++));
    }

    for (int i = 0; i < m; ++i) {
        sum += ((Int64*)addr)[i];
    }

    Int64 x[4];
    _mm256_store_si256((__m256i*)x, sse_sum);
    sum += x[0] + x[1] + x[2] + x[3];
}

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

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
            const IColumn** columns, Arena* arena, bool has_null) override {
        Data& data = this->data(place);
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        if constexpr (std::is_same_v<T, Int32> && std::is_same_v<T, TResult>) {
            simd_sum_int32(&column.get_element(0), batch_begin, batch_end, data.sum);
        } else if constexpr (std::is_same_v<T, UInt32> && std::is_same_v<T, TResult>) {
            simd_sum_int32(&column.get_element(0), batch_begin, batch_end, data.sum);
        } else if constexpr (std::is_same_v<T, Int64> && std::is_same_v<T, TResult>) {
            simd_sum_int64(&column.get_element(0), batch_begin, batch_end, data.sum);
        } else if constexpr (std::is_same_v<T, UInt64> && std::is_same_v<T, TResult>) {
            simd_sum_int64(&column.get_element(0), batch_begin, batch_end, data.sum);
        } else {
            for (size_t i = batch_begin; i <= batch_end; ++i) {
                data.add(column.get_element(i));
            }
        }
    }
    
    void reset(AggregateDataPtr place) const override {
        this->data(place).sum = {};
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf, Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }

    const char* get_header_file_path() const override { return __FILE__; }

private:
    UInt32 scale;
};

AggregateFunctionPtr create_aggregate_function_sum_reader(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const Array& parameters,
                                                   const bool result_is_nullable);

} // namespace doris::vectorized
