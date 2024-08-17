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

#include <map>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/io/io_helper.h"

// TODO: 桶的最大数量界定
// TODO: 支持interval必须参数
// TODO: 支持offset可选参数
// TODO: 支持decimal类型
// TODO: 支持时间类型
// TODO: json格式输出

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionLinearHistogramData {
private:
    T offset = 0;
    T interval = 10;
    std::map<size_t, size_t> buckets;

public:
    // reset
    void reset() {
        offset = 0;
        buckets.clear();
    }
    
    // add
    void add(T value) {
        size_t key = bucket_key(value);
        buckets[key]++;
    }
    
    // merge
    void merge(const AggregateFunctionLinearHistogramData& rhs) {
        assert(offset == rhs.offset);
        assert(interval == rhs.interval);
        for (const auto& [key, count] : rhs.buckets) {
            buckets[key] += count;
        }
    }

    // write
    void write(BufferWritable& buf) const {
        write_binary(offset, buf);
        write_binary(interval, buf);
        write_binary(buckets.size(), buf);
        for (const auto& [key, count] : buckets) {
            write_binary(key, buf);
            write_binary(count, buf);
        }
    }

    // read
    void read(BufferReadable& buf) {
        read_binary(offset, buf);
        read_binary(interval, buf);
        size_t size;
        read_binary(size, buf);
        for (size_t i = 0; i < size; i++) {
            size_t key;
            size_t count;
            read_binary(key, buf);
            read_binary(count, buf);
            buckets[key] = count;
        }
    }

    // insert_result_into
    void insert_result_into(IColumn& to) const {
        auto& column = assert_cast<ColumnString&>(to);
        std::string res = "";
        for (const auto& [key, count] : buckets) {
            res += std::to_string(key) + ":" + std::to_string(count) + ",";
        }
        column.insert_data(res.c_str(), res.length());
    }

    // get
    
    // bucket_key
    inline size_t bucket_key(T value) {
        return static_cast<size_t>(std::floor((value - offset) / interval));
    }
};

template <typename T, typename Data>
class AggregateFunctionLinearHistogram final 
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionLinearHistogram<T, Data>> {
public:
    AggregateFunctionLinearHistogram(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionLinearHistogram<T, Data>>(argument_types_) {}
    
    std::string get_name() const override { return "linear_histogram"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        this->data(place).add(
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*columns[0])
                        .get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
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
