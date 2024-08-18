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

// TODO: 支持时间类型
// TODO: json格式输出
// TODO: 空数据处理
// TODO: 可空列处理
// TODO: 完善单元测试

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionLinearHistogramData {
    // max buckets number
    const static size_t MAX_BUCKETS = std::numeric_limits<size_t>::max() >> 7;

private:
    // influxdb use double
    double interval;
    double offset;
    std::map<size_t, size_t> buckets;

public:
    // reset
    void reset() {
        offset = 0;
        interval = 0;
        buckets.clear();
    }

    void set_parameters(double interval_, double offset_) {
        interval = interval_;
        offset = offset_;
    }
    
    // add
    void add(const T& value) {
        auto key = std::floor((value - offset) / interval);
        if (key < 0) {
            return;
        }
        if (key >= MAX_BUCKETS) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "{} is too large to fit in the buckets",
                                       value);
        }
        buckets[static_cast<size_t>(key)]++;
    }
    
    // merge
    void merge(const AggregateFunctionLinearHistogramData& rhs) {
        if (rhs.interval == 0) {
            return;
        }

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
            res += std::to_string(key * interval + offset) + ":" + std::to_string(count) + ",";
        }
        column.insert_data(res.c_str(), res.length());
    }

    // get
};

template <typename T, typename Data, bool has_offset>
class AggregateFunctionLinearHistogram final 
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionLinearHistogram<T, Data, has_offset>> {
public:
    using ColVecType = ColumnVectorOrDecimal<T>;

    AggregateFunctionLinearHistogram(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionLinearHistogram<T, Data, has_offset>>(argument_types_) {}
    
    std::string get_name() const override { return "linear_histogram"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        double interval = assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[1])
                        .get_data()[row_num];
        if (interval <= 0) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Invalid interval {}, row_num {}",
                                   interval, row_num);
        }

        double offset = 0;
        if constexpr (has_offset) {
            offset = assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*columns[2])
                        .get_data()[row_num];
        }

        this->data(place).set_parameters(interval, offset);
        
        this->data(place).add(
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0])
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
