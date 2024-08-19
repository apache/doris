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

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/io/io_helper.h"

// TODO: 添加upper和lower
// TODO: pre_sum修改为acc_count
// TODO: 避免深拷贝string
// TODO: 支持时间类型
// TODO: 完善单元测试
// TODO: 端到端测试

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionLinearHistogramData {
    // bucket key limits
    const static int32_t MIN_BUCKET_KEY = std::numeric_limits<int32_t>::min();
    const static int32_t MAX_BUCKET_KEY = std::numeric_limits<int32_t>::max();
    
private:
    // influxdb use double
    double interval;
    double offset;
    std::map<int32_t, size_t> buckets;

public:
    // reset
    void reset() {
        offset = 0;
        interval = 0;
        buckets.clear();
    }

    void set_parameters(double input_interval, double input_offset) {
        interval = input_interval;
        offset = input_offset;
    }
    
    // add
    void add(const T& value) {
        auto key = std::floor((value - offset) / interval);
        if (key <= MIN_BUCKET_KEY || key >= MAX_BUCKET_KEY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "{} exceeds the bucket range limit",
                                       value);
        }
        buckets[static_cast<int32_t>(key)]++;
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
            int32_t key;
            size_t count;
            read_binary(key, buf);
            read_binary(count, buf);
            buckets[key] = count;
        }
    }

    // insert_result_into
    void insert_result_into_test(IColumn& to) const {
        auto& column = assert_cast<ColumnString&>(to);
        std::string res = "result: ";
        for (const auto& [key, count] : buckets) {
            res += std::to_string(key * interval + offset) + ":" + std::to_string(count) + ",";
        }
        column.insert_data(res.c_str(), res.length());
    }

    void insert_result_into(IColumn& to) const {
        rapidjson::Document doc;
        doc.SetObject();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

        unsigned num_buckets = buckets.empty() ? 0 : buckets.rbegin()->first + 1;
        doc.AddMember("num_buckets", num_buckets, allocator);

        rapidjson::Value bucket_arr(rapidjson::kArrayType);
        bucket_arr.Reserve(num_buckets, allocator);

        unsigned idx = 0;
        auto data_type = std::make_shared<DataTypeFloat64>();
        double left = offset;
        std::string left_str = data_type->to_string(left);
        size_t count = 0;
        size_t pre_sum = 0;
        rapidjson::Value buf;     

        for (const auto& [key, count_] : buckets) {
            for (; idx <= key; ++idx) {
                rapidjson::Value bucket_json(rapidjson::kObjectType);
                buf.SetString(left_str.data(), static_cast<rapidjson::SizeType>(left_str.size()), allocator);
                bucket_json.AddMember("lower", buf, allocator);
                left += interval;
                left_str = data_type->to_string(left);
                buf.SetString(left_str.data(), static_cast<rapidjson::SizeType>(left_str.size()), allocator);
                bucket_json.AddMember("upper", buf, allocator);
                count = (idx == key) ? count_ : 0;
                bucket_json.AddMember("count", count, allocator);
                bucket_json.AddMember("pre_sum", pre_sum, allocator);
                pre_sum += count;

                bucket_arr.PushBack(bucket_json, allocator);
            }
        }

        doc.AddMember("buckets", bucket_arr, allocator);
        
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        auto res = std::string(buffer.GetString());

        auto& column = assert_cast<ColumnString&>(to);
        column.insert_data(res.c_str(), res.length());
    }
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
