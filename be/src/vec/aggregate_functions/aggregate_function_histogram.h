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

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <cmath>
#include <iostream>

#include "runtime/datetime_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

// TODO: support input parameters and statistics of sampling
const int64_t MAX_BUCKET_SIZE = 128;
const float_t SAMPLE_RATE = 1.0;

template <typename T>
struct Bucket {
public:
    Bucket() = default;
    Bucket(T value, size_t pre_sum)
            : lower(value), upper(value), count(1), pre_sum(pre_sum), ndv(1) {}
    Bucket(T lower, T upper, size_t count, size_t pre_sum, size_t ndv)
            : lower(lower), upper(upper), count(count), pre_sum(pre_sum), ndv(ndv) {}

    T lower;
    T upper;
    int64_t count;
    int64_t pre_sum;
    int64_t ndv;
};

struct AggregateFunctionHistogramBase {
public:
    AggregateFunctionHistogramBase() = default;

    template <typename T>
    static std::vector<Bucket<T>> build_bucket_from_data(const std::vector<T>& sorted_data,
                                                         int64_t max_bucket_size) {
        std::vector<Bucket<T>> buckets;

        if (sorted_data.size() > 0) {
            int64_t data_size = sorted_data.size();
            int num_per_bucket = (int64_t)std::ceil((Float64)data_size / max_bucket_size);

            for (int i = 0; i < data_size; ++i) {
                T v = sorted_data[i];
                if (buckets.empty()) {
                    Bucket<T> bucket(v, 0);
                    buckets.emplace_back(bucket);
                } else {
                    Bucket<T>* bucket = &buckets.back();
                    T upper = bucket->upper;
                    if (upper == v) {
                        bucket->count++;
                    } else if (bucket->count < num_per_bucket) {
                        bucket->count++;
                        bucket->ndv++;
                        bucket->upper = v;
                    } else {
                        int64_t pre_sum = bucket->pre_sum + bucket->count;
                        Bucket<T> new_bucket(v, pre_sum);
                        buckets.emplace_back(new_bucket);
                    }
                }
            }
        }

        return buckets;
    }

    template <typename T>
    static std::string build_json_from_bucket(const std::vector<Bucket<T>>& buckets,
                                              const DataTypePtr& data_type, int64_t max_bucket_size,
                                              int64_t sample_rate) {
        rapidjson::Document doc;
        doc.SetObject();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

        rapidjson::Value max_bucket_size_val(max_bucket_size);
        doc.AddMember("max_bucket_size", max_bucket_size_val, allocator);

        rapidjson::Value sample_rate_val(sample_rate);
        doc.AddMember("sample_rate", sample_rate_val, allocator);

        // buckets
        rapidjson::Value bucket_arr(rapidjson::kArrayType);

        if (!buckets.empty()) {
            int size = buckets.size();
            rapidjson::Value bucket_size_val(size);
            doc.AddMember("bucket_size", bucket_size_val, allocator);

            WhichDataType type(data_type);
            if (type.is_int() || type.is_float() || type.is_decimal() || type.is_string()) {
                for (int i = 0; i < size; ++i) {
                    std::string lower_str = numerical_to_string(buckets[i].lower);
                    std::string upper_str = numerical_to_string(buckets[i].upper);
                    to_bucket_json(allocator, bucket_arr, lower_str, upper_str,
                                   (int64_t)(buckets[i].count), (int64_t)(buckets[i].pre_sum),
                                   (int64_t)(buckets[i].ndv));
                }
            } else if (type.is_date_or_datetime()) {
                for (int i = 0; i < size; ++i) {
                    std::string lower_str = to_date_string(buckets[i].lower);
                    std::string upper_str = to_date_string(buckets[i].upper);
                    to_bucket_json(allocator, bucket_arr, lower_str, upper_str,
                                   (int64_t)(buckets[i].count), (int64_t)(buckets[i].pre_sum),
                                   (int64_t)(buckets[i].ndv));
                }
            } else {
                rapidjson::Value bucket_size_zero(0);
                doc.AddMember("bucket_size", bucket_size_zero, allocator);
                LOG(WARNING) << fmt::format("unable to convert histogram data of type {}",
                                            data_type->get_name());
            }
        }

        doc.AddMember("buckets", bucket_arr, allocator);

        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
        doc.Accept(writer);

        return std::string(sb.GetString());
    }

    static void to_bucket_json(rapidjson::Document::AllocatorType& allocator,
                               rapidjson::Value& bucket_arr, std::string lower, std::string upper,
                               int64 count, int64 pre_sum, int64 ndv) {
        rapidjson::Value bucket(rapidjson::kObjectType);

        rapidjson::Value lower_val(lower.c_str(), allocator);
        bucket.AddMember("lower", lower_val, allocator);

        rapidjson::Value upper_val(upper.c_str(), allocator);
        bucket.AddMember("upper", upper_val, allocator);

        rapidjson::Value count_val(count);
        bucket.AddMember("count", count_val, allocator);

        rapidjson::Value pre_sum_val(pre_sum);
        bucket.AddMember("pre_sum", pre_sum_val, allocator);

        rapidjson::Value ndv_val(ndv);
        bucket.AddMember("ndv", ndv_val, allocator);

        bucket_arr.PushBack(bucket, allocator);
    }

private:
    template <typename T>
    static std::string numerical_to_string(T input) {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", input);
        return std::string(buffer.data(), buffer.size());
    }

    template <typename T>
    static std::string to_date_string(T input) {
        auto* date_int = reinterpret_cast<vectorized::Int64*>(&input);
        auto date_value = binary_cast<vectorized::Int64, vectorized::VecDateTimeValue>(*date_int);
        char buf[32] = {};
        date_value.to_string(buf);
        return std::string(buf, strlen(buf));
    }
};

template <typename T>
struct AggregateFunctionHistogramData : public AggregateFunctionHistogramBase {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    PaddedPODArray<ElementType> data;

    void add(const IColumn& column, size_t row_num) {
        const auto& vec = assert_cast<const ColVecType&>(column).get_data();
        data.push_back(vec[row_num]);
    }

    void write(BufferWritable& buf) const {
        write_var_uint(data.size(), buf);
        buf.write(data.raw_data(), data.size() * sizeof(ElementType));
    }

    void read(BufferReadable& buf) {
        UInt64 rows = 0;
        read_var_uint(rows, buf);
        data.resize(rows);
        buf.read(reinterpret_cast<char*>(data.data()), rows * sizeof(ElementType));
    }

    void merge(const AggregateFunctionHistogramData& rhs) {
        data.insert(rhs.data.begin(), rhs.data.end());
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        size_t old_size = vec.size();
        vec.resize(old_size + data.size());
        memcpy(vec.data() + old_size, data.data(), data.size() * sizeof(ElementType));
    }

    std::string get(const DataTypePtr& data_type) const {
        std::vector<ElementType> vec_data;

        for (size_t i = 0; i < data.size(); ++i) {
            [[maybe_unused]] ElementType d = data[i];
            vec_data.push_back(d);
        }

        std::sort(vec_data.begin(), vec_data.end());
        auto buckets = build_bucket_from_data<ElementType>(vec_data, MAX_BUCKET_SIZE);
        auto result_str = build_json_from_bucket<ElementType>(buckets, data_type, MAX_BUCKET_SIZE,
                                                              SAMPLE_RATE);

        return result_str;
    }

    void reset() { data.clear(); }
};

template <>
struct AggregateFunctionHistogramData<StringRef> : public AggregateFunctionHistogramBase {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr data;

    AggregateFunctionHistogramData<ElementType>() { data = ColVecType::create(); }

    void add(const IColumn& column, size_t row_num) { data->insert_from(column, row_num); }

    void write(BufferWritable& buf) const {
        auto& col = assert_cast<ColVecType&>(*data);

        write_var_uint(col.size(), buf);
        buf.write(col.get_offsets().raw_data(), col.size() * sizeof(IColumn::Offset));

        write_var_uint(col.get_chars().size(), buf);
        buf.write(col.get_chars().raw_data(), col.get_chars().size());
    }

    void read(BufferReadable& buf) {
        auto& col = assert_cast<ColVecType&>(*data);
        UInt64 offs_size = 0;
        read_var_uint(offs_size, buf);
        col.get_offsets().resize(offs_size);
        buf.read(reinterpret_cast<char*>(col.get_offsets().data()),
                 offs_size * sizeof(IColumn::Offset));

        UInt64 chars_size = 0;
        read_var_uint(chars_size, buf);
        col.get_chars().resize(chars_size);
        buf.read(reinterpret_cast<char*>(col.get_chars().data()), chars_size);
    }

    void merge(const AggregateFunctionHistogramData& rhs) {
        data->insert_range_from(*rhs.data, 0, rhs.data->size());
    }

    void insert_result_into(IColumn& to) const {
        auto& to_str = assert_cast<ColVecType&>(to);
        to_str.insert_range_from(*data, 0, data->size());
    }

    std::string get(const DataTypePtr& data_type) const {
        std::vector<std::string> str_data;
        auto* res_column = reinterpret_cast<ColVecType*>(data.get());

        for (int i = 0; i < res_column->size(); ++i) {
            [[maybe_unused]] ElementType c = res_column->get_data_at(i);
            str_data.push_back(c.to_string());
        }

        std::sort(str_data.begin(), str_data.end());
        const auto buckets = build_bucket_from_data<std::string>(str_data, MAX_BUCKET_SIZE);
        auto result_str = build_json_from_bucket<std::string>(buckets, data_type, MAX_BUCKET_SIZE,
                                                              SAMPLE_RATE);

        return result_str;
    }

    void reset() { data->clear(); }
};

template <typename Data, typename T>
class AggregateFunctionHistogram final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionHistogram<Data, T>> {
public:
    AggregateFunctionHistogram() = default;
    AggregateFunctionHistogram(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionHistogram<Data, T>>(
                      argument_types_, {}),
              _argument_type(argument_types_[0]) {}

    std::string get_name() const override { return "histogram"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        if (columns[0]->is_null_at(row_num)) {
            return;
        }

        this->data(place).add(*columns[0], row_num);
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
        const std::string bucket_json = this->data(place).get(_argument_type);
        assert_cast<ColumnString&>(to).insert_data(bucket_json.c_str(), bucket_json.length());
    }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized
