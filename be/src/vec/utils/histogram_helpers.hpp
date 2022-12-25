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

#include <boost/dynamic_bitset.hpp>

#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

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
    uint64_t count;
    uint64_t pre_sum;
    uint64_t ndv;
};

template <typename T>
bool build_bucket_from_data(std::vector<Bucket<T>>& buckets,
                            const std::map<T, uint64_t>& ordered_map, double sample_rate,
                            uint32_t max_bucket_num) {
    if (ordered_map.size() == 0) {
        return false;
    }

    uint64_t element_number = 0;
    for (auto it : ordered_map) {
        element_number += it.second;
    }

    auto sample_number = (uint64_t)std::ceil(element_number * sample_rate);
    auto num_per_bucket = (uint64_t)std::ceil((Float64)sample_number / max_bucket_num);

    LOG(INFO) << fmt::format(
            "histogram bucket info: element number {}, sample number:{}, num per bucket:{}",
            element_number, sample_number, num_per_bucket);

    if (sample_rate == 1) {
        for (auto it : ordered_map) {
            for (auto i = it.second; i > 0; --i) {
                auto v = it.first;
                value_to_bucket(buckets, v, num_per_bucket);
            }
        }
        return true;
    }

    // if sampling is required (0<sample_rate<1),
    // we need to build the sampled data index
    boost::dynamic_bitset<> sample_index(element_number);

    // use a same seed value so that we get
    // same result each time we run this function
    srand(element_number * sample_rate * max_bucket_num);

    while (sample_index.count() < sample_number) {
        uint64_t num = (rand() % (element_number));
        sample_index[num] = true;
    }

    uint64_t element_cnt = 0;
    uint64_t sample_cnt = 0;
    bool break_flag = false;

    for (auto it : ordered_map) {
        if (break_flag) {
            break;
        }
        for (auto i = it.second; i > 0; --i) {
            if (sample_cnt >= sample_number) {
                break_flag = true;
                break;
            }
            if (sample_index[element_cnt]) {
                sample_cnt += 1;
                auto v = it.first;
                value_to_bucket(buckets, v, num_per_bucket);
            }
            element_cnt += 1;
        }
    }

    return true;
}

bool inline bucket_to_json(rapidjson::Document::AllocatorType& allocator,
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

    return bucket_arr.Size() > 0;
}

template <typename T>
bool value_to_bucket(std::vector<Bucket<T>>& buckets, T v, size_t num_per_bucket) {
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
            uint64_t pre_sum = bucket->pre_sum + bucket->count;
            Bucket<T> new_bucket(v, pre_sum);
            buckets.emplace_back(new_bucket);
        }
    }

    return buckets.size() > 0;
}

template <typename T>
bool value_to_string(std::stringstream& ss, T input, const DataTypePtr& data_type) {
    fmt::memory_buffer _insert_stmt_buffer;
    switch (data_type->get_type_id()) {
    case TypeIndex::Int8:
    case TypeIndex::UInt8:
    case TypeIndex::Int16:
    case TypeIndex::UInt16:
    case TypeIndex::Int32:
    case TypeIndex::UInt32:
    case TypeIndex::Int64:
    case TypeIndex::UInt64:
    case TypeIndex::Int128:
    case TypeIndex::UInt128:
    case TypeIndex::Float32:
    case TypeIndex::Float64:
    case TypeIndex::String: {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", input);
        ss << std::string(buffer.data(), buffer.size());
        break;
    }
    case TypeIndex::Decimal32: {
        auto scale = get_decimal_scale(*data_type);
        auto decimal_val = reinterpret_cast<const Decimal32*>(&input);
        write_text(*decimal_val, scale, ss);
        break;
    }
    case TypeIndex::Decimal64: {
        auto scale = get_decimal_scale(*data_type);
        auto decimal_val = reinterpret_cast<const Decimal64*>(&input);
        write_text(*decimal_val, scale, ss);
        break;
    }
    case TypeIndex::Decimal128:
    case TypeIndex::Decimal128I: {
        auto scale = get_decimal_scale(*data_type);
        auto decimal_val = reinterpret_cast<const Decimal128*>(&input);
        write_text(*decimal_val, scale, ss);
        break;
    }
    case TypeIndex::Date:
    case TypeIndex::DateTime: {
        auto* date_int = reinterpret_cast<Int64*>(&input);
        auto date_value = binary_cast<Int64, VecDateTimeValue>(*date_int);
        char buf[32] = {};
        date_value.to_string(buf);
        ss << std::string(buf, strlen(buf));
        break;
    }
    case TypeIndex::DateV2: {
        auto* value = (DateV2Value<DateV2ValueType>*)(&input);
        ss << *value;
        break;
    }
    case TypeIndex::DateTimeV2: {
        auto* value = (DateV2Value<DateTimeV2ValueType>*)(&input);
        ss << *value;
        break;
    }
    default:
        LOG(WARNING) << fmt::format("unable to convert histogram data of type {}",
                                    data_type->get_name());
        return false;
    }

    return true;
}

template <typename T>
bool build_json_from_bucket(rapidjson::StringBuffer& buffer, const std::vector<Bucket<T>>& buckets,
                            const DataTypePtr& data_type, double sample_rate,
                            uint32_t max_bucket_num) {
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    rapidjson::Value sample_rate_val(sample_rate);
    doc.AddMember("sample_rate", sample_rate_val, allocator);

    rapidjson::Value max_bucket_num_val(max_bucket_num);
    doc.AddMember("max_bucket_num", max_bucket_num_val, allocator);

    int bucket_num = buckets.size();
    rapidjson::Value bucket_num_val(bucket_num);
    doc.AddMember("bucket_num", bucket_num_val, allocator);

    rapidjson::Value bucket_arr(rapidjson::kArrayType);
    for (auto i = 0; i < bucket_num; ++i) {
        std::stringstream ss1;
        std::stringstream ss2;
        value_to_string(ss1, buckets[i].lower, data_type);
        value_to_string(ss2, buckets[i].upper, data_type);
        std::string lower_str = ss1.str();
        std::string upper_str = ss2.str();
        bucket_to_json(allocator, bucket_arr, lower_str, upper_str, (uint64_t)(buckets[i].count),
                       (uint64_t)(buckets[i].pre_sum), (uint64_t)(buckets[i].ndv));
    }
    doc.AddMember("buckets", bucket_arr, allocator);

    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    return buffer.GetSize() > 0;
}

} // namespace  doris::vectorized
