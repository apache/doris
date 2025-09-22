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

#include "common/cast_set.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
template <typename T>
struct Bucket {
public:
    Bucket() = default;
    Bucket(T lower, T upper, size_t ndv, size_t count, size_t pre_sum)
            : lower(lower), upper(upper), ndv(ndv), count(count), pre_sum(pre_sum) {}

    T lower;
    T upper;
    size_t ndv;
    size_t count;
    size_t pre_sum;
};

/**
 * Checks if it is possible to assign the provided value_map to the given
 * number of buckets such that no bucket has a size larger than max_bucket_size.
 *
 * @param value_map A mapping of values to their counts.
 * @param max_bucket_size The maximum size that any bucket is allowed to have.
 * @param num_buckets The number of buckets that we want to assign values to.
 *
 * @return true if the values can be assigned to the buckets, false otherwise.
 */
template <typename T>
bool can_assign_into_buckets(const std::map<T, size_t>& value_map, const size_t max_bucket_size,
                             const size_t num_buckets) {
    if (value_map.empty()) {
        return false;
    };

    size_t used_buckets = 1;
    size_t current_bucket_size = 0;

    for (const auto& [value, count] : value_map) {
        current_bucket_size += count;

        // If adding the current value to the current bucket would exceed max_bucket_size,
        // then we start a new bucket.
        if (current_bucket_size > max_bucket_size) {
            ++used_buckets;
            current_bucket_size = count;
        }

        // If we have used more buckets than num_buckets, we cannot assign the values to buckets.
        if (used_buckets > num_buckets) {
            return false;
        }
    }

    return true;
}

/**
 * Calculates the maximum number of values that can fit into each bucket given a set of values
 * and the desired number of buckets.
 *
 * @tparam T the type of the values in the value map
 * @param value_map the map of values and their counts
 * @param num_buckets the desired number of buckets
 * @return the maximum number of values that can fit into each bucket
 */
template <typename T>
size_t calculate_bucket_max_values(const std::map<T, size_t>& value_map, const size_t num_buckets) {
    // Ensure that the value map is not empty
    assert(!value_map.empty());

    // Calculate the total number of values in the map using std::accumulate()
    size_t total_values = 0;
    for (const auto& [value, count] : value_map) {
        total_values += count;
    }

    // If there is only one bucket, then all values will be assigned to that bucket
    if (num_buckets == 1) {
        return total_values;
    }

    // To calculate the maximum value count in each bucket, we first calculate a conservative upper
    // bound, which is equal to 2 * total_values / (max_buckets - 1) + 1. This upper bound may exceed
    // the actual maximum value count, but it does not underestimate it. The subsequent binary search
    // algorithm will approach the actual maximum value count.
    size_t upper_bucket_values = 2 * total_values / (num_buckets - 1) + 1;

    // Initialize the lower bound to 0
    size_t lower_bucket_values = 0;

    // Perform a binary search to find the maximum number of values that can fit into each bucket
    int search_step = 0;
    const int max_search_steps =
            10; // Limit the number of search steps to avoid excessive iteration

    while (upper_bucket_values > lower_bucket_values + 1 && search_step < max_search_steps) {
        // Calculate the midpoint of the upper and lower bounds
        const size_t bucket_values = (upper_bucket_values + lower_bucket_values) / 2;

        // Check if the given number of values can be assigned to the desired number of buckets
        if (can_assign_into_buckets(value_map, bucket_values, num_buckets)) {
            // If it can, then set the upper bound to the midpoint
            upper_bucket_values = bucket_values;
        } else {
            // If it can't, then set the lower bound to the midpoint
            lower_bucket_values = bucket_values;
        }
        // Increment the search step counter
        ++search_step;
    }

    return upper_bucket_values;
}

/**
 * Greedy equi-height histogram construction algorithm, inspired by the MySQL
 * equi_height implementation(https://dev.mysql.com/doc/dev/mysql-server/latest/equi__height_8h.html).
 *
 * Given an ordered collection of [value, count] pairs and a maximum bucket
 * size, construct a histogram by inserting values into a bucket while keeping
 * track of its size. If the insertion of a value into a non-empty bucket
 * causes the bucket to exceed the maximum size, create a new empty bucket and
 * continue.
 *
 * The algorithm guarantees a selectivity estimation error of at most ~2 *
 * #values / #buckets, often less. Values with a higher relative frequency are
 * guaranteed to be placed in singleton buckets.
 *
 * The minimum composite bucket size is used to minimize the worst case
 * selectivity estimation error. In general, the algorithm will adapt to the
 * data distribution to minimize the size of composite buckets. The heavy values
 * can be placed in singleton buckets and the remaining values will be evenly
 * spread across the remaining buckets, leading to a lower composite bucket size.
 *
 * Note: The term "value" refers to an entry in a column and the actual value
 * of an entry. The ordered_map is an ordered collection of [distinct value,
 * value count] pairs. For example, a Value_map<String> could contain the pairs ["a", 1], ["b", 2]
 * to represent one "a" value and two "b" values.
 *
 * @param buckets A vector of empty buckets that will be populated with data.
 * @param ordered_map An ordered map of distinct values and their counts.
 * @param max_num_buckets The maximum number of buckets that can be used.
 *
 * @return True if the buckets were successfully built, false otherwise.
 */
template <typename T>
bool build_histogram(std::vector<Bucket<T>>& buckets, const std::map<T, size_t>& ordered_map,
                     const size_t max_num_buckets) {
    // If the input map is empty, there is nothing to build.
    if (ordered_map.empty()) {
        return false;
    }

    // Calculate the maximum number of values that can be assigned to each bucket.
    auto bucket_max_values = calculate_bucket_max_values(ordered_map, max_num_buckets);

    // Ensure that the capacity is at least max_num_buckets in order to avoid the overhead of additional
    // allocations when inserting buckets.
    buckets.clear();
    buckets.reserve(max_num_buckets);

    // Initialize bucket variables.
    size_t distinct_values_count = 0;
    size_t values_count = 0;
    size_t cumulative_values = 0;

    // Record how many values still need to be assigned.
    auto remaining_distinct_values = ordered_map.size();

    auto it = ordered_map.begin();

    // Lower value of the current bucket.
    const T* lower_value = &it->first;

    // Iterate over the ordered map of distinct values and their counts.
    for (; it != ordered_map.end(); ++it) {
        const auto count = it->second;
        const auto current_value = it->first;

        // Update the bucket counts and track the number of distinct values assigned.
        distinct_values_count++;
        remaining_distinct_values--;
        values_count += count;
        cumulative_values += count;

        // Check whether the current value should be added to the current bucket.
        auto next = std::next(it);
        size_t remaining_empty_buckets = max_num_buckets - buckets.size() - 1;

        if (next != ordered_map.end() && remaining_distinct_values > remaining_empty_buckets &&
            values_count + next->second <= bucket_max_values) {
            // If the current value is the last in the input map and there are more remaining
            // distinct values than empty buckets and adding the value does not cause the bucket
            // to exceed its max size, skip adding the value to the current bucket.
            continue;
        }

        // Finalize the current bucket and add it to our collection of buckets.
        auto pre_sum = cumulative_values - values_count;

        Bucket<T> new_bucket(*lower_value, current_value, distinct_values_count, values_count,
                             pre_sum);
        buckets.push_back(new_bucket);

        // Reset variables for the next bucket.
        if (next != ordered_map.end()) {
            lower_value = &next->first;
        }
        values_count = 0;
        distinct_values_count = 0;
    }

    return true;
}

template <typename T>
bool histogram_to_json(rapidjson::StringBuffer& buffer, const std::vector<Bucket<T>>& buckets,
                       const DataTypePtr& data_type) {
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    int num_buckets = cast_set<int>(buckets.size());
    doc.AddMember("num_buckets", num_buckets, allocator);

    rapidjson::Value bucket_arr(rapidjson::kArrayType);
    bucket_arr.Reserve(num_buckets, allocator);

    std::stringstream ss1;
    std::stringstream ss2;

    rapidjson::Value lower_val;
    rapidjson::Value upper_val;

    // Convert bucket's lower and upper to 2 columns
    MutableColumnPtr lower_column = data_type->create_column();
    MutableColumnPtr upper_column = data_type->create_column();
    for (const auto& bucket : buckets) {
        // String type is different, it has to pass in length
        // if it is string type , directly use string value
        if constexpr (!std::is_same_v<T, std::string>) {
            lower_column->insert_data(reinterpret_cast<const char*>(&bucket.lower), 0);
            upper_column->insert_data(reinterpret_cast<const char*>(&bucket.upper), 0);
        }
    }
    size_t row_num = 0;
    for (const auto& bucket : buckets) {
        if constexpr (std::is_same_v<T, std::string>) {
            lower_val.SetString(bucket.lower.data(),
                                static_cast<rapidjson::SizeType>(bucket.lower.size()), allocator);
            upper_val.SetString(bucket.upper.data(),
                                static_cast<rapidjson::SizeType>(bucket.upper.size()), allocator);
        } else {
            std::string lower_str = data_type->to_string(*lower_column, row_num);
            std::string upper_str = data_type->to_string(*upper_column, row_num);
            ++row_num;
            lower_val.SetString(lower_str.data(),
                                static_cast<rapidjson::SizeType>(lower_str.size()), allocator);
            upper_val.SetString(upper_str.data(),
                                static_cast<rapidjson::SizeType>(upper_str.size()), allocator);
        }
        rapidjson::Value bucket_json(rapidjson::kObjectType);
        bucket_json.AddMember("lower", lower_val, allocator);
        bucket_json.AddMember("upper", upper_val, allocator);
        bucket_json.AddMember("ndv", static_cast<int64_t>(bucket.ndv), allocator);
        bucket_json.AddMember("count", static_cast<int64_t>(bucket.count), allocator);
        bucket_json.AddMember("pre_sum", static_cast<int64_t>(bucket.pre_sum), allocator);

        bucket_arr.PushBack(bucket_json, allocator);
    }

    doc.AddMember("buckets", bucket_arr, allocator);
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    return !buckets.empty() && buffer.GetSize() > 0;
}
#include "common/compile_check_end.h"
} // namespace  doris::vectorized
