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

#include "vec/utils/histogram_helpers.hpp"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <map>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

// Define a helper function to create a test map with specified number of distinct values
// and total counts.
template <typename T>
std::map<T, size_t> create_test_map(const std::vector<T>& values,
                                    const std::vector<size_t>& counts) {
    std::map<T, size_t> test_map;
    for (size_t i = 0; i < values.size(); ++i) {
        test_map[values[i]] = counts[i];
    }
    return test_map;
}

/******************test build_histogram******************/

// Test case 1: Test when input map is empty.
TEST(BuildHistogramTest, empty_map) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> ordered_map;
    size_t num_buckets = 10;
    EXPECT_FALSE(build_histogram(buckets, ordered_map, num_buckets));
    EXPECT_EQ(buckets.size(), 0);
}

// Test case 2: Test when input map has only one element.
TEST(BuildHistogramTest, single_element) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> ordered_map = {{1, 5}};
    size_t num_buckets = 10;
    EXPECT_TRUE(build_histogram(buckets, ordered_map, num_buckets));
    EXPECT_EQ(buckets.size(), 1);
    EXPECT_EQ(buckets[0].lower, 1);
    EXPECT_EQ(buckets[0].upper, 1);
    EXPECT_EQ(buckets[0].ndv, 1);
    EXPECT_EQ(buckets[0].count, 5);
    EXPECT_EQ(buckets[0].pre_sum, 0);
}

// Test case 3: Test when num_buckets >= number of distinct elements in input map.
TEST(BuildHistogramTest, enough_buckets) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3}, {10, 20, 30});
    size_t num_buckets = test_map.size();
    EXPECT_TRUE(build_histogram(buckets, test_map, num_buckets));

    EXPECT_EQ(buckets.size(), num_buckets);
    EXPECT_EQ(buckets[0].lower, 1);
    EXPECT_EQ(buckets[0].upper, 1);
    EXPECT_EQ(buckets[0].ndv, 1);
    EXPECT_EQ(buckets[0].count, 10);
    EXPECT_EQ(buckets[0].pre_sum, 0);

    EXPECT_EQ(buckets[1].lower, 2);
    EXPECT_EQ(buckets[1].upper, 2);
    EXPECT_EQ(buckets[1].ndv, 1);
    EXPECT_EQ(buckets[1].count, 20);
    EXPECT_EQ(buckets[1].pre_sum, 10);

    EXPECT_EQ(buckets[2].lower, 3);
    EXPECT_EQ(buckets[2].upper, 3);
    EXPECT_EQ(buckets[2].ndv, 1);
    EXPECT_EQ(buckets[2].count, 30);
    EXPECT_EQ(buckets[2].pre_sum, 30);
}

// Test case 4: Test when num_buckets < number of distinct elements in input map.
TEST(BuildHistogramTest, not_enough_buckets) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3, 4}, {5, 10, 20, 30});
    size_t num_buckets = 2;
    EXPECT_TRUE(build_histogram(buckets, test_map, num_buckets));
    EXPECT_EQ(buckets.size(), num_buckets);
    EXPECT_EQ(buckets[0].lower, 1);
    EXPECT_EQ(buckets[0].upper, 3);
    EXPECT_EQ(buckets[0].ndv, 3);
    EXPECT_EQ(buckets[0].count, 35);
    EXPECT_EQ(buckets[0].pre_sum, 0);

    EXPECT_EQ(buckets[1].lower, 4);
    EXPECT_EQ(buckets[1].upper, 4);
    EXPECT_EQ(buckets[1].ndv, 1);
    EXPECT_EQ(buckets[1].count, 30);
    EXPECT_EQ(buckets[1].pre_sum, 35);
}

// Test case 5: Test when the sum of counts of two adjacent elements is larger than a single bucket's capacity.
TEST(BuildHistogramTest, two_adjacent_values_larger_than_capacity) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3, 4}, {20, 30, 40, 50});
    size_t num_buckets = 3;
    EXPECT_TRUE(build_histogram(buckets, test_map, num_buckets));
    EXPECT_EQ(buckets[0].lower, 1);
    EXPECT_EQ(buckets[0].upper, 2);
    EXPECT_EQ(buckets.size(), num_buckets);
    EXPECT_EQ(buckets[0].ndv, 2);
    EXPECT_EQ(buckets[0].count, 50);
    EXPECT_EQ(buckets[0].pre_sum, 0);

    EXPECT_EQ(buckets[1].lower, 3);
    EXPECT_EQ(buckets[1].upper, 3);
    EXPECT_EQ(buckets[1].ndv, 1);
    EXPECT_EQ(buckets[1].count, 40);
    EXPECT_EQ(buckets[1].pre_sum, 50);

    EXPECT_EQ(buckets[2].lower, 4);
    EXPECT_EQ(buckets[2].upper, 4);
    EXPECT_EQ(buckets[2].ndv, 1);
    EXPECT_EQ(buckets[2].count, 50);
    EXPECT_EQ(buckets[2].pre_sum, 90);
}

// Test case 6: Test when the sum of counts of all elements is smaller than a single bucket's capacity.
TEST(BuildHistogramTest, all_values_in_one_bucket) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3}, {5, 10, 15});
    size_t num_buckets = 1;
    EXPECT_TRUE(build_histogram(buckets, test_map, num_buckets));
    EXPECT_EQ(buckets[0].lower, 1);
    EXPECT_EQ(buckets[0].upper, 3);
    EXPECT_EQ(buckets.size(), num_buckets);
    EXPECT_EQ(buckets[0].ndv, 3);
    EXPECT_EQ(buckets[0].count, 30);
    EXPECT_EQ(buckets[0].pre_sum, 0);
}

// Test case 7: Test when the sum of counts of all elements is larger than the total capacity of all buckets.
TEST(BuildHistogramTest, all_values_in_multiple_buckets) {
    std::vector<Bucket<int>> buckets;
    std::map<int, size_t> test_map =
            create_test_map<int>({1, 2, 3, 4, 5, 6}, {100, 200, 300, 400, 500, 600});
    size_t num_buckets = 4;
    EXPECT_TRUE(build_histogram(buckets, test_map, num_buckets));
    // Check if the buckets are constructed correctly
    EXPECT_EQ(buckets.size(), num_buckets);
    EXPECT_EQ(buckets[0].lower, 1);
    EXPECT_EQ(buckets[0].upper, 3);
    EXPECT_EQ(buckets[0].ndv, 3);
    EXPECT_EQ(buckets[0].count, 600);
    EXPECT_EQ(buckets[0].pre_sum, 0);

    EXPECT_EQ(buckets[1].lower, 4);
    EXPECT_EQ(buckets[1].upper, 4);
    EXPECT_EQ(buckets[1].ndv, 1);
    EXPECT_EQ(buckets[1].count, 400);
    EXPECT_EQ(buckets[1].pre_sum, 600);

    EXPECT_EQ(buckets[2].lower, 5);
    EXPECT_EQ(buckets[2].upper, 5);
    EXPECT_EQ(buckets[2].ndv, 1);
    EXPECT_EQ(buckets[2].count, 500);
    EXPECT_EQ(buckets[2].pre_sum, 1000);

    EXPECT_EQ(buckets[3].lower, 6);
    EXPECT_EQ(buckets[3].upper, 6);
    EXPECT_EQ(buckets[3].ndv, 1);
    EXPECT_EQ(buckets[3].count, 600);
    EXPECT_EQ(buckets[3].pre_sum, 1500);

    // Check if the pre_sum values are correct.
    std::vector<size_t> pre_sum(num_buckets + 1, 0);
    for (size_t i = 0; i < num_buckets; ++i) {
        pre_sum[i + 1] = pre_sum[i] + buckets[i].count;
    }
    for (size_t i = 0; i < buckets.size(); ++i) {
        EXPECT_EQ(buckets[i].pre_sum, pre_sum[i]);
    }
}

/*****************test histogram_to_json*****************/

// Test case 1: Empty buckets
TEST(HistogramToJsonTest, empty_buckets) {
    rapidjson::StringBuffer buffer;
    std::vector<Bucket<int>> buckets;
    auto data_type = std::make_shared<DataTypeInt8>();
    bool result = histogram_to_json(buffer, buckets, data_type);
    std::string result_str = std::string(buffer.GetString());
    std::string expect_result_str = "{\"num_buckets\":0,\"buckets\":[]}";
    EXPECT_FALSE(result);
    EXPECT_EQ(result_str, expect_result_str);
}

// Test case 2: Non-empty buckets
TEST(HistogramToJsonTest, non_empty_buckets) {
    rapidjson::StringBuffer buffer;
    std::vector<Bucket<int>> buckets = {{0, 10, 5, 20, 100}, {10, 20, 10, 30, 200}};
    auto data_type = std::make_shared<DataTypeInt8>();
    bool result = histogram_to_json(buffer, buckets, data_type);
    std::string result_str = std::string(buffer.GetString());
    std::string expect_result_str =
            "{\"num_buckets\":2,\"buckets\":[{\"lower\":\"0\",\"upper\":\"10\",\"ndv\":5,"
            "\"count\":20,\"pre_sum\":100},{\"lower\":\"10\",\"upper\":\"20\",\"ndv\":10,"
            "\"count\":30,\"pre_sum\":200}]}";
    EXPECT_TRUE(result);
    EXPECT_EQ(result_str, expect_result_str);
}

/************test calculate_bucket_max_values************/

// Test case 1: Test with a map of size 1 and num_buckets = 1
TEST(CalculateBucketMaxValuesTest, single_value_one_bucket) {
    std::map<int, size_t> value_map = {{1, 1}};
    const size_t num_buckets = 1;
    const size_t expected_max_values = 1;
    EXPECT_EQ(calculate_bucket_max_values(value_map, num_buckets), expected_max_values);
}

// Test case 2: Test with a map of size 1 and num_buckets > 1
TEST(CalculateBucketMaxValuesTest, single_value_multiple_buckets) {
    std::map<int, size_t> value_map = {{1, 1}};
    const size_t num_buckets = 10;
    const size_t expected_max_values = 1;
    EXPECT_EQ(calculate_bucket_max_values(value_map, num_buckets), expected_max_values);
}

// Test case 3: Test with a map of size 2 and num_buckets = 1
TEST(CalculateBucketMaxValuesTest, multiple_values_one_bucket) {
    std::map<int, size_t> value_map = {{1, 1}, {2, 2}};
    const size_t num_buckets = 1;
    const size_t expected_max_values = 3;
    EXPECT_EQ(calculate_bucket_max_values(value_map, num_buckets), expected_max_values);
}

// Test case 4: Test with a map of size 2 and num_buckets > 1
TEST(CalculateBucketMaxValuesTest, multiple_values_multiple_buckets) {
    std::map<int, size_t> value_map = {{1, 1}, {2, 2}};
    const size_t num_buckets = 2;
    const size_t expected_max_values = 1;
    EXPECT_EQ(calculate_bucket_max_values(value_map, num_buckets), expected_max_values);
}

// Test case 5: Test with a map of size 3 and num_buckets > 1
TEST(CalculateBucketMaxValuesTest, multiple_values_multiple_buckets2) {
    std::map<int, size_t> value_map = create_test_map<int>({1, 2, 3}, {2, 2, 2});
    const size_t num_buckets = 3;
    const size_t expected_max_values = 2;
    EXPECT_EQ(calculate_bucket_max_values(value_map, num_buckets), expected_max_values);
}

// Test case 6: Test with a map of size 3 and num_buckets > 1
TEST(CalculateBucketMaxValuesTest, multiple_values_multiple_buckets3) {
    std::map<int, size_t> value_map = create_test_map<int>({1, 2, 3}, {2, 2, 2});
    const size_t num_buckets = 4;
    const size_t expected_max_values = 1;
    EXPECT_EQ(calculate_bucket_max_values(value_map, num_buckets), expected_max_values);
}

/**************test can_assign_into_buckets**************/

// Test case 1: Test with empty input map.
TEST(CanAssignIntoBucketsTest, empty_map) {
    std::map<int, size_t> empty_map;
    EXPECT_FALSE(can_assign_into_buckets(empty_map, 1, 1));
}

// Test case 2: Test with a single element in input map.
TEST(CanAssignIntoBucketsTest, one_element_map) {
    std::map<int, size_t> test_map = {{42, 10}};
    EXPECT_FALSE(can_assign_into_buckets(test_map, 1, 1));
    EXPECT_FALSE(can_assign_into_buckets(test_map, 5, 1));
    EXPECT_TRUE(can_assign_into_buckets(test_map, 5, 2));
}

// Test case 3: Test with enough buckets to store all distinct elements.
TEST(CanAssignIntoBucketsTest, enough_buckets) {
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3}, {10, 20, 30});
    EXPECT_FALSE(can_assign_into_buckets(test_map, 1, 3));
    EXPECT_TRUE(can_assign_into_buckets(test_map, 10, 3));
    EXPECT_TRUE(can_assign_into_buckets(test_map, 20, 3));
}

// Test case 4: Test with not enough buckets to store all distinct elements.
TEST(CanAssignIntoBucketsTest, not_enough_buckets) {
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3, 4}, {5, 10, 15, 20});
    EXPECT_TRUE(can_assign_into_buckets(test_map, 30, 2));
    EXPECT_FALSE(can_assign_into_buckets(test_map, 30, 1));
}

// Test case 5: Test with all values in a single bucket.
TEST(CanAssignIntoBucketsTest, all_values_in_one_bucket) {
    std::map<int, size_t> test_map = create_test_map<int>({1, 2, 3, 4}, {5, 10, 15, 20});
    EXPECT_TRUE(can_assign_into_buckets(test_map, 100, 1));
    EXPECT_FALSE(can_assign_into_buckets(test_map, 30, 1));
}

// Test case 6: Test with a single element that has a count greater than the total bucket capacity.
TEST(CanAssignIntoBucketsTest, single_element_larger_than_bucket_capacity) {
    std::map<int, size_t> test_map = {{42, 100}};
    EXPECT_FALSE(can_assign_into_buckets(test_map, 99, 1));
    EXPECT_TRUE(can_assign_into_buckets(test_map, 100, 1));
    EXPECT_TRUE(can_assign_into_buckets(test_map, 200, 1));
}

} // namespace doris::vectorized
