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
// This file is porting from
// https://github.com/trinodb/trino/blob/master/core/trino-main/src/test/java/io/trino/operator/output/TestSkewedPartitionRebalancer.java
// to cpp and modified by Doris

#include "vec/exec/skewed_partition_rebalancer.h"

#include <gtest/gtest.h>

#include <list>

namespace doris::vectorized {

class SkewedPartitionRebalancerTest : public testing::Test {
public:
    SkewedPartitionRebalancerTest() = default;
    virtual ~SkewedPartitionRebalancerTest() = default;

private:
    std::vector<std::list<int>> _get_partition_positions(
            std::unique_ptr<SkewedPartitionRebalancer>& rebalancer,
            std::vector<long>& partition_row_count, int partition_count, int max_position) {
        std::vector<std::list<int>> partitionPositions(rebalancer->get_task_count());

        for (int partition = 0; partition < rebalancer->get_task_count(); partition++) {
            partitionPositions[partition] = std::list<int>();
        }

        for (int position = 0; position < max_position; position++) {
            int partition = position % partition_count;
            partition = rebalancer->get_task_id(partition, partition_row_count[partition]++);
            partitionPositions[partition].push_back(position);
        }

        return partitionPositions;
    }

    static bool _vectors_equal(const std::vector<std::list<int>>& vec1,
                               const std::vector<std::list<int>>& vec2) {
        if (vec1.size() != vec2.size()) {
            return false;
        }
        for (size_t i = 0; i < vec1.size(); i++) {
            if (vec1[i] != vec2[i]) {
                return false;
            }
        }
        return true;
    }

    static bool _compare_vector_of_lists(const std::vector<std::list<int>>& expected,
                                         const std::vector<std::list<int>>& actual) {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (size_t i = 0; i < expected.size(); ++i) {
            if (expected[i] != actual[i]) {
                return false;
            }
        }

        return true;
    }
};

TEST_F(SkewedPartitionRebalancerTest, test_rebalance_with_skewness) {
    const int partitionCount = 3;
    const int taskCount = 3;
    const int taskBucketCount = 3;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1 * MEGABYTE; // 1MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;          // 50MB

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));
    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 1000);
    rebalancer->add_partition_row_count(2, 1000);
    rebalancer->add_data_processed(40 * MEGABYTE);
    rebalancer->rebalance();

    std::vector<long> partitionRowCount(partitionCount, 0);

    ASSERT_TRUE(_vectors_equal(
            {{0, 3, 6, 9, 12, 15}, {1, 4, 7, 10, 13, 16}, {2, 5, 8, 11, 14}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(_compare_vector_of_lists({{0}, {1}, {2}}, rebalancer->get_partition_assignments()));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 1000);
    rebalancer->add_partition_row_count(2, 1000);
    rebalancer->add_data_processed(20 * MEGABYTE);

    // Rebalancing will happen since we crossed the data processed limit.
    // Part0 -> Task1 (Bucket1), Part1 -> Task0 (Bucket1), Part2 -> Task0 (Bucket2)
    rebalancer->rebalance();

    ASSERT_TRUE(_vectors_equal(
            {{0, 2, 4, 6, 8, 10, 12, 14, 16}, {1, 3, 7, 9, 13, 15}, {5, 11}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(_compare_vector_of_lists({{0, 1}, {1, 0}, {2, 0}},
                                         rebalancer->get_partition_assignments()));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 1000);
    rebalancer->add_partition_row_count(2, 1000);
    rebalancer->add_data_processed(200 * MEGABYTE);

    // Rebalancing will happen
    // Part0 -> Task2 (Bucket1), Part1 -> Task2 (Bucket2), Part2 -> Task1 (Bucket2)
    rebalancer->rebalance();

    ASSERT_TRUE(_vectors_equal(
            {{0, 2, 4, 9, 11, 13}, {1, 3, 5, 10, 12, 14}, {6, 7, 8, 15, 16}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(_compare_vector_of_lists({{0, 1, 2}, {1, 0, 2}, {2, 0, 1}},
                                         rebalancer->get_partition_assignments()));
}

TEST_F(SkewedPartitionRebalancerTest, test_rebalance_without_skewness) {
    const int partitionCount = 6;
    const int taskCount = 3;
    const int taskBucketCount = 2;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1 * MEGABYTE; // 1MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;          // 50MB

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));
    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 700);
    rebalancer->add_partition_row_count(2, 600);
    rebalancer->add_partition_row_count(3, 1000);
    rebalancer->add_partition_row_count(4, 700);
    rebalancer->add_partition_row_count(5, 600);

    rebalancer->add_data_processed(500 * MEGABYTE);
    // No rebalancing will happen since there is no skewness across task buckets
    rebalancer->rebalance();

    std::vector<long> partitionRowCount(partitionCount, 0);

    ASSERT_TRUE(_vectors_equal(
            {{0, 3}, {1, 4}, {2, 5}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 6)));
    EXPECT_TRUE(_compare_vector_of_lists({{0}, {1}, {2}, {0}, {1}, {2}},
                                         rebalancer->get_partition_assignments()));
}

TEST_F(SkewedPartitionRebalancerTest,
       test_no_rebalance_when_data_written_is_less_than_the_rebalance_limit) {
    const int partitionCount = 3;
    const int taskCount = 3;
    const int taskBucketCount = 3;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1 * MEGABYTE; // 1MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;          // 50MB

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 0);
    rebalancer->add_partition_row_count(2, 0);

    rebalancer->add_data_processed(40 * MEGABYTE);
    // No rebalancing will happen since we do not cross the max data processed limit of 50MB
    rebalancer->rebalance();

    std::vector<long> partitionRowCount(partitionCount, 0);

    ASSERT_TRUE(_vectors_equal(
            {{0, 3}, {1, 4}, {2, 5}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 6)));
    EXPECT_TRUE(_compare_vector_of_lists({{0}, {1}, {2}}, rebalancer->get_partition_assignments()));
}

TEST_F(SkewedPartitionRebalancerTest,
       test_no_rebalance_when_data_written_by_the_partition_is_less_than_writer_sacling_min_data_processed) {
    const int partitionCount = 3;
    const int taskCount = 3;
    const int taskBucketCount = 3;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE; // 50MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;           // 50MB

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 600);
    rebalancer->add_partition_row_count(2, 0);

    rebalancer->add_data_processed(60 * MEGABYTE);
    // No rebalancing will happen since no partition has crossed the writerScalingMinDataProcessed limit of 50MB
    rebalancer->rebalance();

    std::vector<long> partitionRowCount(partitionCount, 0);

    ASSERT_TRUE(_vectors_equal(
            {{0, 3}, {1, 4}, {2, 5}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 6)));
    EXPECT_TRUE(_compare_vector_of_lists({{0}, {1}, {2}}, rebalancer->get_partition_assignments()));
}

TEST_F(SkewedPartitionRebalancerTest,
       test_rebalance_partition_to_single_task_in_a_rebalancing_loop) {
    const int partitionCount = 3;
    const int taskCount = 3;
    const int taskBucketCount = 3;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1 * MEGABYTE; // 1MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;          // 50MB

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 0);
    rebalancer->add_partition_row_count(2, 0);

    rebalancer->add_data_processed(60 * MEGABYTE);
    // rebalancing will only happen to a single task even though two tasks are available
    rebalancer->rebalance();

    std::vector<long> partitionRowCount(partitionCount, 0);

    ASSERT_TRUE(_vectors_equal(
            {{0, 6, 12}, {1, 3, 4, 7, 9, 10, 13, 15, 16}, {2, 5, 8, 11, 14}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(
            _compare_vector_of_lists({{0, 1}, {1}, {2}}, rebalancer->get_partition_assignments()));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 0);
    rebalancer->add_partition_row_count(2, 0);
    rebalancer->add_data_processed(60 * MEGABYTE);
    rebalancer->rebalance();

    ASSERT_TRUE(_vectors_equal(
            {{0, 9}, {1, 3, 4, 7, 10, 12, 13, 16}, {2, 5, 6, 8, 11, 14, 15}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(_compare_vector_of_lists({{0, 1, 2}, {1}, {2}},
                                         rebalancer->get_partition_assignments()));
}

TEST_F(SkewedPartitionRebalancerTest, test_consider_skewed_partition_only_within_a_cycle) {
    const int partitionCount = 3;
    const int taskCount = 3;
    const int taskBucketCount = 1;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1 * MEGABYTE; // 1MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;          // 50MB

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));

    rebalancer->add_partition_row_count(0, 1000);
    rebalancer->add_partition_row_count(1, 800);
    rebalancer->add_partition_row_count(2, 0);

    rebalancer->add_data_processed(60 * MEGABYTE);
    // rebalancing will happen for partition 0 to task 2 since partition 0 is skewed.
    rebalancer->rebalance();

    std::vector<long> partitionRowCount(partitionCount, 0);

    ASSERT_TRUE(_vectors_equal(
            {{0, 6, 12}, {1, 4, 7, 10, 13, 16}, {2, 3, 5, 8, 9, 11, 14, 15}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(
            _compare_vector_of_lists({{0, 2}, {1}, {2}}, rebalancer->get_partition_assignments()));

    rebalancer->add_partition_row_count(0, 0);
    rebalancer->add_partition_row_count(1, 800);
    rebalancer->add_partition_row_count(2, 1000);
    // rebalancing will happen for partition 2 to task 0 since partition 2 is skewed. Even though partition 1 has
    // written more amount of data from start, it will not be considered since it is not the most skewed in
    // this rebalancing cycle.
    rebalancer->add_data_processed(60 * MEGABYTE);
    rebalancer->rebalance();

    ASSERT_TRUE(_vectors_equal(
            {{0, 2, 6, 8, 12, 14}, {1, 4, 7, 10, 13, 16}, {3, 5, 9, 11, 15}},
            _get_partition_positions(rebalancer, partitionRowCount, partitionCount, 17)));
    EXPECT_TRUE(_compare_vector_of_lists({{0, 2}, {1}, {2, 0}},
                                         rebalancer->get_partition_assignments()));
}

} // namespace doris::vectorized
