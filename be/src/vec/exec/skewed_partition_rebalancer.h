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
// https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/operator/output/SkewedPartitionRebalancer.java
// to cpp and modified by Doris

/**
 * Helps in distributing big or skewed partitions across available tasks to improve the performance of
 * partitioned writes.
 * <p>
 * This rebalancer initialize a bunch of buckets for each task based on a given taskBucketCount and then tries to
 * uniformly distribute partitions across those buckets. This helps to mitigate two problems:
 * 1. Mitigate skewness across tasks.
 * 2. Scale few big partitions across tasks even if there's no skewness among them. This will essentially speed the
 *    local scaling without impacting much overall resource utilization.
 * <p>
 * Example:
 * <p>
 * Before: 3 tasks, 3 buckets per task, and 2 skewed partitions
 * Task1                Task2               Task3
 * Bucket1 (Part 1)     Bucket1 (Part 2)    Bucket1
 * Bucket2              Bucket2             Bucket2
 * Bucket3              Bucket3             Bucket3
 * <p>
 * After rebalancing:
 * Task1                Task2               Task3
 * Bucket1 (Part 1)     Bucket1 (Part 2)    Bucket1 (Part 1)
 * Bucket2 (Part 2)     Bucket2 (Part 1)    Bucket2 (Part 2)
 * Bucket3              Bucket3             Bucket3
 */

#pragma once

#include <algorithm>
#include <iostream>
#include <list>
#include <optional>
#include <vector>

#include "util/indexed_priority_queue.hpp"

namespace doris::vectorized {

class SkewedPartitionRebalancer {
private:
    struct TaskBucket {
        int task_id;
        int id;

        TaskBucket(int task_id_, int bucket_id_, int task_bucket_count_)
                : task_id(task_id_), id(task_id_ * task_bucket_count_ + bucket_id_) {}

        bool operator==(const TaskBucket& other) const { return id == other.id; }

        bool operator<(const TaskBucket& other) const { return id < other.id; }

        bool operator>(const TaskBucket& other) const { return id > other.id; }
    };

public:
    SkewedPartitionRebalancer(int partition_count, int task_count, int task_bucket_count,
                              long min_partition_data_processed_rebalance_threshold,
                              long min_data_processed_rebalance_threshold);

    std::vector<std::list<int>> get_partition_assignments();
    int get_task_count();
    int get_task_id(int partition_id, int64_t index);
    void add_data_processed(long data_size);
    void add_partition_row_count(int partition, long row_count);
    void rebalance();

private:
    void _calculate_partition_data_size(long data_processed);
    long _calculate_task_bucket_data_size_since_last_rebalance(
            IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                    max_partitions);
    void _rebalance_based_on_task_bucket_skewness(
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                    max_task_buckets,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets,
            std::vector<
                    IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>&
                    task_bucket_max_partitions);
    std::vector<TaskBucket> _find_skewed_min_task_buckets(
            const TaskBucket& max_task_bucket,
            const IndexedPriorityQueue<TaskBucket,
                                       IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets);
    bool _rebalance_partition(
            int partition_id, const TaskBucket& to_task_bucket,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                    max_task_buckets,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets);

    bool _should_rebalance(long data_processed);
    void _rebalance_partitions(long data_processed);

private:
    static constexpr double TASK_BUCKET_SKEWNESS_THRESHOLD = 0.7;

    int _partition_count;
    int _task_count;
    int _task_bucket_count;
    long _min_partition_data_processed_rebalance_threshold;
    long _min_data_processed_rebalance_threshold;
    std::vector<long> _partition_row_count;
    long _data_processed;
    long _data_processed_at_last_rebalance;
    std::vector<long> _partition_data_size;
    std::vector<long> _partition_data_size_at_last_rebalance;
    std::vector<long> _partition_data_size_since_last_rebalance_per_task;
    std::vector<long> _estimated_task_bucket_data_size_since_last_rebalance;

    std::vector<std::vector<TaskBucket>> _partition_assignments;
};
} // namespace doris::vectorized