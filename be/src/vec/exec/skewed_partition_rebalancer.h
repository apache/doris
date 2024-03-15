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

#include <algorithm>
#include <iostream>
#include <list>
#include <optional>
#include <vector>

#include "util/indexed_priority_queue.hpp"

namespace doris::vectorized {

class SkewedPartitionRebalancer {
private:
    static constexpr int SCALE_WRITERS_PARTITION_COUNT = 4096;
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

    struct TaskBucket {
        int task_id;
        int id;

        TaskBucket(int task_id_, int bucket_id_, int task_bucket_count_)
                : task_id(task_id_), id(task_id_ * task_bucket_count_ + bucket_id_) {}

        bool operator==(const TaskBucket& other) const { return id == other.id; }

        bool operator<(const TaskBucket& other) const { return id < other.id; }

        bool operator>(const TaskBucket& other) const { return id > other.id; }
    };

    struct TaskBucketHash {
        std::size_t operator()(const TaskBucket& bucket) const {
            std::size_t hash_task_id = std::hash<int>()(bucket.task_id);
            std::size_t hash_id = std::hash<int>()(bucket.id);
            return hash_task_id ^
                   (hash_id + 0x9e3779b9 + (hash_task_id << 6) + (hash_task_id >> 2));
        }
    };

    std::vector<std::vector<TaskBucket>> _partition_assignments;

public:
    SkewedPartitionRebalancer(int partition_count, int task_count, int task_bucket_count,
                              long min_partition_data_processed_rebalance_threshold,
                              long max_data_processed_rebalance_threshold);

    std::vector<std::list<int>> get_partition_assignments();
    int get_task_count();
    int get_task_id(int partition_id, int64_t index);
    void add_data_processed(long data_size);
    void add_partition_row_count(int partition, long row_count);
    void rebalance();

private:
    void calculate_partition_data_size(long data_processed);
    long calculate_task_bucket_data_size_since_last_rebalance(
            IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                    max_partitions);
    void rebalance_based_on_task_bucket_skewness(
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                    max_task_buckets,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets,
            std::vector<
                    IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>&
                    task_bucket_max_partitions);
    std::vector<TaskBucket> find_skewed_min_task_buckets(
            const TaskBucket& max_task_bucket,
            const IndexedPriorityQueue<TaskBucket,
                                       IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets);
    bool rebalance_partition(
            int partition_id, const TaskBucket& to_task_bucket,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                    max_task_buckets,
            IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                    min_task_buckets);

    bool should_rebalance(long data_processed);
    void rebalance_partitions(long dataProcessed);
};
} // namespace doris::vectorized