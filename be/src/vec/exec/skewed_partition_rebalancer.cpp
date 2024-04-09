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

#include "vec/exec/skewed_partition_rebalancer.h"

#include <glog/logging.h>

#include <cmath>
#include <list>

namespace doris::vectorized {

SkewedPartitionRebalancer::SkewedPartitionRebalancer(
        int partition_count, int task_count, int task_bucket_count,
        long min_partition_data_processed_rebalance_threshold,
        long min_data_processed_rebalance_threshold, const std::vector<std::string>* task_addresses)
        : _partition_count(partition_count),
          _task_count(task_count),
          _task_bucket_count(task_bucket_count),
          _min_partition_data_processed_rebalance_threshold(
                  min_partition_data_processed_rebalance_threshold),
          _min_data_processed_rebalance_threshold(
                  std::max(min_partition_data_processed_rebalance_threshold,
                           min_data_processed_rebalance_threshold)),
          _partition_row_count(partition_count, 0),
          _data_processed(0),
          _data_processed_at_last_rebalance(0),
          _partition_data_size(partition_count, 0),
          _partition_data_size_at_last_rebalance(partition_count, 0),
          _partition_data_size_since_last_rebalance_per_task(partition_count, 0),
          _estimated_task_bucket_data_size_since_last_rebalance(task_count * task_bucket_count, 0),
          _partition_assignments(partition_count) {
    if (task_addresses != nullptr) {
        CHECK(task_addresses->size() == task_count);
        _task_addresses = *task_addresses;
        for (int i = 0; i < _task_addresses.size(); ++i) {
            auto it = _assigned_address_to_task_buckets_num.find(_task_addresses[i]);
            if (it == _assigned_address_to_task_buckets_num.end()) {
                _assigned_address_to_task_buckets_num.insert({_task_addresses[i], 0});
            }
        }
    } else {
        _assigned_address_to_task_buckets_num.insert({TASK_BUCKET_ADDRESS_NOT_SET, 0});
    }

    std::vector<int> task_bucket_ids(task_count, 0);

    for (int partition = 0; partition < partition_count; partition++) {
        int task_id = partition % task_count;
        int bucket_id = task_bucket_ids[task_id]++ % task_bucket_count;
        TaskBucket task_bucket(
                task_id, bucket_id, task_bucket_count,
                (_task_addresses.empty()) ? TASK_BUCKET_ADDRESS_NOT_SET : _task_addresses[task_id]);
        _partition_assignments[partition].emplace_back(std::move(task_bucket));

        for (int i = 0; i < _partition_assignments[partition].size(); ++i) {
            auto it = _assigned_address_to_task_buckets_num.find(
                    _partition_assignments[partition][i].task_address);
            if (it != _assigned_address_to_task_buckets_num.end()) {
                _assigned_address_to_task_buckets_num[_partition_assignments[partition][i]
                                                              .task_address]++;
            } else {
                LOG(FATAL) << "__builtin_unreachable";
                __builtin_unreachable();
            }
        }
    }
}

std::vector<std::list<int>> SkewedPartitionRebalancer::get_partition_assignments() {
    std::vector<std::list<int>> assigned_tasks;

    for (const auto& partition_assignment : _partition_assignments) {
        std::list<int> tasks;
        std::transform(partition_assignment.begin(), partition_assignment.end(),
                       std::back_inserter(tasks),
                       [](const TaskBucket& task_bucket) { return task_bucket.task_id; });
        assigned_tasks.push_back(tasks);
    }

    return assigned_tasks;
}

int SkewedPartitionRebalancer::get_task_count() {
    return _task_count;
}

int SkewedPartitionRebalancer::get_task_id(int partition_id, int64_t index) {
    const std::vector<TaskBucket>& task_ids = _partition_assignments[partition_id];

    int task_id_index = (index % task_ids.size() + task_ids.size()) % task_ids.size();

    return task_ids[task_id_index].task_id;
}

void SkewedPartitionRebalancer::add_data_processed(long data_size) {
    _data_processed += data_size;
}

void SkewedPartitionRebalancer::add_partition_row_count(int partition, long row_count) {
    _partition_row_count[partition] += row_count;
}

void SkewedPartitionRebalancer::rebalance() {
    long current_data_processed = _data_processed;
    if (_should_rebalance(current_data_processed)) {
        _rebalance_partitions(current_data_processed);
    }
}

void SkewedPartitionRebalancer::_calculate_partition_data_size(long data_processed) {
    long total_partition_row_count = 0;
    for (int partition = 0; partition < _partition_count; partition++) {
        total_partition_row_count += _partition_row_count[partition];
    }

    for (int partition = 0; partition < _partition_count; partition++) {
        _partition_data_size[partition] = std::max(
                (_partition_row_count[partition] * data_processed) / total_partition_row_count,
                _partition_data_size[partition]);
    }
}

long SkewedPartitionRebalancer::_calculate_task_bucket_data_size_since_last_rebalance(
        IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                max_partitions) {
    long estimated_data_size_since_last_rebalance = 0;
    for (auto& elem : max_partitions) {
        estimated_data_size_since_last_rebalance +=
                _partition_data_size_since_last_rebalance_per_task[elem];
    }
    return estimated_data_size_since_last_rebalance;
}

void SkewedPartitionRebalancer::_rebalance_based_on_task_bucket_skewness(
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                max_task_buckets,
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                min_task_buckets,
        std::vector<IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>&
                task_bucket_max_partitions) {
    std::vector<int> scaled_partitions;
    while (true) {
        std::optional<TaskBucket> max_task_bucket = max_task_buckets.poll();
        if (!max_task_bucket.has_value()) {
            break;
        }

        IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                max_partitions = task_bucket_max_partitions[max_task_bucket->id];
        if (max_partitions.is_empty()) {
            continue;
        }

        std::multimap<std::string, SkewedPartitionRebalancer::TaskBucket> min_skewed_task_buckets =
                _find_skewed_min_task_buckets(max_task_bucket.value(), min_task_buckets);
        if (min_skewed_task_buckets.empty()) {
            break;
        }

        while (true) {
            std::optional<int> max_partition = max_partitions.poll();
            if (!max_partition.has_value()) {
                break;
            }
            int max_partition_value = max_partition.value();

            if (std::find(scaled_partitions.begin(), scaled_partitions.end(),
                          max_partition_value) != scaled_partitions.end()) {
                continue;
            }

            int total_assigned_tasks = _partition_assignments[max_partition_value].size();
            if (_partition_data_size[max_partition_value] >=
                (_min_partition_data_processed_rebalance_threshold * total_assigned_tasks)) {
                bool found = false;
                std::vector<std::pair<std::string, int>>
                        sorted_assigned_address_to_task_buckets_num(
                                _assigned_address_to_task_buckets_num.begin(),
                                _assigned_address_to_task_buckets_num.end());

                std::sort(sorted_assigned_address_to_task_buckets_num.begin(),
                          sorted_assigned_address_to_task_buckets_num.end(),
                          [](const auto& a, const auto& b) { return a.second < b.second; });
                for (auto& pair : sorted_assigned_address_to_task_buckets_num) {
                    auto range = min_skewed_task_buckets.equal_range(pair.first);
                    for (auto it = range.first; it != range.second; ++it) {
                        TaskBucket& task_bucket = it->second;
                        if (_rebalance_partition(max_partition_value, task_bucket, max_task_buckets,
                                                 min_task_buckets)) {
                            scaled_partitions.push_back(max_partition_value);
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }
}

std::multimap<std::string, SkewedPartitionRebalancer::TaskBucket>
SkewedPartitionRebalancer::_find_skewed_min_task_buckets(
        const TaskBucket& max_task_bucket,
        const IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                min_task_buckets) {
    std::multimap<std::string, SkewedPartitionRebalancer::TaskBucket> min_skewed_task_buckets;

    for (const auto& min_task_bucket : min_task_buckets) {
        double skewness =
                static_cast<double>(
                        _estimated_task_bucket_data_size_since_last_rebalance[max_task_bucket.id] -
                        _estimated_task_bucket_data_size_since_last_rebalance[min_task_bucket.id]) /
                _estimated_task_bucket_data_size_since_last_rebalance[max_task_bucket.id];
        if (skewness <= TASK_BUCKET_SKEWNESS_THRESHOLD || std::isnan(skewness)) {
            break;
        }
        if (max_task_bucket.task_id != min_task_bucket.task_id) {
            min_skewed_task_buckets.insert({min_task_bucket.task_address, min_task_bucket});
        }
    }
    return min_skewed_task_buckets;
}

bool SkewedPartitionRebalancer::_rebalance_partition(
        int partition_id, const TaskBucket& to_task_bucket,
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>&
                max_task_buckets,
        IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>&
                min_task_buckets) {
    std::vector<TaskBucket>& assignments = _partition_assignments[partition_id];
    if (std::any_of(assignments.begin(), assignments.end(),
                    [&to_task_bucket](const TaskBucket& task_bucket) {
                        return task_bucket.task_id == to_task_bucket.task_id;
                    })) {
        return false;
    }

    assignments.push_back(to_task_bucket);
    _assigned_address_to_task_buckets_num[to_task_bucket.task_address]++;

    int new_task_count = assignments.size();
    int old_task_count = new_task_count - 1;
    for (const TaskBucket& task_bucket : assignments) {
        if (task_bucket == to_task_bucket) {
            _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id] +=
                    (_partition_data_size_since_last_rebalance_per_task[partition_id] *
                     old_task_count) /
                    new_task_count;
        } else {
            _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id] -=
                    _partition_data_size_since_last_rebalance_per_task[partition_id] /
                    new_task_count;
        }
        max_task_buckets.add_or_update(
                task_bucket, _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id]);
        min_task_buckets.add_or_update(
                task_bucket, _estimated_task_bucket_data_size_since_last_rebalance[task_bucket.id]);
    }

    return true;
}

bool SkewedPartitionRebalancer::_should_rebalance(long data_processed) {
    return (data_processed - _data_processed_at_last_rebalance) >=
           _min_data_processed_rebalance_threshold;
}

void SkewedPartitionRebalancer::_rebalance_partitions(long data_processed) {
    if (!_should_rebalance(data_processed)) {
        return;
    }

    _calculate_partition_data_size(data_processed);

    for (int partition = 0; partition < _partition_count; partition++) {
        int total_assigned_tasks = _partition_assignments[partition].size();
        long data_size = _partition_data_size[partition];
        _partition_data_size_since_last_rebalance_per_task[partition] =
                (data_size - _partition_data_size_at_last_rebalance[partition]) /
                total_assigned_tasks;
        _partition_data_size_at_last_rebalance[partition] = data_size;
    }

    std::vector<IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>>
            task_bucket_max_partitions;

    for (int i = 0; i < _task_count * _task_bucket_count; ++i) {
        task_bucket_max_partitions.push_back(
                IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>());
    }

    for (int partition = 0; partition < _partition_count; partition++) {
        auto& taskAssignments = _partition_assignments[partition];
        for (const auto& taskBucket : taskAssignments) {
            auto& queue = task_bucket_max_partitions[taskBucket.id];
            queue.add_or_update(partition,
                                _partition_data_size_since_last_rebalance_per_task[partition]);
        }
    }

    IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>
            max_task_buckets;
    IndexedPriorityQueue<TaskBucket, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH>
            min_task_buckets;

    for (int task_id = 0; task_id < _task_count; task_id++) {
        for (int bucket_id = 0; bucket_id < _task_bucket_count; bucket_id++) {
            TaskBucket task_bucket1(task_id, bucket_id, _task_bucket_count,
                                    (_task_addresses.empty()) ? TASK_BUCKET_ADDRESS_NOT_SET
                                                              : _task_addresses[task_id]);
            TaskBucket task_bucket2(task_id, bucket_id, _task_bucket_count,
                                    (_task_addresses.empty()) ? TASK_BUCKET_ADDRESS_NOT_SET
                                                              : _task_addresses[task_id]);
            _estimated_task_bucket_data_size_since_last_rebalance[task_bucket1.id] =
                    _calculate_task_bucket_data_size_since_last_rebalance(
                            task_bucket_max_partitions[task_bucket1.id]);
            max_task_buckets.add_or_update(
                    std::move(task_bucket1),
                    _estimated_task_bucket_data_size_since_last_rebalance[task_bucket1.id]);
            min_task_buckets.add_or_update(
                    std::move(task_bucket2),
                    _estimated_task_bucket_data_size_since_last_rebalance[task_bucket2.id]);
        }
    }

    _rebalance_based_on_task_bucket_skewness(max_task_buckets, min_task_buckets,
                                             task_bucket_max_partitions);
    _data_processed_at_last_rebalance = data_processed;
}
} // namespace doris::vectorized
