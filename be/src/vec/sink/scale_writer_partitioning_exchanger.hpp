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
#include <functional>
#include <iostream>
#include <vector>

#include "vec/core/block.h"
#include "vec/exec/skewed_partition_rebalancer.h"

namespace doris::vectorized {

template <typename PartitionFunction>
class ScaleWriterPartitioningExchanger {
public:
    ScaleWriterPartitioningExchanger(int channel_size, PartitionFunction& partition_function,
                                     int partition_count, int task_count, int task_bucket_count,
                                     long min_partition_data_processed_rebalance_threshold,
                                     long min_data_processed_rebalance_threshold)
            : _channel_size(channel_size),
              _partition_function(partition_function),
              _partition_rebalancer(partition_count, task_count, task_bucket_count,
                                    min_partition_data_processed_rebalance_threshold,
                                    min_data_processed_rebalance_threshold),
              _partition_row_counts(partition_count, 0),
              _partition_writer_ids(partition_count, -1),
              _partition_writer_indexes(partition_count, 0) {}

    std::vector<std::vector<uint32_t>> accept(Block* block) {
        std::vector<std::vector<uint32_t>> writerAssignments(_channel_size,
                                                             std::vector<uint32_t>());
        for (int partition_id = 0; partition_id < _partition_row_counts.size(); partition_id++) {
            _partition_row_counts[partition_id] = 0;
            _partition_writer_ids[partition_id] = -1;
        }

        _partition_rebalancer.rebalance();

        for (int position = 0; position < block->rows(); position++) {
            int partition_id = _partition_function.get_partition(block, position);
            _partition_row_counts[partition_id] += 1;

            // Get writer id for this partition by looking at the scaling state
            int writer_id = _partition_writer_ids[partition_id];
            if (writer_id == -1) {
                writer_id = get_next_writer_id(partition_id);
                _partition_writer_ids[partition_id] = writer_id;
            }
            writerAssignments[writer_id].push_back(position);
        }

        for (int partition_id = 0; partition_id < _partition_row_counts.size(); partition_id++) {
            _partition_rebalancer.add_partition_row_count(partition_id,
                                                          _partition_row_counts[partition_id]);
        }
        _partition_rebalancer.add_data_processed(block->bytes());

        return writerAssignments;
    }

    int get_next_writer_id(int partition_id) {
        return _partition_rebalancer.get_task_id(partition_id,
                                                 _partition_writer_indexes[partition_id]++);
    }

private:
    int _channel_size;
    PartitionFunction& _partition_function;
    SkewedPartitionRebalancer _partition_rebalancer;
    std::vector<int> _partition_row_counts;
    std::vector<int> _partition_writer_ids;
    std::vector<int> _partition_writer_indexes;
};

} // namespace doris::vectorized