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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioner/writer_assigner.h"

#include "exec/connector/skewed_partition_rebalancer.h"

namespace doris {

namespace {
Status validate_assignment_input(const std::vector<uint32_t>& partition_ids,
                                 const std::vector<uint8_t>* mask, size_t rows) {
    if (partition_ids.size() < rows) {
        return Status::InvalidArgument("Writer assignment has {} partition ids for {} rows",
                                       partition_ids.size(), rows);
    }
    if (mask != nullptr && mask->size() < rows) {
        return Status::InvalidArgument("Writer assignment mask has {} entries for {} rows",
                                       mask->size(), rows);
    }
    return Status::OK();
}
} // namespace

Status IdentityWriterAssigner::assign(const std::vector<uint32_t>& partition_ids,
                                      const std::vector<uint8_t>* mask, size_t rows,
                                      size_t /*block_bytes*/, std::vector<uint32_t>& writer_ids) {
    RETURN_IF_ERROR(validate_assignment_input(partition_ids, mask, rows));
    if (writer_ids.size() != rows && &writer_ids != &partition_ids) {
        writer_ids.resize(rows);
    }
    for (size_t row = 0; row < rows; ++row) {
        if (mask != nullptr && (*mask)[row] == 0) {
            continue;
        }
        if (partition_ids[row] >= _writer_count) {
            return Status::InvalidArgument("Logical partition {} exceeds writer count {}",
                                           partition_ids[row], _writer_count);
        }
        writer_ids[row] = partition_ids[row];
    }
    return Status::OK();
}

SkewedWriterAssigner::SkewedWriterAssigner(int partition_count, int task_count,
                                           int task_bucket_count,
                                           long min_partition_data_processed_rebalance_threshold,
                                           long min_data_processed_rebalance_threshold)
        : _rebalancer(std::make_unique<SkewedPartitionRebalancer>(
                  partition_count, task_count, task_bucket_count,
                  min_partition_data_processed_rebalance_threshold,
                  min_data_processed_rebalance_threshold)),
          _writer_count(task_count),
          _partition_row_counts(partition_count, 0),
          _partition_writer_ids(partition_count, -1),
          _partition_writer_indexes(partition_count, 0) {}

SkewedWriterAssigner::~SkewedWriterAssigner() = default;

Status SkewedWriterAssigner::assign(const std::vector<uint32_t>& partition_ids,
                                    const std::vector<uint8_t>* mask, size_t rows,
                                    size_t block_bytes, std::vector<uint32_t>& writer_ids) {
    RETURN_IF_ERROR(validate_assignment_input(partition_ids, mask, rows));
    if (rows == 0) {
        return Status::OK();
    }
    if (_partition_row_counts.empty()) {
        return Status::InvalidArgument("Skewed writer assignment has no logical partitions");
    }
    if (writer_ids.size() != rows && &writer_ids != &partition_ids) {
        writer_ids.resize(rows);
    }

    std::fill(_partition_row_counts.begin(), _partition_row_counts.end(), 0);
    std::fill(_partition_writer_ids.begin(), _partition_writer_ids.end(), -1);
    _rebalancer->rebalance();

    const size_t partition_count = _partition_row_counts.size();
    for (size_t row = 0; row < rows; ++row) {
        if (mask != nullptr && (*mask)[row] == 0) {
            continue;
        }
        const uint32_t partition_id = partition_ids[row];
        if (partition_id >= partition_count) {
            return Status::InvalidArgument("Logical partition {} exceeds partition count {}",
                                           partition_id, partition_count);
        }
        _partition_row_counts[partition_id] += 1;
        int writer_id = _partition_writer_ids[partition_id];
        if (writer_id == -1) {
            writer_id = _get_next_writer_id(partition_id);
            if (writer_id < 0 || writer_id >= _writer_count) {
                return Status::InternalError("Skewed writer assignment returned invalid writer {}",
                                             writer_id);
            }
            _partition_writer_ids[partition_id] = writer_id;
        }
        writer_ids[row] = static_cast<uint32_t>(writer_id);
    }

    for (size_t partition_id = 0; partition_id < partition_count; ++partition_id) {
        if (_partition_row_counts[partition_id] > 0) {
            _rebalancer->add_partition_row_count(static_cast<int>(partition_id),
                                                 _partition_row_counts[partition_id]);
        }
    }
    _rebalancer->add_data_processed(static_cast<long>(block_bytes));
    return Status::OK();
}

int SkewedWriterAssigner::_get_next_writer_id(uint32_t partition_id) {
    return _rebalancer->get_task_id(partition_id, _partition_writer_indexes[partition_id]++);
}

int64_t scale_writer_threshold_by_task(int64_t value, int task_num) {
    if (task_num <= 0) {
        return value;
    }
    int64_t scaled = value / task_num;
    return scaled == 0 ? value : scaled;
}

} // namespace doris
