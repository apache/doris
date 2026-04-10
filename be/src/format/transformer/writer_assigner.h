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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "exec/connector/skewed_partition_rebalancer.h"

namespace doris {

class WriterAssigner {
public:
    virtual ~WriterAssigner() = default;

    virtual void assign(const std::vector<uint32_t>& partition_ids,
                        const std::vector<uint8_t>* mask, size_t rows, size_t block_bytes,
                        std::vector<uint32_t>& writer_ids) = 0;
};

class IdentityWriterAssigner final : public WriterAssigner {
public:
    void assign(const std::vector<uint32_t>& partition_ids, const std::vector<uint8_t>* mask,
                size_t rows, size_t /*block_bytes*/, std::vector<uint32_t>& writer_ids) override {
        if (rows == 0) {
            return;
        }
        if (writer_ids.size() != rows && &writer_ids != &partition_ids) {
            writer_ids.resize(rows);
        }
        if (mask == nullptr) {
            for (size_t i = 0; i < rows; ++i) {
                writer_ids[i] = partition_ids[i];
            }
            return;
        }
        for (size_t i = 0; i < rows; ++i) {
            if ((*mask)[i] == 0) {
                continue;
            }
            writer_ids[i] = partition_ids[i];
        }
    }
};

class SkewedWriterAssigner final : public WriterAssigner {
public:
    SkewedWriterAssigner(int partition_count, int task_count, int task_bucket_count,
                         long min_partition_data_processed_rebalance_threshold,
                         long min_data_processed_rebalance_threshold)
            : _rebalancer(partition_count, task_count, task_bucket_count,
                          min_partition_data_processed_rebalance_threshold,
                          min_data_processed_rebalance_threshold),
              _partition_row_counts(partition_count, 0),
              _partition_writer_ids(partition_count, -1),
              _partition_writer_indexes(partition_count, 0) {}

    void assign(const std::vector<uint32_t>& partition_ids, const std::vector<uint8_t>* mask,
                size_t rows, size_t block_bytes, std::vector<uint32_t>& writer_ids) override {
        if (rows == 0 || _partition_row_counts.empty()) {
            return;
        }
        if (writer_ids.size() != rows && &writer_ids != &partition_ids) {
            writer_ids.resize(rows);
        }

        std::fill(_partition_row_counts.begin(), _partition_row_counts.end(), 0);
        std::fill(_partition_writer_ids.begin(), _partition_writer_ids.end(), -1);
        _rebalancer.rebalance();

        const size_t partition_count = _partition_row_counts.size();
        for (size_t i = 0; i < rows; ++i) {
            if (mask != nullptr && (*mask)[i] == 0) {
                continue;
            }
            const uint32_t partition_id = partition_ids[i];
            if (partition_id >= partition_count) {
                continue;
            }
            _partition_row_counts[partition_id] += 1;
            int writer_id = _partition_writer_ids[partition_id];
            if (writer_id == -1) {
                writer_id = _get_next_writer_id(partition_id);
                _partition_writer_ids[partition_id] = writer_id;
            }
            writer_ids[i] = static_cast<uint32_t>(writer_id);
        }

        for (size_t i = 0; i < partition_count; ++i) {
            if (_partition_row_counts[i] > 0) {
                _rebalancer.add_partition_row_count(static_cast<int>(i), _partition_row_counts[i]);
            }
        }
        _rebalancer.add_data_processed(static_cast<long>(block_bytes));
    }

private:
    int _get_next_writer_id(uint32_t partition_id) {
        return _rebalancer.get_task_id(partition_id, _partition_writer_indexes[partition_id]++);
    }

    SkewedPartitionRebalancer _rebalancer;
    std::vector<int> _partition_row_counts;
    std::vector<int> _partition_writer_ids;
    std::vector<int> _partition_writer_indexes;
};

} // namespace doris
