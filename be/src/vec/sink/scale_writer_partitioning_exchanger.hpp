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
#include "vec/runtime/partitioner.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class ScaleWriterPartitioner final : public PartitionerBase {
public:
    using HashValType = uint32_t;
    ScaleWriterPartitioner(int channel_size, int partition_count, int task_count,
                           int task_bucket_count,
                           long min_partition_data_processed_rebalance_threshold,
                           long min_data_processed_rebalance_threshold)
            : PartitionerBase(partition_count),
              _channel_size(channel_size),
              _partition_rebalancer(partition_count, task_count, task_bucket_count,
                                    min_partition_data_processed_rebalance_threshold,
                                    min_data_processed_rebalance_threshold),
              _partition_row_counts(partition_count, 0),
              _partition_writer_ids(partition_count, -1),
              _partition_writer_indexes(partition_count, 0),
              _task_count(task_count),
              _task_bucket_count(task_bucket_count),
              _min_partition_data_processed_rebalance_threshold(
                      min_partition_data_processed_rebalance_threshold),
              _min_data_processed_rebalance_threshold(min_data_processed_rebalance_threshold) {
        _crc_partitioner =
                std::make_unique<vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>>(
                        _partition_count);
    }

    ~ScaleWriterPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override {
        return _crc_partitioner->init(texprs);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return _crc_partitioner->prepare(state, row_desc);
    }

    Status open(RuntimeState* state) override { return _crc_partitioner->open(state); }

    Status close(RuntimeState* state) override { return _crc_partitioner->close(state); }

    Status do_partitioning(RuntimeState* state, Block* block, bool eos,
                           bool* already_sent) const override {
        _hash_vals.resize(block->rows());
        for (int partition_id = 0; partition_id < _partition_row_counts.size(); partition_id++) {
            _partition_row_counts[partition_id] = 0;
            _partition_writer_ids[partition_id] = -1;
        }

        _partition_rebalancer.rebalance();

        RETURN_IF_ERROR(_crc_partitioner->do_partitioning(state, block));
        const auto* crc_values = _crc_partitioner->get_channel_ids().get<uint32_t>();
        for (size_t position = 0; position < block->rows(); position++) {
            int partition_id = crc_values[position];
            _partition_row_counts[partition_id] += 1;

            // Get writer id for this partition by looking at the scaling state
            int writer_id = _partition_writer_ids[partition_id];
            if (writer_id == -1) {
                writer_id = _get_next_writer_id(partition_id);
                _partition_writer_ids[partition_id] = writer_id;
            }
            _hash_vals[position] = writer_id;
        }

        for (int partition_id = 0; partition_id < _partition_row_counts.size(); partition_id++) {
            _partition_rebalancer.add_partition_row_count(partition_id,
                                                          _partition_row_counts[partition_id]);
        }
        _partition_rebalancer.add_data_processed(block->bytes());

        return Status::OK();
    }

    ChannelField get_channel_ids() const override {
        return {_hash_vals.data(), sizeof(HashValType)};
    }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override {
        partitioner.reset(new ScaleWriterPartitioner(
                _channel_size, (int)_partition_count, _task_count, _task_bucket_count,
                _min_partition_data_processed_rebalance_threshold,
                _min_data_processed_rebalance_threshold));
        return Status::OK();
    }

private:
    int _get_next_writer_id(int partition_id) const {
        return _partition_rebalancer.get_task_id(partition_id,
                                                 _partition_writer_indexes[partition_id]++);
    }

    int _channel_size;
    std::unique_ptr<PartitionerBase> _crc_partitioner;
    mutable SkewedPartitionRebalancer _partition_rebalancer;
    mutable std::vector<int> _partition_row_counts;
    mutable std::vector<int> _partition_writer_ids;
    mutable std::vector<int> _partition_writer_indexes;
    mutable std::vector<HashValType> _hash_vals;
    const int _task_count;
    const int _task_bucket_count;
    const long _min_partition_data_processed_rebalance_threshold;
    const long _min_data_processed_rebalance_threshold;
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized