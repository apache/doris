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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"

namespace doris {
class SkewedPartitionRebalancer;
}

namespace doris {
#include "common/compile_check_begin.h"

// Maps logical partitions computed by a PartitionFunction to Doris exchange channels.
class WriterAssigner {
public:
    virtual ~WriterAssigner() = default;

    virtual Status assign(const std::vector<uint32_t>& partition_ids,
                          const std::vector<uint8_t>* mask, size_t rows, size_t block_bytes,
                          std::vector<uint32_t>& writer_ids) = 0;
};

// Preserves stable ownership: one logical partition always maps to one writer id.
class IdentityWriterAssigner final : public WriterAssigner {
public:
    explicit IdentityWriterAssigner(uint32_t writer_count) : _writer_count(writer_count) {}

    Status assign(const std::vector<uint32_t>& partition_ids, const std::vector<uint8_t>* mask,
                  size_t rows, size_t block_bytes, std::vector<uint32_t>& writer_ids) override;

private:
    uint32_t _writer_count;
};

// Allows a hot logical partition to use multiple writers while retaining the existing
// ScaleWriter affinity and rebalance behavior.
class SkewedWriterAssigner final : public WriterAssigner {
public:
    SkewedWriterAssigner(int partition_count, int task_count, int task_bucket_count,
                         long min_partition_data_processed_rebalance_threshold,
                         long min_data_processed_rebalance_threshold);

    ~SkewedWriterAssigner() override;

    Status assign(const std::vector<uint32_t>& partition_ids, const std::vector<uint8_t>* mask,
                  size_t rows, size_t block_bytes, std::vector<uint32_t>& writer_ids) override;

private:
    int _get_next_writer_id(uint32_t partition_id);

    std::unique_ptr<SkewedPartitionRebalancer> _rebalancer;
    int _writer_count;
    std::vector<int> _partition_row_counts;
    std::vector<int> _partition_writer_ids;
    std::vector<int> _partition_writer_indexes;
};

// Scale table-sink thresholds by local pipeline task count while preserving the historical
// behavior for very small values.
int64_t scale_writer_threshold_by_task(int64_t value, int task_num);

#include "common/compile_check_end.h"
} // namespace doris
