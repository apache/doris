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

#include <gen_cpp/Partitions_types.h>

#include <string>

#include "exec/partitioner/partitioner.h"
#include "format/transformer/writer_assigner.h"

namespace doris {

class MergePartitioner final : public PartitionerBase {
public:
    MergePartitioner(size_t partition_count, const TMergePartitionInfo& merge_info,
                     bool use_new_shuffle_hash_method);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_partitioning(RuntimeState* state, Block* block) const override;
    const std::vector<HashValType>& get_channel_ids() const override { return _channel_ids; }
    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

private:
    void _apply_insert_rebalance(const std::vector<int8_t>& ops,
                                 std::vector<uint32_t>& insert_hashes, size_t block_bytes) const;
    void _init_insert_scaling(RuntimeState* state);
    uint32_t _next_rr_channel() const;
    Status _clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src,
                            VExprContextSPtrs& dst) const;
    ShuffleHashMethod _hash_method() const {
        return _use_new_shuffle_hash_method ? ShuffleHashMethod::CRC32C : ShuffleHashMethod::CRC32;
    }

    TMergePartitionInfo _merge_info;
    bool _use_new_shuffle_hash_method = false;
    bool _insert_random = false;
    bool _enable_insert_rebalance = false;
    size_t _insert_partition_count = 0;
    mutable int64_t _insert_data_processed = 0;
    mutable int _insert_writer_count = 1;
    int64_t _non_partition_scaling_threshold = 0;
    VExprContextSPtrs _operation_expr_ctxs;
    std::unique_ptr<PartitionFunction> _insert_partition_function;
    std::unique_ptr<PartitionFunction> _delete_partition_function;
    mutable std::unique_ptr<SkewedWriterAssigner> _insert_writer_assigner;
    mutable std::vector<uint32_t> _channel_ids;
    mutable uint32_t _rr_offset = 0;
};

} // namespace doris
