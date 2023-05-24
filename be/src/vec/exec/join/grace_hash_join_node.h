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
#include <memory>

#include "common/status.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {
namespace vectorized {

using InMemoryHashJoinNodeUPtr = std::unique_ptr<HashJoinNode>;

class JoinPartition {
public:
    Status prepare();
    Status add_build_block(Block& block) {
        build_stream_->add_block(block);
        return Status::OK();
    }
    Status add_build_rows(Block* block, const std::vector<int>& rows);

    Status add_probe_block(Block& block) {
        probe_stream_->add_block(block);
        return Status::OK();
    }
    Status push_probe_block(Block& block) {
        // in_mem_hash_join_node_->push(block);
        return Status::OK();
    }
    Status build_hash_table() {
        // bool eos = false;
        // in_mem_hash_join_node_ = std::make_unique<HashJoinNode>();
        // while (!eos) {
        //     Block block;
        //     build_stream_->get_next(&block, &eos)
        //     RETURN_IF_ERROR(in_mem_hash_join_node_->sink(block, eos));
        // }
        return Status::OK();
    }
    // force spill build blocks
    Status flush_build_stream() { return build_stream_->flush(); }

    bool has_hash_table() const { return in_mem_hash_join_node_ != nullptr; }

private:
    bool _reach_limit() const {
        return _mutable_block->rows() > BLOCK_ROWS || _mutable_block->bytes() > BLOCK_BYTES;
    }

    static constexpr size_t BLOCK_ROWS = 1024 * 1024;
    static constexpr size_t BLOCK_BYTES = 64 << 20;

    InMemoryHashJoinNodeUPtr in_mem_hash_join_node_;
    SpillStreamSPtr build_stream_;
    SpillStreamSPtr probe_stream_;

    std::unique_ptr<MutableBlock> _mutable_block;
};
using JoinPartitionSPtr = std::shared_ptr<JoinPartition>;

class GraceHashJoinNode : public ExecNode {
public:
    GraceHashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    // 对Build数据进行分区，必要时落盘。
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    // 对Probe数据进行分区和probe。
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

private:
    static constexpr int PARTITION_COUNT = 16;

    TPlanNode t_plan_node_;
    DescriptorTbl desc_tbl_;

    using VExprContexts = std::vector<VExprContext*>;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;

    // current processing partitions: the inital partitons or splitted sub partitions
    // of a large partition
    std::vector<JoinPartitionSPtr> current_partitions_;

    // all the partitions that are spilled and waiting for further processing,
    // will be processed one by one
    std::vector<JoinPartitionSPtr> spilled_partitions_;

    // the current spilled partition that are processing, may need split if it does
    // not fit in the memory.
    JoinPartitionSPtr current_spilled_partition_;

    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
};
} // namespace vectorized
} // namespace doris
