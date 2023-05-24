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

#include "vec/exec/join/grace_hash_join_node.h"

#include <memory>

#include "common/status.h"
namespace doris {
namespace vectorized {
Status JoinPartition::prepare() {
    return Status::OK();
}

Status JoinPartition::add_build_rows(Block* block, const std::vector<int>& rows) {
    if (_mutable_block == nullptr) {
        _mutable_block = MutableBlock::create_unique(block->clone_empty());
    }
    _mutable_block->add_rows(block, &rows[0], &rows[0] + rows.size());
    if (_reach_limit()) {
        auto block = _mutable_block->to_block();
        build_stream_->add_block(block);
    }
    return Status::OK();
}
GraceHashJoinNode::GraceHashJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), t_plan_node_(tnode), desc_tbl_(descs) {}

Status GraceHashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.right, &ctx));
        _build_expr_ctxs.push_back(ctx);
    }
    return Status::OK();
}

Status GraceHashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    auto build_phase_profile = runtime_profile()->create_child("BuildPhase", true, true);
    _build_expr_call_timer = ADD_TIMER(build_phase_profile, "BuildExprCallTime");
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");

    current_partitions_.resize(PARTITION_COUNT);
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        current_partitions_[i] = std::make_shared<JoinPartition>();
    }

    return Status::OK();
}

Status GraceHashJoinNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                               bool eos) {
    std::vector<int> res_col_ids(_build_expr_ctxs.size());
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, _build_expr_ctxs,
                                                 *_build_expr_call_timer, res_col_ids));

    int rows = input_block->rows();
    std::vector<uint64_t> hash_vals(rows);
    auto* __restrict hashes = hash_vals.data();
    std::vector<SipHash> siphashs(rows);
    // result[j] means column index, i means rows index
    for (int i = 0; i < res_col_ids.size(); ++i) {
        input_block->get_by_position(res_col_ids[i]).column->update_hashes_with_value(siphashs);
    }
    for (int i = 0; i < rows; i++) {
        hashes[i] = siphashs[i].get64() % PARTITION_COUNT;
    }

    std::vector<int> partition2rows[PARTITION_COUNT];
    for (int i = 0; i < rows; i++) {
        partition2rows[hashes[i]].emplace_back(i);
    }
    Status status;
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        status = current_partitions_[i]->add_build_rows(input_block, partition2rows[i]);
        if (status.is<ErrorCode::END_OF_FILE>()) {
            current_partitions_[i]->flush_build_stream();
        }
        RETURN_IF_ERROR(status);
    }
    return Status::OK();
}
Status GraceHashJoinNode::push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    return Status::OK();
}
Status GraceHashJoinNode::pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
