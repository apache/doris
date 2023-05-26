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

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "common/status.h"
#include "vec/spill/spill_stream_manager.h"
namespace doris {
namespace vectorized {
Status JoinPartition::prepare(RuntimeState* state, RuntimeProfile* profile,
                              const std::string& operator_name, int node_id) {
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            build_stream_, print_id(state->query_id()), operator_name + "-build", node_id,
            BLOCK_ROWS, profile));
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            probe_stream_, print_id(state->query_id()), operator_name + "-probe", node_id,
            BLOCK_ROWS, profile));
    return Status::OK();
}

Status JoinPartition::add_build_rows(Block* block, std::vector<int>& rows) {
    build_row_count_ += rows.size();
    if (_mutable_block == nullptr) {
        _mutable_block = MutableBlock::create_unique(block->clone_empty());
    }
    // TODO: ColumnString::reserver is not accurate
    auto status = _mutable_block->reserve(_mutable_block->rows() + rows.size());
    // If add rows will cause MEM_LIMIT_EXCEEDED: temporarily skip memcheck, add build rows to _mutable block
    // and then spill build blocks to release memory.
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        SKIP_MEMORY_CHECK(
                RETURN_IF_ERROR(_mutable_block->reserve(_mutable_block->rows() + rows.size())));
        RETURN_IF_ERROR(_add_rows_skip_mem_check(block, rows));
        return flush_build_stream();
    }
    return _add_rows_skip_mem_check(block, rows);
}

Status JoinPartition::_add_rows_skip_mem_check(Block* block, std::vector<int>& rows) {
    auto pre_bytes_ = _mutable_block->bytes();
    RETURN_IF_CATCH_EXCEPTION(_mutable_block->add_rows(block, &rows[0], &rows[0] + rows.size()));
    auto new_bytes = _mutable_block->bytes();
    build_data_bytes_ += new_bytes - pre_bytes_;
    if (_reach_limit()) {
        auto block = _mutable_block->to_block();
        build_stream_->add_block(block);
        _mutable_block->clear_column_data();
    }
    return Status::OK();
}

Status JoinPartition::restore_build_data() {
    if (!build_stream_->is_spilled()) {
        return Status::OK();
    }
    return build_stream_->restore();
}

Status JoinPartition::build_hash_table(RuntimeState* state, ObjectPool* pool,
                                       const TPlanNode& t_plan_node,
                                       const DescriptorTbl& desc_tbl) {
    bool eos = false;
    DCHECK(!build_stream_->is_spilled());
    DCHECK(!is_building_hash_table_);
    is_building_hash_table_ = true;
    in_mem_hash_join_node_ = std::make_unique<HashJoinNode>(pool, t_plan_node, desc_tbl);
    while (!eos) {
        Block block;
        build_stream_->get_next(&block, &eos);
        auto status = in_mem_hash_join_node_->sink(state, &block, eos);
        if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
            auto build_blocks = in_mem_hash_join_node_->release_build_blocks();
            in_mem_hash_join_node_->close(state);
            in_mem_hash_join_node_.reset();
            build_stream_->add_blocks(std::move(*build_blocks));
            RETURN_IF_ERROR(flush_build_stream());
            return status;
        }
        RETURN_IF_ERROR(status);
    }
    return Status::OK();
}

GraceHashJoinNode::GraceHashJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), t_plan_node_(tnode), desc_tbl_(descs) {
    io_thread_pool_ = ExecEnv::GetInstance()->spill_io_pool();
}

Status GraceHashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    state_ = state;
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.left, &ctx));
        probe_expr_ctxs_.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.right, &ctx));
        build_expr_ctxs_.push_back(ctx);
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
        current_partitions_[i]->prepare(state, runtime_profile(), "hashjoin", id());
    }

    return Status::OK();
}

Status GraceHashJoinNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                               bool eos) {
    std::vector<int> res_col_ids(build_expr_ctxs_.size());
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, build_expr_ctxs_,
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
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        RETURN_IF_ERROR(current_partitions_[i]->add_build_rows(input_block, partition2rows[i]));
    }
    return Status::OK();
}

Status GraceHashJoinNode::_trigger_build_hash_tables(RuntimeState* state) {
    int n = 0;
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        // build hash table for not spilled partitions first
        if (!current_partitions_[i]->is_spilled()) {
            ++n;
            processing_partitions_.emplace_back(current_partitions_[i]);
        }
    }
    if (0 == n) {
        build_hash_table_latch_.reset(1);
        auto index = _smallest_join_partition();
        RETURN_IF_ERROR(current_partitions_[index]->restore_build_data());
        processing_partitions_.emplace_back(current_partitions_[index]);
    } else {
        build_hash_table_latch_.reset(n);
        auto status = io_thread_pool_->submit_func([this, state] {
            for (int i = 0; i < PARTITION_COUNT; ++i) {
                // build hash table for not spilled partitions first
                if (current_partitions_[i]->is_spilled()) {
                    continue;
                }
                auto st = current_partitions_[i]->build_hash_table(state, _pool, t_plan_node_,
                                                                   desc_tbl_);
                build_hash_table_latch_.count_down();
                if (st.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
                    break;
                }
                RETURN_IF_ERROR(st);
            }
            return Status::OK();
        });
    }

    // need to build hash table for at least one partition
    return Status::OK();
}

Status GraceHashJoinNode::_build_partition_hash_table(RuntimeState* state, int i) {
    auto status = current_partitions_[i]->build_hash_table(state, _pool, t_plan_node_, desc_tbl_);
    RETURN_IF_ERROR(status);
    processing_partitions_.emplace_back(current_partitions_[i]);
    return Status::OK();
}

bool GraceHashJoinNode::need_more_input_data() const {
    // build partitions have unfinished flushing jobs
    if (!can_sink_write()) {
        return false;
    }
    if (!build_hash_table_latch_.ready()) {
        return false;
    }
    if (0 == processing_partitions_.size()) {
        _as_mutable()->_trigger_build_hash_tables(state_);
        return false;
    }
    if (!probe_block_.empty()) {
        return false;
    }
    return true;
}

Status GraceHashJoinNode::push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    DCHECK(probe_block_.empty());

    if (input_block->rows() == 0) {
        return Status::OK();
    }
    int probe_expr_ctxs_sz = probe_expr_ctxs_.size();

    std::vector<int> res_col_ids(probe_expr_ctxs_sz);
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, probe_expr_ctxs_,
                                                 *_probe_expr_call_timer, res_col_ids));
    if (!build_hash_table_latch_.ready()) {
        input_block->swap(probe_block_);
    }

    if (0 == processing_partitions_.size()) {
        input_block->swap(probe_block_);
        return _trigger_build_hash_tables(state);
    }
    return Status::OK();
}
Status GraceHashJoinNode::pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
