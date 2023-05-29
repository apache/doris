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
    if (mutable_build_block_ == nullptr) {
        mutable_build_block_ = MutableBlock::create_unique(block->clone_empty());
    }
    // TODO: ColumnString::reserver is not accurate
    auto status = mutable_build_block_->reserve(mutable_build_block_->rows() + rows.size());
    // If add rows will cause MEM_LIMIT_EXCEEDED: temporarily skip memcheck, add build rows to _mutable block
    // and then spill build blocks to release memory.
    // TODO: spill the biggest partition
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        SKIP_MEMORY_CHECK(RETURN_IF_ERROR(
                mutable_build_block_->reserve(mutable_build_block_->rows() + rows.size())));
        RETURN_IF_ERROR(_add_rows_skip_mem_check(block, rows));
        return flush_build_stream();
    }
    return _add_rows_skip_mem_check(block, rows);
}

Status JoinPartition::_add_rows_skip_mem_check(Block* block, std::vector<int>& rows) {
    auto pre_bytes_ = mutable_build_block_->bytes();
    RETURN_IF_CATCH_EXCEPTION(
            mutable_build_block_->add_rows(block, &rows[0], &rows[0] + rows.size()));
    auto new_bytes = mutable_build_block_->bytes();
    build_data_bytes_ += new_bytes - pre_bytes_;
    if (_block_reach_limit(mutable_build_block_.get())) {
        auto block = mutable_build_block_->to_block();
        build_stream_->add_block(block);
        mutable_build_block_->clear_column_data();
    }
    return Status::OK();
}

Status JoinPartition::prepare_add_probe_rows(Block* block, std::vector<int>& rows) {
    if (mutable_probe_block_ == nullptr) {
        mutable_probe_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    return mutable_probe_block_->reserve(mutable_probe_block_->rows() + rows.size());
}

Status JoinPartition::add_probe_rows(RuntimeState* state, Block* block, std::vector<int>& rows,
                                     bool eos) {
    probe_row_count_ += rows.size();
    if (mutable_probe_block_ == nullptr) {
        mutable_probe_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    RETURN_IF_CATCH_EXCEPTION(
            mutable_build_block_->add_rows(block, &rows[0], &rows[0] + rows.size()));
    if (is_ready_for_probe()) {
        auto block = mutable_build_block_->to_block();
        RETURN_IF_ERROR(in_mem_hash_join_node_->push(state, &block, eos));
    }
    return Status::OK();
}

Status JoinPartition::restore_build_data() {
    if (!build_stream_->is_spilled()) {
        return Status::OK();
    }
    return build_stream_->restore();
}

Status JoinPartition::_build_hash_table(RuntimeState* state, ObjectPool* pool,
                                        const TPlanNode& t_plan_node,
                                        const DescriptorTbl& desc_tbl) {
    bool eos = false;
    in_mem_hash_join_node_ = std::make_unique<HashJoinNode>(pool, t_plan_node, desc_tbl);
    while (!eos) {
        Block block;
        RETURN_IF_ERROR(build_stream_->get_next(&block, &eos));
        auto status = in_mem_hash_join_node_->sink(state, &block, eos);
        if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
            build_status_ = status;
            auto build_blocks = in_mem_hash_join_node_->release_build_blocks();
            in_mem_hash_join_node_->close(state);
            in_mem_hash_join_node_.reset();
            build_stream_->add_blocks(std::move(*build_blocks));
            // return flush_build_stream();
            return Status::OK();
        }
        RETURN_IF_ERROR(status);
    }
    is_ready_for_probe_ = true;
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

void GraceHashJoinNode::_calc_columns_hash(vectorized::Block* input_block,
                                           const std::vector<int>& column_ids,
                                           std::vector<int> (&partition2rows)[PARTITION_COUNT]) {
    int rows = input_block->rows();
    std::vector<uint64_t> hash_vals(rows);
    auto* __restrict hashes = hash_vals.data();
    std::vector<SipHash> siphashs(rows);
    // result[j] means column index, i means rows index
    for (int i = 0; i < column_ids.size(); ++i) {
        input_block->get_by_position(column_ids[i]).column->update_hashes_with_value(siphashs);
    }
    for (int i = 0; i < rows; i++) {
        hashes[i] = siphashs[i].get64() % PARTITION_COUNT;
    }

    for (int i = 0; i < rows; i++) {
        partition2rows[hashes[i]].emplace_back(i);
    }
}

Status GraceHashJoinNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                               bool eos) {
    std::vector<int> res_col_ids(build_expr_ctxs_.size());
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, build_expr_ctxs_,
                                                 *_build_expr_call_timer, res_col_ids));

    std::vector<int> partition2rows[PARTITION_COUNT];
    _calc_columns_hash(input_block, res_col_ids, partition2rows);
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!partition2rows[i].empty()) {
            RETURN_IF_ERROR(current_partitions_[i]->add_build_rows(input_block, partition2rows[i]));
        }
    }
    if (eos) {
        return _build_hash_tables(state);
    }
    return Status::OK();
}

Status GraceHashJoinNode::_build_hash_tables(RuntimeState* state) {
    std::vector<int> build_partition_indice;
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!current_partitions_[i]->is_spilled()) {
            build_partition_indice.push_back(i);
        }
    }
    if (build_partition_indice.empty()) {
        // If all build partitions are spilled,
        // need to build hash table for at least one partition
        auto index = _smallest_join_partition();
        build_partition_indice.push_back(index);
    }

    build_hash_table_latch_.reset(build_partition_indice.size());

    for (auto i : build_partition_indice) {
        auto status = io_thread_pool_->submit_func([this, state, i] {
            _update_status(current_partitions_[i]->_build_hash_table(state, _pool, t_plan_node_,
                                                                     desc_tbl_));

            build_hash_table_latch_.count_down();

            processing_partitions_.emplace_back(current_partitions_[i]);
        });
        RETURN_IF_ERROR(status);
    }

    return Status::OK();
}

void GraceHashJoinNode::_update_status(Status status) {
    if (!status.ok()) {
        std::lock_guard guard(status_lock_);
        status_ = std::move(status);
    }
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
        _as_mutable()->_build_hash_tables(state_);
        return false;
    }
    if (!probe_block_.empty()) {
        return false;
    }

    if (!spill_probe_latch_.ready()) {
        return false;
    }
    return true;
}

Status GraceHashJoinNode::push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    DCHECK(probe_block_.empty());
    probe_eos_ = eos;

    if (input_block->rows() == 0) {
        return Status::OK();
    }

    std::vector<int> res_col_ids(probe_expr_ctxs_.size());
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, probe_expr_ctxs_,
                                                 *_probe_expr_call_timer, res_col_ids));
    _calc_columns_hash(input_block, res_col_ids, probe_partition2rows_);

    // waiting for hash table(s) ready
    if (!build_hash_table_latch_.ready()) {
        input_block->swap(probe_block_);
        return Status::OK();
    }

    if (0 == processing_partitions_.size()) {
        input_block->swap(probe_block_);
        return _build_hash_tables(state);
    }
    if (0 == _ready_build_partitions_count()) {
        // failed to build hash table for all current processing paritions,
        // repartition
        DCHECK(0);
    }

    return _push_probe_block(state, &probe_block_);
}

Status GraceHashJoinNode::_push_probe_block(RuntimeState* state, Block* input_block) {
    bool need_spill = false;
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!probe_partition2rows_[i].empty()) {
            auto status = current_partitions_[i]->prepare_add_probe_rows(input_block,
                                                                         probe_partition2rows_[i]);
            if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
                need_spill = true;
                break;
            }
        }
    }

    if (need_spill) {
        int n = 0;
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            if (!current_partitions_[i]->is_ready_for_probe()) {
                RETURN_IF_ERROR(current_partitions_[i]->flush_probe_stream());
                ++n;
            }
        }
        spill_probe_latch_.reset(n);
        return Status::OK();
    }

    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!probe_partition2rows_[i].empty()) {
            RETURN_IF_ERROR(current_partitions_[i]->add_probe_rows(
                    state, input_block, probe_partition2rows_[i], probe_eos_));
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    if (!probe_block_.empty()) {
        RETURN_IF_ERROR(_push_probe_block(state, &probe_block_));
    }
    if (!spill_probe_latch_.ready()) {
        return Status::OK();
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
