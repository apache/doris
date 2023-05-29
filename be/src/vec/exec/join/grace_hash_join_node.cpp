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
#include "exec/exec_node.h"
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

Status JoinPartition::prepare_add_build_rows(Block* block, std::vector<int>& rows) {
    if (mutable_build_block_ == nullptr) {
        mutable_build_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    // TODO: ColumnString::reserver is not accurate
    return mutable_build_block_->reserve(mutable_build_block_->rows() + rows.size());
}

Status JoinPartition::prepare_add_probe_rows(Block* block, std::vector<int>& rows) {
    if (mutable_probe_block_ == nullptr) {
        mutable_probe_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    return mutable_probe_block_->reserve(mutable_probe_block_->rows() + rows.size());
}

Status JoinPartition::add_build_rows(Block* block, std::vector<int>& rows) {
    build_row_count_ += rows.size();
    if (mutable_build_block_ == nullptr) {
        mutable_build_block_ = MutableBlock::create_unique(block->clone_empty());
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

Status JoinPartition::add_probe_rows(RuntimeState* state, Block* block, std::vector<int>& rows,
                                     bool eos) {
    probe_row_count_ += rows.size();
    if (mutable_probe_block_ == nullptr) {
        mutable_probe_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    auto pre_bytes_ = mutable_probe_block_->bytes();

    RETURN_IF_CATCH_EXCEPTION(
            mutable_probe_block_->add_rows(block, &rows[0], &rows[0] + rows.size()));

    auto new_bytes = mutable_probe_block_->bytes();
    probe_data_bytes_ += new_bytes - pre_bytes_;

    if (is_ready_for_probe()) {
        auto block = mutable_probe_block_->to_block();
        mutable_probe_block_->clear_column_data();
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

    DCHECK(!in_mem_hash_join_node_);

    in_mem_hash_join_node_ = std::make_unique<HashJoinNode>(pool, t_plan_node, desc_tbl);
    in_mem_hash_join_node_->set_children(parent_->get_children());
    in_mem_hash_join_node_->set_prepare_children(false);
    RETURN_IF_ERROR(in_mem_hash_join_node_->init(t_plan_node, state));
    RETURN_IF_ERROR(in_mem_hash_join_node_->prepare(state));
    RETURN_IF_ERROR(in_mem_hash_join_node_->alloc_resource(state));

    Status status;
    while (!eos) {
        Block block;
        status = build_stream_->get_next(&block, &eos);
        if (!status.ok()) {
            return status;
        }
        status = in_mem_hash_join_node_->sink(state, &block, eos);
        if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
            // need to check if HashJoinNode::sink is exception safe
            build_status_ = status;
            auto build_blocks = in_mem_hash_join_node_->release_build_blocks();
            in_mem_hash_join_node_->close(state);
            in_mem_hash_join_node_.reset();
            build_stream_->add_blocks(std::move(*build_blocks));
            return status;
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

    RETURN_IF_ERROR(VExpr::prepare(build_expr_ctxs_, state, child(1)->row_desc()));
    RETURN_IF_ERROR(VExpr::prepare(probe_expr_ctxs_, state, child(0)->row_desc()));

    current_partitions_.resize(PARTITION_COUNT);
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        current_partitions_[i] = std::make_shared<JoinPartition>(this);
        current_partitions_[i]->prepare(state, runtime_profile(), "hashjoin", id());
    }

    return Status::OK();
}

Status GraceHashJoinNode::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VExpr::open(build_expr_ctxs_, state));
    RETURN_IF_ERROR(VExpr::open(probe_expr_ctxs_, state));
    return Status::OK();
}

void GraceHashJoinNode::release_resource(RuntimeState* state) {
    VExpr::close(build_expr_ctxs_, state);
    VExpr::close(probe_expr_ctxs_, state);

    ExecNode::release_resource(state);
}

// sink build blocks
Status GraceHashJoinNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                               bool eos) {
    sink_eos_ = eos;

    RETURN_IF_ERROR(_status());

    DCHECK(build_block_.empty());

    int rows = input_block->rows();
    if (rows == 0) {
        return Status::OK();
    }

    std::vector<int> res_col_ids(build_expr_ctxs_.size());
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, build_expr_ctxs_,
                                                 *_build_expr_call_timer, res_col_ids));

    for (auto& partition_rows : build_partition_rows_) {
        partition_rows.clear();
    }
    _calc_columns_hash(input_block, res_col_ids, build_partition_rows_);

    // reserve memory for build partition
    auto status = _reserve_memory_for_build_partitions(input_block);
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        latch_.reset(1);
        input_block->swap(build_block_);

        // spill biggest build partition until there is enough memory
        auto i = _max_in_memory_build_partition();
        return current_partitions_[i]->flush_build_stream_async(
                std::bind<void>(std::mem_fn(&GraceHashJoinNode::flush_build_partition_cb), this,
                                std::placeholders::_1));
    }
    RETURN_IF_ERROR(status);

    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!build_partition_rows_[i].empty()) {
            RETURN_IF_ERROR(
                    current_partitions_[i]->add_build_rows(input_block, build_partition_rows_[i]));
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    probe_eos_ = eos;

    RETURN_IF_ERROR(_status());

    DCHECK(latch_.ready());

    if (input_block->rows() == 0) {
        return Status::OK();
    }

    std::vector<int> res_col_ids(probe_expr_ctxs_.size());
    RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*input_block, probe_expr_ctxs_,
                                                 *_probe_expr_call_timer, res_col_ids));
    _calc_columns_hash(input_block, res_col_ids, probe_partition_rows_);

    if (0 == _ready_build_partitions_count()) {
        // failed to build hash table even for on parition, maybe because
        // repartition
        DCHECK(0);
    }

    auto status = _reserve_memory_for_probe_partitions(input_block);
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        latch_.reset(1);
        input_block->swap(probe_block_);

        // spill biggest probe partition until there is enough memory
        auto i = _max_probe_partition_to_spill();
        return current_partitions_[i]->flush_probe_stream_async(
                std::bind<void>(std::mem_fn(&GraceHashJoinNode::flush_probe_partition_cb), this,
                                std::placeholders::_1));
    }
    RETURN_IF_ERROR(status);

    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!probe_partition_rows_[i].empty()) {
            RETURN_IF_ERROR(current_partitions_[i]->add_probe_rows(
                    state, input_block, probe_partition_rows_[i], probe_eos_));
        }
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

Status GraceHashJoinNode::_reserve_memory_for_build_partitions(vectorized::Block* input_block) {
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!build_partition_rows_[i].empty()) {
            auto status = current_partitions_[i]->prepare_add_build_rows(input_block,
                                                                         build_partition_rows_[i]);
            RETURN_IF_ERROR(status);
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::_reserve_memory_for_probe_partitions(vectorized::Block* input_block) {
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!probe_partition_rows_[i].empty()) {
            RETURN_IF_ERROR(current_partitions_[i]->prepare_add_probe_rows(
                    input_block, probe_partition_rows_[i]));
        }
    }
    return Status::OK();
}

void GraceHashJoinNode::flush_build_partition_cb(const Status& flush_status) {
    _update_status(flush_status);
    if (!flush_status.ok()) {
        latch_.count_down();
        return;
    }
    auto status = _reserve_memory_for_build_partitions(&build_block_);
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        auto i = _max_in_memory_build_partition();
        current_partitions_[i]->flush_build_stream_async(
                std::bind<void>(std::mem_fn(&GraceHashJoinNode::flush_build_partition_cb), this,
                                std::placeholders::_1));
    } else {
        _update_status(status);
        if (!status.ok()) {
            latch_.count_down();
            return;
        }
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            if (!build_partition_rows_[i].empty()) {
                auto st = current_partitions_[i]->add_build_rows(&build_block_,
                                                                 build_partition_rows_[i]);
                _update_status(st);
                if (!st.ok()) {
                    break;
                }
            }
        }
        build_block_.swap(Block());
        latch_.count_down();
    }
}

void GraceHashJoinNode::flush_probe_partition_cb(const Status& flush_status) {
    _update_status(flush_status);
    if (!flush_status.ok()) {
        latch_.count_down();
        return;
    }
    auto status = _reserve_memory_for_probe_partitions(&probe_block_);
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        auto i = _max_probe_partition_to_spill();
        current_partitions_[i]->flush_probe_stream_async(
                std::bind<void>(std::mem_fn(&GraceHashJoinNode::flush_probe_partition_cb), this,
                                std::placeholders::_1));
    } else {
        _update_status(status);
        if (!status.ok()) {
            latch_.count_down();
            return;
        }
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            if (!probe_partition_rows_[i].empty()) {
                auto st = current_partitions_[i]->add_probe_rows(
                        state_, &probe_block_, probe_partition_rows_[i], probe_eos_);
                _update_status(st);
                if (!st.ok()) {
                    break;
                }
            }
        }
        probe_block_.swap(Block());
        latch_.count_down();
    }
}

Status GraceHashJoinNode::_build_hash_tables_async(RuntimeState* state) {
    latch_.reset(1);

    auto status = io_thread_pool_->submit_func([this, state] {
        // build as much hash tables as possible
        for (auto& partition : processing_partitions_) {
            auto st = partition->_build_hash_table(state, _pool, t_plan_node_, desc_tbl_);
            if (st.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
                break;
            }
            _update_status(st);
            if (!st.ok()) {
                break;
            }
        }
        latch_.count_down();
    });
    return status;
}

void GraceHashJoinNode::_update_status(Status status) {
    if (!status.ok()) {
        std::lock_guard guard(status_lock_);
        status_ = std::move(status);
    }
}

void GraceHashJoinNode::_get_partitions_to_process() {
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!current_partitions_[i]->is_processed() &&
            !current_partitions_[i]->is_build_partition_spilled()) {
            processing_partitions_.push_back(current_partitions_[i]);
        }
    }
}

void GraceHashJoinNode::_get_spilled_partition_to_process() {
    if (!spilled_partitions_.empty()) {
        current_spilled_partition_ = spilled_partitions_[spilled_partitions_.size() - 1];
    }
}

bool GraceHashJoinNode::need_more_input_data() const {
    if (processing_partitions_.empty()) {
        _as_mutable()->_get_partitions_to_process();
    }

    if (processing_partitions_.empty()) {
        _as_mutable()->_get_spilled_partition_to_process();
    }

    // all partitions are processed
    if (processing_partitions_.empty() && !current_spilled_partition_) {
        return false;
    }

    // is building hash table(s) or flushing probe partitions
    if (!latch_.ready()) {
        return false;
    }

    if (0 == _ready_build_partitions_count()) {
        _as_mutable()->_build_hash_tables_async(state_);
        return false;
    }

    for (auto& partition : processing_partitions_) {
        if (partition->is_ready_for_probe() && !partition->need_more_probe_data()) {
            return false;
        }
    }

    return true;
}

Status GraceHashJoinNode::pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    RETURN_IF_ERROR(_status());

    if (!latch_.ready()) {
        return Status::OK();
    }

    for (auto& partition : processing_partitions_) {
        if (partition->is_ready_for_probe() && partition->current_probe_finished()) {
            bool partition_eos = false;
            return partition->probe(state, output_block, &partition_eos);
        }
    }

    // push finished
    if (probe_eos_) {
        for (auto& partition : processing_partitions_) {
            // finished probing for this partition
            if (partition->is_ready_for_probe()) {
                partition->set_is_processed();
                continue;
            }
            spilled_partitions_.push_back(partition);
        }
        processing_partitions_.clear();
    }

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
