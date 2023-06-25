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
#include <future>
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
            BLOCK_ROWS, BLOCK_BYTES, profile));
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            probe_stream_, print_id(state->query_id()), operator_name + "-probe", node_id,
            BLOCK_ROWS, BLOCK_BYTES, profile));
    return Status::OK();
}

void JoinPartition::close_build(RuntimeState* state) {
    if (in_mem_hash_join_node_) {
        in_mem_hash_join_node_->close(state);
    }
    build_stream_->close();
}
void JoinPartition::close_probe() {
    probe_stream_->close();
}
Status JoinPartition::add_build_rows(Block* block, const std::vector<int>& rows, bool eos) {
    return build_stream_->add_rows(block, rows, false, eos);
}

Status JoinPartition::add_probe_rows(RuntimeState* state, Block* input_block,
                                     std::vector<int>& rows, bool eos) {
    if (is_ready_for_probe()) {
        probe_row_count_ += rows.size();
        mutable_probe_block_ = MutableBlock::create_unique(input_block->clone_empty());

        RETURN_IF_CATCH_EXCEPTION(
                mutable_probe_block_->add_rows(input_block, &rows[0], &rows[0] + rows.size()));

        auto new_bytes = mutable_probe_block_->bytes();
        probe_data_bytes_ += new_bytes;

        auto block = mutable_probe_block_->to_block();
        mutable_probe_block_->clear_column_data();
        in_mem_hash_join_node_->set_probe_column_ids(parent_->probe_column_ids_);

        in_mem_hash_join_node_->prepare_for_next();
        return in_mem_hash_join_node_->push(state, &block, eos);
    } else {
        return probe_stream_->add_rows(input_block, rows, false, eos);
    }
}

Status JoinPartition::unpin_build_stream() {
    build_stream_->unpin();
    return Status::OK();
}

Status JoinPartition::unpin_probe_stream() {
    probe_stream_->unpin();
    return Status::OK();
}

Status JoinPartition::_build_hash_table(RuntimeState* state, ObjectPool* pool,
                                        const TPlanNode& t_plan_node,
                                        const DescriptorTbl& desc_tbl) {
    DCHECK(!in_mem_hash_join_node_);

    in_mem_hash_join_node_ =
            std::make_unique<HashJoinNode>(pool, t_plan_node, desc_tbl, false, false);
    in_mem_hash_join_node_->set_children(parent_->get_children());
    in_mem_hash_join_node_->set_prepare_children(false);
    in_mem_hash_join_node_->set_build_column_ids(parent_->build_column_ids_);
    in_mem_hash_join_node_->set_probe_column_ids(parent_->probe_column_ids_);
    RETURN_IF_ERROR(in_mem_hash_join_node_->init(t_plan_node, state));
    RETURN_IF_ERROR(in_mem_hash_join_node_->prepare(state));
    RETURN_IF_ERROR(in_mem_hash_join_node_->alloc_resource(state));

    Status status;
    bool eos = false;
    while (!eos) {
        Block block;
        RETURN_IF_ERROR(build_stream_->get_next(&block, &eos));
        status = in_mem_hash_join_node_->sink(state, &block, eos);
        if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
            if (in_mem_hash_join_node_->is_merge_build_block_oom()) {
                build_stream_->add_blocks({std::move(block)}, false);
            }
            auto build_blocks = in_mem_hash_join_node_->release_build_blocks();
            in_mem_hash_join_node_->close(state);
            in_mem_hash_join_node_.reset();
            build_stream_->add_blocks(std::move(*build_blocks), false);
            return status;
        }
        RETURN_IF_ERROR(status);
    }
    in_mem_hash_join_node_->prepare_for_next();
    is_ready_for_probe_ = true;
    return Status::OK();
}

GraceHashJoinNode::GraceHashJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), t_plan_node_(tnode), desc_tbl_(descs) {
    DCHECK(tnode.__isset.hash_join_node);
    io_thread_pool_ = ExecEnv::GetInstance()->spill_io_pool();
    _output_row_desc.reset(
            new RowDescriptor(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}));
    _intermediate_row_desc.reset(new RowDescriptor(
            descs, tnode.hash_join_node.vintermediate_tuple_id_list,
            std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())));
}

Status GraceHashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    state_ = state;
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<bool> probe_not_ignore_null(eq_join_conjuncts.size());
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        probe_expr_ctxs_.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
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

    return _prepare_current_partitions(state, 0);
}

Status GraceHashJoinNode::_prepare_current_partitions(RuntimeState* state, int level) {
    DCHECK(current_partitions_.empty());
    if (level >= MAX_PARTITION_DEPTH) {
        return Status::Cancelled("hash join max partition depth");
    }
    current_partitions_.resize(PARTITION_COUNT);
    for (int i = 0; i < PARTITION_COUNT; ++i) {
        current_partitions_[i] = std::make_shared<JoinPartition>(this, level);
        RETURN_IF_ERROR(
                current_partitions_[i]->prepare(state, runtime_profile(), "hashjoin", id()));
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
    _close_current_partitions(state);

    ExecNode::release_resource(state);
}

// sink build blocks
Status GraceHashJoinNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                               bool eos) {
    sink_eos_ = eos;

    RETURN_IF_ERROR(_status());

    int rows = input_block->rows();

    if (rows > 0) {
        RETURN_IF_ERROR(_partition_build_block(state, input_block, eos, false));
    }
    if (eos) {
        LOG(WARNING) << "GraceHashJoinNode::sink, eos";
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            RETURN_IF_ERROR(current_partitions_[i]->build_eos());
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::_partition_build_block(RuntimeState* state, Block* block, bool eos,
                                                 bool is_repartition) {
    if (!is_repartition) {
        build_column_ids_.resize(build_expr_ctxs_.size());
        RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*block, build_expr_ctxs_,
                                                     *_build_expr_call_timer, build_column_ids_));
    }

    std::vector<int> build_partition_rows[PARTITION_COUNT];
    _calc_columns_hash(block, build_column_ids_, build_partition_rows);

    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!build_partition_rows[i].empty()) {
            RETURN_IF_ERROR(
                    current_partitions_[i]->add_build_rows(block, build_partition_rows[i], eos));
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    probe_push_eos_ = eos;

    RETURN_IF_ERROR(_status());

    // if (first_push_) {
    //     first_push_ = false;
    //     RETURN_IF_ERROR(_build_hash_tables_async(state));
    // }

    if (input_block->rows() > 0) {
        RETURN_IF_ERROR(_partition_probe_block(state, input_block, eos, false));
    }

    if (eos) {
        LOG(WARNING) << "GraceHashJoinNode::push, eos";
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            RETURN_IF_ERROR(current_partitions_[i]->probe_eos());
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::pull(RuntimeState* state, Block* output_block, bool* eos) {
    RETURN_IF_ERROR(_status());

    if (is_building_hash_table_) {
        return Status::OK();
    }

    if (is_spilled_probe_) {
        // still building hash tables
        if (is_building_hash_table_) {
            LOG(WARNING) << "GraceHashJoinNode::pull, spilled probe, hash table not ready";
            return Status::OK();
        }
        // need further repartition
        if (_ready_build_partitions_count() == 0) {
            LOG(WARNING) << "GraceHashJoinNode::pull, spilled probe, need further repartition";
            return _prepare_spilled_probe(state, eos);
        } else {
            return _probe_spilled_partition(state, output_block, eos);
        }
    } else {
        // LOG(WARNING) << "GraceHashJoinNode::pull, initial probe";
        for (auto& partition : current_partitions_) {
            if (partition->is_ready_for_probe() && !partition->current_probe_finished()) {
                bool partition_eos = false;
                auto st = partition->probe(state, output_block, &partition_eos);
                return st;
            }
        }
        // all processing partition finished probing current blocks
        if (!probe_push_eos_) {
            return Status::OK();
        }

        // probe data finished
        LOG(WARNING) << "GraceHashJoinNode::pull, initial probe, push eos";
        for (auto& partition : current_partitions_) {
            // finished probing for this partition
            if (partition->is_ready_for_probe()) {
                partition->close(state);
                continue;
            }
            spilled_partitions_.push_back(partition);
        }
        // _close_current_partitions(state);
        current_partitions_.clear();
        // all partitions are processed
        if (spilled_partitions_.empty()) {
            LOG(WARNING) << "GraceHashJoinNode::pull, initial probe, all probe eos, sink eos: "
                         << sink_eos_ << ", push eos: " << probe_push_eos_;
            *eos = true;
            return Status::OK();
        }

        LOG(WARNING) << "GraceHashJoinNode::pull, initial probe end, start processing spilled "
                        "partitions, build hash tables";
        return _prepare_spilled_probe(state, eos);
    }
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

Status GraceHashJoinNode::_partition_probe_block(RuntimeState* state, Block* block, bool eos,
                                                 bool is_repartition) {
    if (!is_repartition) {
        probe_column_ids_.resize(probe_expr_ctxs_.size());
        RETURN_IF_ERROR(HashJoinNode::evaluate_exprs(*block, probe_expr_ctxs_,
                                                     *_probe_expr_call_timer, probe_column_ids_));
    }

    std::vector<int> probe_partition_rows[PARTITION_COUNT];
    _calc_columns_hash(block, probe_column_ids_, probe_partition_rows);

    for (int i = 0; i < PARTITION_COUNT; ++i) {
        if (!probe_partition_rows[i].empty()) {
            RETURN_IF_ERROR(current_partitions_[i]->add_probe_rows(state, block,
                                                                   probe_partition_rows[i], eos));
        }
    }
    return Status::OK();
}

Status GraceHashJoinNode::_build_hash_tables_async(RuntimeState* state) {
    DCHECK(!is_building_hash_table_);

    is_building_hash_table_ = true;
    auto status = io_thread_pool_->submit_func([this] {
        // build as much hash tables as possible
        Status st;
        Defer defer {[&]() { is_building_hash_table_ = false; }};

        int success_count = 0;
        st = _build_in_memory_hash_tables_sync(success_count);
        _update_status(st);
    });
    return status;
}

Status GraceHashJoinNode::_build_in_memory_hash_tables_sync(int& success_count) {
    success_count = 0;
    Status st;
    static bool first = true;
    for (auto& partition : current_partitions_) {
        if (partition->is_build_partition_spilled()) {
            continue;
        }
        st = partition->_build_hash_table(state_, _pool, t_plan_node_, desc_tbl_);
        if (st.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
            partition->unpin_build_stream();
            break;
        }
        _update_status(st);
        RETURN_IF_ERROR(st);
        ++success_count;
        // simulate spill
        if (first && 3 == success_count) {
            break;
        }
    }
    if (first) {
        first = false;
    }
    return Status::OK();
}

Status GraceHashJoinNode::_repartition_build_data(RuntimeState* state,
                                                  JoinPartitionSPtr partition) {
    bool eos = false;
    while (!eos) {
        Block block;
        RETURN_IF_ERROR(partition->build_stream_->get_next(&block, &eos));
        RETURN_IF_ERROR(_partition_build_block(state, &block, eos, true));
    }

    // release build data
    partition->close_build(state);
    return Status::OK();
}

Status GraceHashJoinNode::_repartition_probe_data(RuntimeState* state,
                                                  JoinPartitionSPtr partition) {
    bool eos = false;
    while (!eos) {
        Block block;
        RETURN_IF_ERROR(partition->probe_stream_->get_next(&block, &eos));
        RETURN_IF_ERROR(_partition_probe_block(state, &block, eos, true));
    }

    // release build data
    partition->close_probe();
    return Status::OK();
}
void GraceHashJoinNode::_update_status(Status status) {
    if (!status.ok()) {
        std::lock_guard guard(status_lock_);
        status_ = std::move(status);
    }
}

bool GraceHashJoinNode::need_more_probe_data() const {
    // check if some build partitions still have unfinished probe block
    for (auto& partition : current_partitions_) {
        if (partition->is_ready_for_probe() && !partition->need_more_probe_data()) {
            // LOG(WARNING) << "GraceHashJoinNode::need_more_input_data, false2";
            return false;
        }
    }

    // LOG(WARNING) << "GraceHashJoinNode::need_more_input_data, true";
    return true;
}
bool GraceHashJoinNode::need_more_input_data() const {
    if (probe_push_eos_) {
        return false;
    }

    if (is_building_hash_table_) {
        return false;
    }

    if (0 == _ready_build_partitions_count()) {
        // first time before pushing probe block, or all partitions build hash table failed
        _as_mutable()->_build_hash_tables_async(state_);
        if (first_push_) {
            _as_mutable()->first_push_ = false;
        }
        LOG(WARNING) << "GraceHashJoinNode::need_more_input_data, false1";
        return false;
    }

    // check whether is flushing probe partitions

    return need_more_probe_data();
}

// pick a spilled partition, try to build hash table, and repartition it if failed,
// until succeed to build at least one hash table for a partition
Status GraceHashJoinNode::_prepare_spilled_probe(RuntimeState* state, bool* eos) {
#define CHECK_STATUS(st)    \
    do {                    \
        _update_status(st); \
        if (!status_.ok()) {     \
            return;         \
        }                   \
    } while (0)

    DCHECK(!is_building_hash_table_);
    is_spilled_probe_ = true;

    if (current_spilled_partition_) {
        current_spilled_partition_->close(state);
        current_spilled_partition_ = nullptr;
        spilled_partitions_.pop_back();
    }
    if (!current_partitions_.empty()) {
        spilled_partitions_.insert(spilled_partitions_.end(), current_partitions_.begin(),
                                   current_partitions_.end());
        current_partitions_.clear();
    }
    if (spilled_partitions_.empty()) {
        *eos = true;
        return Status::OK();
    }

    is_building_hash_table_ = true;
    auto status = io_thread_pool_->submit_func([this, state] {
        Status st;
        Defer defer {[&]() { is_building_hash_table_ = false; }};
        int success_count = 0;
        while (0 == success_count) {
            current_spilled_partition_ = spilled_partitions_.back();
            st = current_spilled_partition_->_build_hash_table(state, _pool, t_plan_node_,
                                                               desc_tbl_);
            if (st.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
                // repartition
                st = _prepare_current_partitions(state_, current_spilled_partition_->level_ + 1);
                CHECK_STATUS(st);

                st = _repartition_build_data(state, current_spilled_partition_);
                CHECK_STATUS(st);

                st = _build_in_memory_hash_tables_sync(success_count);
                CHECK_STATUS(st);
                if (success_count == 0) {
                    st = _repartition_probe_data(state, current_spilled_partition_);
                    CHECK_STATUS(st);

                    spilled_partitions_.pop_back();

                    spilled_partitions_.insert(spilled_partitions_.end(),
                                               current_partitions_.begin(),
                                               current_partitions_.end());
                    current_partitions_.clear();
                }
            } else {
                CHECK_STATUS(st);
                success_count = 1;
            }
        }
    });
    return status;
}

Status GraceHashJoinNode::_repartitioned_probe(RuntimeState* state, Block* output_block,
                                               bool* eos) {
    if (need_more_probe_data() && current_spilled_partition_->has_nex_probe_block()) {
        Block block;
        bool partition_eos = false;
        RETURN_IF_ERROR(
                current_spilled_partition_->get_next_probe_block(state, &block, &partition_eos));
        RETURN_IF_ERROR(_partition_probe_block(state, &block, partition_eos, true));
    }
    for (auto& partition : current_partitions_) {
        if (partition->is_ready_for_probe() && !partition->current_probe_finished()) {
            bool partition_eos = false;
            LOG(WARNING) << "GraceHashJoinNode::pull, probe";
            return partition->probe(state, output_block, &partition_eos);
        }
    }
    if (!current_spilled_partition_->has_nex_probe_block()) {
        *eos = true;
    }
    return Status::OK();
}

Status GraceHashJoinNode::_probe_spilled_partition(RuntimeState* state, Block* output_block,
                                                   bool* eos) {
    // probing spilled partition
    if (!current_partitions_.empty()) {
        // repartitioned
        LOG(WARNING) << "GraceHashJoinNode::pull, spilled probe, repartitioned,";
        return _repartitioned_probe(state, output_block, eos);
    } else {
        // not repartitioned
        LOG(WARNING) << "GraceHashJoinNode::pull, spilled probe, not repartitioned,";
        bool partition_eos = false;
        auto st = current_spilled_partition_->spilled_probe_not_repartitioned(state, output_block,
                                                                              &partition_eos);
        if (st.is<ErrorCode::PIP_WAIT_FOR_IO>()) {
            is_io_task_running_ = true;
        }
        // return ErrorCode::PIP_WAIT_FOR_IO if triggered reading from disk
        RETURN_IF_ERROR(st);
        // current spilled partition finished processing, pick next spilled partition to process
        if (partition_eos) {
            LOG(WARNING) << "GraceHashJoinNode::pull, spilled probe, not repartitioned, finished "
                            "current partition";
            return _prepare_spilled_probe(state, eos);
        }
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
