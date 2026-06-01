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

#include "exec/operator/partitioned_hash_join_sink_operator.h"

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_writer.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_profile.h"
#include "util/pretty_printer.h"

namespace doris {

Status PartitionedHashJoinSinkLocalState::init(doris::RuntimeState* state,
                                               doris::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    _shared_state->_partitioned_build_blocks.resize(p._partition_count);
    _shared_state->_spilled_build_groups.resize(p._partition_count);
    _build_writers.resize(p._partition_count);

    _rows_in_partitions.assign(p._partition_count, 0);

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _partition_shuffle_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRePartitionTime", 1);
    _spill_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillBuildTime", 1);
    _in_mem_rows_counter =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "SpillInMemRow", TUnit::UNIT, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);
    RETURN_IF_ERROR(_setup_internal_operator(state));
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::open(state));
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    return p._partitioner->clone(state, _partitioner);
}

Status PartitionedHashJoinSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(PipelineXSpillSinkLocalState::exec_time_counter());
    SCOPED_TIMER(PipelineXSpillSinkLocalState::_close_timer);
    if (PipelineXSpillSinkLocalState::_closed) {
        return Status::OK();
    }
    DCHECK(_shared_state->_inner_runtime_state != nullptr);
    VLOG_DEBUG << "Query:" << print_id(state->query_id())
               << ", hash join sink:" << _parent->node_id() << ", task:" << state->task_id()
               << ", close";
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    if (!_shared_state->_is_spilled && _shared_state->_inner_runtime_state) {
        RETURN_IF_ERROR(p._inner_sink_operator->close(_shared_state->_inner_runtime_state.get(),
                                                      exec_status));
    }

    for (auto& writer : _build_writers) {
        if (writer) {
            RETURN_IF_ERROR(writer->close());
            writer.reset();
        }
    }
    _build_writers.clear();

    return PipelineXSpillSinkLocalState::close(state, exec_status);
}

size_t PartitionedHashJoinSinkLocalState::revocable_mem_size(RuntimeState* state) const {
    /// If no need to spill, all rows were sunk into the `_inner_sink_operator` without partitioned.
    if (!_shared_state->_is_spilled) {
        if (_shared_state->_inner_shared_state) {
            auto* inner_sink_state_ = _shared_state->_inner_runtime_state->get_sink_local_state();
            if (inner_sink_state_) {
                auto* inner_sink_state =
                        assert_cast<HashJoinBuildSinkLocalState*>(inner_sink_state_);
                return inner_sink_state->_build_blocks_memory_usage->value();
            }
        }
        return 0;
    }

    size_t mem_size = 0;
    auto& partitioned_blocks = _shared_state->_partitioned_build_blocks;
    for (auto& block : partitioned_blocks) {
        if (block) {
            auto block_bytes = block->allocated_bytes();
            if (block_bytes >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
                mem_size += block_bytes;
            }
        }
    }
    return mem_size > state->spill_min_revocable_mem() ? mem_size : 0;
}

void PartitionedHashJoinSinkLocalState::update_memory_usage() {
    if (!_shared_state->_is_spilled) {
        if (_shared_state->_inner_shared_state) {
            auto* inner_sink_state_ = _shared_state->_inner_runtime_state->get_sink_local_state();
            if (inner_sink_state_) {
                auto* inner_sink_state =
                        assert_cast<HashJoinBuildSinkLocalState*>(inner_sink_state_);
                COUNTER_SET(_memory_used_counter, inner_sink_state->_memory_used_counter->value());
            }
        }
        return;
    }

    int64_t mem_size = 0;
    auto& partitioned_blocks = _shared_state->_partitioned_build_blocks;
    for (auto& block : partitioned_blocks) {
        if (block) {
            mem_size += block->allocated_bytes();
        }
    }
    COUNTER_SET(_memory_used_counter, mem_size);
}

size_t PartitionedHashJoinSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    size_t size_to_reserve = 0;
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();

    if (_shared_state->_is_spilled) {
        size_to_reserve = p._partition_count * SpillFile::MIN_SPILL_WRITE_BATCH_MEM;
    } else {
        if (_shared_state->_inner_runtime_state) {
            size_to_reserve = p._inner_sink_operator->get_reserve_mem_size(
                    _shared_state->_inner_runtime_state.get(), eos);
        }
    }

    COUNTER_SET(_memory_usage_reserved, int64_t(size_to_reserve));
    return size_to_reserve;
}

Dependency* PartitionedHashJoinSinkLocalState::finishdependency() {
    return _finish_dependency.get();
}

Status PartitionedHashJoinSinkLocalState::_revoke_unpartitioned_block(RuntimeState* state) {
    // SpillFiles and writers will be created lazily in _spill_to_disk
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    HashJoinBuildSinkLocalState* inner_sink_state {nullptr};
    if (auto* tmp_sink_state = _shared_state->_inner_runtime_state->get_sink_local_state()) {
        inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(tmp_sink_state);
    }
    DCHECK_EQ(_shared_state->_inner_shared_state->hash_table_variant_vector.size(), 1);
    _shared_state->_inner_shared_state->hash_table_variant_vector.front().reset();
    if (inner_sink_state) {
        COUNTER_UPDATE(_memory_used_counter,
                       -(inner_sink_state->_hash_table_memory_usage->value() +
                         inner_sink_state->_build_arena_memory_usage->value()));
    }
    const auto& row_desc = p._child->row_desc();
    const auto num_slots = row_desc.num_slots();
    Block build_block;
    int64_t block_old_mem = 0;
    if (inner_sink_state) {
        build_block = inner_sink_state->_build_side_mutable_block.to_block();
        block_old_mem = build_block.allocated_bytes();
        // If spilling was triggered, constructing runtime filters is meaningless,
        // therefore, all runtime filters are temporarily disabled.
        RETURN_IF_ERROR(inner_sink_state->_runtime_filter_producer_helper->skip_process(
                _shared_state->_inner_runtime_state.get()));
        _finish_dependency->set_ready();
    }

    if (build_block.rows() <= 1) {
        LOG(WARNING) << fmt::format(
                "Query:{}, hash join sink:{}, task:{},"
                " has no data to revoke",
                print_id(state->query_id()), _parent->node_id(), state->task_id());
        return Status::OK();
    }

    if (build_block.columns() > num_slots) {
        Block::erase_useless_column(&build_block, num_slots);
        COUNTER_UPDATE(_memory_used_counter, build_block.allocated_bytes() - block_old_mem);
    }

    // The inner sink's _build_side_mutable_block has a sentinel row at
    // index 0 (used for column type evaluation), so real data starts at
    // row 1.  Split the big block into sub-blocks and reuse the normal
    // _partition_block + _execute_spill_partitioned_blocks path to avoid
    // duplicating the partitioning logic.
    // make the batch size larger, because after partitioning, the
    // number of rows per partition is much smaller. To avoid too much small blocks.
    const size_t batch_size = 4096 * _shared_state->_partitioned_build_blocks.size();
    const size_t total_rows = build_block.rows();
    for (size_t offset = 1; offset < total_rows;) {
        const size_t this_run = std::min(batch_size, total_rows - offset);
        auto sub_block = build_block.clone_empty();
        for (size_t c = 0; c != build_block.columns(); ++c) {
            sub_block.get_by_position(c).column =
                    build_block.get_by_position(c).column->cut(offset, this_run);
        }
        offset += this_run;
        RETURN_IF_ERROR(_partition_block(state, &sub_block, 0, sub_block.rows()));
        RETURN_IF_ERROR(_execute_spill_partitioned_blocks(state, true /*force_spill*/));
    }
    RETURN_IF_ERROR(_force_flush_partitions(state));
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::terminate(RuntimeState* state) {
    if (_terminated) {
        return Status::OK();
    }
    // Walk the chain `_shared_state -> _inner_runtime_state` defensively.
    // The inner runtime state is built separately from the outer sink's
    // setup_local_state path, so its atomicity is weaker than a top-level
    // local-state init. Any null in this chain on a cancel / early-wake
    // path would NPE inside terminate.
    if (_shared_state == nullptr || _shared_state->_inner_runtime_state == nullptr) {
        return PipelineXSpillSinkLocalState<PartitionedHashJoinSharedState>::terminate(state);
    }
    HashJoinBuildSinkLocalState* inner_sink_state {nullptr};
    if (auto* tmp_sink_state = _shared_state->_inner_runtime_state->get_sink_local_state()) {
        inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(tmp_sink_state);
    }
    if (inner_sink_state) {
        if (_parent->cast<PartitionedHashJoinSinkOperatorX>()._inner_sink_operator &&
            inner_sink_state->_runtime_filter_producer_helper) {
            RETURN_IF_ERROR(inner_sink_state->_runtime_filter_producer_helper->skip_process(state));
        }
        inner_sink_state->_terminated = true;
    }
    return PipelineXSpillSinkLocalState<PartitionedHashJoinSharedState>::terminate(state);
}

Status PartitionedHashJoinSinkLocalState::_force_flush_partitions(RuntimeState* state) {
    for (size_t i = 0; i != _shared_state->_partitioned_build_blocks.size(); ++i) {
        auto& block = _shared_state->_partitioned_build_blocks[i];
        if (block && block->rows() > 0) {
            auto spilled_block = block->to_block();
            block.reset();
            // Lazy-create SpillFile + Writer if needed
            auto& spill_file = _shared_state->_spilled_build_groups[i];
            auto& writer = _build_writers[i];
            if (!writer) {
                auto label = fmt::format("hash_build_sink_{}", i);
                auto relative_path = fmt::format(
                        "{}/{}-{}-{}-{}", print_id(state->query_id()), label, _parent->node_id(),
                        state->task_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
                        relative_path, spill_file));
                RETURN_IF_ERROR(spill_file->create_writer(state, operator_profile(), writer));
            }
            RETURN_IF_ERROR(writer->write_block(state, spilled_block));
        }
    }
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::_finish_spilling(RuntimeState* state) {
    if (!_shared_state->_is_spilled) {
        return Status::OK();
    }
    // Close all writers (Writer::close() finalizes SpillFile metadata)
    for (auto& writer : _build_writers) {
        if (writer) {
            RETURN_IF_ERROR(writer->close());
        }
    }
    return Status::OK();
}

/// If revoke memory API call this method, we has to flush all memory to avoid dead loop. For example, maybe
/// revocable memory size calcuateld by memory usage is not enough using limit 100K, but we can't spill all memory to disk
/// because we use limit 1MB here. So we need to force spill all memory to disk to make sure we can make progress.
Status PartitionedHashJoinSinkLocalState::_execute_spill_partitioned_blocks(RuntimeState* state,
                                                                            bool force_spill) {
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::revoke_memory_cancel", {
        auto status = Status::InternalError(
                "fault_inject partitioned_hash_join_sink revoke_memory canceled");
        state->get_query_ctx()->cancel(status);
        return status;
    });
    SCOPED_TIMER(_spill_build_timer);

    for (size_t i = 0; i != _shared_state->_partitioned_build_blocks.size(); ++i) {
        auto& mutable_block = _shared_state->_partitioned_build_blocks[i];
        if (!mutable_block) {
            continue;
        }
        if (force_spill || mutable_block->allocated_bytes() >= state->spill_buffer_size_bytes()) {
            RETURN_IF_ERROR(_spill_to_disk(static_cast<uint32_t>(i)));
        }
    }
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::revoke_memory(RuntimeState* state) {
    VLOG_DEBUG << fmt::format("Query:{}, hash join sink:{}, task:{}, revoke_memory, eos:{}",
                              print_id(state->query_id()), _parent->node_id(), state->task_id(),
                              _child_eos);

    if (!_shared_state->_is_spilled) {
        custom_profile()->add_info_string("Spilled", "true");
        _shared_state->_is_spilled = true;
        Status st = _revoke_unpartitioned_block(state);
        DCHECK(revocable_mem_size(state) == 0);
        return st;
    }
    RETURN_IF_ERROR(_execute_spill_partitioned_blocks(state, true /*force_spill*/));
    // force flush all partitions to make sure data is written to disk
    RETURN_IF_ERROR(_force_flush_partitions(state));
    DCHECK(revocable_mem_size(state) == 0);
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::_partition_block(RuntimeState* state, Block* in_block,
                                                           size_t begin, size_t end) {
    const auto rows = in_block->rows();
    if (!rows) {
        return Status::OK();
    }

    SCOPED_TIMER(_partition_shuffle_timer);
    Defer defer {[&]() { update_memory_usage(); }};
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, in_block));

    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    const auto& channel_ids = _partitioner->get_channel_ids();
    std::vector<std::vector<uint32_t>> partition_indexes(p._partition_count);
    DCHECK_LT(begin, end);
    for (size_t i = begin; i != end; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    auto& partitioned_blocks = _shared_state->_partitioned_build_blocks;
    for (uint32_t i = 0; i != p._partition_count; ++i) {
        const auto count = partition_indexes[i].size();
        if (UNLIKELY(count == 0)) {
            continue;
        }

        if (UNLIKELY(!partitioned_blocks[i])) {
            partitioned_blocks[i] = MutableBlock::create_unique(in_block->clone_empty());
        }
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(in_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));
        _rows_in_partitions[i] += count;
    }

    update_max_min_rows_counter();

    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::_spill_to_disk(uint32_t partition_index) {
    auto& partitioned_block = _shared_state->_partitioned_build_blocks[partition_index];

    auto block = partitioned_block->to_block();
    int64_t block_mem_usage = block.allocated_bytes();
    Defer defer {[&]() { COUNTER_UPDATE(memory_used_counter(), -block_mem_usage); }};
    partitioned_block = MutableBlock::create_unique(block.clone_empty());

    // Lazy-create SpillFile + Writer if needed
    auto& spill_file = _shared_state->_spilled_build_groups[partition_index];
    auto& writer = _build_writers[partition_index];
    if (!writer) {
        auto label = fmt::format("hash_build_sink_{}", partition_index);
        auto relative_path = fmt::format("{}/{}-{}-{}-{}", print_id(state()->query_id()), label,
                                         _parent->node_id(), state()->task_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                    spill_file));
        RETURN_IF_ERROR(spill_file->create_writer(state(), operator_profile(), writer));
    }
    return writer->write_block(state(), block);
}

PartitionedHashJoinSinkOperatorX::PartitionedHashJoinSinkOperatorX(ObjectPool* pool,
                                                                   int operator_id, int dest_id,
                                                                   const TPlanNode& tnode,
                                                                   const DescriptorTbl& descs)
        : JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>(pool, operator_id, dest_id,
                                                                    tnode, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[1]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs) {
    _spillable = true;
}

Status PartitionedHashJoinSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    _partition_count = state->spill_hash_join_partition_count();
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    _name = "PARTITIONED_HASH_JOIN_SINK_OPERATOR";
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<TExpr> partition_exprs;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_exprs.emplace_back(eq_join_conjunct.right);
        partition_exprs.emplace_back(eq_join_conjunct.right);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_build_exprs));

    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>::prepare(state));
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_child));
    RETURN_IF_ERROR(_partitioner->prepare(state, _child->row_desc()));
    RETURN_IF_ERROR(_partitioner->open(state));
    return _inner_sink_operator->prepare(state);
}

Status PartitionedHashJoinSinkLocalState::_setup_internal_operator(RuntimeState* state) {
    auto inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    inner_runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    inner_runtime_state->set_be_number(state->be_number());

    inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    inner_runtime_state->resize_op_id_to_local_state(-1);
    inner_runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    auto inner_shared_state = std::dynamic_pointer_cast<HashJoinSharedState>(
            p._inner_sink_operator->create_shared_state());
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = inner_shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    RETURN_IF_ERROR(p._inner_sink_operator->setup_local_state(inner_runtime_state.get(), info));
    auto* sink_local_state = inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = inner_shared_state.get(),
                               .shared_state_map = {},
                               .task_idx = 0};

    RETURN_IF_ERROR(
            p._inner_probe_operator->setup_local_state(inner_runtime_state.get(), state_info));
    auto* probe_local_state =
            inner_runtime_state->get_local_state(p._inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));
    RETURN_IF_ERROR(sink_local_state->open(state));

    _finish_dependency = sink_local_state->finishdependency()->shared_from_this();

    /// Set these two values after all the work is ready.
    _shared_state->_inner_shared_state = std::move(inner_shared_state);
    _shared_state->_inner_runtime_state = std::move(inner_runtime_state);
    // The inner (spill) runtime state registers its own runtime filters. Merge those IDs
    // into the parent state so they are tracked for deregistration during recursive CTE rerun.
    state->merge_register_runtime_filter(
            _shared_state->_inner_runtime_state->get_deregister_runtime_filter());
    return Status::OK();
}

#define UPDATE_COUNTER_FROM_INNER(name) \
    update_profile_from_inner_profile<false>(name, custom_profile(), inner_profile)

void PartitionedHashJoinSinkLocalState::update_profile_from_inner() {
    auto* sink_local_state = _shared_state->_inner_runtime_state->get_sink_local_state();
    if (sink_local_state) {
        auto* inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(sink_local_state);
        auto* inner_profile = inner_sink_state->custom_profile();
        UPDATE_COUNTER_FROM_INNER("BuildHashTableTime");
        UPDATE_COUNTER_FROM_INNER("MergeBuildBlockTime");
        UPDATE_COUNTER_FROM_INNER("BuildTableInsertTime");
        UPDATE_COUNTER_FROM_INNER("BuildExprCallTime");
        UPDATE_COUNTER_FROM_INNER("MemoryUsageBuildBlocks");
        UPDATE_COUNTER_FROM_INNER("MemoryUsageHashTable");
        UPDATE_COUNTER_FROM_INNER("MemoryUsageBuildKeyArena");
    }
}

#undef UPDATE_COUNTER_FROM_INNER

Status PartitionedHashJoinSinkOperatorX::sink(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    const auto rows = in_block->rows();
    if (rows > 0) {
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)rows);
    }

    if (local_state._shared_state->_is_spilled) {
        // ---- Spilled path: data is partitioned and spilled to disk ----
        if (rows > 0) {
            RETURN_IF_ERROR(local_state._partition_block(state, in_block, 0, rows));
            // If any partition block exceeds the spill buffer size, immediately spill that partition to disk to avoid large block accumulation.
            RETURN_IF_ERROR(
                    local_state._execute_spill_partitioned_blocks(state, false /*force_spill*/));
        }

        // Flush partitioned blocks when eos or when accumulated data is large enough.
        if (revocable_mem_size(state) > state->spill_join_build_sink_mem_limit_bytes()) {
            RETURN_IF_ERROR(revoke_memory(state));
        }
    } else {
        // ---- Non-spill path: forward data to the inner hash join sink ----
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::sink", {
            return Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_sink sink failed");
        });

        // Sink the block into the inner (non-partitioned) hash join build operator.
        RETURN_IF_ERROR(_inner_sink_operator->sink(
                local_state._shared_state->_inner_runtime_state.get(), in_block, eos));
        local_state.update_memory_usage();
        local_state.update_profile_from_inner();
    }

    if (eos) {
        local_state._child_eos = true;
        if (local_state._shared_state->_is_spilled) {
            RETURN_IF_ERROR(local_state._force_flush_partitions(state));
            RETURN_IF_ERROR(local_state._finish_spilling(state));
        }
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t PartitionedHashJoinSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._child_eos) {
        return 0;
    }
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revocable_mem_size(state);
}

Status PartitionedHashJoinSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    CHECK(!local_state._child_eos);
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revoke_memory(state);
}

size_t PartitionedHashJoinSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}

bool PartitionedHashJoinSinkLocalState::is_blockable() const {
    return _shared_state->_is_spilled;
}

} // namespace doris
