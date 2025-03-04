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

#include "partitioned_hash_join_probe_operator.h"

#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : PipelineXSpillLocalState(state, parent),
          _child_block(vectorized::Block::create_unique()) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::init(state, info));
    init_spill_write_counters();

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile.reset(new RuntimeProfile("internal_profile"));
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _partitioned_blocks.resize(p._partition_count);
    _probe_spilling_streams.resize(p._partition_count);

    _spill_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                  "HashJoinProbeSpillDependency", true);
    init_counters();
    return Status::OK();
}

void PartitionedHashJoinProbeLocalState::init_counters() {
    _partition_timer = ADD_TIMER(custom_profile(), "SpillPartitionTime");
    _partition_shuffle_timer = ADD_TIMER(custom_profile(), "SpillPartitionShuffleTime");
    _spill_build_rows = ADD_COUNTER(custom_profile(), "SpillBuildRows", TUnit::UNIT);
    _spill_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillBuildTime", 1);
    _recovery_build_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildRows", TUnit::UNIT);
    _recovery_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryBuildTime", 1);
    _spill_probe_rows = ADD_COUNTER(custom_profile(), "SpillProbeRows", TUnit::UNIT);
    _recovery_probe_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeRows", TUnit::UNIT);
    _spill_build_blocks = ADD_COUNTER(custom_profile(), "SpillBuildBlocks", TUnit::UNIT);
    _recovery_build_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildBlocks", TUnit::UNIT);
    _spill_probe_blocks = ADD_COUNTER(custom_profile(), "SpillProbeBlocks", TUnit::UNIT);
    _spill_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillProbeTime", 1);
    _recovery_probe_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeBlocks", TUnit::UNIT);
    _recovery_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryProbeTime", 1);
    _get_child_next_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "GetChildNextTime", 1);

    _probe_blocks_bytes =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "ProbeBloksBytesInMem", TUnit::BYTES, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_build_custom_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("BuildHashTableTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MergeBuildBlockTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("BuildTableInsertTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("BuildExprCallTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageBuildBlocks", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageHashTable", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageBuildKeyArena", custom_profile(),
                                               child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_build_common_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("MemoryUsage", common_profile(), child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_probe_custom_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("JoinFilterTimer", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("BuildOutputBlock", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("ProbeRows", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("ProbeExprCallTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenSearchHashTableTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenBuildSideOutputTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenProbeSideOutputTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("NonEqualJoinConjunctEvaluationTime",
                                               custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("InitProbeSideTime", custom_profile(),
                                               child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_probe_common_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("MemoryUsage", common_profile(), child_profile);
}

void PartitionedHashJoinProbeLocalState::update_profile_from_inner() {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    if (_shared_state->inner_runtime_state) {
        auto* sink_local_state = _shared_state->inner_runtime_state->get_sink_local_state();
        auto* probe_local_state = _shared_state->inner_runtime_state->get_local_state(
                p._inner_probe_operator->operator_id());
        if (_shared_state->need_to_spill) {
            update_build_custom_profile<true>(sink_local_state->custom_profile());
            update_probe_custom_profile<true>(probe_local_state->custom_profile());
            update_build_common_profile<true>(sink_local_state->common_profile());
            update_probe_common_profile<true>(probe_local_state->common_profile());
        } else {
            update_build_custom_profile<false>(sink_local_state->custom_profile());
            update_probe_custom_profile<false>(probe_local_state->custom_profile());
            update_build_common_profile<false>(sink_local_state->common_profile());
            update_probe_common_profile<false>(probe_local_state->common_profile());
        }
    }
}

Status PartitionedHashJoinProbeLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::open(state));
    return _parent->cast<PartitionedHashJoinProbeOperatorX>()._partitioner->clone(state,
                                                                                  _partitioner);
}
Status PartitionedHashJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(PipelineXSpillLocalState::close(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::spill_probe_blocks(RuntimeState* state) {
    auto* spill_io_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    auto query_id = state->query_id();

    auto spill_func = [query_id, state, this] {
        SCOPED_TIMER(_spill_probe_timer);

        size_t not_revoked_size = 0;
        auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
        for (uint32_t partition_index = 0; partition_index != p._partition_count;
             ++partition_index) {
            auto& blocks = _probe_blocks[partition_index];
            auto& partitioned_block = _partitioned_blocks[partition_index];
            if (partitioned_block) {
                const auto size = partitioned_block->allocated_bytes();
                if (size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                    blocks.emplace_back(partitioned_block->to_block());
                    partitioned_block.reset();
                } else {
                    not_revoked_size += size;
                }
            }

            if (blocks.empty()) {
                continue;
            }

            auto& spilling_stream = _probe_spilling_streams[partition_index];
            if (!spilling_stream) {
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, spilling_stream, print_id(state->query_id()), "hash_probe",
                        _parent->node_id(), std::numeric_limits<int32_t>::max(),
                        std::numeric_limits<size_t>::max(), operator_profile()));
            }

            auto merged_block = vectorized::MutableBlock::create_unique(std::move(blocks.back()));
            blocks.pop_back();

            while (!blocks.empty() && !state->is_cancelled()) {
                auto block = std::move(blocks.back());
                blocks.pop_back();

                RETURN_IF_ERROR(merged_block->merge(std::move(block)));
                DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks", {
                    return Status::Error<INTERNAL_ERROR>(
                            "fault_inject partitioned_hash_join_probe "
                            "spill_probe_blocks failed");
                });
            }

            if (!merged_block->empty()) [[likely]] {
                COUNTER_UPDATE(_spill_probe_rows, merged_block->rows());
                RETURN_IF_ERROR(
                        spilling_stream->spill_block(state, merged_block->to_block(), false));
                COUNTER_UPDATE(_spill_probe_blocks, 1);
            }
        }

        COUNTER_SET(_probe_blocks_bytes, int64_t(not_revoked_size));

        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " spill_probe_blocks done",
                print_id(query_id), p.node_id(), state->task_id());
        return Status::OK();
    };

    auto exception_catch_func = [query_id, spill_func]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "spill_probe_blocks canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, status);
            return status;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        return status;
    };

    _spill_dependency->block();
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe spill_probe_blocks "
                "submit_func failed");
    });

    auto spill_runnable = std::make_shared<SpillNonSinkRunnable>(
            state, _spill_dependency, operator_profile(), _shared_state->shared_from_this(),
            exception_catch_func);
    return spill_io_pool->submit(std::move(spill_runnable));
}

Status PartitionedHashJoinProbeLocalState::finish_spilling(uint32_t partition_index) {
    auto& probe_spilling_stream = _probe_spilling_streams[partition_index];

    if (probe_spilling_stream) {
        RETURN_IF_ERROR(probe_spilling_stream->spill_eof());
        probe_spilling_stream->set_read_counters(operator_profile());
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_disk(RuntimeState* state,
                                                                          uint32_t partition_index,
                                                                          bool& has_data) {
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " partition:{}, recover_build_blocks_from_disk",
            print_id(state->query_id()), _parent->node_id(), state->task_id(), partition_index);
    auto& spilled_stream = _shared_state->spilled_streams[partition_index];
    has_data = false;
    if (!spilled_stream) {
        return Status::OK();
    }
    spilled_stream->set_read_counters(operator_profile());

    auto query_id = state->query_id();

    auto read_func = [this, query_id, state, spilled_stream = spilled_stream, partition_index] {
        SCOPED_TIMER(_recovery_build_timer);

        bool eos = false;
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, recoverying build data",
                print_id(state->query_id()), _parent->node_id(), state->task_id(), partition_index);
        Status status;
        while (!eos) {
            vectorized::Block block;
            DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks", {
                status = Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_probe "
                        "recover_build_blocks failed");
            });
            if (status.ok()) {
                status = spilled_stream->read_next_block_sync(&block, &eos);
            }
            if (!status.ok()) {
                break;
            }
            COUNTER_UPDATE(_recovery_build_rows, block.rows());
            COUNTER_UPDATE(_recovery_build_blocks, 1);

            if (block.empty()) {
                continue;
            }

            if (UNLIKELY(state->is_cancelled())) {
                LOG(INFO) << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, recovery build data canceled",
                        print_id(state->query_id()), _parent->node_id(), state->task_id(),
                        partition_index);
                break;
            }

            if (!_recovered_build_block) {
                _recovered_build_block = vectorized::MutableBlock::create_unique(std::move(block));
            } else {
                DCHECK_EQ(_recovered_build_block->columns(), block.columns());
                status = _recovered_build_block->merge(std::move(block));
                if (!status.ok()) {
                    break;
                }
            }

            if (_recovered_build_block->allocated_bytes() >=
                vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }

        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            _shared_state->spilled_streams[partition_index].reset();
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery build data eos",
                    print_id(state->query_id()), _parent->node_id(), state->task_id(),
                    partition_index);
        }
        return status;
    };

    auto exception_catch_func = [read_func, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_build_blocks canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    auto* spill_io_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    has_data = true;
    _spill_dependency->block();

    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_build_blocks submit_func failed");
                    });

    auto spill_runnable = std::make_shared<SpillRecoverRunnable>(
            state, _spill_dependency, operator_profile(), _shared_state->shared_from_this(),
            exception_catch_func);
    return spill_io_pool->submit(std::move(spill_runnable));
}

std::string PartitionedHashJoinProbeLocalState::debug_string(int indentation_level) const {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    bool need_more_input_data;
    if (_shared_state->need_to_spill) {
        need_more_input_data = !_child_eos;
    } else if (_shared_state->inner_runtime_state) {
        need_more_input_data = p._inner_probe_operator->need_more_input_data(
                _shared_state->inner_runtime_state.get());
    } else {
        need_more_input_data = true;
    }
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, short_circuit_for_probe: {}, need_to_spill: {}, child_eos: {}, "
                   "_shared_state->inner_runtime_state: {}, need_more_input_data: {}",
                   PipelineXSpillLocalState<PartitionedHashJoinSharedState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL",
                   _shared_state->need_to_spill, _child_eos,
                   _shared_state->inner_runtime_state != nullptr, need_more_input_data);
    return fmt::to_string(debug_string_buffer);
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_disk(RuntimeState* state,
                                                                          uint32_t partition_index,
                                                                          bool& has_data) {
    auto& spilled_stream = _probe_spilling_streams[partition_index];
    has_data = false;
    if (!spilled_stream) {
        return Status::OK();
    }

    spilled_stream->set_read_counters(operator_profile());
    auto& blocks = _probe_blocks[partition_index];

    auto query_id = state->query_id();

    auto read_func = [this, query_id, partition_index, &spilled_stream, &blocks] {
        SCOPED_TIMER(_recovery_probe_timer);

        vectorized::Block block;
        bool eos = false;
        Status st;
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_probe recover_probe_blocks failed");
        });

        size_t read_size = 0;
        while (!eos && !_state->is_cancelled() && st.ok()) {
            st = spilled_stream->read_next_block_sync(&block, &eos);
            if (!st.ok()) {
                break;
            } else if (!block.empty()) {
                COUNTER_UPDATE(_recovery_probe_rows, block.rows());
                COUNTER_UPDATE(_recovery_probe_blocks, 1);
                read_size += block.allocated_bytes();
                blocks.emplace_back(std::move(block));
            }

            if (read_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }
        if (eos) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery probe data done",
                    print_id(query_id), _parent->node_id(), _state->task_id(), partition_index);
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            spilled_stream.reset();
        }
        return st;
    };

    auto exception_catch_func = [read_func, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_probe_blocks canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    auto* spill_io_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    DCHECK(spill_io_pool != nullptr);
    _spill_dependency->block();
    has_data = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_probe_blocks submit_func failed");
                    });
    return spill_io_pool->submit(std::make_shared<SpillRecoverRunnable>(
            state, _spill_dependency, operator_profile(), _shared_state->shared_from_this(),
            exception_catch_func));
}

PartitionedHashJoinProbeOperatorX::PartitionedHashJoinProbeOperatorX(ObjectPool* pool,
                                                                     const TPlanNode& tnode,
                                                                     int operator_id,
                                                                     const DescriptorTbl& descs,
                                                                     uint32_t partition_count)
        : JoinProbeOperatorX<PartitionedHashJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[0]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {}

Status PartitionedHashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX::init(tnode, state));
    _op_name = "PARTITIONED_HASH_JOIN_PROBE_OPERATOR";
    auto tnode_ = _tnode;
    tnode_.runtime_filters.clear();

    for (const auto& conjunct : tnode.hash_join_node.eq_join_conjuncts) {
        _probe_exprs.emplace_back(conjunct.left);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_probe_exprs));

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::prepare(RuntimeState* state) {
    // to avoid open _child twice
    auto child = std::move(_child);
    RETURN_IF_ERROR(JoinProbeOperatorX::prepare(state));
    RETURN_IF_ERROR(_inner_probe_operator->set_child(child));
    DCHECK(_build_side_child != nullptr);
    _inner_probe_operator->set_build_side_child(_build_side_child);
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_build_side_child));
    RETURN_IF_ERROR(_inner_probe_operator->prepare(state));
    RETURN_IF_ERROR(_inner_sink_operator->prepare(state));
    _child = std::move(child);
    RETURN_IF_ERROR(_partitioner->prepare(state, _child->row_desc()));
    RETURN_IF_ERROR(_partitioner->open(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                               bool eos) const {
    auto& local_state = get_local_state(state);
    const auto rows = input_block->rows();
    auto& partitioned_blocks = local_state._partitioned_blocks;
    if (rows == 0) {
        if (eos) {
            for (uint32_t i = 0; i != _partition_count; ++i) {
                if (partitioned_blocks[i] && !partitioned_blocks[i]->empty()) {
                    local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
                    partitioned_blocks[i].reset();
                }
            }
        }
        return Status::OK();
    }
    {
        SCOPED_TIMER(local_state._partition_timer);
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block));
    }

    std::vector<std::vector<uint32_t>> partition_indexes(_partition_count);
    const auto* channel_ids = local_state._partitioner->get_channel_ids().get<uint32_t>();
    for (uint32_t i = 0; i != rows; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    SCOPED_TIMER(local_state._partition_shuffle_timer);
    int64_t bytes_of_blocks = 0;
    for (uint32_t i = 0; i != _partition_count; ++i) {
        const auto count = partition_indexes[i].size();
        if (UNLIKELY(count == 0)) {
            continue;
        }

        if (!partitioned_blocks[i]) {
            partitioned_blocks[i] =
                    vectorized::MutableBlock::create_unique(input_block->clone_empty());
        }
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(input_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));

        if (partitioned_blocks[i]->rows() > 2 * 1024 * 1024 ||
            (eos && partitioned_blocks[i]->rows() > 0)) {
            local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
            partitioned_blocks[i].reset();
        } else {
            bytes_of_blocks += partitioned_blocks[i]->allocated_bytes();
        }

        for (auto& block : local_state._probe_blocks[i]) {
            bytes_of_blocks += block.allocated_bytes();
        }
    }

    COUNTER_SET(local_state._probe_blocks_bytes, bytes_of_blocks);

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    local_state._shared_state->inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._shared_state->inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._shared_state->inner_runtime_state->set_be_number(state->be_number());

    local_state._shared_state->inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._shared_state->inner_runtime_state->resize_op_id_to_local_state(-1);
    local_state._shared_state->inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    // set sink local state
    LocalSinkStateInfo info {0,  local_state._internal_runtime_profile.get(),
                             -1, local_state._in_mem_shared_state_sptr.get(),
                             {}, {}};
    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), info));

    LocalStateInfo state_info {local_state._internal_runtime_profile.get(),
                               {},
                               local_state._in_mem_shared_state_sptr.get(),
                               {},
                               0};
    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), state_info));

    auto* sink_local_state = local_state._shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state = local_state._shared_state->inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    auto& partitioned_block =
            local_state._shared_state->partitioned_build_blocks[local_state._partition_cursor];
    vectorized::Block block;
    if (partitioned_block && partitioned_block->rows() > 0) {
        block = partitioned_block->to_block();
        partitioned_block.reset();
    }
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::sink", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe sink failed");
    });

    RETURN_IF_ERROR(_inner_sink_operator->sink(local_state._shared_state->inner_runtime_state.get(),
                                               &block, true));
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " internal build operator finished, partition:{}, rows:{}, memory usage:{}",
            print_id(state->query_id()), node_id(), state->task_id(), local_state._partition_cursor,
            block.rows(),
            _inner_sink_operator->get_memory_usage(
                    local_state._shared_state->inner_runtime_state.get()));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state,
                                               vectorized::Block* output_block, bool* eos) const {
    auto& local_state = get_local_state(state);

    const auto partition_index = local_state._partition_cursor;
    auto& probe_blocks = local_state._probe_blocks[partition_index];

    if (local_state._recovered_build_block && !local_state._recovered_build_block->empty()) {
        local_state._estimate_memory_usage += local_state._recovered_build_block->allocated_bytes();
        auto& mutable_block = local_state._shared_state->partitioned_build_blocks[partition_index];
        if (!mutable_block) {
            mutable_block = std::move(local_state._recovered_build_block);
        } else {
            RETURN_IF_ERROR(mutable_block->merge(local_state._recovered_build_block->to_block()));
            local_state._recovered_build_block.reset();
        }
    }

    if (local_state._need_to_setup_internal_operators) {
        bool has_data = false;
        RETURN_IF_ERROR(local_state.recover_build_blocks_from_disk(
                state, local_state._partition_cursor, has_data));
        if (has_data) {
            return Status::OK();
        }

        *eos = false;
        RETURN_IF_ERROR(local_state.finish_spilling(partition_index));
        RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
        local_state._need_to_setup_internal_operators = false;
        auto& mutable_block = local_state._partitioned_blocks[partition_index];
        if (mutable_block && !mutable_block->empty()) {
            probe_blocks.emplace_back(mutable_block->to_block());
        }
    }
    bool in_mem_eos = false;
    auto* runtime_state = local_state._shared_state->inner_runtime_state.get();
    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            *eos = false;
            bool has_data = false;
            RETURN_IF_ERROR(
                    local_state.recover_probe_blocks_from_disk(state, partition_index, has_data));
            if (!has_data) {
                vectorized::Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, has no data to recovery",
                        print_id(state->query_id()), node_id(), state->task_id(), partition_index);
                break;
            } else {
                return Status::OK();
            }
        }

        auto block = std::move(probe_blocks.back());
        probe_blocks.pop_back();
        if (!block.empty()) {
            RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, false));
        }
    }

    RETURN_IF_ERROR(_inner_probe_operator->pull(
            local_state._shared_state->inner_runtime_state.get(), output_block, &in_mem_eos));

    *eos = false;
    if (in_mem_eos) {
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, probe done",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._partition_cursor);
        local_state._partition_cursor++;
        local_state.update_profile_from_inner();
        if (local_state._partition_cursor == _partition_count) {
            *eos = true;
        } else {
            local_state._need_to_setup_internal_operators = true;
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->need_to_spill) {
        return !local_state._child_eos;
    } else if (local_state._shared_state->inner_runtime_state) {
        return _inner_probe_operator->need_more_input_data(
                local_state._shared_state->inner_runtime_state.get());
    } else {
        return true;
    }
}

size_t PartitionedHashJoinProbeOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._child_eos) {
        return 0;
    }

    auto revocable_size = _revocable_mem_size(state, true);
    if (_child) {
        revocable_size += _child->revocable_mem_size(state);
    }
    return revocable_size;
}

size_t PartitionedHashJoinProbeOperatorX::_revocable_mem_size(RuntimeState* state,
                                                              bool force) const {
    const auto spill_size_threshold = force ? vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM
                                            : vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
    auto& local_state = get_local_state(state);
    size_t mem_size = 0;
    auto& probe_blocks = local_state._probe_blocks;
    for (uint32_t i = 0; i < _partition_count; ++i) {
        for (auto& block : probe_blocks[i]) {
            mem_size += block.allocated_bytes();
        }

        auto& partitioned_block = local_state._partitioned_blocks[i];
        if (partitioned_block) {
            auto block_bytes = partitioned_block->allocated_bytes();
            if (block_bytes >= spill_size_threshold) {
                mem_size += block_bytes;
            }
        }
    }
    return mem_size;
}

size_t PartitionedHashJoinProbeOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    const auto need_to_spill = local_state._shared_state->need_to_spill;
    if (!need_to_spill || local_state._child_eos) {
        return Base::get_reserve_mem_size(state);
    }

    size_t size_to_reserve = vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;

    if (local_state._need_to_setup_internal_operators) {
        const size_t rows =
                (local_state._recovered_build_block ? local_state._recovered_build_block->rows()
                                                    : 0) +
                state->batch_size();
        size_t bucket_size = JoinHashTable<StringRef>::calc_bucket_size(rows);

        size_to_reserve += bucket_size * sizeof(uint32_t); // JoinHashTable::first
        size_to_reserve += rows * sizeof(uint32_t);        // JoinHashTable::next

        if (_join_op == TJoinOp::FULL_OUTER_JOIN || _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
            _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN) {
            size_to_reserve += rows * sizeof(uint8_t); // JoinHashTable::visited
        }
    }

    COUNTER_SET(local_state._memory_usage_reserved, int64_t(size_to_reserve));
    return size_to_reserve;
}

Status PartitionedHashJoinProbeOperatorX::_revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    VLOG_DEBUG << fmt::format("Query:{}, hash join probe:{}, task:{}, revoke_memory",
                              print_id(state->query_id()), node_id(), state->task_id());

    RETURN_IF_ERROR(local_state.spill_probe_blocks(state));
    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::_should_revoke_memory(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->need_to_spill) {
        const auto revocable_size = _revocable_mem_size(state);

        if (local_state.low_memory_mode()) {
            return revocable_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM;
        } else {
            return revocable_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
        }
    }
    return false;
}

Status PartitionedHashJoinProbeOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                    bool* eos) {
    *eos = false;
    auto& local_state = get_local_state(state);
    local_state.copy_shared_spill_profile();
    const auto need_to_spill = local_state._shared_state->need_to_spill;
#ifndef NDEBUG
    Defer eos_check_defer([&] {
        if (*eos) {
            LOG(INFO) << fmt::format(
                    "Query:{}, hash join probe:{}, task:{}, child eos:{}, need spill:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._child_eos, need_to_spill);
        }
    });
#endif

    Defer defer([&]() {
        COUNTER_SET(local_state._memory_usage_reserved,
                    int64_t(local_state.estimate_memory_usage()));
    });

    if (need_more_input_data(state)) {
        {
            SCOPED_TIMER(local_state._get_child_next_timer);
            RETURN_IF_ERROR(_child->get_block_after_projects(state, local_state._child_block.get(),
                                                             &local_state._child_eos));
        }

        SCOPED_TIMER(local_state.exec_time_counter());
        if (local_state._child_block->rows() == 0 && !local_state._child_eos) {
            return Status::OK();
        }

        Defer clear_defer([&] { local_state._child_block->clear_column_data(); });
        if (need_to_spill) {
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
            if (_should_revoke_memory(state)) {
                return _revoke_memory(state);
            }
        } else {
            DCHECK(local_state._shared_state->inner_runtime_state);
            RETURN_IF_ERROR(_inner_probe_operator->push(
                    local_state._shared_state->inner_runtime_state.get(),
                    local_state._child_block.get(), local_state._child_eos));
        }
    }

    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
        if (need_to_spill) {
            RETURN_IF_ERROR(pull(state, block, eos));
        } else {
            RETURN_IF_ERROR(_inner_probe_operator->pull(
                    local_state._shared_state->inner_runtime_state.get(), block, eos));
            local_state.update_profile_from_inner();
        }

        local_state.add_num_rows_returned(block->rows());
        COUNTER_UPDATE(local_state._blocks_returned_counter, 1);
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
