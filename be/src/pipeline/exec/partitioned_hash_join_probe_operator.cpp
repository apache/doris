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
#include <array>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/runtime_profile.h"
#include "vec/common/hash_table/join_hash_table.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
namespace {
// Reuse the shared hierarchical spill helpers (see `hierarchical_spill_partition.h`) so both
// hash join and aggregation share identical partition ID encoding and bit-slicing behavior.

HashJoinSpillPartition& get_or_create_partition(HashJoinSpillPartitionMap& partitions,
                                                const HashJoinSpillPartitionId& id) {
    auto [it, inserted] = partitions.try_emplace(id.key());
    if (inserted) {
        it->second.id = id;
    }
    return it->second;
}

HashJoinSpillBuildPartition& get_or_create_build_partition(
        HashJoinSpillBuildPartitionMap& partitions, const HashJoinSpillPartitionId& id) {
    auto [it, inserted] = partitions.try_emplace(id.key());
    if (inserted) {
        it->second.id = id;
    }
    return it->second;
}

HashJoinSpillPartitionId find_partition_for_hash(
        uint32_t hash, const HashJoinSpillBuildPartitionMap& build_partitions) {
    // Follow build-side split hierarchy so probe rows land in the final partition.
    HashJoinSpillPartitionId id {0, spill_partition_index(hash, 0)};
    auto it = build_partitions.find(id.key());
    while (it != build_partitions.end() && it->second.is_split &&
           id.level < kHashJoinSpillMaxDepth) {
        const auto child_index = spill_partition_index(hash, id.level + 1);
        id = id.child(child_index);
        it = build_partitions.find(id.key());
    }
    return id;
}

} // namespace

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : PipelineXSpillLocalState(state, parent),
          _child_block(vectorized::Block::create_unique()) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::init(state, info));
    init_spill_write_counters();

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _pending_partitions.clear();
    _has_current_partition = false;
    _current_partition_id = {};
    auto& partitions = _shared_state->probe_partitions;
    partitions.clear();
    auto& build_partitions = _shared_state->build_partitions;
    build_partitions.clear();
    for (uint32_t i = 0; i < p._partition_count; ++i) {
        HashJoinSpillPartitionId id {0, i};
        get_or_create_partition(partitions, id);
        get_or_create_build_partition(build_partitions, id);
    }
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

    _probe_partition_splits =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "ProbePartitionSplits", TUnit::UNIT, 1);
    _build_partition_splits =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "BuildPartitionSplits", TUnit::UNIT, 1);
    _handled_partition_count =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "HandledPartitionCount", TUnit::UNIT, 1);
    _max_partition_level =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MaxPartitionLevel", TUnit::UNIT, 1);
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
        if (_shared_state->is_spilled) {
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
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    RETURN_IF_ERROR(p._partitioner->clone(state, _partitioner));
    if (p._build_partitioner) {
        RETURN_IF_ERROR(p._build_partitioner->clone(state, _build_partitioner));
    }
    return Status::OK();
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
    auto query_id = state->query_id();

    auto spill_func = [query_id, state, this] {
        SCOPED_TIMER(_spill_probe_timer);

        size_t not_revoked_size = 0;
        size_t total_revoked_size = 0;
        auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
        auto spill_partition = [&](HashJoinSpillPartition& partition) -> Status {
            auto& blocks = partition.blocks;
            auto& accumulating_block = partition.accumulating_block;
            auto& spilling_stream = partition.spill_stream;

            if (accumulating_block) {
                const auto size = accumulating_block->allocated_bytes();
                if (size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                    blocks.emplace_back(accumulating_block->to_block());
                    accumulating_block.reset();
                } else {
                    not_revoked_size += size;
                }
            }

            if (blocks.empty()) {
                // Update in_mem_bytes for remaining data
                partition.in_mem_bytes =
                        accumulating_block ? accumulating_block->allocated_bytes() : 0;
                return Status::OK();
            }

            if (!spilling_stream) {
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, spilling_stream, print_id(state->query_id()), "hash_probe",
                        _parent->node_id(), std::numeric_limits<int32_t>::max(),
                        std::numeric_limits<size_t>::max(), operator_profile()));
            }

            if (spilling_stream->ready_for_reading()) {
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " spill_probe_blocks return(ready_for_reading).",
                        print_id(query_id), p.node_id(), state->task_id());
                return Status::OK();
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
                size_t spill_bytes = merged_block->allocated_bytes();
                COUNTER_UPDATE(_spill_probe_rows, merged_block->rows());
                RETURN_IF_ERROR(
                        spilling_stream->spill_block(state, merged_block->to_block(), false));
                COUNTER_UPDATE(_spill_probe_blocks, 1);
                // Track spilled bytes
                partition.spilled_bytes += spill_bytes;
                total_revoked_size += spill_bytes;
            }
            // Update in_mem_bytes after spilling
            partition.in_mem_bytes = accumulating_block ? accumulating_block->allocated_bytes() : 0;
            return Status::OK();
        };

        // Spill all probe partitions from unified storage
        auto& partitions = _shared_state->probe_partitions;
        for (auto& [_, partition] : partitions) {
            RETURN_IF_ERROR(spill_partition(partition));
        }

        COUNTER_SET(_probe_blocks_bytes, int64_t(not_revoked_size));

        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " spill_probe_blocks done, total_revoked_size: {}, not_revoked_size: {}",
                print_id(query_id), p.node_id(), state->task_id(), total_revoked_size,
                not_revoked_size);
        return Status::OK();
    };

    auto exception_catch_func = [query_id, state, spill_func]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "spill_probe_blocks canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe spill_probe_blocks "
                "submit_func failed");
    });

    SpillNonSinkRunnable spill_runnable(state, operator_profile(), exception_catch_func);
    return spill_runnable.run();
}

Status PartitionedHashJoinProbeLocalState::finish_spilling(uint32_t partition_index) {
    return finish_spilling(HashJoinSpillPartitionId {.level = 0, .path = partition_index});
}

Status PartitionedHashJoinProbeLocalState::finish_spilling(
        const HashJoinSpillPartitionId& partition_id) {
    // Always read from probe_partitions
    vectorized::SpillStreamSPtr probe_spilling_stream;
    auto& partitions = _shared_state->probe_partitions;
    auto it = partitions.find(partition_id.key());
    if (it != partitions.end()) {
        probe_spilling_stream = it->second.spill_stream;
    }

    if (probe_spilling_stream && !probe_spilling_stream->ready_for_reading()) {
        RETURN_IF_ERROR(probe_spilling_stream->spill_eof());
        probe_spilling_stream->set_read_counters(operator_profile());
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_disk(RuntimeState* state,
                                                                          uint32_t partition_index,
                                                                          bool& has_data) {
    return recover_build_blocks_from_disk(
            state, HashJoinSpillPartitionId {.level = 0, .path = partition_index}, has_data);
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_disk(
        RuntimeState* state, const HashJoinSpillPartitionId& partition_id, bool& has_data) {
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " partition:{}, recover_build_blocks_from_disk",
            print_id(state->query_id()), _parent->node_id(), state->task_id(),
            base_partition_index(partition_id));
    // Always read from build_partitions.
    vectorized::SpillStreamSPtr* spilled_stream = nullptr;
    auto it = _shared_state->build_partitions.find(partition_id.key());
    if (it != _shared_state->build_partitions.end()) {
        spilled_stream = &it->second.spill_stream;
    }
    has_data = false;
    if (!spilled_stream || !*spilled_stream) {
        return Status::OK();
    }
    (*spilled_stream)->set_read_counters(operator_profile());

    auto query_id = state->query_id();

    auto read_func = [this, query_id, state, spilled_stream, partition_id] {
        SCOPED_TIMER(_recovery_build_timer);

        bool eos = false;
        size_t read_limit =
                static_cast<size_t>(std::max<int64_t>(state->spill_recover_max_read_bytes(), 1));
        read_limit = std::min(read_limit, vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, recoverying build data",
                print_id(state->query_id()), _parent->node_id(), state->task_id(),
                base_partition_index(partition_id));
        Status status;
        while (!eos) {
            // 内存检查：如果内存不足，提前退出循环，外层会保持 has_data = true
            auto* mem_dep = state->get_query_ctx()->get_memory_sufficient_dependency();
            if (mem_dep && !mem_dep->ready()) {
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, memory not sufficient, pause recovery",
                        print_id(state->query_id()), _parent->node_id(), state->task_id(),
                        base_partition_index(partition_id));
                break;
            }

            vectorized::Block block;
            DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks", {
                status = Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_probe "
                        "recover_build_blocks failed");
            });
            if (status.ok()) {
                status = (*spilled_stream)->read_next_block_sync(&block, &eos);
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
                        base_partition_index(partition_id));
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

            if (_recovered_build_block->allocated_bytes() >= read_limit) {
                break;
            }
        }

        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(*spilled_stream);
            spilled_stream->reset();
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery build data eos",
                    print_id(state->query_id()), _parent->node_id(), state->task_id(),
                    base_partition_index(partition_id));
        }
        return status;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_build_blocks canceled");

            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    has_data = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_build_blocks submit_func failed");
                    });

    SpillRecoverRunnable spill_runnable(state, operator_profile(), exception_catch_func);
    return spill_runnable.run();
}

std::string PartitionedHashJoinProbeLocalState::debug_string(int indentation_level) const {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    bool need_more_input_data;
    if (_shared_state->is_spilled) {
        need_more_input_data = !_child_eos;
    } else if (_shared_state->inner_runtime_state) {
        need_more_input_data = p._inner_probe_operator->need_more_input_data(
                _shared_state->inner_runtime_state.get());
    } else {
        need_more_input_data = true;
    }
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, short_circuit_for_probe: {}, is_spilled: {}, child_eos: {}, "
                   "_shared_state->inner_runtime_state: {}, need_more_input_data: {}",
                   PipelineXSpillLocalState<PartitionedHashJoinSharedState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL",
                   _shared_state->is_spilled, _child_eos,
                   _shared_state->inner_runtime_state != nullptr, need_more_input_data);
    return fmt::to_string(debug_string_buffer);
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_disk(RuntimeState* state,
                                                                          uint32_t partition_index,
                                                                          bool& has_data) {
    return recover_probe_blocks_from_disk(state, HashJoinSpillPartitionId {0, partition_index},
                                          has_data);
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_disk(
        RuntimeState* state, const HashJoinSpillPartitionId& partition_id, bool& has_data) {
    // Always read from probe_partitions
    vectorized::SpillStreamSPtr* spilled_stream = nullptr;
    std::vector<vectorized::Block>* blocks = nullptr;
    auto& partitions = _shared_state->probe_partitions;
    auto it = partitions.find(partition_id.key());
    if (it != partitions.end()) {
        spilled_stream = &it->second.spill_stream;
        blocks = &it->second.blocks;
    }

    has_data = false;
    if (!spilled_stream || !*spilled_stream || !blocks) {
        return Status::OK();
    }

    (*spilled_stream)->set_read_counters(operator_profile());

    auto query_id = state->query_id();

    auto read_func = [this, query_id, partition_id, spilled_stream, blocks] {
        SCOPED_TIMER(_recovery_probe_timer);

        vectorized::Block block;
        bool eos = false;
        Status st;
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_probe recover_probe_blocks failed");
        });

        size_t read_size = 0;
        size_t read_limit =
                static_cast<size_t>(std::max<int64_t>(_state->spill_recover_max_read_bytes(), 1));
        read_limit = std::min(read_limit, vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);
        while (!eos && !_state->is_cancelled() && st.ok()) {
            st = (*spilled_stream)->read_next_block_sync(&block, &eos);
            if (!st.ok()) {
                break;
            } else if (!block.empty()) {
                COUNTER_UPDATE(_recovery_probe_rows, block.rows());
                COUNTER_UPDATE(_recovery_probe_blocks, 1);
                read_size += block.allocated_bytes();
                blocks->emplace_back(std::move(block));
            }

            if (read_size >= read_limit) {
                break;
            }
        }
        if (eos) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery probe data done",
                    print_id(query_id), _parent->node_id(), _state->task_id(),
                    base_partition_index(partition_id));
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(*spilled_stream);
            spilled_stream->reset();
        }
        return st;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_probe_blocks canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    has_data = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_probe_blocks submit_func failed");
                    });
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

bool PartitionedHashJoinProbeLocalState::is_blockable() const {
    return _shared_state->is_spilled;
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
          _partition_count(kHashJoinSpillFanout) {}

Status PartitionedHashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX::init(tnode, state));
    _op_name = "PARTITIONED_HASH_JOIN_PROBE_OPERATOR";
    DCHECK_EQ(_partition_count, kHashJoinSpillFanout);
    auto tnode_ = _tnode;
    tnode_.runtime_filters.clear();

    for (const auto& conjunct : tnode.hash_join_node.eq_join_conjuncts) {
        _probe_exprs.emplace_back(conjunct.left);
        _build_exprs.emplace_back(conjunct.right);
    }
    _partitioner = std::make_unique<SpillHashPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_probe_exprs));
    _build_partitioner = std::make_unique<SpillHashPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_build_partitioner->init(_build_exprs));

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
    if (_build_partitioner) {
        RETURN_IF_ERROR(_build_partitioner->prepare(state, _build_side_child->row_desc()));
        RETURN_IF_ERROR(_build_partitioner->open(state));
    }
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                               bool eos) const {
    auto& local_state = get_local_state(state);
    const auto rows = input_block->rows();
    if (rows == 0) {
        if (eos) {
            // Flush all accumulating blocks from unified probe_partitions storage
            auto& partitions = local_state._shared_state->probe_partitions;
            for (auto& [_, partition] : partitions) {
                if (partition.accumulating_block && !partition.accumulating_block->empty()) {
                    partition.blocks.emplace_back(partition.accumulating_block->to_block());
                    partition.accumulating_block.reset();
                }
            }
        }
        return Status::OK();
    }
    {
        SCOPED_TIMER(local_state._partition_timer);
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block));
    }

    const auto& channel_ids = local_state._partitioner->get_channel_ids();
    // Build-side split decisions determine the final probe partition target.
    auto& build_partitions = local_state._shared_state->build_partitions;
    struct PartitionRowIndexes {
        HashJoinSpillPartitionId id;
        std::vector<uint32_t> row_indexes;
    };
    // Group rows by final partition (base or split child).
    std::unordered_map<uint32_t, PartitionRowIndexes> partition_indexes;
    partition_indexes.reserve(kHashJoinSpillFanout);
    for (uint32_t i = 0; i != rows; ++i) {
        auto id = find_partition_for_hash(channel_ids[i], build_partitions);
        auto [it, inserted] = partition_indexes.try_emplace(
                id.key(), PartitionRowIndexes {.id = id, .row_indexes = {}});
        it->second.row_indexes.emplace_back(i);
    }
    CHECK_LE(partition_indexes.size(), kHashJoinSpillFanout);

    SCOPED_TIMER(local_state._partition_shuffle_timer);
    auto append_rows = [&](std::unique_ptr<vectorized::MutableBlock>& accumulating_block,
                           std::vector<vectorized::Block>& blocks,
                           const std::vector<uint32_t>& row_indexes) -> Status {
        if (row_indexes.empty()) {
            return Status::OK();
        }
        if (!accumulating_block) {
            accumulating_block =
                    vectorized::MutableBlock::create_unique(input_block->clone_empty());
        }
        RETURN_IF_ERROR(accumulating_block->add_rows(input_block, row_indexes.data(),
                                                     row_indexes.data() + row_indexes.size()));
        if (accumulating_block->rows() > 2 * 1024 * 1024 ||
            (eos && accumulating_block->rows() > 0)) {
            blocks.emplace_back(accumulating_block->to_block());
            accumulating_block.reset();
        }
        return Status::OK();
    };

    auto& partitions = local_state._shared_state->probe_partitions;
    for (auto& [_, entry] : partition_indexes) {
        // Always write to probe_partitions for both base and split levels
        auto& partition = get_or_create_partition(partitions, entry.id);
        RETURN_IF_ERROR(
                append_rows(partition.accumulating_block, partition.blocks, entry.row_indexes));
        // Update in_mem_bytes for this partition
        partition.in_mem_bytes = 0;
        if (partition.accumulating_block) {
            partition.in_mem_bytes += partition.accumulating_block->allocated_bytes();
        }
        for (const auto& block : partition.blocks) {
            partition.in_mem_bytes += block.allocated_bytes();
        }
    }

    // Calculate bytes from unified probe_partitions storage
    int64_t bytes_of_blocks = 0;
    for (auto& [_, partition] : partitions) {
        if (partition.accumulating_block) {
            bytes_of_blocks += partition.accumulating_block->allocated_bytes();
        }
        for (auto& block : partition.blocks) {
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
    local_state._shared_state->inner_runtime_state->resize_op_id_to_local_state(-2);
    local_state._shared_state->inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    // set sink local state
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = local_state._internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = local_state._in_mem_shared_state_sptr.get(),
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), info));

    LocalStateInfo state_info {.parent_profile = local_state._internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = local_state._in_mem_shared_state_sptr.get(),
                               .shared_state_map = {},
                               .task_idx = 0};
    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), state_info));

    auto* sink_local_state = local_state._shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state = local_state._shared_state->inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    const auto partition_index = base_partition_index(local_state._current_partition_id);
    vectorized::Block block;
    // Always read from build_partitions for both base and split levels.
    auto& build_partition = get_or_create_build_partition(
            local_state._shared_state->build_partitions, local_state._current_partition_id);
    if (build_partition.build_block && build_partition.build_block->rows() > 0) {
        block = build_partition.build_block->to_block();
        build_partition.build_block.reset();
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
            print_id(state->query_id()), node_id(), state->task_id(), partition_index, block.rows(),
            _inner_sink_operator->get_memory_usage(
                    local_state._shared_state->inner_runtime_state.get()));
    RETURN_IF_ERROR(_inner_sink_operator->close(
            local_state._shared_state->inner_runtime_state.get(), Status::OK()));
    return Status::OK();
}

size_t PartitionedHashJoinProbeOperatorX::_build_partition_bytes(
        const PartitionedHashJoinProbeLocalState& local_state,
        const HashJoinSpillPartitionId& partition_id) const {
    // Always read from build_partitions for both base and split levels.
    auto& partitions = local_state._shared_state->build_partitions;
    auto it = partitions.find(partition_id.key());
    if (it == partitions.end()) {
        return 0;
    }
    return it->second.total_bytes();
}

size_t PartitionedHashJoinProbeOperatorX::_build_partition_rows(
        const PartitionedHashJoinProbeLocalState& local_state,
        const HashJoinSpillPartitionId& partition_id) const {
    auto& partitions = local_state._shared_state->build_partitions;
    auto it = partitions.find(partition_id.key());
    if (it == partitions.end()) {
        return 0;
    }
    return it->second.row_count;
}

Status PartitionedHashJoinProbeOperatorX::_maybe_split_build_partition(
        RuntimeState* state, PartitionedHashJoinProbeLocalState& local_state) const {
    if (!local_state._shared_state->is_spilled) {
        return Status::OK();
    }

    const auto& partition_id = local_state._current_partition_id;
    if (partition_id.level >= kHashJoinSpillMaxDepth) {
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{}, partition level {} >= max depth, skip "
                "split",
                print_id(state->query_id()), node_id(), state->task_id(), partition_id.level);
        return Status::OK();
    }

    auto& build_partitions = local_state._shared_state->build_partitions;
    auto& build_partition = get_or_create_build_partition(build_partitions, partition_id);
    if (build_partition.is_split) {
        // This should not happen in normal flow - a split partition should not be
        // selected again because:
        // 1. For level-0: build_partition.is_split prevents re-selection
        // 2. For child partitions: they are popped from pending queue after processing

        // Children should already be enqueued in pending queue when the build partition was split.
        DCHECK(false) << "Unexpected: selected a partition that was already split. "
                      << "partition_id: level=" << partition_id.level
                      << ", path=" << partition_id.path;
        local_state._has_current_partition = false;
        return Status::OK();
    }

    const auto bytes = _build_partition_bytes(local_state, partition_id);
    if (bytes >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
        RETURN_IF_ERROR(_split_build_partition(state, local_state, partition_id));
        RETURN_IF_ERROR(local_state.finish_spilling(partition_id));
        RETURN_IF_ERROR(_split_probe_partition(state, local_state, partition_id));
        local_state._has_current_partition = false;
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{}, bytes: {} partition level: {}, path: "
                "{} "
                "_maybe_split_build_partition triggered by revoke_memory",
                print_id(state->query_id()), node_id(), state->task_id(), bytes, partition_id.level,
                partition_id.path);
    }
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_split_probe_partition(
        RuntimeState* state, PartitionedHashJoinProbeLocalState& local_state,
        const HashJoinSpillPartitionId& partition_id) const {
    if (partition_id.level >= kHashJoinSpillMaxDepth) {
        return Status::OK();
    }
    // Repartition probe rows to follow build split; handles in-memory and spilled data.
    std::vector<vectorized::Block> blocks;
    vectorized::SpillStreamSPtr parent_stream;
    // Always read from probe_partitions
    auto& partitions = local_state._shared_state->probe_partitions;
    auto it = partitions.find(partition_id.key());
    if (it != partitions.end()) {
        auto& partition = it->second;
        if (partition.accumulating_block && !partition.accumulating_block->empty()) {
            blocks.emplace_back(partition.accumulating_block->to_block());
            partition.accumulating_block.reset();
        }
        while (!partition.blocks.empty()) {
            blocks.emplace_back(std::move(partition.blocks.back()));
            partition.blocks.pop_back();
        }
        parent_stream = partition.spill_stream;
        partition.spill_stream.reset();
    }

    if (blocks.empty() && !parent_stream) {
        return Status::OK();
    }

    std::array<std::unique_ptr<vectorized::MutableBlock>, kHashJoinSpillFanout> child_blocks;
    std::array<vectorized::SpillStreamSPtr, kHashJoinSpillFanout> child_streams;
    std::array<size_t, kHashJoinSpillFanout>
            child_spilled_bytes {}; // Track spilled bytes per child

    auto acquire_spill_stream = [&](vectorized::SpillStreamSPtr& stream) {
        if (!stream) {
            RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    state, stream, print_id(state->query_id()), "hash_probe_split", node_id(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
                    local_state.operator_profile()));
        }
        return Status::OK();
    };

    auto partition_block = [&](vectorized::Block& block) -> Status {
        // Partition by hash-only channel ids and route to child partitions.
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, &block));

        std::vector<std::vector<uint32_t>> partition_indexes(kHashJoinSpillFanout);
        const auto& hashes = local_state._partitioner->get_channel_ids();
        for (uint32_t i = 0; i < block.rows(); ++i) {
            const auto child_index = spill_partition_index(hashes[i], partition_id.level + 1);
            partition_indexes[child_index].emplace_back(i);
        }

        for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
            const auto count = partition_indexes[i].size();
            if (count == 0) {
                continue;
            }
            if (!child_blocks[i]) {
                child_blocks[i] = vectorized::MutableBlock::create_unique(block.clone_empty());
            }
            RETURN_IF_ERROR(child_blocks[i]->add_rows(&block, partition_indexes[i].data(),
                                                      partition_indexes[i].data() + count));

            if (child_blocks[i]->allocated_bytes() >=
                vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                RETURN_IF_ERROR(acquire_spill_stream(child_streams[i]));
                auto spill_block = child_blocks[i]->to_block();
                child_spilled_bytes[i] += spill_block.allocated_bytes();
                RETURN_IF_ERROR(child_streams[i]->spill_block(state, spill_block, false));
                child_blocks[i].reset();
            }
        }
        return Status::OK();
    };

    for (auto& block : blocks) {
        RETURN_IF_ERROR(partition_block(block));
    }

    if (parent_stream) {
        // Read parent spill stream and repartition into child streams.
        parent_stream->set_read_counters(local_state.operator_profile());
        bool eos = false;
        while (!eos) {
            vectorized::Block block;
            RETURN_IF_ERROR(parent_stream->read_next_block_sync(&block, &eos));
            if (block.empty()) {
                continue;
            }
            RETURN_IF_ERROR(partition_block(block));
        }
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(parent_stream);
    }

    auto& parent = get_or_create_partition(partitions, partition_id);
    parent.is_split = true;
    // Reset parent's memory tracking after split
    parent.in_mem_bytes = 0;
    // Note: spilled_bytes remains for historical tracking
    COUNTER_UPDATE(local_state._probe_partition_splits, 1);
    // Materialize child partitions and enqueue them for processing.
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        auto child_id = partition_id.child(i);
        auto& child_partition = get_or_create_partition(partitions, child_id);
        if (child_blocks[i]) {
            RETURN_IF_ERROR(acquire_spill_stream(child_streams[i]));
            auto spill_block = child_blocks[i]->to_block();
            child_spilled_bytes[i] += spill_block.allocated_bytes();
            RETURN_IF_ERROR(child_streams[i]->spill_block(state, spill_block, false));
            child_blocks[i].reset();
        }

        if (child_streams[i]) {
            RETURN_IF_ERROR(child_streams[i]->spill_eof());
            child_partition.spill_stream = std::move(child_streams[i]);
            child_partition.spilled_bytes = child_spilled_bytes[i];
        }
    }

    // Calculate bytes from unified probe_partitions storage
    size_t bytes = 0;
    for (const auto& [_, partition] : local_state._shared_state->probe_partitions) {
        if (partition.accumulating_block) {
            bytes += partition.accumulating_block->allocated_bytes();
        }
        for (const auto& block : partition.blocks) {
            bytes += block.allocated_bytes();
        }
    }
    COUNTER_SET(local_state._probe_blocks_bytes, int64_t(bytes));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_split_build_partition(
        RuntimeState* state, PartitionedHashJoinProbeLocalState& local_state,
        const HashJoinSpillPartitionId& partition_id) const {
    if (partition_id.level >= kHashJoinSpillMaxDepth) {
        return Status::OK();
    }
    DCHECK(local_state._build_partitioner);

    // Split build partition to avoid oversized hash tables.
    auto& build_partitions = local_state._shared_state->build_partitions;
    auto& parent = get_or_create_build_partition(build_partitions, partition_id);
    DCHECK(!parent.is_split);

    std::vector<vectorized::Block> blocks;
    vectorized::SpillStreamSPtr parent_stream;
    // Always read from build_partitions.
    if (local_state._recovered_build_block && !local_state._recovered_build_block->empty()) {
        blocks.emplace_back(local_state._recovered_build_block->to_block());
        local_state._recovered_build_block.reset();
    }

    if (parent.build_block && !parent.build_block->empty()) {
        blocks.emplace_back(parent.build_block->to_block());
        parent.build_block.reset();
    }

    parent_stream = parent.spill_stream;
    parent.spill_stream.reset();

    if (blocks.empty() && !parent_stream) {
        return Status::OK();
    }

    std::array<std::unique_ptr<vectorized::MutableBlock>, kHashJoinSpillFanout> child_blocks;
    std::array<vectorized::SpillStreamSPtr, kHashJoinSpillFanout> child_streams;
    std::array<size_t, kHashJoinSpillFanout>
            child_spilled_bytes {};                               // Track spilled bytes per child
    std::array<size_t, kHashJoinSpillFanout> child_row_counts {}; // Track row count per child

    auto acquire_spill_stream = [&](vectorized::SpillStreamSPtr& stream) {
        if (!stream) {
            RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    state, stream, print_id(state->query_id()), "hash_build_split", node_id(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
                    local_state.operator_profile()));
        }
        return Status::OK();
    };

    auto partition_block = [&](vectorized::Block& block) -> Status {
        // Partition by build keys into child build partitions.
        RETURN_IF_ERROR(local_state._build_partitioner->do_partitioning(state, &block));

        std::vector<std::vector<uint32_t>> partition_indexes(kHashJoinSpillFanout);
        const auto& hashes = local_state._build_partitioner->get_channel_ids();
        for (uint32_t i = 0; i < block.rows(); ++i) {
            const auto child_index = spill_partition_index(hashes[i], partition_id.level + 1);
            partition_indexes[child_index].emplace_back(i);
        }

        for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
            const auto count = partition_indexes[i].size();
            if (count == 0) {
                continue;
            }
            child_row_counts[i] += count;
            if (!child_blocks[i]) {
                child_blocks[i] = vectorized::MutableBlock::create_unique(block.clone_empty());
            }
            RETURN_IF_ERROR(child_blocks[i]->add_rows(&block, partition_indexes[i].data(),
                                                      partition_indexes[i].data() + count));

            if (child_blocks[i]->allocated_bytes() >=
                vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                RETURN_IF_ERROR(acquire_spill_stream(child_streams[i]));
                auto spill_block = child_blocks[i]->to_block();
                child_spilled_bytes[i] += spill_block.allocated_bytes();
                RETURN_IF_ERROR(child_streams[i]->spill_block(state, spill_block, false));
                child_blocks[i].reset();
            }
        }
        return Status::OK();
    };

    for (auto& block : blocks) {
        RETURN_IF_ERROR(partition_block(block));
    }

    if (parent_stream) {
        // Repartition spilled build data into child spill streams.
        parent_stream->set_read_counters(local_state.operator_profile());
        bool eos = false;
        while (!eos) {
            vectorized::Block block;
            RETURN_IF_ERROR(parent_stream->read_next_block_sync(&block, &eos));
            if (block.empty()) {
                continue;
            }
            RETURN_IF_ERROR(partition_block(block));
        }
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(parent_stream);
    }

    parent.is_split = true;
    // Reset parent's memory tracking after split
    parent.in_mem_bytes = 0;
    parent.row_count = 0;
    COUNTER_UPDATE(local_state._build_partition_splits, 1);
    if (partition_id.level + 1 > local_state._max_partition_level->value()) {
        local_state._max_partition_level->set(int64_t(partition_id.level + 1));
    }

    // Persist child partitions for later processing.
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        if (child_row_counts[i] == 0) {
            continue;
        }
        auto child_id = partition_id.child(i);
        auto& child = get_or_create_build_partition(build_partitions, child_id);
        child.row_count = child_row_counts[i];
        if (child_blocks[i]) {
            RETURN_IF_ERROR(acquire_spill_stream(child_streams[i]));
            auto spill_block = child_blocks[i]->to_block();
            child_spilled_bytes[i] += spill_block.allocated_bytes();
            RETURN_IF_ERROR(child_streams[i]->spill_block(state, spill_block, false));
            child_blocks[i].reset();
        }

        if (child_streams[i]) {
            RETURN_IF_ERROR(child_streams[i]->spill_eof());
            child.spill_stream = std::move(child_streams[i]);
            child.spilled_bytes = child_spilled_bytes[i];
        }
    }

    // IMPORTANT: enqueue ALL children for processing, even if a child has no build rows.
    // - Probe rows arriving after the build split are routed directly to these children
    //   (see find_partition_for_hash).
    // - Some join types can produce output even when probe is empty (RIGHT/FULL OUTER),
    //   and others must still output probe rows when build child is empty (e.g. LEFT OUTER).
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        auto child_id = partition_id.child(i);
        get_or_create_build_partition(build_partitions, child_id);
        local_state._pending_partitions.emplace_back(child_id);
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_select_partition_if_needed(
        PartitionedHashJoinProbeLocalState& local_state, bool* eos) const {
    *eos = false;
    if (local_state._has_current_partition) {
        return Status::OK();
    }

    // Prefer split children from pending queue (handles multi-level splits).
    if (!local_state._pending_partitions.empty()) {
        local_state._current_partition_id = local_state._pending_partitions.front();
        local_state._pending_partitions.pop_front();
        local_state._has_current_partition = true;
        // Don't modify _partition_cursor when processing child partitions.
        return Status::OK();
    }

    // Skip all base partitions that have been split (their children are in pending queue
    // or have already been processed).
    auto& build_partitions = local_state._shared_state->build_partitions;
    while (local_state._partition_cursor < _partition_count) {
        HashJoinSpillPartitionId id {.level = 0, .path = local_state._partition_cursor};
        auto it = build_partitions.find(id.key());
        if (it == build_partitions.end() || !it->second.is_split) {
            break;
        }
        local_state._partition_cursor++;
    }

    if (local_state._partition_cursor >= _partition_count) {
        *eos = true;
        return Status::OK();
    }

    local_state._current_partition_id =
            HashJoinSpillPartitionId {.level = 0, .path = local_state._partition_cursor};
    local_state._has_current_partition = true;
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_prepare_hash_table(
        RuntimeState* state, PartitionedHashJoinProbeLocalState& local_state,
        bool* need_wait) const {
    *need_wait = false;
    if (!local_state._need_to_setup_internal_operators) {
        return Status::OK();
    }

    // Merge any recovered build batch back into its partition buffer.
    if (local_state._recovered_build_block && !local_state._recovered_build_block->empty()) {
        local_state._estimate_memory_usage += local_state._recovered_build_block->allocated_bytes();
        // Always use build_partitions for both base and split levels.
        auto& build_partition = get_or_create_build_partition(
                local_state._shared_state->build_partitions, local_state._current_partition_id);
        if (!build_partition.build_block) {
            build_partition.build_block = std::move(local_state._recovered_build_block);
        } else {
            RETURN_IF_ERROR(build_partition.build_block->merge(
                    local_state._recovered_build_block->to_block()));
            local_state._recovered_build_block.reset();
        }
    }

    bool has_data = false;

    RETURN_IF_ERROR(local_state.recover_build_blocks_from_disk(
            state, local_state._current_partition_id, has_data));
    if (has_data) {
        *need_wait = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
    local_state._need_to_setup_internal_operators = false;
    // Always move buffered probe rows from unified source to blocks.
    auto& partition = get_or_create_partition(local_state._shared_state->probe_partitions,
                                              local_state._current_partition_id);
    if (partition.accumulating_block && !partition.accumulating_block->empty()) {
        partition.blocks.emplace_back(partition.accumulating_block->to_block());
        partition.accumulating_block.reset();
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state,
                                               vectorized::Block* output_block, bool* eos) const {
    auto& local_state = get_local_state(state);
    const auto partition_index = base_partition_index(local_state._current_partition_id);
    // Always read from probe_partitions for both base and split levels
    auto& probe_blocks = get_or_create_partition(local_state._shared_state->probe_partitions,
                                                 local_state._current_partition_id)
                                 .blocks;

    bool in_mem_eos = false;
    auto* runtime_state = local_state._shared_state->inner_runtime_state.get();
    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            *eos = false;
            bool has_data = false;
            RETURN_IF_ERROR(local_state.recover_probe_blocks_from_disk(
                    state, local_state._current_partition_id, has_data));
            if (!has_data) {
                vectorized::Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, has no data to recovery",
                        print_id(state->query_id()), node_id(), state->task_id(), partition_index);
                break;
            }
            return Status::OK();
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
        COUNTER_UPDATE(local_state._handled_partition_count, 1);
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, probe done",
                print_id(state->query_id()), node_id(), state->task_id(), partition_index);
        // Only advance cursor when completing a non-split base partition.
        // Split base partitions are skipped in _select_partition_if_needed.
        // Child partitions (level > 0) don't affect cursor.
        auto it = local_state._shared_state->build_partitions.find(
                local_state._current_partition_id.key());
        if (local_state._current_partition_id.level == 0) {
            if (it == local_state._shared_state->build_partitions.end() || !it->second.is_split) {
                local_state._partition_cursor++;
            }
        }

        if (it != local_state._shared_state->build_partitions.end()) {
            CHECK(!it->second.build_block || it->second.build_block->rows() == 0);
            local_state._shared_state->build_partitions.erase(it);
        }

        {
            auto probe_it = local_state._shared_state->probe_partitions.find(
                    local_state._current_partition_id.key());
            if (probe_it != local_state._shared_state->probe_partitions.end()) {
                CHECK_EQ(probe_it->second.blocks.size(), 0);
                CHECK(!probe_it->second.accumulating_block ||
                      probe_it->second.accumulating_block->empty());
                local_state._shared_state->probe_partitions.erase(probe_it);
            }
        }

        local_state._has_current_partition = false;
        local_state.update_profile_from_inner();
        if (local_state._partition_cursor >= _partition_count &&
            local_state._pending_partitions.empty()) {
            *eos = true;
        } else {
            local_state._need_to_setup_internal_operators = true;
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->is_spilled) {
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
    size_t revocable_size = 0;

    // Probe data is revocable while still receiving probe input
    if (!local_state._child_eos) {
        if (_child) {
            revocable_size += _child->revocable_mem_size(state);
        }
    }

    if (!local_state._shared_state->is_spilled) {
        return revocable_size;
    }

    revocable_size += _revocable_mem_size(state, true);

    if (local_state._has_current_partition &&
        local_state._current_partition_id.level < kHashJoinSpillMaxDepth &&
        local_state._need_to_setup_internal_operators) {
        auto it = local_state._shared_state->build_partitions.find(
                local_state._current_partition_id.key());
        if (it == local_state._shared_state->build_partitions.end()) {
            return revocable_size;
        }

        if (it->second.total_bytes() < vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            return revocable_size;
        } else {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{}, partition level:{}, path:{} "
                    "build partition total bytes:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._current_partition_id.level, local_state._current_partition_id.path,
                    it->second.total_bytes());
        }

        if (it->second.build_block) {
            revocable_size += it->second.build_block->allocated_bytes();
        }

        if (local_state._recovered_build_block) {
            revocable_size += local_state._recovered_build_block->allocated_bytes();
        }
    }

    return revocable_size;
}

size_t PartitionedHashJoinProbeOperatorX::_revocable_mem_size(RuntimeState* state,
                                                              bool force) const {
    const auto spill_size_threshold = force ? vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM
                                            : vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
    auto& local_state = get_local_state(state);
    size_t mem_size = 0;
    // Calculate from unified probe_partitions storage
    auto& partitions = local_state._shared_state->probe_partitions;
    for (auto& [_, partition] : partitions) {
        for (auto& block : partition.blocks) {
            mem_size += block.allocated_bytes();
        }
        if (partition.accumulating_block) {
            auto block_bytes = partition.accumulating_block->allocated_bytes();
            if (block_bytes >= spill_size_threshold) {
                mem_size += block_bytes;
            }
        }
    }

    return mem_size;
}

size_t PartitionedHashJoinProbeOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    const auto is_spilled = local_state._shared_state->is_spilled;

    // Not spilled: use base implementation
    if (!is_spilled) {
        return Base::get_reserve_mem_size(state);
    }

    size_t size_to_reserve = 0;

    // Reserve for spill write buffer + child while receiving probe data
    if (!local_state._child_eos) {
        size_to_reserve += vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
        if (_child) {
            size_to_reserve += _child->get_reserve_mem_size(state);
        }
    } else {
        size_to_reserve += state->spill_recover_max_read_bytes();
    }

    // Reserve for hash table construction during recovery phase (after probe input is complete).
    if (local_state._need_to_setup_internal_operators && local_state._child_eos) {
        // Determine partition to estimate rows from
        HashJoinSpillPartitionId partition_id;
        if (local_state._has_current_partition) {
            partition_id = local_state._current_partition_id;
        } else {
            // No current partition set, find the next partition similar to _select_partition_if_needed
            auto& build_partitions = local_state._shared_state->build_partitions;
            if (!local_state._pending_partitions.empty()) {
                // Use the first pending child partition
                partition_id = local_state._pending_partitions.front();
            } else {
                // Find next non-split base partition
                uint32_t cursor = local_state._partition_cursor;
                while (cursor < _partition_count) {
                    HashJoinSpillPartitionId id {0, cursor};
                    auto it = build_partitions.find(id.key());
                    if (it == build_partitions.end() || !it->second.is_split) {
                        partition_id = id;
                        break;
                    }
                    cursor++;
                }
                // If no partition found, use default estimation
                if (cursor >= _partition_count) {
                    partition_id = {.level = 0, .path = 0};
                }
            }
        }

        // Get rows from partition data
        size_t rows = _build_partition_rows(local_state, partition_id);
        if (rows == 0) {
            rows = state->batch_size();
        }
        // Include key storage estimation for more accurate memory reservation
        size_to_reserve += estimate_hash_table_mem_size(rows, _join_op, true);
        size_to_reserve += double(_build_partition_bytes(local_state, partition_id)) * 0.8;
        COUNTER_SET(local_state._memory_usage_reserved, int64_t(size_to_reserve));
    }

    // Ensure minimum reserve
    const auto min_reserve_size = state->minimum_operator_memory_required_bytes();
    return std::max(size_to_reserve, min_reserve_size);
}

Status PartitionedHashJoinProbeOperatorX::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    auto& local_state = get_local_state(state);
    VLOG_DEBUG << fmt::format("Query:{}, hash join probe:{}, task:{}, revoke_memory",
                              print_id(state->query_id()), node_id(), state->task_id());

    if (!local_state._child_eos) {
        RETURN_IF_ERROR(_child->revoke_memory(state, spill_context));
    }

    if (local_state._shared_state->is_spilled && local_state._has_current_partition &&
        local_state._need_to_setup_internal_operators) {
        RETURN_IF_ERROR(_maybe_split_build_partition(state, local_state));
        if (!local_state._has_current_partition) {
            return Status::OK();
        }
    }

    RETURN_IF_ERROR(local_state.spill_probe_blocks(state));

    return Status::OK();
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
    if (local_state._shared_state->is_spilled) {
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
    const auto is_spilled = local_state._shared_state->is_spilled;
#ifndef NDEBUG
    Defer eos_check_defer([&] {
        if (*eos) {
            LOG(INFO) << fmt::format(
                    "Query:{}, hash join probe:{}, task:{}, child eos:{}, need spill:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._child_eos, is_spilled);
        }
    });
#endif

    if (is_spilled && local_state._child_eos) {
        // After probe input is fully received, prepare hash table and process partitions.
        bool no_more_partitions = false;
        RETURN_IF_ERROR(_select_partition_if_needed(local_state, &no_more_partitions));
        if (no_more_partitions) {
            *eos = true;
            return Status::OK();
        }
        bool need_wait = false;
        RETURN_IF_ERROR(_prepare_hash_table(state, local_state, &need_wait));
        if (need_wait || !local_state._has_current_partition) {
            return Status::OK();
        }
    }

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
        if (is_spilled) {
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
            if (local_state._child_eos) {
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
        if (is_spilled) {
            if (local_state._child_eos) {
                RETURN_IF_ERROR(local_state.finish_spilling(local_state._current_partition_id));
            }
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