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

#include "exec/operator/partitioned_hash_join_probe_operator.h"

#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "exec/spill/spill_repartitioner.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_profile.h"

namespace doris {

#include "common/compile_check_begin.h"

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : PipelineXSpillLocalState(state, parent), _child_block(Block::create_unique()) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::init(state, info));
    init_spill_write_counters();

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _partitioned_blocks.resize(p._partition_count);
    _probe_spilling_groups.resize(p._partition_count);
    _probe_writers.resize(p._partition_count);
    // The repartitioner fanout will be configured when the repartitioner is
    // initialized with a fanout-sized partitioner clone in the repartition path.
    init_counters();
    return Status::OK();
}

void PartitionedHashJoinProbeLocalState::init_counters() {
    _partition_shuffle_timer = ADD_TIMER(custom_profile(), "SpillRePartitionTime");
    _spill_build_rows = ADD_COUNTER(custom_profile(), "SpillBuildRows", TUnit::UNIT);
    _spill_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillBuildTime", 1);
    _recovery_build_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildRows", TUnit::UNIT);
    _recovery_level0_build_rows =
            ADD_COUNTER(custom_profile(), "SpillRecoveryLevel0BuildRows", TUnit::UNIT);
    _recovery_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryBuildTime", 1);
    _spill_probe_rows = ADD_COUNTER(custom_profile(), "SpillProbeRows", TUnit::UNIT);
    _build_rows = ADD_COUNTER(custom_profile(), "BuildRows", TUnit::UNIT);
    _recovery_probe_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeRows", TUnit::UNIT);
    _spill_build_blocks = ADD_COUNTER(custom_profile(), "SpillBuildBlocks", TUnit::UNIT);
    _recovery_build_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildBlocks", TUnit::UNIT);
    _spill_probe_blocks = ADD_COUNTER(custom_profile(), "SpillProbeBlocks", TUnit::UNIT);
    _spill_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillProbeTime", 1);
    _recovery_probe_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeBlocks", TUnit::UNIT);
    _recovery_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryProbeTime", 1);
    _get_child_next_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "GetChildNextTime", 1);

    _probe_blocks_bytes =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "ProbeBlocksBytesInMem", TUnit::BYTES, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);

    // Counters for partition spill metrics
    _max_partition_level = ADD_COUNTER(custom_profile(), "SpillMaxPartitionLevel", TUnit::UNIT);
    _total_partition_spills = ADD_COUNTER(custom_profile(), "SpillTotalPartitions", TUnit::UNIT);
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
    if (_shared_state->_inner_runtime_state) {
        auto* sink_local_state = _shared_state->_inner_runtime_state->get_sink_local_state();
        auto* probe_local_state = _shared_state->_inner_runtime_state->get_local_state(
                p._inner_probe_operator->operator_id());
        if (_shared_state->_is_spilled) {
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

    // Create a fanout-sized partitioner for repartitioning.
    // Use operator-configured partition count instead of static FANOUT.
    _fanout_partitioner =
            std::make_unique<SpillRePartitionerType>(static_cast<int>(p._partition_count));
    RETURN_IF_ERROR(_fanout_partitioner->init(p._probe_exprs));
    RETURN_IF_ERROR(_fanout_partitioner->prepare(state, p._child->row_desc()));
    RETURN_IF_ERROR(_fanout_partitioner->open(state));

    _build_fanout_partitioner =
            std::make_unique<SpillRePartitionerType>(static_cast<int>(p._partition_count));
    RETURN_IF_ERROR(_build_fanout_partitioner->init(p._build_exprs));
    RETURN_IF_ERROR(_build_fanout_partitioner->prepare(state, p._build_side_child->row_desc()));
    RETURN_IF_ERROR(_build_fanout_partitioner->open(state));

    return Status::OK();
}
Status PartitionedHashJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    Status first_error;
    for (auto& writer : _probe_writers) {
        if (writer) {
            auto st = writer->close();
            if (!st.ok() && first_error.ok()) {
                first_error = st;
            }
            writer.reset();
        }
    }
    _probe_writers.clear();

    if (_current_build_reader) {
        auto st = _current_build_reader->close();
        if (!st.ok() && first_error.ok()) {
            first_error = st;
        }
        _current_build_reader.reset();
    }
    if (_current_probe_reader) {
        auto st = _current_probe_reader->close();
        if (!st.ok() && first_error.ok()) {
            first_error = st;
        }
        _current_probe_reader.reset();
    }

    // Clean up any remaining spill partition queue entries
    for (auto& entry : _spill_partition_queue) {
        if (entry.build_file) {
            ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(entry.build_file);
        }
        if (entry.probe_file) {
            ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(entry.probe_file);
        }
    }
    _spill_partition_queue.clear();
    if (_current_partition.build_file) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(_current_partition.build_file);
    }
    if (_current_partition.probe_file) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(_current_partition.probe_file);
    }
    _current_partition = JoinSpillPartitionInfo {};
    _queue_probe_blocks.clear();

    auto st = PipelineXSpillLocalState::close(state);
    if (!first_error.ok()) {
        return first_error;
    }
    return st;
}

Status PartitionedHashJoinProbeLocalState::acquire_spill_writer(RuntimeState* state,
                                                                int partition_index,
                                                                SpillFileWriterSPtr& writer) {
    if (!_probe_writers[partition_index]) {
        auto& spill_file = _probe_spilling_groups[partition_index];
        auto relative_path = fmt::format("{}/{}-{}-{}-{}", print_id(state->query_id()),
                                         "hash_probe", _parent->node_id(), state->task_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                    spill_file));
        RETURN_IF_ERROR(spill_file->create_writer(state, operator_profile(),
                                                  _probe_writers[partition_index]));
    }
    writer = _probe_writers[partition_index];
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::spill_probe_blocks(RuntimeState* state, bool flush_all) {
    auto query_id = state->query_id();
    SCOPED_TIMER(_spill_probe_timer);

    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel", {
        return Status::InternalError(
                "fault_inject partitioned_hash_join_probe "
                "spill_probe_blocks canceled");
    });
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    for (uint32_t partition_index = 0; partition_index != p._partition_count; ++partition_index) {
        auto& partitioned_block = _partitioned_blocks[partition_index];
        if (!partitioned_block || partitioned_block->empty()) {
            continue;
        }

        if (!flush_all &&
            partitioned_block->allocated_bytes() < SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
            continue;
        }

        SpillFileWriterSPtr writer;
        RETURN_IF_ERROR(acquire_spill_writer(state, partition_index, writer));

        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks", {
            return Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_probe "
                    "spill_probe_blocks failed");
        });

        COUNTER_UPDATE(_spill_probe_rows, partitioned_block->rows());
        RETURN_IF_ERROR(writer->write_block(state, partitioned_block->to_block()));
        COUNTER_UPDATE(_spill_probe_blocks, 1);
        partitioned_block.reset();
    }

    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " spill_probe_blocks done",
            print_id(query_id), p.node_id(), state->task_id());
    return Status::OK();
}

std::string PartitionedHashJoinProbeLocalState::debug_string(int indentation_level) const {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    bool need_more_input_data;
    if (_shared_state->_is_spilled) {
        need_more_input_data = !_child_eos;
    } else if (_shared_state->_inner_runtime_state) {
        need_more_input_data = p._inner_probe_operator->need_more_input_data(
                _shared_state->_inner_runtime_state.get());
    } else {
        need_more_input_data = true;
    }
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, short_circuit_for_probe: {}, is_spilled: {}, child_eos: {}, "
                   "_shared_state->_inner_runtime_state: {}, need_more_input_data: {}",
                   PipelineXSpillLocalState<PartitionedHashJoinSharedState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL",
                   _shared_state->_is_spilled, _child_eos,
                   _shared_state->_inner_runtime_state != nullptr, need_more_input_data);
    return fmt::to_string(debug_string_buffer);
}

bool PartitionedHashJoinProbeLocalState::is_blockable() const {
    return _shared_state->_is_spilled;
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_partition(
        RuntimeState* state, JoinSpillPartitionInfo& partition_info) {
    if (!partition_info.build_file) {
        // Build file is already exhausted for this partition.
        return Status::OK();
    }
    SCOPED_TIMER(_recovery_build_timer);
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks", {
        return Status::InternalError(
                "fault_inject partitioned_hash_join_probe "
                "recover_build_blocks failed");
    });
    // Create reader if needed (persistent across scheduling slices)
    if (!_current_build_reader) {
        _current_build_reader = partition_info.build_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_current_build_reader->open());
    }
    bool eos = false;
    while (!eos) {
        Block block;
        RETURN_IF_ERROR(_current_build_reader->read(&block, &eos));
        COUNTER_UPDATE(_recovery_build_rows, block.rows());
        if (partition_info.level == 0) {
            COUNTER_UPDATE(_recovery_level0_build_rows, block.rows());
        }
        if (block.empty()) {
            continue;
        }
        COUNTER_UPDATE(_recovery_build_blocks, 1);
        if (UNLIKELY(state->is_cancelled())) {
            return state->cancel_reason();
        }
        if (!_recovered_build_block) {
            // This will merge the block to recover build block, so that has to use else here.
            _recovered_build_block = MutableBlock::create_unique(std::move(block));
        } else {
            RETURN_IF_ERROR(_recovered_build_block->merge(std::move(block)));
        }
        if (_recovered_build_block->allocated_bytes() >= state->spill_buffer_size_bytes()) {
            return Status::OK(); // yield — buffer full, more data may remain
        }
    }
    // Build file fully consumed.
    RETURN_IF_ERROR(_current_build_reader->close());
    _current_build_reader.reset();
    partition_info.build_file.reset();
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_partition(
        RuntimeState* state, JoinSpillPartitionInfo& partition_info) {
    if (!partition_info.probe_file) {
        // Probe file is already exhausted for this partition.
        return Status::OK();
    }

    // For multi-level queue partitions, store recovered probe blocks in _queue_probe_blocks.
    SCOPED_TIMER(_recovery_probe_timer);
    size_t read_size = 0;
    // Create reader if needed
    if (!_current_probe_reader) {
        _current_probe_reader = partition_info.probe_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(_current_probe_reader->open());
    }
    bool eos = false;
    while (!eos && !state->is_cancelled()) {
        Block block;
        RETURN_IF_ERROR(_current_probe_reader->read(&block, &eos));
        if (!block.empty()) {
            COUNTER_UPDATE(_recovery_probe_rows, block.rows());
            COUNTER_UPDATE(_recovery_probe_blocks, 1);
            read_size += block.allocated_bytes();
            _queue_probe_blocks.emplace_back(std::move(block));
        }
        if (read_size >= state->spill_buffer_size_bytes()) {
            return Status::OK(); // yield — enough data read
        }
    }
    // Probe file fully consumed.
    RETURN_IF_ERROR(_current_probe_reader->close());
    _current_probe_reader.reset();
    partition_info.probe_file.reset();
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::repartition_current_partition(
        RuntimeState* state, JoinSpillPartitionInfo& partition) {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    const int new_level = partition.level + 1;

    if (new_level >= p._repartition_max_depth) {
        return Status::InternalError(
                "query:{}, node:{}, Hash join spill repartition exceeded max depth {}. "
                "Likely due to extreme data skew.",
                print_id(state->query_id()), p.node_id(), p._repartition_max_depth);
    }

    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{}, repartitioning partition at level {} to "
            "level {}",
            print_id(state->query_id()), p.node_id(), state->task_id(), partition.level, new_level);

    // Create a partitioner for repartitioning build data.
    std::unique_ptr<PartitionerBase> build_fanout_clone;
    RETURN_IF_ERROR(_build_fanout_partitioner->clone(state, build_fanout_clone));
    _repartitioner.init(std::move(build_fanout_clone), operator_profile(),
                        static_cast<int>(p._partition_count), new_level);

    // Repartition build files
    std::vector<SpillFileSPtr> build_output_spill_files;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_spill_files(
            state, p.node_id(), fmt::format("hash_build_repart_l{}", new_level),
            static_cast<int>(p._partition_count), build_output_spill_files));

    RETURN_IF_ERROR(_repartitioner.setup_output(state, build_output_spill_files));

    // Route already-recovered in-memory build data first — these rows have
    // already been read from the file by _current_build_reader and must not
    // be re-read by the repartitioner.
    if (_recovered_build_block && _recovered_build_block->rows() > 0) {
        auto recovered_block = _recovered_build_block->to_block();
        RETURN_IF_ERROR(_repartitioner.route_block(state, recovered_block));
    }

    // Repartition the remaining unread portion of the build file.
    // If _current_build_reader exists the file was partially read; pass the
    // reader directly so the repartitioner continues from the current
    // position instead of re-reading from block 0 (which would duplicate
    // the rows already routed above).
    if (_current_build_reader) {
        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, _current_build_reader, &done));
        }
        // reader is reset by repartitioner on completion
    } else if (partition.build_file) {
        // No partial read — repartition the entire file from scratch.
        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, partition.build_file, &done));
        }
    }
    RETURN_IF_ERROR(_repartitioner.finalize());
    _recovered_build_block.reset();
    _current_build_reader.reset(); // clear any leftover reader state
    partition.build_file.reset();

    // Repartition probe files
    std::vector<SpillFileSPtr> probe_output_spill_files;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_spill_files(
            state, p.node_id(), fmt::format("hash_probe_repart_l{}", new_level),
            static_cast<int>(p._partition_count), probe_output_spill_files));

    if (partition.probe_file) {
        // Re-init repartitioner with a fresh FANOUT partitioner clone for probe data
        std::unique_ptr<PartitionerBase> probe_fanout_clone;
        RETURN_IF_ERROR(_fanout_partitioner->clone(state, probe_fanout_clone));
        _repartitioner.init(std::move(probe_fanout_clone), operator_profile(),
                            static_cast<int>(p._partition_count), new_level);

        RETURN_IF_ERROR(_repartitioner.setup_output(state, probe_output_spill_files));

        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, partition.probe_file, &done));
        }
        partition.probe_file.reset();

        RETURN_IF_ERROR(_repartitioner.finalize());
        _current_probe_reader.reset();
    }

    // Push all sub-partitions into work queue; build/probe emptiness is handled
    // later during recovery.  New sub-partitions start with build_finished =
    // probe_finished = false (via constructor).
    for (int i = 0; i < static_cast<int>(p._partition_count); ++i) {
        _spill_partition_queue.emplace_back(std::move(build_output_spill_files[i]),
                                            std::move(probe_output_spill_files[i]), new_level);
        // Metrics
        COUNTER_UPDATE(_total_partition_spills, 1);
        if (new_level > _max_partition_level_seen) {
            _max_partition_level_seen = new_level;
            COUNTER_SET(_max_partition_level, int64_t(_max_partition_level_seen));
        }
    }

    return Status::OK();
}

PartitionedHashJoinProbeOperatorX::PartitionedHashJoinProbeOperatorX(ObjectPool* pool,
                                                                     const TPlanNode& tnode,
                                                                     int operator_id,
                                                                     const DescriptorTbl& descs)
        : JoinProbeOperatorX<PartitionedHashJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[0]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs) {}

Status PartitionedHashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    _partition_count = state->spill_hash_join_partition_count();
    if (_partition_count < 2 || _partition_count > 32) {
        return Status::InternalError(
                "query:{}, node:{}, invalid partition count {}. Must be between 2 and 32.",
                print_id(state->query_id()), node_id(), _partition_count);
    }

    // default repartition max depth; can be overridden from session variable
    _repartition_max_depth = state->spill_repartition_max_depth();
    RETURN_IF_ERROR(JoinProbeOperatorX::init(tnode, state));
    _op_name = "PARTITIONED_HASH_JOIN_PROBE_OPERATOR";
    auto tnode_ = _tnode;
    tnode_.runtime_filters.clear();

    for (const auto& conjunct : tnode.hash_join_node.eq_join_conjuncts) {
        _probe_exprs.emplace_back(conjunct.left);
        _build_exprs.emplace_back(conjunct.right);
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

Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, Block* input_block,
                                               bool eos) const {
    auto& local_state = get_local_state(state);
    const auto rows = input_block->rows();
    auto& partitioned_blocks = local_state._partitioned_blocks;
    if (rows == 0) {
        return Status::OK();
    }
    SCOPED_TIMER(local_state._partition_shuffle_timer);

    RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block));

    std::vector<std::vector<uint32_t>> partition_indexes(_partition_count);
    const auto& channel_ids = local_state._partitioner->get_channel_ids();
    for (uint32_t i = 0; i != rows; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    int64_t bytes_of_blocks = 0;
    for (uint32_t i = 0; i != _partition_count; ++i) {
        const auto count = partition_indexes[i].size();
        if (UNLIKELY(count == 0)) {
            continue;
        }

        if (!partitioned_blocks[i]) {
            partitioned_blocks[i] = MutableBlock::create_unique(input_block->clone_empty());
        }
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(input_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));

        const auto bytes = partitioned_blocks[i]->allocated_bytes();
        if (bytes >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
            SpillFileWriterSPtr writer;
            RETURN_IF_ERROR(local_state.acquire_spill_writer(state, i, writer));

            COUNTER_UPDATE(local_state._spill_probe_rows, partitioned_blocks[i]->rows());
            RETURN_IF_ERROR(writer->write_block(state, partitioned_blocks[i]->to_block()));
            COUNTER_UPDATE(local_state._spill_probe_blocks, 1);
            partitioned_blocks[i].reset();
        } else {
            bytes_of_blocks += bytes;
        }
    }

    COUNTER_SET(local_state._probe_blocks_bytes, bytes_of_blocks);

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators_from_partition(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    local_state._shared_state->_inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._shared_state->_inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._shared_state->_inner_runtime_state->set_be_number(state->be_number());

    local_state._shared_state->_inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._shared_state->_inner_runtime_state->resize_op_id_to_local_state(-1);
    local_state._shared_state->_inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = local_state._internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = local_state._in_mem_shared_state_sptr.get(),
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->_inner_runtime_state.get(), info));

    LocalStateInfo state_info {.parent_profile = local_state._internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = local_state._in_mem_shared_state_sptr.get(),
                               .shared_state_map = {},
                               .task_idx = 0};
    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->_inner_runtime_state.get(), state_info));

    auto* sink_local_state =
            local_state._shared_state->_inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state = local_state._shared_state->_inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    // Use the recovered build block from the partition stream
    Block block;
    if (local_state._recovered_build_block && local_state._recovered_build_block->rows() > 0) {
        block = local_state._recovered_build_block->to_block();
        local_state._recovered_build_block.reset();
    }

    COUNTER_UPDATE(local_state._build_rows, block.rows());

    RETURN_IF_ERROR(_inner_sink_operator->sink(
            local_state._shared_state->_inner_runtime_state.get(), &block, true));
    local_state._current_partition.build_finished = true;
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " internal build from partition (level:{}) finished, rows:{}, memory usage:{}",
            print_id(state->query_id()), node_id(), state->task_id(),
            local_state._current_partition.level, block.rows(),
            _inner_sink_operator->get_memory_usage(
                    local_state._shared_state->_inner_runtime_state.get()));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state, Block* output_block,
                                               bool* eos) const {
    auto& local_state = get_local_state(state);

    // On first entry after child EOS, populate _spill_partition_queue from the
    // per-partition build and probe spill streams. After this point every partition
    // (including the original "level-0" ones) is accessed uniformly via the queue.
    if (!local_state._spill_queue_initialized) {
        DCHECK(local_state._child_eos) << "pull() with is_spilled=true called before child EOS";
        // There maybe some blocks still in partitioned block or probe blocks. Flush them to disk.
        RETURN_IF_ERROR(local_state.spill_probe_blocks(state, true));
        // Close all probe writers so that SpillFile metadata (part_count, etc.)
        // is finalized and the files become readable. Without this the readers
        // would see _part_count == 0 and return no data.
        for (auto& writer : local_state._probe_writers) {
            if (writer) {
                RETURN_IF_ERROR(writer->close());
            }
        }
        for (uint32_t i = 0; i < _partition_count; ++i) {
            auto& build_file = local_state._shared_state->_spilled_build_groups[i];
            auto& probe_file = local_state._probe_spilling_groups[i];
            // Transfer SpillFiles into JoinSpillPartitionInfo unconditionally.
            local_state._spill_partition_queue.emplace_back(std::move(build_file),
                                                            std::move(probe_file), 0);
            // Metrics: count this queued partition
            COUNTER_UPDATE(local_state._total_partition_spills, 1);
        }
        local_state._max_partition_level_seen = 0;
        COUNTER_SET(local_state._max_partition_level,
                    int64_t(local_state._max_partition_level_seen));
        local_state._spill_queue_initialized = true;
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{}, initialized spill queue with {} partitions",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._spill_partition_queue.size());
    }

    return _pull_from_spill_queue(local_state, state, output_block, eos);
}

Status PartitionedHashJoinProbeOperatorX::_pull_from_spill_queue(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state, Block* output_block,
        bool* eos) const {
    *eos = false;

    if (local_state._need_to_setup_queue_partition) {
        // No more partitions to process and no active partition — EOS.
        if (local_state._spill_partition_queue.empty() &&
            (!local_state._current_partition.is_valid() ||
             local_state._current_partition.probe_finished)) {
            *eos = true;
            return Status::OK();
        }

        // Pop next partition to process.
        // Invariant: we only pop when there is no active current partition and
        // no pending recovered build data waiting to be consumed.
        if (!local_state._current_partition.is_valid() ||
            local_state._current_partition.probe_finished) {
            local_state._current_partition = std::move(local_state._spill_partition_queue.front());
            local_state._spill_partition_queue.pop_front();
            local_state._recovered_build_block.reset();
            local_state._queue_probe_blocks.clear();
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " processing queue partition at level:{}, queue remaining:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._current_partition.level,
                    local_state._spill_partition_queue.size());
        }

        // Continue recovering build data while there is unread build file.
        if (local_state._current_partition.build_file) {
            // Partially read build data — yield so it can be consumed
            // before continuing recovery in the next scheduling slice.
            return local_state.recover_build_blocks_from_partition(state,
                                                                   local_state._current_partition);
        }
        RETURN_IF_ERROR(_setup_internal_operators_from_partition(local_state, state));
        local_state._current_partition.build_finished = true;
        local_state._need_to_setup_queue_partition = false;
        return Status::OK();
    }

    // Probe phase: feed probe blocks from the current partition's probe stream
    // into the inner probe operator.
    bool in_mem_eos = false;
    auto* runtime_state = local_state._shared_state->_inner_runtime_state.get();
    auto& probe_blocks = local_state._queue_probe_blocks;

    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            // Try to recover more probe blocks. If the probe stream is
            // finished (probe_file == nullptr) and no blocks are buffered,
            // we send EOS to the inner probe operator.
            if (!local_state._current_partition.probe_file) {
                Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " queue partition (level:{}) probe eos",
                        print_id(state->query_id()), node_id(), state->task_id(),
                        local_state._current_partition.level);
                break;
            }

            // Probe data recovered — yield to let the pipeline scheduler
            // re-schedule us so we can push the recovered blocks.
            return local_state.recover_probe_blocks_from_partition(state,
                                                                   local_state._current_partition);
        }

        auto block = std::move(probe_blocks.back());
        probe_blocks.pop_back();
        if (!block.empty()) {
            RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, false));
        }
    }
    RETURN_IF_ERROR(_inner_probe_operator->pull(runtime_state, output_block, &in_mem_eos));
    if (in_mem_eos) {
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " queue partition (level:{}) probe done",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._current_partition.level);
        local_state.update_profile_from_inner();
        local_state._current_partition.probe_finished = true;

        // Reset for next queue entry — default-constructed partition has
        // is_valid() == false, signaling "no partition in progress".
        local_state._current_partition = JoinSpillPartitionInfo {};
        local_state._need_to_setup_queue_partition = true;
        local_state._queue_probe_blocks.clear();

        if (local_state._spill_partition_queue.empty()) {
            *eos = true;
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->_is_spilled) {
        return !local_state._child_eos;
    } else if (local_state._shared_state->_inner_runtime_state) {
        return _inner_probe_operator->need_more_input_data(
                local_state._shared_state->_inner_runtime_state.get());
    } else {
        return true;
    }
}

// Report only this operator's own revocable memory. The pipeline task
// iterates all operators to sum revocable sizes and revoke each individually.
// Sum up memory used by in-memory probe blocks and any partially-recovered build block for the current partition.
// This is the memory that can be freed if we choose to revoke and repartition the current
size_t PartitionedHashJoinProbeOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state._shared_state->_is_spilled) {
        return 0;
    }

    size_t mem_size = 0;
    if (!local_state._child_eos) {
        for (uint32_t i = 0; i < _partition_count; ++i) {
            auto& partitioned_block = local_state._partitioned_blocks[i];
            if (!partitioned_block) {
                continue;
            }
            const auto bytes = partitioned_block->allocated_bytes();
            if (bytes >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
                mem_size += bytes;
            }
        }
        return mem_size > state->spill_min_revocable_mem() ? mem_size : 0;
    }
    if (!local_state._current_partition.is_valid() ||
        local_state._current_partition.build_finished) {
        // No active partition — no revocable memory.
        // Or if current partition has finished build hash table.
        return 0;
    }

    // Include build-side memory that has been recovered but not yet consumed by the hash table.
    // This data is revocable because we can repartition instead of building the hash table.
    if (local_state._recovered_build_block) {
        mem_size += local_state._recovered_build_block->allocated_bytes();
    }

    return mem_size > state->spill_min_revocable_mem() ? mem_size : 0;
}

size_t PartitionedHashJoinProbeOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    const bool is_spilled = local_state._shared_state->_is_spilled;

    // Non-spill path: delegate to the inner probe operator / base class.
    if (!is_spilled) {
        return Base::get_reserve_mem_size(state);
    }

    // Spill path, probe data still flowing in (child not yet EOS):
    // We only need room for incoming probe blocks being partitioned. Reserve
    // one batch worth of spill-write memory; no hash table will be built yet.
    if (!local_state._child_eos) {
        return state->minimum_operator_memory_required_bytes();
    }

    // Spill path, child EOS — we are in the recovery / build / probe phase.
    // Baseline reservation is one block of spill I/O.
    size_t size_to_reserve = state->minimum_operator_memory_required_bytes();

    const bool about_to_build = local_state._current_partition.is_valid() &&
                                !local_state._current_partition.build_finished;

    if (about_to_build && local_state._recovered_build_block) {
        // Estimate rows that will land in the hash table so we can reserve
        // enough for JoinHashTable::first[] + JoinHashTable::next[].
        size_t rows = std::max(static_cast<size_t>(state->batch_size()),
                               static_cast<size_t>(local_state._recovered_build_block->rows()));

        const size_t bucket_size = hash_join_table_calc_bucket_size(rows);
        size_to_reserve += bucket_size * sizeof(uint32_t); // JoinHashTable::first
        size_to_reserve += rows * sizeof(uint32_t);        // JoinHashTable::next

        if (_join_op == TJoinOp::FULL_OUTER_JOIN || _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
            _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN) {
            size_to_reserve += rows * sizeof(uint8_t); // JoinHashTable::visited
        }

        // It is hard to precisely estimate the memory needed for serialized
        // keys when building the hash table, so use the current build block
        // size as an estimate. This may be imprecise, but it should not
        // underestimate the requirement. Hash table construction also merges
        // blocks, so this approximation is reasonable here.
        size_to_reserve += local_state._recovered_build_block->allocated_bytes();
    }
    // Otherwise (not about to build): we only need the spill I/O baseline
    // already included above — no hash table allocation is imminent.

    COUNTER_SET(local_state._memory_usage_reserved, int64_t(size_to_reserve));
    return size_to_reserve;
}

// Revoke in-memory build data by repartitioning and spilling to disk.
//
// Called when `revoke_memory` is invoked after child EOS. At that point all
// build data is represented as JoinSpillPartitionInfo entries in _spill_partition_queue
// (after queue initialization). The current partition being processed may have
// partially-recovered build data in _recovered_build_block. We repartition that
// data into FANOUT sub-partitions and push them back onto _spill_partition_queue
// so the hash table build can proceed later under a smaller memory footprint.
//
// Build data lives in either:
//   (a) _current_partition.build_file    (SpillFile, may have been partially read)
//   (b) _recovered_build_block           (partially-recovered MutableBlock)
//
// During repartition we route (b) directly into sub-streams first, then
// continue reading (a), avoiding an extra round of spill write/read for (b).
Status PartitionedHashJoinProbeLocalState::revoke_build_data(RuntimeState* state) {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    DCHECK(_child_eos) << "revoke_build_data should only be called after child EOS";
    DCHECK(_spill_queue_initialized) << "queue must be initialized before revoke_build_data";

    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{}, revoke_build_data: "
            "repartitioning queue partition at level {} (build in SpillFile)",
            print_id(state->query_id()), p.node_id(), state->task_id(), _current_partition.level);

    RETURN_IF_ERROR(repartition_current_partition(state, _current_partition));

    _current_partition = JoinSpillPartitionInfo {};
    _need_to_setup_queue_partition = true;
    _queue_probe_blocks.clear();

    return Status::OK();
}

// Public revoke_memory: called by the pipeline task scheduler when memory
// pressure requires this operator's in-memory data to be spilled.
//
// Before child EOS: probe blocks are still being accumulated → spill them.
// After child EOS: we are in the recovery/build phase. All build data is
//   represented in _spill_partition_queue (after queue initialization). The
//   current partition's in-memory recovered data (_recovered_build_block) is
//   repartitioned and pushed back to the queue so the hash table build can
//   proceed later with a smaller footprint.
Status PartitionedHashJoinProbeOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    VLOG_DEBUG << fmt::format("Query:{}, hash join probe:{}, task:{}, revoke_memory, child_eos:{}",
                              print_id(state->query_id()), node_id(), state->task_id(),
                              local_state._child_eos);

    if (!local_state._child_eos) {
        // Probe-data accumulation phase: spill in-memory probe blocks to disk.
        return local_state.spill_probe_blocks(state, false);
    }
    if (!local_state._current_partition.is_valid() ||
        local_state._current_partition.build_finished) {
        return Status::OK();
    }
    // Recovery/build phase: repartition the current partition's in-memory build
    // data so the hash table build can be deferred to a smaller sub-partition.
    return local_state.revoke_build_data(state);
}

Status PartitionedHashJoinProbeOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    *eos = false;
    auto& local_state = get_local_state(state);
    const bool is_spilled = local_state._shared_state->_is_spilled;
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
        if (is_spilled) {
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
        } else {
            DCHECK(local_state._shared_state->_inner_runtime_state);
            RETURN_IF_ERROR(_inner_probe_operator->push(
                    local_state._shared_state->_inner_runtime_state.get(),
                    local_state._child_block.get(), local_state._child_eos));
        }
    }

    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
        if (is_spilled) {
            RETURN_IF_ERROR(pull(state, block, eos));
        } else {
            RETURN_IF_ERROR(_inner_probe_operator->pull(
                    local_state._shared_state->_inner_runtime_state.get(), block, eos));
            local_state.update_profile_from_inner();
        }

        if (!block->empty()) {
            local_state.add_num_rows_returned(block->rows());
            COUNTER_UPDATE(local_state._blocks_returned_counter, 1);
        }
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
