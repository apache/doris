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

#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/mem_info.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : PipelineXSpillLocalState(state, parent),
          _child_block(vectorized::Block::create_unique()) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile.reset(new RuntimeProfile("internal_profile"));
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _partitioned_blocks.resize(p._partition_count);
    _probe_spilling_streams.resize(p._partition_count);

    _spill_and_partition_label = ADD_LABEL_COUNTER(profile(), "Partition");
    _partition_timer = ADD_CHILD_TIMER(profile(), "PartitionTime", "Partition");
    _partition_shuffle_timer = ADD_CHILD_TIMER(profile(), "PartitionShuffleTime", "Partition");
    _spill_build_rows = ADD_CHILD_COUNTER(profile(), "SpillBuildRows", TUnit::UNIT, "Spill");
    _spill_build_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "SpillBuildTime", "Spill", 1);
    _recovery_build_rows = ADD_CHILD_COUNTER(profile(), "RecoveryBuildRows", TUnit::UNIT, "Spill");
    _recovery_build_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "RecoveryBuildTime", "Spill", 1);
    _spill_probe_rows = ADD_CHILD_COUNTER(profile(), "SpillProbeRows", TUnit::UNIT, "Spill");
    _recovery_probe_rows = ADD_CHILD_COUNTER(profile(), "RecoveryProbeRows", TUnit::UNIT, "Spill");
    _spill_build_blocks = ADD_CHILD_COUNTER(profile(), "SpillBuildBlocks", TUnit::UNIT, "Spill");
    _recovery_build_blocks =
            ADD_CHILD_COUNTER(profile(), "RecoveryBuildBlocks", TUnit::UNIT, "Spill");
    _spill_probe_blocks = ADD_CHILD_COUNTER(profile(), "SpillProbeBlocks", TUnit::UNIT, "Spill");
    _spill_probe_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "SpillProbeTime", "Spill", 1);
    _recovery_probe_blocks =
            ADD_CHILD_COUNTER(profile(), "RecoveryProbeBlocks", TUnit::UNIT, "Spill");
    _recovery_probe_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "RecoveryProbeTime", "Spill", 1);

    _spill_serialize_block_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillSerializeBlockTime", "Spill", 1);
    _spill_write_disk_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWriteDiskTime", "Spill", 1);
    _spill_data_size = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillWriteDataSize",
                                                    TUnit::BYTES, "Spill", 1);
    _spill_block_count = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillWriteBlockCount",
                                                      TUnit::UNIT, "Spill", 1);

    // Build phase
    _build_phase_label = ADD_LABEL_COUNTER(profile(), "BuildPhase");
    _build_rows_counter = ADD_CHILD_COUNTER(profile(), "BuildRows", TUnit::UNIT, "BuildPhase");
    _publish_runtime_filter_timer =
            ADD_CHILD_TIMER(profile(), "PublishRuntimeFilterTime", "BuildPhase");
    _runtime_filter_compute_timer =
            ADD_CHILD_TIMER(profile(), "RuntimeFilterComputeTime", "BuildPhase");
    _build_table_timer = ADD_CHILD_TIMER(profile(), "BuildTableTime", "BuildPhase");
    _build_side_merge_block_timer =
            ADD_CHILD_TIMER(profile(), "BuildSideMergeBlockTime", "BuildPhase");
    _build_table_insert_timer = ADD_CHILD_TIMER(profile(), "BuildTableInsertTime", "BuildPhase");
    _build_expr_call_timer = ADD_CHILD_TIMER(profile(), "BuildExprCallTime", "BuildPhase");
    _build_side_compute_hash_timer =
            ADD_CHILD_TIMER(profile(), "BuildSideHashComputingTime", "BuildPhase");
    _allocate_resource_timer = ADD_CHILD_TIMER(profile(), "AllocateResourceTime", "BuildPhase");

    // Probe phase
    _probe_phase_label = ADD_LABEL_COUNTER(profile(), "ProbePhase");
    _probe_next_timer = ADD_CHILD_TIMER(profile(), "ProbeFindNextTime", "ProbePhase");
    _probe_expr_call_timer = ADD_CHILD_TIMER(profile(), "ProbeExprCallTime", "ProbePhase");
    _search_hashtable_timer =
            ADD_CHILD_TIMER(profile(), "ProbeWhenSearchHashTableTime", "ProbePhase");
    _build_side_output_timer =
            ADD_CHILD_TIMER(profile(), "ProbeWhenBuildSideOutputTime", "ProbePhase");
    _probe_side_output_timer =
            ADD_CHILD_TIMER(profile(), "ProbeWhenProbeSideOutputTime", "ProbePhase");
    _probe_process_hashtable_timer =
            ADD_CHILD_TIMER(profile(), "ProbeWhenProcessHashTableTime", "ProbePhase");
    _process_other_join_conjunct_timer =
            ADD_CHILD_TIMER(profile(), "OtherJoinConjunctTime", "ProbePhase");
    _init_probe_side_timer = ADD_CHILD_TIMER(profile(), "InitProbeSideTime", "ProbePhase");
    _probe_timer = ADD_CHILD_TIMER(profile(), "ProbeTime", "ProbePhase");
    _join_filter_timer = ADD_CHILD_TIMER(profile(), "JoinFilterTimer", "ProbePhase");
    _build_output_block_timer = ADD_CHILD_TIMER(profile(), "BuildOutputBlock", "ProbePhase");
    _probe_rows_counter = ADD_CHILD_COUNTER(profile(), "ProbeRows", TUnit::UNIT, "ProbePhase");
    return Status::OK();
}
#define UPDATE_PROFILE(counter, name)                           \
    do {                                                        \
        auto* child_counter = child_profile->get_counter(name); \
        if (child_counter != nullptr) {                         \
            COUNTER_UPDATE(counter, child_counter->value());    \
        }                                                       \
    } while (false)

void PartitionedHashJoinProbeLocalState::update_build_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE(_build_rows_counter, "BuildRows");
    UPDATE_PROFILE(_publish_runtime_filter_timer, "PublishRuntimeFilterTime");
    UPDATE_PROFILE(_runtime_filter_compute_timer, "RuntimeFilterComputeTime");
    UPDATE_PROFILE(_build_table_timer, "BuildTableTime");
    UPDATE_PROFILE(_build_side_merge_block_timer, "BuildSideMergeBlockTime");
    UPDATE_PROFILE(_build_table_insert_timer, "BuildTableInsertTime");
    UPDATE_PROFILE(_build_expr_call_timer, "BuildExprCallTime");
    UPDATE_PROFILE(_build_side_compute_hash_timer, "BuildSideHashComputingTime");
    UPDATE_PROFILE(_allocate_resource_timer, "AllocateResourceTime");
}

void PartitionedHashJoinProbeLocalState::update_probe_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE(_probe_timer, "ProbeTime");
    UPDATE_PROFILE(_join_filter_timer, "JoinFilterTimer");
    UPDATE_PROFILE(_build_output_block_timer, "BuildOutputBlock");
    UPDATE_PROFILE(_probe_rows_counter, "ProbeRows");
    UPDATE_PROFILE(_probe_next_timer, "ProbeFindNextTime");
    UPDATE_PROFILE(_probe_expr_call_timer, "ProbeExprCallTime");
    UPDATE_PROFILE(_search_hashtable_timer, "ProbeWhenSearchHashTableTime");
    UPDATE_PROFILE(_build_side_output_timer, "ProbeWhenBuildSideOutputTime");
    UPDATE_PROFILE(_probe_side_output_timer, "ProbeWhenProbeSideOutputTime");
    UPDATE_PROFILE(_probe_process_hashtable_timer, "ProbeWhenProcessHashTableTime");
    UPDATE_PROFILE(_process_other_join_conjunct_timer, "OtherJoinConjunctTime");
    UPDATE_PROFILE(_init_probe_side_timer, "InitProbeSideTime");
}

#undef UPDATE_PROFILE

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
    dec_running_big_mem_op_num(state);
    RETURN_IF_ERROR(PipelineXSpillLocalState::close(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::spill_probe_blocks(RuntimeState* state) {
    auto* spill_io_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    auto query_id = state->query_id();

    MonotonicStopWatch submit_timer;
    submit_timer.start();
    auto spill_func = [query_id, state, submit_timer, this] {
        _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
        SCOPED_TIMER(_spill_probe_timer);

        auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
        for (uint32_t partition_index = 0; partition_index != p._partition_count;
             ++partition_index) {
            auto& blocks = _probe_blocks[partition_index];
            auto& partitioned_block = _partitioned_blocks[partition_index];
            if (partitioned_block && partitioned_block->allocated_bytes() >=
                                             vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                blocks.emplace_back(partitioned_block->to_block());
                partitioned_block.reset();
            }

            auto& spilling_stream = _probe_spilling_streams[partition_index];
            if (!spilling_stream) {
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, spilling_stream, print_id(state->query_id()), "hash_probe",
                        _parent->node_id(), std::numeric_limits<int32_t>::max(),
                        std::numeric_limits<size_t>::max(), _runtime_profile.get()));
                RETURN_IF_ERROR(spilling_stream->prepare_spill());
                spilling_stream->set_write_counters(
                        _spill_serialize_block_timer, _spill_block_count, _spill_data_size,
                        _spill_write_disk_timer, _spill_write_wait_io_timer);
            }

            COUNTER_UPDATE(_spill_probe_blocks, blocks.size());
            while (!blocks.empty() && !state->is_cancelled()) {
                auto block = std::move(blocks.back());
                blocks.pop_back();
                DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks", {
                    return Status::Error<INTERNAL_ERROR>(
                            "fault_inject partitioned_hash_join_probe spill_probe_blocks failed");
                });
                RETURN_IF_ERROR(spilling_stream->spill_block(state, block, false));
                COUNTER_UPDATE(_spill_probe_rows, block.rows());
            }
        }
        VLOG_DEBUG << "query: " << print_id(query_id)
                   << " hash probe revoke done, node: " << p.node_id()
                   << ", task: " << state->task_id();
        return Status::OK();
    };

    auto exception_catch_func = [query_id, spill_func, this]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel", {
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    query_id, Status::InternalError("fault_inject partitioned_hash_join_probe "
                                                    "spill_probe_blocks canceled"));
            return;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();

        if (!status.ok()) {
            _spill_status_ok = false;
            _spill_status = std::move(status);
        }
        _dependency->set_ready();
    };

    _dependency->block();
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe spill_probe_blocks submit_func failed");
    });

    auto spill_runnable = std::make_shared<SpillRunnable>(state, _shared_state->shared_from_this(),
                                                          exception_catch_func);
    return spill_io_pool->submit(std::move(spill_runnable));
}

Status PartitionedHashJoinProbeLocalState::finish_spilling(uint32_t partition_index) {
    auto& build_spilling_stream = _shared_state->spilled_streams[partition_index];
    if (build_spilling_stream) {
        RETURN_IF_ERROR(build_spilling_stream->spill_eof());
        build_spilling_stream->set_read_counters(_spill_read_data_time, _spill_deserialize_time,
                                                 _spill_read_bytes, _spill_read_wait_io_timer);
    }

    auto& probe_spilling_stream = _probe_spilling_streams[partition_index];

    if (probe_spilling_stream) {
        RETURN_IF_ERROR(probe_spilling_stream->spill_eof());
        probe_spilling_stream->set_read_counters(_spill_read_data_time, _spill_deserialize_time,
                                                 _spill_read_bytes, _spill_read_wait_io_timer);
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::recovery_build_blocks_from_disk(RuntimeState* state,
                                                                           uint32_t partition_index,
                                                                           bool& has_data) {
    VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << _parent->node_id()
               << ", task id: " << state->task_id() << ", partition: " << partition_index
               << " recovery_build_blocks_from_disk";
    auto& spilled_stream = _shared_state->spilled_streams[partition_index];
    has_data = false;
    if (!spilled_stream) {
        return Status::OK();
    }

    auto& mutable_block = _shared_state->partitioned_build_blocks[partition_index];
    if (!mutable_block) {
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
        spilled_stream.reset();
        return Status::OK();
    }

    std::weak_ptr<PartitionedHashJoinSharedState> shared_state_holder =
            _shared_state->shared_from_this();

    auto query_id = state->query_id();

    MonotonicStopWatch submit_timer;
    submit_timer.start();

    auto read_func = [this, query_id, state, spilled_stream = spilled_stream, &mutable_block,
                      shared_state_holder, submit_timer, partition_index] {
        auto shared_state_sptr = shared_state_holder.lock();
        if (!shared_state_sptr || state->is_cancelled()) {
            LOG(INFO) << "query: " << print_id(query_id)
                      << " execution_context released, maybe query was cancelled.";
            return;
        }

        _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
        SCOPED_TIMER(_recovery_build_timer);

        bool eos = false;
        VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << _parent->node_id()
                   << ", task id: " << state->task_id() << ", partition: " << partition_index
                   << ", recoverying build data";
        while (!eos) {
            vectorized::Block block;
            Status st;
            DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks", {
                st = Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_probe recover_build_blocks failed");
            });
            if (st.ok()) {
                st = spilled_stream->read_next_block_sync(&block, &eos);
            }
            if (!st.ok()) {
                _spill_status_ok = false;
                _spill_status = std::move(st);
                break;
            }
            COUNTER_UPDATE(_recovery_build_rows, block.rows());
            COUNTER_UPDATE(_recovery_build_blocks, 1);

            if (block.empty()) {
                continue;
            }

            if (UNLIKELY(state->is_cancelled())) {
                LOG(INFO) << "recovery build block when canceled.";
                break;
            }

            if (mutable_block->empty()) {
                *mutable_block = std::move(block);
            } else {
                DCHECK_EQ(mutable_block->columns(), block.columns());
                st = mutable_block->merge(std::move(block));
                if (!st.ok()) {
                    _spill_status_ok = false;
                    _spill_status = std::move(st);
                    break;
                }
            }
        }

        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
        shared_state_sptr->spilled_streams[partition_index].reset();
        VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << _parent->node_id()
                   << ", task id: " << state->task_id() << ", partition: " << partition_index
                   << ", recovery build data done";
    };

    auto exception_catch_func = [read_func, query_id, this]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel", {
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    query_id, Status::InternalError("fault_inject partitioned_hash_join_probe "
                                                    "recover_build_blocks canceled"));
            return;
        });

        auto status = [&]() {
            RETURN_IF_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        if (!status.ok()) {
            _spill_status_ok = false;
            _spill_status = std::move(status);
        }
        _dependency->set_ready();
    };

    auto* spill_io_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    has_data = true;
    _dependency->block();
    {
        auto* pipeline_task = state->get_task();
        if (pipeline_task) {
            auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
            VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << p.node_id()
                       << ", task id: " << state->task_id() << ", partition: " << partition_index
                       << ", dependency: " << _dependency
                       << ", task debug_string: " << pipeline_task->debug_string();
        }
    }

    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_build_blocks submit_func failed");
                    });
    auto spill_runnable = std::make_shared<SpillRunnable>(state, _shared_state->shared_from_this(),
                                                          exception_catch_func);
    VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << _parent->node_id()
               << ", task id: " << state->task_id() << ", partition: " << partition_index
               << " recovery_build_blocks_from_disk submit func";
    return spill_io_pool->submit(std::move(spill_runnable));
}

std::string PartitionedHashJoinProbeLocalState::debug_string(int indentation_level) const {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    bool need_more_input_data;
    if (_shared_state->need_to_spill) {
        need_more_input_data = !_child_eos;
    } else if (_runtime_state) {
        need_more_input_data = p._inner_probe_operator->need_more_input_data(_runtime_state.get());
    } else {
        need_more_input_data = true;
    }
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, short_circuit_for_probe: {}, need_to_spill: {}, child_eos: {}, "
                   "_runtime_state: {}, need_more_input_data: {}",
                   PipelineXSpillLocalState<PartitionedHashJoinSharedState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL",
                   _shared_state->need_to_spill, _child_eos, _runtime_state != nullptr,
                   need_more_input_data);
    return fmt::to_string(debug_string_buffer);
}

Status PartitionedHashJoinProbeLocalState::recovery_probe_blocks_from_disk(RuntimeState* state,
                                                                           uint32_t partition_index,
                                                                           bool& has_data) {
    auto& spilled_stream = _probe_spilling_streams[partition_index];
    has_data = false;
    if (!spilled_stream) {
        return Status::OK();
    }

    auto& blocks = _probe_blocks[partition_index];

    auto query_id = state->query_id();
    MonotonicStopWatch submit_timer;
    submit_timer.start();

    auto read_func = [this, query_id, &spilled_stream, &blocks, submit_timer] {
        _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
        SCOPED_TIMER(_recovery_probe_timer);

        vectorized::Block block;
        bool eos = false;
        Status st;
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_probe recover_probe_blocks failed");
        });
        if (st.ok()) {
            st = spilled_stream->read_next_block_sync(&block, &eos);
        }
        if (!st.ok()) {
            _spill_status_ok = false;
            _spill_status = std::move(st);
        } else {
            COUNTER_UPDATE(_recovery_probe_rows, block.rows());
            COUNTER_UPDATE(_recovery_probe_blocks, 1);
            blocks.emplace_back(std::move(block));
        }

        if (eos) {
            VLOG_DEBUG << "query: " << print_id(query_id)
                       << ", recovery probe data done: " << spilled_stream->get_spill_dir();
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            spilled_stream.reset();
        }
    };

    auto exception_catch_func = [read_func, query_id, this]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel", {
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    query_id, Status::InternalError("fault_inject partitioned_hash_join_probe "
                                                    "recover_probe_blocks canceled"));
            return;
        });

        auto status = [&]() {
            RETURN_IF_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        if (!status.ok()) {
            _spill_status_ok = false;
            _spill_status = std::move(status);
        }
        _dependency->set_ready();
    };

    auto* spill_io_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    DCHECK(spill_io_pool != nullptr);
    _dependency->block();
    has_data = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_probe_blocks submit_func failed");
                    });
    return spill_io_pool->submit(std::make_shared<SpillRunnable>(
            state, _shared_state->shared_from_this(), exception_catch_func));
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

    for (auto& conjunct : tnode.hash_join_node.eq_join_conjuncts) {
        _probe_exprs.emplace_back(conjunct.left);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_probe_exprs));

    return Status::OK();
}
Status PartitionedHashJoinProbeOperatorX::prepare(RuntimeState* state) {
    // to avoid prepare _child_x twice
    auto child_x = std::move(_child_x);
    RETURN_IF_ERROR(JoinProbeOperatorX::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    RETURN_IF_ERROR(_inner_probe_operator->set_child(child_x));
    DCHECK(_build_side_child != nullptr);
    _inner_probe_operator->set_build_side_child(_build_side_child);
    RETURN_IF_ERROR(_inner_probe_operator->prepare(state));
    _child_x = std::move(child_x);
    RETURN_IF_ERROR(_partitioner->prepare(state, _child_x->row_desc()));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::open(RuntimeState* state) {
    // to avoid open _child_x twice
    auto child_x = std::move(_child_x);
    RETURN_IF_ERROR(JoinProbeOperatorX::open(state));
    RETURN_IF_ERROR(_inner_probe_operator->open(state));
    _child_x = std::move(child_x);
    RETURN_IF_ERROR(_partitioner->open(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                               bool eos) const {
    auto& local_state = get_local_state(state);
    local_state.inc_running_big_mem_op_num(state);
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
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block,
                                                                  local_state._mem_tracker.get()));
    }

    std::vector<std::vector<uint32_t>> partition_indexes(_partition_count);
    const auto* channel_ids = local_state._partitioner->get_channel_ids().get<uint32_t>();
    for (uint32_t i = 0; i != rows; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    SCOPED_TIMER(local_state._partition_shuffle_timer);
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
        }
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operator_for_non_spill(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) {
    DCHECK(local_state._shared_state->inner_runtime_state);
    local_state._runtime_state = std::move(local_state._shared_state->inner_runtime_state);
    local_state._in_mem_shared_state_sptr =
            std::move(local_state._shared_state->inner_shared_state);
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    if (local_state._runtime_state) {
        _update_profile_from_internal_states(local_state);
    }

    local_state._runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._runtime_state->set_be_number(state->be_number());

    local_state._runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._runtime_state->resize_op_id_to_local_state(-1);
    local_state._runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    // set sink local state
    LocalSinkStateInfo info {0,  local_state._internal_runtime_profile.get(),
                             -1, local_state._in_mem_shared_state_sptr.get(),
                             {}, {}};
    RETURN_IF_ERROR(
            _inner_sink_operator->setup_local_state(local_state._runtime_state.get(), info));

    LocalStateInfo state_info {local_state._internal_runtime_profile.get(),
                               {},
                               local_state._in_mem_shared_state_sptr.get(),
                               {},
                               0};
    RETURN_IF_ERROR(
            _inner_probe_operator->setup_local_state(local_state._runtime_state.get(), state_info));

    auto* sink_local_state = local_state._runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state =
            local_state._runtime_state->get_local_state(_inner_probe_operator->operator_id());
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
    RETURN_IF_ERROR(_inner_sink_operator->sink(local_state._runtime_state.get(), &block, true));
    VLOG_DEBUG << "query: " << print_id(state->query_id())
               << ", internal build operator finished, node id: " << node_id()
               << ", task id: " << state->task_id()
               << ", partition: " << local_state._partition_cursor;
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state,
                                               vectorized::Block* output_block, bool* eos) const {
    auto& local_state = get_local_state(state);
    if (!local_state._spill_status_ok) {
        DCHECK_NE(local_state._spill_status.code(), 0);
        return local_state._spill_status;
    }

    const auto partition_index = local_state._partition_cursor;
    auto& probe_blocks = local_state._probe_blocks[partition_index];
    if (local_state._need_to_setup_internal_operators) {
        *eos = false;
        bool has_data = false;
        RETURN_IF_ERROR(local_state.recovery_build_blocks_from_disk(
                state, local_state._partition_cursor, has_data));
        if (has_data) {
            return Status::OK();
        }
        RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
        local_state._need_to_setup_internal_operators = false;
        auto& mutable_block = local_state._partitioned_blocks[partition_index];
        if (mutable_block && !mutable_block->empty()) {
            probe_blocks.emplace_back(mutable_block->to_block());
        }
    }
    bool in_mem_eos = false;
    auto* runtime_state = local_state._runtime_state.get();
    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            *eos = false;
            bool has_data = false;
            RETURN_IF_ERROR(
                    local_state.recovery_probe_blocks_from_disk(state, partition_index, has_data));
            if (!has_data) {
                vectorized::Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << node_id()
                           << ", task: " << state->task_id() << "partition: " << partition_index
                           << " has no data to recovery";
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

    RETURN_IF_ERROR(_inner_probe_operator->pull(local_state._runtime_state.get(), output_block,
                                                &in_mem_eos));

    *eos = false;
    if (in_mem_eos) {
        VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", node: " << node_id()
                   << ", task: " << state->task_id()
                   << ", partition: " << local_state._partition_cursor;
        local_state._partition_cursor++;
        if (local_state._partition_cursor == _partition_count) {
            *eos = true;
        } else {
            RETURN_IF_ERROR(local_state.finish_spilling(local_state._partition_cursor));
            local_state._need_to_setup_internal_operators = true;
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->need_to_spill) {
        return !local_state._child_eos;
    } else if (local_state._runtime_state) {
        return _inner_probe_operator->need_more_input_data(local_state._runtime_state.get());
    } else {
        return true;
    }
}

size_t PartitionedHashJoinProbeOperatorX::revocable_mem_size(RuntimeState* state) const {
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
            if (block_bytes >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                mem_size += block_bytes;
            }
        }
    }
    return mem_size;
}

Status PartitionedHashJoinProbeOperatorX::_revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    VLOG_DEBUG << "query: " << print_id(state->query_id()) << ", hash probe node: " << node_id()
               << ", task: " << state->task_id();

    RETURN_IF_ERROR(local_state.spill_probe_blocks(state));
    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::_should_revoke_memory(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    const auto revocable_size = revocable_mem_size(state);
    if (PipelineTask::should_revoke_memory(state, revocable_size)) {
        return true;
    }
    if (local_state._shared_state->need_to_spill) {
        const auto min_revocable_size = state->min_revocable_mem();
        return revocable_size > min_revocable_size;
    }
    return false;
}

void PartitionedHashJoinProbeOperatorX::_update_profile_from_internal_states(
        PartitionedHashJoinProbeLocalState& local_state) const {
    if (local_state._runtime_state) {
        auto* sink_local_state = local_state._runtime_state->get_sink_local_state();
        local_state.update_build_profile(sink_local_state->profile());
        auto* probe_local_state =
                local_state._runtime_state->get_local_state(_inner_probe_operator->operator_id());
        local_state.update_probe_profile(probe_local_state->profile());
    }
}

Status PartitionedHashJoinProbeOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                    bool* eos) {
    *eos = false;
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    const auto need_to_spill = local_state._shared_state->need_to_spill;
#ifndef NDEBUG
    Defer eos_check_defer([&] {
        if (*eos) {
            LOG(INFO) << "query: " << print_id(state->query_id())
                      << ", hash probe node: " << node_id() << ", task: " << state->task_id()
                      << ", eos with child eos: " << local_state._child_eos
                      << ", need spill: " << need_to_spill;
        }
    });
#endif
    if (need_more_input_data(state)) {
        if (need_to_spill && _should_revoke_memory(state)) {
            return _revoke_memory(state);
        }

        RETURN_IF_ERROR(_child_x->get_block_after_projects(state, local_state._child_block.get(),
                                                           &local_state._child_eos));

        if (need_to_spill && local_state._child_eos) {
            RETURN_IF_ERROR(local_state.finish_spilling(0));
        }

        if (local_state._child_block->rows() == 0 && !local_state._child_eos) {
            return Status::OK();
        }

        Defer defer([&] { local_state._child_block->clear_column_data(); });
        if (need_to_spill) {
            SCOPED_TIMER(local_state.exec_time_counter());
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
        } else {
            if (UNLIKELY(!local_state._runtime_state)) {
                RETURN_IF_ERROR(_setup_internal_operator_for_non_spill(local_state, state));
            }

            RETURN_IF_ERROR(_inner_probe_operator->push(local_state._runtime_state.get(),
                                                        local_state._child_block.get(),
                                                        local_state._child_eos));
        }
    }

    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
        if (need_to_spill) {
            RETURN_IF_ERROR(pull(state, block, eos));
        } else {
            RETURN_IF_ERROR(
                    _inner_probe_operator->pull(local_state._runtime_state.get(), block, eos));
            if (*eos) {
                _update_profile_from_internal_states(local_state);
                local_state._runtime_state.reset();
            }
        }

        local_state.add_num_rows_returned(block->rows());
        if (*eos) {
            _update_profile_from_internal_states(local_state);
        }
    }
    return Status::OK();
}

} // namespace doris::pipeline
