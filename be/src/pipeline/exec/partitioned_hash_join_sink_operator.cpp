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

#include "partitioned_hash_join_sink_operator.h"

#include "pipeline/exec/operator.h"
#include "util/mem_info.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

Status PartitionedHashJoinSinkLocalState::init(doris::RuntimeState* state,
                                               doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::init(state, info));
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    _shared_state->partitioned_build_blocks.resize(p._partition_count);
    _shared_state->spilled_streams.resize(p._partition_count);

    _partitioner = std::make_unique<PartitionerType>(p._partition_count);
    RETURN_IF_ERROR(_partitioner->init(p._build_exprs));

    _partition_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "PartitionTime", "Spill", 1);
    _partition_shuffle_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile(), "PartitionShuffleTime", "Spill", 1);
    _spill_build_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile(), "SpillBuildTime", "Spill", 1);

    return _partitioner->prepare(state, p._child_x->row_desc());
}

Status PartitionedHashJoinSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::open(state));
    return _partitioner->open(state);
}
Status PartitionedHashJoinSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(PipelineXSpillSinkLocalState::exec_time_counter());
    SCOPED_TIMER(PipelineXSpillSinkLocalState::_close_timer);
    if (PipelineXSpillSinkLocalState::_closed) {
        return Status::OK();
    }
    dec_running_big_mem_op_num(state);
    return PipelineXSpillSinkLocalState::close(state, exec_status);
}

Status PartitionedHashJoinSinkLocalState::revoke_memory(RuntimeState* state) {
    LOG(INFO) << "hash join sink " << _parent->id() << " revoke_memory"
              << ", eos: " << _child_eos;
    DCHECK_EQ(_spilling_streams_count, 0);
    _spilling_streams_count = _shared_state->partitioned_build_blocks.size();
    for (size_t i = 0; i != _shared_state->partitioned_build_blocks.size(); ++i) {
        vectorized::SpillStreamSPtr& spilling_stream = _shared_state->spilled_streams[i];
        auto& mutable_block = _shared_state->partitioned_build_blocks[i];

        if (!mutable_block ||
            mutable_block->allocated_bytes() < vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            --_spilling_streams_count;
            continue;
        }

        if (!spilling_stream) {
            RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    state, spilling_stream, print_id(state->query_id()), "hash_build_sink",
                    _parent->id(), std::numeric_limits<int32_t>::max(),
                    std::numeric_limits<size_t>::max(), _profile));
            RETURN_IF_ERROR(spilling_stream->prepare_spill());
            spilling_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                                _spill_data_size, _spill_write_disk_timer,
                                                _spill_write_wait_io_timer);
        }

        auto* spill_io_pool =
                ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool();
        DCHECK(spill_io_pool != nullptr);
        auto execution_context = state->get_task_execution_context();
        _shared_state_holder = _shared_state->shared_from_this();

        MonotonicStopWatch submit_timer;
        submit_timer.start();

        auto st = spill_io_pool->submit_func(
                [this, execution_context, state, spilling_stream, i, submit_timer] {
                    auto execution_context_lock = execution_context.lock();
                    if (!execution_context_lock) {
                        LOG(INFO) << "execution_context released, maybe query was cancelled.";
                        return;
                    }
                    _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
                    SCOPED_TIMER(_spill_build_timer);
                    (void)state; // avoid ut compile error
                    SCOPED_ATTACH_TASK(state);
                    _spill_to_disk(i, spilling_stream);
                });

        if (!st.ok()) {
            --_spilling_streams_count;
            return st;
        }
    }

    if (_spilling_streams_count > 0) {
        _shared_state->need_to_spill = true;
        std::unique_lock<std::mutex> lock(_spill_lock);
        if (_spilling_streams_count > 0) {
            _dependency->block();
        } else if (_child_eos) {
            LOG(INFO) << "hash join sink " << _parent->id() << " set_ready_to_read"
                      << ", task id: " << state->task_id();
            _dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}

void PartitionedHashJoinSinkLocalState::_spill_to_disk(
        uint32_t partition_index, const vectorized::SpillStreamSPtr& spilling_stream) {
    auto& partitioned_block = _shared_state->partitioned_build_blocks[partition_index];

    if (_spill_status_ok) {
        auto block = partitioned_block->to_block();
        partitioned_block = vectorized::MutableBlock::create_unique(block.clone_empty());
        auto st = spilling_stream->spill_block(block, false);
        if (!st.ok()) {
            _spill_status_ok = false;
            std::lock_guard<std::mutex> l(_spill_status_lock);
            _spill_status = st;
        }
    }

    --_spilling_streams_count;
    DCHECK_GE(_spilling_streams_count, 0);

    if (_spilling_streams_count == 0) {
        std::unique_lock<std::mutex> lock(_spill_lock);
        _dependency->set_ready();
        if (_child_eos) {
            LOG(INFO) << "hash join sink " << _parent->id() << " set_ready_to_read"
                      << ", task id: " << state()->task_id();
            _dependency->set_ready_to_read();
        }
    }
}

PartitionedHashJoinSinkOperatorX::PartitionedHashJoinSinkOperatorX(
        ObjectPool* pool, int operator_id, const TPlanNode& tnode, const DescriptorTbl& descs,
        bool use_global_rf, uint32_t partition_count)
        : JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>(pool, operator_id, tnode,
                                                                    descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[1]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {}

Status PartitionedHashJoinSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    _name = "PARTITIONED_HASH_JOIN_SINK_OPERATOR";
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<TExpr> partition_exprs;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_exprs.emplace_back(eq_join_conjunct.right);
        partition_exprs.emplace_back(eq_join_conjunct.right);
    }

    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::prepare(RuntimeState* state) {
    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::open(RuntimeState* state) {
    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                              bool eos) {
    auto& local_state = get_local_state(state);
    local_state.inc_running_big_mem_op_num(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (!local_state._spill_status_ok) {
        DCHECK_NE(local_state._spill_status.code(), 0);
        return local_state._spill_status;
    }

    local_state._child_eos = eos;

    const auto rows = in_block->rows();

    if (rows > 0) {
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        /// TODO: DO NOT execute build exprs twice(when partition and building hash table)
        {
            SCOPED_TIMER(local_state._partition_timer);
            RETURN_IF_ERROR(local_state._partitioner->do_partitioning(
                    state, in_block, local_state._mem_tracker.get()));
        }

        SCOPED_TIMER(local_state._partition_shuffle_timer);
        auto* channel_ids =
                reinterpret_cast<uint64_t*>(local_state._partitioner->get_channel_ids());
        std::vector<uint32_t> partition_indexes[_partition_count];
        for (uint32_t i = 0; i != rows; ++i) {
            partition_indexes[channel_ids[i]].emplace_back(i);
        }

        auto& partitioned_blocks = local_state._shared_state->partitioned_build_blocks;
        for (uint32_t i = 0; i != _partition_count; ++i) {
            const auto count = partition_indexes[i].size();
            if (UNLIKELY(count == 0)) {
                continue;
            }

            if (UNLIKELY(!partitioned_blocks[i])) {
                partitioned_blocks[i] =
                        vectorized::MutableBlock::create_unique(in_block->clone_empty());
            }
            partitioned_blocks[i]->add_rows(in_block, &(partition_indexes[i][0]),
                                            &(partition_indexes[i][count]));
        }

        if (local_state._shared_state->need_to_spill) {
            const auto revocable_size = revocable_mem_size(state);
            if (revocable_size > state->min_revocable_mem()) {
                return local_state.revoke_memory(state);
            }
        }
    }

    if (eos) {
        LOG(INFO) << "hash join sink " << id() << " sink eos, set_ready_to_read"
                  << ", task id: " << state->task_id();
        local_state._dependency->set_ready_to_read();
    }

    return Status::OK();
}

size_t PartitionedHashJoinSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    auto& partitioned_blocks = local_state._shared_state->partitioned_build_blocks;

    size_t mem_size = 0;
    for (uint32_t i = 0; i != _partition_count; ++i) {
        auto& block = partitioned_blocks[i];
        if (block) {
            auto block_bytes = block->allocated_bytes();
            if (block_bytes >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                mem_size += block_bytes;
            }
        }
    }
    return mem_size;
}

Status PartitionedHashJoinSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revoke_memory(state);
}

} // namespace doris::pipeline
