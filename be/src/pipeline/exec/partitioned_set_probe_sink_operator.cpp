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

#include "partitioned_set_probe_sink_operator.h"

#include <glog/logging.h>

#include <memory>

#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "vec/common/hash_table/hash_table_set_probe.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
Status PartitionedSetProbeSinkOperatorX<is_intersect>::init(const TPlanNode& tnode,
                                                            RuntimeState* state) {
    DataSinkOperatorX<PartitionedSetProbeSinkLocalState<is_intersect>>::_name =
            "PARTITIONED_SET_PROBE_SINK_OPERATOR";
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkOperatorX<is_intersect>::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(
            DataSinkOperatorX<PartitionedSetProbeSinkLocalState<is_intersect>>::prepare(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkOperatorX<is_intersect>::open(RuntimeState* state) {
    RETURN_IF_ERROR(
            DataSinkOperatorX<PartitionedSetProbeSinkLocalState<is_intersect>>::open(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkOperatorX<is_intersect>::sink(RuntimeState* state,
                                                            vectorized::Block* in_block, bool eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    auto probe_rows = in_block->rows();
    local_state._child_eos = eos;

    if (local_state._shared_state->need_to_spill) {
        if (!local_state._spill_status.ok()) {
            return local_state._spill_status.status();
        }

        if (probe_rows > 0) {
            RETURN_IF_ERROR(local_state.partition_block(in_block, state));
            if (eos) {
                RETURN_IF_ERROR(local_state.revoke_memory(state, true));
            }
        }

        if (eos && local_state._spilling_tasks_count == 0) {
            local_state._finalize_probe();
        }
        return Status::OK();
    }

    if (probe_rows > 0) {
        if (!local_state._shared_state->inner_probe_runtime_state) [[unlikely]] {
            RETURN_IF_ERROR(local_state.setup_inner_operator(state));
        }

        RETURN_IF_ERROR(_inner_sink_operator->sink(
                local_state._shared_state->inner_runtime_state.get(), in_block, eos));

        if (eos) {
            local_state._finalize_probe();
        }
    }
    return Status::OK();
}

template <bool is_intersect>
void PartitionedSetProbeSinkLocalState<is_intersect>::_finalize_probe() {
    auto& parent = _parent->cast<PartitionedSetProbeSinkOperatorX<is_intersect>>();
    if (parent._cur_child_id != (_shared_state->child_quantity - 1)) {
        _shared_state->probe_finished_children_dependency[parent._cur_child_id + 1]->set_ready();
    } else {
        _dependency->set_ready_to_read();
    }
}

template <bool is_intersect>
Status PartitionedSetProbeSinkLocalState<is_intersect>::init(RuntimeState* state,
                                                             LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& parent = _parent->cast<Parent>();
    _shared_state->probe_finished_children_dependency[parent._cur_child_id] = _dependency;
    _dependency->block();

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(parent._partition_exprs));

    _shared_state->probe_spill_streams[parent._cur_child_id - 1].resize(_partition_count);

    _partitioned_blocks.resize(_partition_count);
    _spill_streams.resize(_partition_count);

    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkLocalState<is_intersect>::setup_inner_operator(RuntimeState* state) {
    DCHECK(!_shared_state->inner_probe_runtime_state);

    _shared_state->inner_probe_runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    _shared_state->inner_probe_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    _shared_state->inner_probe_runtime_state->set_be_number(state->be_number());

    _shared_state->inner_probe_runtime_state->set_desc_tbl(&state->desc_tbl());
    _shared_state->inner_probe_runtime_state->resize_op_id_to_local_state(-1);
    _shared_state->inner_probe_runtime_state->set_pipeline_x_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    auto& parent = _parent->cast<PartitionedSetProbeSinkOperatorX<is_intersect>>();
    LocalSinkStateInfo info {0,  _internal_runtime_profile.get(),
                             -1, _shared_state->inner_shared_state.get(),
                             {}, {}};

    RETURN_IF_ERROR(parent._inner_sink_operator->setup_local_state(
            _shared_state->inner_probe_runtime_state.get(), info));
    auto* sink_local_state = _shared_state->inner_probe_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkLocalState<is_intersect>::async_spill_block(vectorized::Block&& block,
                                                                          RuntimeState* state,
                                                                          uint32_t partition_idx) {
    auto query_id = state->query_id();
    auto& parent = _parent->cast<Parent>();
    auto& stream = _shared_state->probe_spill_streams[parent._cur_child_id - 1][partition_idx];

    _spilling_tasks_count++;
    _dependency->block();

    auto* spill_stream_manager = ExecEnv::GetInstance()->spill_stream_mgr();

    if (!stream) {
        RETURN_IF_ERROR(spill_stream_manager->register_spill_stream(
                state, stream, print_id(query_id), "PartitionedSetProbeSinkLocalState",
                _parent->node_id(), std::numeric_limits<int32_t>::max(),
                std::numeric_limits<int32_t>::max(), _profile));
    }

    auto spill_runnable = std::make_shared<SpillRunnable>(
            state, _shared_state->shared_from_this(),
            [this, stream, state, block = std::move(block)]() mutable {
                Defer defer([&] {
                    if (!_spill_status.ok()) {
                        _dependency->set_ready();
                        return;
                    }

                    if (_spilling_tasks_count.fetch_sub(1) == 1) {
                        _dependency->set_ready();
                        if (_child_eos) {
                            _finalize_probe();
                        }
                    }
                });

                if (!_spill_status.ok()) {
                    return;
                }

                auto status = [&] {
                    RETURN_IF_CATCH_EXCEPTION(return stream->spill_block(state, block, false));
                }();

                if (!status.ok()) {
                    _spill_status.update(status);
                }
            });
    auto* thread_pool = spill_stream_manager->get_spill_io_thread_pool();
    return thread_pool->submit(std::move(spill_runnable));
}

template <bool is_intersect>
Status PartitionedSetProbeSinkLocalState<is_intersect>::partition_block(vectorized::Block* block,
                                                                        RuntimeState* state) {
    const size_t rows = block->rows();
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, block, _mem_tracker.get()));

    const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();
    std::vector<std::vector<uint32_t>> partitioned_indexes(_partition_count);
    for (size_t i = 0; i != rows; ++i) {
        partitioned_indexes[channel_ids[i]].emplace_back(i);
    }

    for (uint32_t i = 0; i != _partition_count; ++i) {
        const auto partition_rows = partitioned_indexes[i].size();
        if (partition_rows == 0) {
            continue;
        }

        auto& partitioned_block = _partitioned_blocks[i];

        if (!partitioned_block) {
            partitioned_block = vectorized::MutableBlock::create_unique(block->clone_empty());
        }

        const auto* begin = partitioned_indexes[i].data();
        RETURN_IF_ERROR(partitioned_block->add_rows(block, begin, begin + partition_rows));
    }

    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkLocalState<is_intersect>::revoke_memory(RuntimeState* state,
                                                                      bool force) {
    for (uint32_t i = 0; i != _partition_count; ++i) {
        auto& partitioned_block = _partitioned_blocks[i];
        if (!partitioned_block ||
            (!force &&
             partitioned_block->bytes() < vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM)) {
            continue;
        }

        RETURN_IF_ERROR(async_spill_block(partitioned_block->to_block(), state, i));
        partitioned_block.reset();
    }

    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetProbeSinkOperatorX<is_intersect>::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state);
}

template <bool is_intersect>
size_t PartitionedSetProbeSinkOperatorX<is_intersect>::revocable_mem_size(
        RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state._shared_state->need_to_spill) {
        return 0;
    }

    size_t revocable_size = 0;
    for (uint32_t i = 0; i != local_state._partition_count; ++i) {
        auto& partitioned_block = local_state._partitioned_blocks[i];
        if (partitioned_block &&
            partitioned_block->bytes() >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            revocable_size += partitioned_block->allocated_bytes();
        }
    }

    return revocable_size;
}

template class PartitionedSetProbeSinkLocalState<true>;
template class PartitionedSetProbeSinkLocalState<false>;
template class PartitionedSetProbeSinkOperatorX<true>;
template class PartitionedSetProbeSinkOperatorX<false>;

} // namespace doris::pipeline
