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

#include "partitioned_set_source_operator.h"

#include <memory>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

template <bool is_intersect>
Status PartitionedSetSourceLocalState<is_intersect>::init(RuntimeState* state,
                                                          LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    auto child_quantity =
            _parent->cast<PartitionedSetSourceOperatorX<is_intersect>>()._child_quantity;

    _shared_state->probe_finished_children_dependency.resize(child_quantity, nullptr);

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _probe_runtime_states.resize(child_quantity);

    _build_rows_counter = ADD_COUNTER_WITH_LEVEL(profile(), "BuildRows", TUnit::UNIT, 1);
    for (uint32_t i = 1; i != child_quantity; ++i) {
        _probe_rows_counter_map[i] =
                ADD_COUNTER_WITH_LEVEL(profile(), fmt::format("ProbeRows[{}]", i), TUnit::UNIT, 1);
    }
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSourceLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSourceLocalState<is_intersect>::do_partitioned_probe(RuntimeState* state) {
    DCHECK(_shared_state->need_to_spill);
    auto& parent = _parent->cast<PartitionedSetSourceOperatorX<is_intersect>>();
    auto probe_operator = parent._inner_probe_sink_operators[_probe_child_cursor];
    auto* inner_state = _probe_runtime_states[_probe_child_cursor].get();

    auto& probe_spill_streams = _shared_state->probe_spill_streams[_probe_child_cursor];
    DCHECK_EQ(probe_spill_streams.size(), _partition_count);

    auto& stream = probe_spill_streams[_partition_cursor];
    if (!stream) {
        _probe_child_cursor++;
        vectorized::Block block;
        return probe_operator->sink(inner_state, &block, true);
    }

    stream->set_read_counters(_spill_read_data_time, _spill_deserialize_time, _spill_read_bytes,
                              _spill_read_wait_io_timer);

    auto* io_thread_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();

    _dependency->block();
    return io_thread_pool->submit(std::make_shared<SpillRunnable>(
            state, _shared_state->shared_from_this(),
            [this, state, inner_state, stream = std::move(stream), probe_operator]() {
                bool eos = false;
                Status status;
                while (!eos && status.ok() && !state->is_cancelled()) {
                    vectorized::Block block;
                    status = stream->read_next_block_sync(&block, &eos);
                    if (status.ok() && (eos || !block.empty())) {
                        COUNTER_UPDATE(_probe_rows_counter_map[_probe_child_cursor + 1],
                                       block.rows());
                        status = probe_operator->sink(inner_state, &block, eos);
                    }
                }

                if (!status.ok()) {
                    _spill_status.update(status);
                }
                _probe_child_cursor.fetch_add(1);
                _dependency->set_ready();
                VLOG_DEBUG << "query: " << print_id(state->query_id())
                           << ", internal set probe operator(" << _probe_child_cursor
                           << ") finished, node id: " << _parent->node_id()
                           << ", task id: " << state->task_id()
                           << ", partition: " << _partition_cursor;
            }));
}

template <bool is_intersect>
Status PartitionedSetSourceLocalState<is_intersect>::recovery_build_blocks(RuntimeState* state) {
    auto& stream = _shared_state->spill_streams[_partition_cursor];
    DCHECK(stream);

    stream->set_read_counters(_spill_read_data_time, _spill_deserialize_time, _spill_read_bytes,
                              _spill_read_wait_io_timer);

    auto* io_thread_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    auto& partitioned_block = _shared_state->partitioned_build_blocks[_partition_cursor];
    _dependency->block();
    return io_thread_pool->submit(std::make_shared<SpillRunnable>(
            state, _shared_state->shared_from_this(),
            [this, state, &partitioned_block, stream = std::move(stream)]() {
                bool eos = false;
                vectorized::Block block;
                Status status;
                while (!eos && !state->is_cancelled() && status.ok()) {
                    status = stream->read_next_block_sync(&block, &eos);
                    if (status.ok() && !block.empty()) {
                        if (!partitioned_block) [[unlikely]] {
                            partitioned_block =
                                    vectorized::MutableBlock::create_unique(std::move(block));
                        } else {
                            status = partitioned_block->merge(block);
                        }
                    }
                }

                if (!status.ok()) {
                    _spill_status.update(status);
                }
                _dependency->set_ready();
            }));
}

template <bool is_intersect>
Status PartitionedSetSourceLocalState<is_intersect>::setup_internal_operators(RuntimeState* state) {
    auto& stream = _shared_state->spill_streams[_partition_cursor];
    if (stream) {
        return recovery_build_blocks(state);
    }

    _need_to_setup_internal_operators = false;
    _probe_child_cursor = 0;
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(-1);
    _runtime_state->set_pipeline_x_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = _parent->cast<PartitionedSetSourceOperatorX<is_intersect>>();

    _in_mem_shared_state_sptr = parent._inner_sink_operator->create_shared_state();

    LocalStateInfo state_info {
            _internal_runtime_profile.get(), {}, _in_mem_shared_state_sptr.get(), {}, 0};
    RETURN_IF_ERROR(
            parent._inner_source_operator->setup_local_state(_runtime_state.get(), state_info));

    // set sink local state
    LocalSinkStateInfo info {
            0, _internal_runtime_profile.get(), -1, _in_mem_shared_state_sptr.get(), {}, {}};
    RETURN_IF_ERROR(parent._inner_sink_operator->setup_local_state(_runtime_state.get(), info));

    auto* sink_local_state = _runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* source_local_state =
            _runtime_state->get_local_state(parent._inner_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    RETURN_IF_ERROR(source_local_state->open(state));

    for (size_t i = 0; i != parent._inner_probe_sink_operators.size(); ++i) {
        auto& probe_sink_operator = parent._inner_probe_sink_operators[i];
        _probe_runtime_states[i] = RuntimeState::create_unique(
                nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
                state->query_options(), TQueryGlobals {}, state->exec_env(),
                state->get_query_ctx());
        _probe_runtime_states[i]->set_task_execution_context(
                state->get_task_execution_context().lock());
        _probe_runtime_states[i]->set_be_number(state->be_number());
        _probe_runtime_states[i]->set_desc_tbl(&state->desc_tbl());
        _probe_runtime_states[i]->resize_op_id_to_local_state(-1);

        LocalSinkStateInfo probe_sink_info {
                0, _internal_runtime_profile.get(), -1, _in_mem_shared_state_sptr.get(), {}, {}};
        RETURN_IF_ERROR(probe_sink_operator->setup_local_state(_probe_runtime_states[i].get(),
                                                               probe_sink_info));
        auto* probe_sink_state = _probe_runtime_states[i]->get_sink_local_state();
        RETURN_IF_ERROR(probe_sink_state->open(state));
    }

    auto& partitioned_block = _shared_state->partitioned_build_blocks[_partition_cursor];
    vectorized::Block block;
    if (partitioned_block && partitioned_block->rows() > 0) {
        block = partitioned_block->to_block();
        partitioned_block.reset();
        COUNTER_UPDATE(_build_rows_counter, block.rows());
    }

    RETURN_IF_ERROR(parent._inner_sink_operator->sink(_runtime_state.get(), &block, true));
    VLOG_DEBUG << "query: " << print_id(state->query_id())
               << ", internal build operator finished, node id: " << parent.node_id()
               << ", task id: " << state->task_id() << ", partition: " << _partition_cursor;
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSourceOperatorX<is_intersect>::init(const TPlanNode& tnode,
                                                         RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    RETURN_IF_ERROR(_inner_source_operator->init(tnode, state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSourceOperatorX<is_intersect>::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(_inner_source_operator->prepare(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSourceOperatorX<is_intersect>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    RETURN_IF_ERROR(_inner_source_operator->open(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSourceOperatorX<is_intersect>::get_block(RuntimeState* state,
                                                              vectorized::Block* block, bool* eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    *eos = false;
    if (local_state._shared_state->need_to_spill) {
        if (!local_state._spill_status.ok()) [[unlikely]] {
            return local_state._spill_status.status();
        }

        if (local_state._need_to_setup_internal_operators) {
            DCHECK_EQ(local_state._dependency->is_blocked_by(nullptr), nullptr);
            return local_state.setup_internal_operators(state);
        }

        if (local_state._probe_child_cursor < _child_quantity - 1) {
            DCHECK_EQ(local_state._dependency->is_blocked_by(nullptr), nullptr);
            return local_state.do_partitioned_probe(state);
        }

        bool inner_eos = false;
        RETURN_IF_ERROR(_inner_source_operator->get_block(local_state._runtime_state.get(), block,
                                                          &inner_eos));

        if (inner_eos) {
            local_state._partition_cursor++;
            if (local_state._partition_cursor < local_state._partition_count) {
                local_state._need_to_setup_internal_operators = true;
            } else {
                *eos = true;
            }
        }
    } else {
        if (!local_state._in_mem_shared_state_sptr) {
            local_state._in_mem_shared_state_sptr = local_state._shared_state->shared_from_this();
        }

        auto* inner_state = local_state._shared_state->inner_runtime_state.get();
        RETURN_IF_ERROR(_inner_source_operator->get_block(inner_state, block, eos));
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

template class PartitionedSetSourceLocalState<true>;
template class PartitionedSetSourceLocalState<false>;
template class PartitionedSetSourceOperatorX<true>;
template class PartitionedSetSourceOperatorX<false>;

} // namespace doris::pipeline
