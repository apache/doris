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

#include "partitioned_set_sink_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "vec/common/hash_table/hash_table_set_build.h"
#include "vec/core/materialize_block.h"

namespace doris::pipeline {

template <bool is_intersect>
Status PartitionedSetSinkOperatorX<is_intersect>::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state);
}

template <bool is_intersect>
size_t PartitionedSetSinkOperatorX<is_intersect>::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state.revocable_mem_size(state);
}

template <bool is_intersect>
Status PartitionedSetSinkOperatorX<is_intersect>::sink(RuntimeState* state,
                                                       vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);

    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    local_state._child_eos = eos;

    if (local_state._shared_state->need_to_spill) {
        if (!local_state._spill_status.ok()) [[unlikely]] {
            return local_state._spill_status.status();
        }
        RETURN_IF_ERROR(local_state.partition_block(in_block, state));

        if (eos) {
            RETURN_IF_ERROR(local_state._make_spill_streams_eof());
            local_state._shared_state->probe_finished_children_dependency[1]->set_ready();
        }
    } else {
        RETURN_IF_ERROR(_inner_sink_operator->sink(
                local_state._shared_state->inner_runtime_state.get(), in_block, eos));
        if (eos) {
            local_state._shared_state->probe_finished_children_dependency[1]->set_ready();
        }
    }
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::init(RuntimeState* state,
                                                        LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _build_timer = ADD_TIMER(_profile, "BuildTime");
    auto& parent = _parent->cast<Parent>();
    _shared_state->probe_finished_children_dependency[parent._cur_child_id] = _dependency;
    DCHECK(parent._cur_child_id == 0);

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _shared_state->child_quantity = parent._child_quantity;
    _shared_state->partitioned_build_blocks.resize(_partition_count);
    _shared_state->spill_streams.resize(_partition_count);
    _shared_state->probe_spill_streams.resize(parent._child_quantity - 1);
    _shared_state->inner_probe_runtime_states.resize(parent._child_quantity);

    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(parent._partition_exprs));

    auto* spill_manager = ExecEnv::GetInstance()->spill_stream_mgr();
    for (size_t i = 0; i != _partition_count; ++i) {
        RETURN_IF_ERROR(spill_manager->register_spill_stream(
                state, _shared_state->spill_streams[i], print_id(state->query_id()),
                "SetSinkOperator", _parent->node_id(), std::numeric_limits<int32_t>::max(),
                std::numeric_limits<size_t>::max(), _profile));
        auto& spilling_stream = _shared_state->spill_streams[i];
        RETURN_IF_ERROR(spilling_stream->prepare_spill());
        spilling_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                            _spill_data_size, _spill_write_disk_timer,
                                            _spill_write_wait_io_timer);
    }

    RETURN_IF_ERROR(setup_inner_operator(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::setup_inner_operator(RuntimeState* state) {
    DCHECK(!_shared_state->inner_runtime_state);

    _shared_state->inner_runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    _shared_state->inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    _shared_state->inner_runtime_state->set_be_number(state->be_number());

    _shared_state->inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    _shared_state->inner_runtime_state->resize_op_id_to_local_state(-1);
    _shared_state->inner_runtime_state->set_pipeline_x_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    auto& parent = _parent->cast<PartitionedSetSinkOperatorX<is_intersect>>();
    _shared_state->inner_shared_state = std::dynamic_pointer_cast<SetSharedState>(
            parent._inner_sink_operator->create_shared_state());
    LocalSinkStateInfo info {0,  _internal_runtime_profile.get(),
                             -1, _shared_state->inner_shared_state.get(),
                             {}, {}};

    LocalStateInfo source_info {
            _internal_runtime_profile.get(), {}, _shared_state->inner_shared_state.get(), {}, 0};

    RETURN_IF_ERROR(parent._inner_source_operator->setup_local_state(
            _shared_state->inner_runtime_state.get(), source_info));

    RETURN_IF_ERROR(parent._inner_sink_operator->setup_local_state(
            _shared_state->inner_runtime_state.get(), info));
    auto* sink_local_state = _shared_state->inner_runtime_state->get_sink_local_state();
    auto* source_local_state = _shared_state->inner_runtime_state->get_local_state(
            parent._inner_source_operator->operator_id());
    DCHECK(sink_local_state != nullptr);
    DCHECK(source_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));
    RETURN_IF_ERROR(source_local_state->open(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    RETURN_IF_ERROR(_partitioner->prepare(state, _parent->child_x()->row_desc()));
    RETURN_IF_ERROR(_partitioner->open(state));
    return Status::OK();
}

template <bool is_intersect>
size_t PartitionedSetSinkLocalState<is_intersect>::revocable_mem_size(RuntimeState* state) const {
    if (_shared_state->need_to_spill) {
        size_t revocable_size = 0;
        for (uint32_t i = 0; i != _partition_count; ++i) {
            auto& partitioned_block = _shared_state->partitioned_build_blocks[i];
            if (partitioned_block &&
                partitioned_block->bytes() >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                revocable_size += partitioned_block->allocated_bytes();
            }
        }
        return revocable_size;
    } else {
        if (!_shared_state->inner_runtime_state) {
            return 0;
        }

        auto* inner_sink_state_ = _shared_state->inner_runtime_state->get_sink_local_state();
        if (!inner_sink_state_) {
            return 0;
        }

        auto* inner_sink_state = assert_cast<SetSinkLocalState<is_intersect>*>(inner_sink_state_);
        return inner_sink_state->_mutable_block.allocated_bytes();
    }
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::partition_block(vectorized::Block* block,
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

        auto& partitioned_block = _shared_state->partitioned_build_blocks[i];

        if (!partitioned_block) {
            partitioned_block = vectorized::MutableBlock::create_unique(block->clone_empty());
        }

        const auto* begin = partitioned_indexes[i].data();
        RETURN_IF_ERROR(partitioned_block->add_rows(block, begin, begin + partition_rows));
    }

    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::revoke_memory(RuntimeState* state) {
    if (!_shared_state->need_to_spill) {
        _shared_state->need_to_spill = true;
        return revoke_unpartitioned_block(state);
    }

    for (uint32_t i = 0; i != _partition_count; ++i) {
        auto& partitioned_block = _shared_state->partitioned_build_blocks[i];
        if (!partitioned_block ||
            partitioned_block->bytes() < vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            continue;
        }

        RETURN_IF_ERROR(async_spill_block(partitioned_block->to_block(), state, i));
        partitioned_block.reset();
    }

    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::async_spill_block(vectorized::Block&& block,
                                                                     RuntimeState* state,
                                                                     uint32_t partition_idx) {
    auto& stream = _shared_state->spill_streams[partition_idx];
    _dependency->block();
    _spilling_tasks_count++;

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
                            auto st = _make_spill_streams_eof();
                            if (!st.ok()) {
                                _spill_status.update(st);
                                state->cancel(st);
                                return;
                            }
                            _shared_state->probe_finished_children_dependency[1]->set_ready();
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
    auto* spill_stream_manager = ExecEnv::GetInstance()->spill_stream_mgr();
    auto* thread_pool = spill_stream_manager->get_spill_io_thread_pool();
    return thread_pool->submit(std::move(spill_runnable));
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::revoke_unpartitioned_block(RuntimeState* state) {
    DCHECK(_shared_state->inner_runtime_state);
    auto* inner_sink_state_ = _shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(inner_sink_state_);

    auto* inner_sink_state = assert_cast<SetSinkLocalState<is_intersect>*>(inner_sink_state_);

    auto block = inner_sink_state->_mutable_block.to_block();
    inner_sink_state->_mutable_block.clear();

    const size_t rows = block.rows();
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, &block, _mem_tracker.get()));

    const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();

    std::vector<std::vector<uint32_t>> partitioned_indexes(_partition_count);
    for (size_t i = 0; i != rows; ++i) {
        partitioned_indexes[channel_ids[i]].emplace_back(i);
    }

    for (uint32_t i = 0; i != _partition_count; ++i) {
        auto rows_available = partitioned_indexes[i].size();
        if (rows_available == 0) [[unlikely]] {
            continue;
        }

        auto& partitioned_block = _shared_state->partitioned_build_blocks[i];

        auto* begin = partitioned_indexes[i].data();
        partitioned_block = vectorized::MutableBlock::create_unique(block.clone_empty());
        RETURN_IF_ERROR(partitioned_block->add_rows(&block, begin, begin + rows_available));
        if (partitioned_block->allocated_bytes() >=
            vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            RETURN_IF_ERROR(async_spill_block(partitioned_block->to_block(), state, i));
            partitioned_block = vectorized::MutableBlock::create_unique(block.clone_empty());
        }
    }

    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkLocalState<is_intersect>::_make_spill_streams_eof() {
    bool spill_oef = false;
    if (!_spill_eof.compare_exchange_strong(spill_oef, true)) {
        return Status::OK();
    }

    for (auto& stream : _shared_state->spill_streams) {
        if (stream) {
            RETURN_IF_ERROR(stream->spill_eof());
        }
    }
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkOperatorX<is_intersect>::init(const TPlanNode& tnode,
                                                       RuntimeState* state) {
    Base::_name = "PARTITIONED_SET_SINK_OPERATOR";
    RETURN_IF_ERROR(_inner_sink_operator->init(tnode, state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkOperatorX<is_intersect>::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_child_x));
    RETURN_IF_ERROR(_inner_sink_operator->prepare(state));
    return Status::OK();
}

template <bool is_intersect>
Status PartitionedSetSinkOperatorX<is_intersect>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    RETURN_IF_ERROR(_inner_sink_operator->open(state));
    return Status::OK();
}

template class PartitionedSetSinkLocalState<true>;
template class PartitionedSetSinkLocalState<false>;
template class PartitionedSetSinkOperatorX<true>;
template class PartitionedSetSinkOperatorX<false>;

} // namespace doris::pipeline
