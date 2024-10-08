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

#include "spill_sort_source_operator.h"

#include "common/status.h"
#include "pipeline/exec/spill_utils.h"
#include "runtime/fragment_mgr.h"
#include "sort_source_operator.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
SpillSortLocalState::SpillSortLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {
    if (state->external_sort_bytes_threshold() > 0) {
        _external_sort_bytes_threshold = state->external_sort_bytes_threshold();
    }
}
Status SpillSortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _spill_timer = ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillMergeSortTime", "Spill", 1);
    _spill_merge_sort_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillMergeSortTime", "Spill", 1);
    _spill_serialize_block_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillSerializeBlockTime", "Spill", 1);
    _spill_write_disk_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWriteDiskTime", "Spill", 1);
    _spill_data_size = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillWriteDataSize",
                                                    TUnit::BYTES, "Spill", 1);
    _spill_block_count = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillWriteBlockCount",
                                                      TUnit::UNIT, "Spill", 1);
    _spill_wait_in_queue_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile(), "SpillWaitInQueueTime", "Spill", 1);
    return Status::OK();
}

Status SpillSortLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }
    RETURN_IF_ERROR(setup_in_memory_sort_op(state));
    return Base::open(state);
}

Status SpillSortLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    dec_running_big_mem_op_num(state);
    return Base::close(state);
}
int SpillSortLocalState::_calc_spill_blocks_to_merge() const {
    int count = _external_sort_bytes_threshold / SpillSortSharedState::SORT_BLOCK_SPILL_BATCH_BYTES;
    return std::max(2, count);
}
Status SpillSortLocalState::initiate_merge_sort_spill_streams(RuntimeState* state) {
    auto& parent = Base::_parent->template cast<Parent>();
    VLOG_DEBUG << "query " << print_id(state->query_id()) << " sort node " << _parent->node_id()
               << " merge spill data";
    _dependency->Dependency::block();

    auto query_id = state->query_id();

    MonotonicStopWatch submit_timer;
    submit_timer.start();

    auto spill_func = [this, state, query_id, &parent, submit_timer] {
        _spill_wait_in_queue_timer->update(submit_timer.elapsed_time());
        SCOPED_TIMER(_spill_merge_sort_timer);
        Defer defer {[&]() {
            if (!_status.ok() || state->is_cancelled()) {
                if (!_status.ok()) {
                    LOG(WARNING) << "query " << print_id(query_id) << " sort node "
                                 << _parent->node_id() << " merge spill data error: " << _status;
                }
                _shared_state->close();
                for (auto& stream : _current_merging_streams) {
                    (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                }
                _current_merging_streams.clear();
            } else {
                VLOG_DEBUG << "query " << print_id(query_id) << " sort node " << _parent->node_id()
                           << " merge spill data finish";
            }
            _dependency->Dependency::set_ready();
        }};
        vectorized::Block merge_sorted_block;
        vectorized::SpillStreamSPtr tmp_stream;
        while (!state->is_cancelled()) {
            int max_stream_count = _calc_spill_blocks_to_merge();
            VLOG_DEBUG << "query " << print_id(query_id) << " sort node " << _parent->node_id()
                       << " merge spill streams, streams count: "
                       << _shared_state->sorted_streams.size()
                       << ", curren merge max stream count: " << max_stream_count;
            {
                SCOPED_TIMER(Base::_spill_recover_time);
                _status = _create_intermediate_merger(
                        max_stream_count,
                        parent._sort_source_operator->get_sort_description(_runtime_state.get()));
            }
            RETURN_IF_ERROR(_status);

            // all the remaining streams can be merged in a run
            if (_shared_state->sorted_streams.empty()) {
                return Status::OK();
            }

            {
                _status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, tmp_stream, print_id(state->query_id()), "sort", _parent->node_id(),
                        _shared_state->spill_block_batch_row_count,
                        SpillSortSharedState::SORT_BLOCK_SPILL_BATCH_BYTES, profile());
                RETURN_IF_ERROR(_status);
                _status = tmp_stream->prepare_spill();
                RETURN_IF_ERROR(_status);

                _shared_state->sorted_streams.emplace_back(tmp_stream);

                bool eos = false;
                tmp_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                               _spill_data_size, _spill_write_disk_timer,
                                               _spill_write_wait_io_timer);
                while (!eos && !state->is_cancelled()) {
                    merge_sorted_block.clear_column_data();
                    {
                        SCOPED_TIMER(Base::_spill_recover_time);
                        DBUG_EXECUTE_IF("fault_inject::spill_sort_source::recover_spill_data", {
                            _status = Status::Error<INTERNAL_ERROR>(
                                    "fault_inject spill_sort_source "
                                    "recover_spill_data failed");
                        });
                        if (_status.ok()) {
                            _status = _merger->get_next(&merge_sorted_block, &eos);
                        }
                    }
                    RETURN_IF_ERROR(_status);
                    _status = tmp_stream->spill_block(state, merge_sorted_block, eos);
                    if (_status.ok()) {
                        DBUG_EXECUTE_IF("fault_inject::spill_sort_source::spill_merged_data", {
                            _status = Status::Error<INTERNAL_ERROR>(
                                    "fault_inject spill_sort_source "
                                    "spill_merged_data failed");
                        });
                    }
                    RETURN_IF_ERROR(_status);
                }
            }
            for (auto& stream : _current_merging_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            _current_merging_streams.clear();
        }
        return Status::OK();
    };

    auto exception_catch_func = [this, spill_func]() {
        _status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
    };

    DBUG_EXECUTE_IF("fault_inject::spill_sort_source::merge_sort_spill_data_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject spill_sort_source "
                "merge_sort_spill_data submit_func failed");
    });
    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::make_shared<SpillRunnable>(state, _shared_state->shared_from_this(),
                                            exception_catch_func));
}

Status SpillSortLocalState::_create_intermediate_merger(
        int num_blocks, const vectorized::SortDescription& sort_description) {
    std::vector<vectorized::BlockSupplier> child_block_suppliers;
    _merger = std::make_unique<vectorized::VSortedRunMerger>(
            sort_description, _shared_state->spill_block_batch_row_count,
            Base::_shared_state->in_mem_shared_state->sorter->limit(),
            Base::_shared_state->in_mem_shared_state->sorter->offset(), profile());

    _current_merging_streams.clear();
    for (int i = 0; i < num_blocks && !_shared_state->sorted_streams.empty(); ++i) {
        auto stream = _shared_state->sorted_streams.front();
        stream->set_read_counters(Base::_spill_read_data_time, Base::_spill_deserialize_time,
                                  Base::_spill_read_bytes, Base::_spill_read_wait_io_timer);
        _current_merging_streams.emplace_back(stream);
        child_block_suppliers.emplace_back(
                std::bind(std::mem_fn(&vectorized::SpillStream::read_next_block_sync), stream.get(),
                          std::placeholders::_1, std::placeholders::_2));

        _shared_state->sorted_streams.pop_front();
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}
Status SpillSortLocalState::setup_in_memory_sort_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    DCHECK(_shared_state->in_mem_shared_state);
    LocalStateInfo state_info {
            _internal_runtime_profile.get(), {}, _shared_state->in_mem_shared_state, {}, 0};

    auto& parent = Base::_parent->template cast<Parent>();
    RETURN_IF_ERROR(
            parent._sort_source_operator->setup_local_state(_runtime_state.get(), state_info));

    auto* source_local_state =
            _runtime_state->get_local_state(parent._sort_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    return source_local_state->open(state);
}
SpillSortSourceOperatorX::SpillSortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                   int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {
    _sort_source_operator = std::make_unique<SortSourceOperatorX>(pool, tnode, operator_id, descs);
}
Status SpillSortSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    _op_name = "SPILL_SORT_SOURCE_OPERATOR";
    return _sort_source_operator->init(tnode, state);
}

Status SpillSortSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::open(state));
    return _sort_source_operator->open(state);
}

Status SpillSortSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _sort_source_operator->close(state);
}

Status SpillSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) {
    auto& local_state = get_local_state(state);
    Defer defer {[&]() {
        if (!local_state._status.ok() || *eos) {
            local_state._shared_state->close();
            for (auto& stream : local_state._current_merging_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            local_state._current_merging_streams.clear();
        }
    }};
    local_state.inc_running_big_mem_op_num(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._status);

    if (local_state._shared_state->is_spilled) {
        if (!local_state._merger) {
            local_state._status = local_state.initiate_merge_sort_spill_streams(state);
            return local_state._status;
        } else {
            local_state._status = local_state._merger->get_next(block, eos);
            RETURN_IF_ERROR(local_state._status);
        }
    } else {
        local_state._status =
                _sort_source_operator->get_block(local_state._runtime_state.get(), block, eos);
        RETURN_IF_ERROR(local_state._status);
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}
} // namespace doris::pipeline