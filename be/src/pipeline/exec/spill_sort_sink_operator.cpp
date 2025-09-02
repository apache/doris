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

#include "spill_sort_sink_operator.h"

#include "common/status.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
SpillSortSinkLocalState::SpillSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
        : Base(parent, state) {}

Status SpillSortSinkLocalState::init(doris::RuntimeState* state,
                                     doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _init_counters();

    _spill_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                  "SortSinkSpillDependency", true);
    RETURN_IF_ERROR(setup_in_memory_sort_op(state));

    Base::_shared_state->in_mem_shared_state->sorter->set_enable_spill();
    return Status::OK();
}

Status SpillSortSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    _shared_state->setup_shared_profile(custom_profile());
    return Base::open(state);
}

void SpillSortSinkLocalState::_init_counters() {
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _spill_merge_sort_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillMergeSortTime", 1);
}

#define UPDATE_PROFILE(name) \
    update_profile_from_inner_profile<true>(name, custom_profile(), child_profile)

void SpillSortSinkLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE("PartialSortTime");
    UPDATE_PROFILE("MergeBlockTime");
    UPDATE_PROFILE("MemoryUsageSortBlocks");
}
#undef UPDATE_PROFILE

Status SpillSortSinkLocalState::close(RuntimeState* state, Status execsink_status) {
    return Base::close(state, execsink_status);
}

Status SpillSortSinkLocalState::setup_in_memory_sort_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->in_mem_shared_state_sptr =
            parent._sort_sink_operator->create_shared_state();
    Base::_shared_state->in_mem_shared_state =
            static_cast<SortSharedState*>(Base::_shared_state->in_mem_shared_state_sptr.get());

    LocalSinkStateInfo info {0,  _internal_runtime_profile.get(),
                             -1, Base::_shared_state->in_mem_shared_state,
                             {}, {}};
    RETURN_IF_ERROR(parent._sort_sink_operator->setup_local_state(_runtime_state.get(), info));
    auto* sink_local_state = _runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    RETURN_IF_ERROR(sink_local_state->open(state));

    custom_profile()->add_info_string(
            "TOP-N", *sink_local_state->custom_profile()->get_info_string("TOP-N"));
    return Status::OK();
}

SpillSortSinkOperatorX::SpillSortSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                               const TPlanNode& tnode, const DescriptorTbl& descs,
                                               bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode.node_id, dest_id) {
    _spillable = true;
    _sort_sink_operator = std::make_unique<SortSinkOperatorX>(pool, operator_id, dest_id, tnode,
                                                              descs, require_bucket_distribution);
}

Status SpillSortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    _name = "SPILL_SORT_SINK_OPERATOR";
    return _sort_sink_operator->init(tnode, state);
}

Status SpillSortSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::prepare(state));
    return _sort_sink_operator->prepare(state);
}

size_t SpillSortSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}
Status SpillSortSinkOperatorX::revoke_memory(RuntimeState* state,
                                             const std::shared_ptr<SpillContext>& spill_context) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state, spill_context);
}

size_t SpillSortSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return _sort_sink_operator->get_revocable_mem_size(local_state._runtime_state.get());
}

Status SpillSortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                                    bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        local_state._shared_state->update_spill_block_batch_row_count(state, in_block);
    }
    local_state._eos = eos;
    DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::sink",
                    { return Status::InternalError("fault_inject spill_sort_sink sink failed"); });
    RETURN_IF_ERROR(_sort_sink_operator->sink(local_state._runtime_state.get(), in_block, false));

    int64_t data_size = local_state._shared_state->in_mem_shared_state->sorter->data_size();
    COUNTER_SET(local_state._memory_used_counter, data_size);

    if (eos) {
        if (local_state._shared_state->is_spilled) {
            if (revocable_mem_size(state) > 0) {
                RETURN_IF_ERROR(revoke_memory(state, nullptr));
            } else {
                local_state._dependency->set_ready_to_read();
            }
        } else {
            RETURN_IF_ERROR(
                    local_state._shared_state->in_mem_shared_state->sorter->prepare_for_read(
                            false));
            local_state._dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}

size_t SpillSortSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& parent = Base::_parent->template cast<Parent>();
    return parent._sort_sink_operator->get_reserve_mem_size_for_next_sink(_runtime_state.get(),
                                                                          eos);
}

Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state,
                                              const std::shared_ptr<SpillContext>& spill_context) {
    auto& parent = Base::_parent->template cast<Parent>();
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        _shared_state->limit = parent._sort_sink_operator->limit();
        _shared_state->offset = parent._sort_sink_operator->offset();
        custom_profile()->add_info_string("Spilled", "true");
    }

    VLOG_DEBUG << fmt::format("Query:{}, sort sink:{}, task:{}, revoke_memory, eos:{}",
                              print_id(state->query_id()), _parent->node_id(), state->task_id(),
                              _eos);

    int32_t batch_size =
            _shared_state->spill_block_batch_row_count > std::numeric_limits<int32_t>::max()
                    ? std::numeric_limits<int32_t>::max()
                    : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
    auto status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            state, _spilling_stream, print_id(state->query_id()), "sort", _parent->node_id(),
            batch_size, state->spill_sort_batch_bytes(), operator_profile());
    RETURN_IF_ERROR(status);

    _shared_state->sorted_streams.emplace_back(_spilling_stream);

    auto query_id = state->query_id();

    auto spill_func = [this, state, query_id, &parent] {
        Status status;
        Defer defer {[&]() {
            if (!status.ok() || state->is_cancelled()) {
                if (!status.ok()) {
                    LOG(WARNING) << fmt::format(
                            "Query:{}, sort sink:{}, task:{}, revoke memory error:{}",
                            print_id(query_id), _parent->node_id(), state->task_id(), status);
                }
                _shared_state->close();
            } else {
                VLOG_DEBUG << fmt::format("Query:{}, sort sink:{}, task:{}, revoke memory finish",
                                          print_id(query_id), _parent->node_id(), state->task_id());
            }

            if (!status.ok()) {
                _shared_state->close();
            }

            _spilling_stream.reset();
            state->get_query_ctx()
                    ->resource_ctx()
                    ->task_controller()
                    ->decrease_revoking_tasks_count();
            if (_eos) {
                _dependency->set_ready_to_read();
            }
        }};

        status = parent._sort_sink_operator->prepare_for_spill(_runtime_state.get());
        RETURN_IF_ERROR(status);

        auto* sink_local_state = _runtime_state->get_sink_local_state();
        update_profile(sink_local_state->custom_profile());

        bool eos = false;
        vectorized::Block block;

        int32_t batch_size =
                _shared_state->spill_block_batch_row_count > std::numeric_limits<int32_t>::max()
                        ? std::numeric_limits<int32_t>::max()
                        : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
        while (!eos && !state->is_cancelled()) {
            {
                SCOPED_TIMER(_spill_merge_sort_timer);
                status = parent._sort_sink_operator->merge_sort_read_for_spill(
                        _runtime_state.get(), &block, batch_size, &eos);
            }
            RETURN_IF_ERROR(status);
            status = _spilling_stream->spill_block(state, block, eos);
            RETURN_IF_ERROR(status);
            block.clear_column_data();
        }
        parent._sort_sink_operator->reset(_runtime_state.get());

        return Status::OK();
    };

    auto exception_catch_func = [query_id, spill_func]() {
        DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::revoke_memory_cancel", {
            auto status = Status::InternalError(
                    "fault_inject spill_sort_sink "
                    "revoke_memory canceled");
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_id, status);
            return status;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();

        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::revoke_memory_submit_func", {
        status = Status::Error<INTERNAL_ERROR>(
                "fault_inject spill_sort_sink "
                "revoke_memory submit_func failed");
    });

    RETURN_IF_ERROR(status);
    state->get_query_ctx()->resource_ctx()->task_controller()->increase_revoking_tasks_count();

    _spill_dependency->block();
    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::make_shared<SpillSinkRunnable>(
                    state, spill_context, _spill_dependency, operator_profile(),
                    _shared_state->shared_from_this(), exception_catch_func));
}
#include "common/compile_check_end.h"
} // namespace doris::pipeline