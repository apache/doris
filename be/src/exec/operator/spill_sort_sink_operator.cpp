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

#include "exec/operator/spill_sort_sink_operator.h"

#include "common/status.h"
#include "exec/operator/sort_sink_operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_writer.h"
#include "runtime/fragment_mgr.h"

namespace doris {
SpillSortSinkLocalState::SpillSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
        : Base(parent, state) {}

Status SpillSortSinkLocalState::init(doris::RuntimeState* state, doris::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _init_counters();
    RETURN_IF_ERROR(setup_in_memory_sort_op(state));

    Base::_shared_state->in_mem_shared_state->sorter->set_enable_spill();
    return Status::OK();
}

Status SpillSortSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
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
    if (_spilling_writer) {
        RETURN_IF_ERROR(_spilling_writer->close());
        _spilling_writer.reset();
    }
    _spilling_file.reset();
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

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = Base::_shared_state->in_mem_shared_state,
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(parent._sort_sink_operator->setup_local_state(_runtime_state.get(), info));
    auto* sink_local_state = _runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    RETURN_IF_ERROR(sink_local_state->open(state));

    custom_profile()->add_info_string(
            "TOP-N", *sink_local_state->custom_profile()->get_info_string("TOP-N"));
    return Status::OK();
}

bool SpillSortSinkLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

SpillSortSinkOperatorX::SpillSortSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                               const TPlanNode& tnode, const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id, dest_id) {
    _spillable = true;
    _sort_sink_operator =
            std::make_unique<SortSinkOperatorX>(pool, operator_id, dest_id, tnode, descs);
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
Status SpillSortSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state);
}

size_t SpillSortSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    auto mem_size = _sort_sink_operator->get_revocable_mem_size(local_state._runtime_state.get());
    return mem_size > state->spill_min_revocable_mem() ? mem_size : 0;
}

Status SpillSortSinkOperatorX::sink(doris::RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        local_state._shared_state->update_spill_block_batch_row_count(state, in_block);
    }
    DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::sink",
                    { return Status::InternalError("fault_inject spill_sort_sink sink failed"); });
    RETURN_IF_ERROR(_sort_sink_operator->sink(local_state._runtime_state.get(), in_block, false));

    int64_t data_size = local_state._shared_state->in_mem_shared_state->sorter->data_size();
    COUNTER_SET(local_state._memory_used_counter, data_size);

    // Proactive spill: if already spilled, flush when accumulated data exceeds threshold.
    if (local_state._shared_state->is_spilled) {
        if (revocable_mem_size(state) >= state->spill_sort_sink_mem_limit_bytes()) {
            RETURN_IF_ERROR(revoke_memory(state));
            DCHECK(revocable_mem_size(state) == 0);
        }
    }

    if (eos) {
        if (local_state._shared_state->is_spilled) {
            RETURN_IF_ERROR(revoke_memory(state));
            // TODO guolei: There is one step that needs to change the state.
        } else {
            RETURN_IF_ERROR(
                    local_state._shared_state->in_mem_shared_state->sorter->prepare_for_read(
                            false));
        }
        local_state._eos = eos;
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t SpillSortSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& parent = Base::_parent->template cast<Parent>();
    return parent._sort_sink_operator->get_reserve_mem_size_for_next_sink(_runtime_state.get(),
                                                                          eos);
}

Status SpillSortSinkLocalState::_execute_spill_sort(RuntimeState* state) {
    auto& parent = Base::_parent->template cast<Parent>();
    state->get_query_ctx()->resource_ctx()->task_controller()->increase_revoking_tasks_count();
    Defer defer {[&]() {
        _spilling_writer.reset();
        _spilling_file.reset();
        state->get_query_ctx()->resource_ctx()->task_controller()->decrease_revoking_tasks_count();
    }};

    RETURN_IF_ERROR(parent._sort_sink_operator->prepare_for_spill(_runtime_state.get()));

    auto* sink_local_state = _runtime_state->get_sink_local_state();
    update_profile(sink_local_state->custom_profile());

    bool eos = false;

    Block block;
    while (!eos && !state->is_cancelled()) {
        {
            SCOPED_TIMER(_spill_merge_sort_timer);
            // Currently, using 4096 as batch size, maybe using adptive size is better by using memory size.
            RETURN_IF_ERROR(parent._sort_sink_operator->merge_sort_read_for_spill(
                    _runtime_state.get(), &block, 4096, &eos));
        }
        RETURN_IF_ERROR(_spilling_writer->write_block(state, block));
        block.clear_column_data();
    }
    RETURN_IF_ERROR(_spilling_writer->close());
    RETURN_IF_ERROR(parent._sort_sink_operator->reset(_runtime_state.get()));
    return Status::OK();
}

Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state) {
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
    _spilling_file.reset();
    _spilling_writer.reset();
    auto relative_path =
            fmt::format("{}/{}-{}-{}-{}", print_id(state->query_id()), "sort", _parent->node_id(),
                        state->task_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                _spilling_file));
    RETURN_IF_ERROR(_spilling_file->create_writer(state, operator_profile(), _spilling_writer));
    _shared_state->sorted_spill_groups.emplace_back(_spilling_file);
    return _execute_spill_sort(state);
}
} // namespace doris