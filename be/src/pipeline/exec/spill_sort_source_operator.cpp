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

#include <glog/logging.h>

#include <cstdint>
#include <limits>

#include "common/status.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "sort_source_operator.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
SpillSortLocalState::SpillSortLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status SpillSortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    init_spill_write_counters();
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _spill_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                  "SortSourceSpillDependency", true);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _spill_merge_sort_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillMergeSortTime", 1);
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
    return Base::close(state);
}

int SpillSortLocalState::_calc_spill_blocks_to_merge(RuntimeState* state) const {
    auto count = state->spill_sort_mem_limit() / state->spill_sort_batch_bytes();
    if (count > std::numeric_limits<int>::max()) [[unlikely]] {
        return std::numeric_limits<int>::max();
    }
    return std::max(2, static_cast<int32_t>(count));
}

Status SpillSortLocalState::initiate_merge_sort_spill_streams(RuntimeState* state) {
    auto& parent = Base::_parent->template cast<Parent>();
    VLOG_DEBUG << fmt::format("Query:{}, sort source:{}, task:{}, merge spill data",
                              print_id(state->query_id()), _parent->node_id(), state->task_id());
    _spill_dependency->Dependency::block();

    auto query_id = state->query_id();

    auto spill_func = [this, state, query_id, &parent] {
        SCOPED_TIMER(_spill_merge_sort_timer);
        Status status;
        Defer defer {[&]() {
            if (!status.ok() || state->is_cancelled()) {
                if (!status.ok()) {
                    LOG(WARNING) << fmt::format(
                            "Query:{}, sort source:{}, task:{}, merge spill data error:{}",
                            print_id(query_id), _parent->node_id(), state->task_id(), status);
                }
                _shared_state->close();
                for (auto& stream : _current_merging_streams) {
                    (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                }
                _current_merging_streams.clear();
            } else {
                VLOG_DEBUG << fmt::format(
                        "Query:{}, sort source:{}, task:{}, merge spill data finish",
                        print_id(query_id), _parent->node_id(), state->task_id());
            }
        }};
        vectorized::Block merge_sorted_block;
        vectorized::SpillStreamSPtr tmp_stream;
        while (!state->is_cancelled()) {
            int max_stream_count = _calc_spill_blocks_to_merge(state);
            VLOG_DEBUG << fmt::format(
                    "Query:{}, sort source:{}, task:{}, merge spill streams, streams count:{}, "
                    "curren merge max stream count:{}",
                    print_id(query_id), _parent->node_id(), state->task_id(),
                    _shared_state->sorted_streams.size(), max_stream_count);
            {
                SCOPED_TIMER(Base::_spill_recover_time);
                status = _create_intermediate_merger(
                        max_stream_count,
                        parent._sort_source_operator->get_sort_description(_runtime_state.get()));
            }
            RETURN_IF_ERROR(status);

            // all the remaining streams can be merged in a run
            if (_shared_state->sorted_streams.empty()) {
                return Status::OK();
            }

            {
                int32_t batch_size =
                        _shared_state->spill_block_batch_row_count >
                                        std::numeric_limits<int32_t>::max()
                                ? std::numeric_limits<int32_t>::max()
                                : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
                status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, tmp_stream, print_id(state->query_id()), "sort", _parent->node_id(),
                        batch_size, state->spill_sort_batch_bytes(), operator_profile());
                RETURN_IF_ERROR(status);

                _shared_state->sorted_streams.emplace_back(tmp_stream);

                bool eos = false;
                while (!eos && !state->is_cancelled()) {
                    merge_sorted_block.clear_column_data();
                    {
                        SCOPED_TIMER(Base::_spill_recover_time);
                        DBUG_EXECUTE_IF("fault_inject::spill_sort_source::recover_spill_data", {
                            status = Status::Error<INTERNAL_ERROR>(
                                    "fault_inject spill_sort_source "
                                    "recover_spill_data failed");
                        });
                        if (status.ok()) {
                            status = _merger->get_next(&merge_sorted_block, &eos);
                        }
                    }
                    RETURN_IF_ERROR(status);
                    status = tmp_stream->spill_block(state, merge_sorted_block, eos);
                    if (status.ok()) {
                        DBUG_EXECUTE_IF("fault_inject::spill_sort_source::spill_merged_data", {
                            status = Status::Error<INTERNAL_ERROR>(
                                    "fault_inject spill_sort_source "
                                    "spill_merged_data failed");
                        });
                    }
                    RETURN_IF_ERROR(status);
                }
            }
            for (auto& stream : _current_merging_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            _current_merging_streams.clear();
        }
        return Status::OK();
    };

    auto exception_catch_func = [spill_func]() {
        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::spill_sort_source::merge_sort_spill_data_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject spill_sort_source "
                "merge_sort_spill_data submit_func failed");
    });

    return ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->submit(
            std::make_shared<SpillRecoverRunnable>(state, _spill_dependency, operator_profile(),
                                                   _shared_state->shared_from_this(),
                                                   exception_catch_func));
}

Status SpillSortLocalState::_create_intermediate_merger(
        int num_blocks, const vectorized::SortDescription& sort_description) {
    std::vector<vectorized::BlockSupplier> child_block_suppliers;
    int64_t limit = -1;
    int64_t offset = 0;
    if (num_blocks >= _shared_state->sorted_streams.size()) {
        // final round use real limit and offset
        limit = Base::_shared_state->limit;
        offset = Base::_shared_state->offset;
    }

    _merger = std::make_unique<vectorized::VSortedRunMerger>(
            sort_description, _runtime_state->batch_size(), limit, offset, custom_profile());

    _current_merging_streams.clear();
    for (int i = 0; i < num_blocks && !_shared_state->sorted_streams.empty(); ++i) {
        auto stream = _shared_state->sorted_streams.front();
        stream->set_read_counters(operator_profile());
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
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
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

Status SpillSortSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _sort_source_operator->prepare(state);
}

Status SpillSortSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _sort_source_operator->close(state);
}

Status SpillSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) {
    auto& local_state = get_local_state(state);
    local_state.copy_shared_spill_profile();
    Status status;
    Defer defer {[&]() {
        if (!status.ok() || *eos) {
            local_state._shared_state->close();
            for (auto& stream : local_state._current_merging_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            local_state._current_merging_streams.clear();
        }
    }};
    SCOPED_TIMER(local_state.exec_time_counter());

    if (local_state._shared_state->is_spilled) {
        if (!local_state._merger) {
            status = local_state.initiate_merge_sort_spill_streams(state);
            return status;
        } else {
            SCOPED_TIMER(local_state._spill_total_timer);
            status = local_state._merger->get_next(block, eos);
            RETURN_IF_ERROR(status);
        }
    } else {
        status = _sort_source_operator->get_block(local_state._runtime_state.get(), block, eos);
        RETURN_IF_ERROR(status);
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::pipeline