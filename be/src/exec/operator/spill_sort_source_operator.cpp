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

#include "exec/operator/spill_sort_source_operator.h"

#include <glog/logging.h>

#include <cstdint>
#include <limits>

#include "common/status.h"
#include "exec/operator/sort_source_operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "runtime/fragment_mgr.h"

namespace doris {
SpillSortLocalState::SpillSortLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status SpillSortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    init_spill_write_counters();
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

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

    for (auto& reader : _current_merging_readers) {
        if (reader) {
            RETURN_IF_ERROR(reader->close());
            reader.reset();
        }
    }
    _current_merging_readers.clear();
    _current_merging_files.clear();
    _merger.reset();

    return Base::close(state);
}

int SpillSortLocalState::_calc_spill_blocks_to_merge(RuntimeState* state) const {
    auto count = state->spill_sort_merge_mem_limit_bytes() / state->spill_buffer_size_bytes();
    return std::max(8, static_cast<int32_t>(count));
}

Status SpillSortLocalState::execute_merge_sort_spill_files(RuntimeState* state) {
    auto& parent = Base::_parent->template cast<Parent>();
    SCOPED_TIMER(_spill_merge_sort_timer);
    Status status;

    Block merge_sorted_block;
    auto query_id = state->query_id();
    while (!state->is_cancelled()) {
        int max_stream_count = _calc_spill_blocks_to_merge(state);
        VLOG_DEBUG << fmt::format(
                "Query:{}, sort source:{}, task:{}, merge spill streams, streams count:{}, "
                "curren merge max spill file count:{}",
                print_id(query_id), _parent->node_id(), state->task_id(),
                _shared_state->sorted_spill_groups.size(), max_stream_count);
        RETURN_IF_ERROR(_create_intermediate_merger(
                state, max_stream_count,
                parent._sort_source_operator->get_sort_description(_runtime_state.get())));
        // It is a fast path, because all the remaining streams can be merged in a run
        if (_shared_state->sorted_spill_groups.empty()) {
            return Status::OK();
        }

        SpillFileSPtr tmp_file;
        auto label = "sort";
        auto relative_path = fmt::format("{}/{}-{}-{}-{}", print_id(state->query_id()), label,
                                         _parent->node_id(), state->task_id(),
                                         ExecEnv::GetInstance()->spill_file_mgr()->next_id());
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                    tmp_file));
        SpillFileWriterSPtr tmp_writer;
        RETURN_IF_ERROR(tmp_file->create_writer(state, operator_profile(), tmp_writer));
        _shared_state->sorted_spill_groups.emplace_back(tmp_file);

        bool eos = false;
        while (!eos && !state->is_cancelled()) {
            merge_sorted_block.clear_column_data();
            {
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
            status = tmp_writer->write_block(state, merge_sorted_block);
            if (status.ok()) {
                DBUG_EXECUTE_IF("fault_inject::spill_sort_source::spill_merged_data", {
                    status = Status::Error<INTERNAL_ERROR>(
                            "fault_inject spill_sort_source "
                            "spill_merged_data failed");
                });
            }
            RETURN_IF_ERROR(status);
        }
        RETURN_IF_ERROR(tmp_writer->close());
    }
    return Status::OK();
}

Status SpillSortLocalState::_create_intermediate_merger(RuntimeState* state, int num_blocks,
                                                        const SortDescription& sort_description) {
    std::vector<BlockSupplier> child_block_suppliers;
    int64_t limit = -1;
    int64_t offset = 0;
    if (num_blocks >= _shared_state->sorted_spill_groups.size()) {
        // final round use real limit and offset
        limit = Base::_shared_state->limit;
        offset = Base::_shared_state->offset;
    }

    _merger = std::make_unique<VSortedRunMerger>(sort_description, state->batch_size(), limit,
                                                 offset, custom_profile());

    _current_merging_files.clear();
    _current_merging_readers.clear();
    for (int i = 0; i < num_blocks && !_shared_state->sorted_spill_groups.empty(); ++i) {
        auto spill_file = _shared_state->sorted_spill_groups.front();
        _shared_state->sorted_spill_groups.pop_front();
        _current_merging_files.emplace_back(spill_file);

        // Each SpillFile's reader handles multi-part reading internally.
        auto reader = spill_file->create_reader(state, operator_profile());
        RETURN_IF_ERROR(reader->open());

        auto reader_ptr = reader.get();
        _current_merging_readers.emplace_back(std::move(reader));

        child_block_suppliers.emplace_back([reader_ptr](Block* block, bool* eos) -> Status {
            return reader_ptr->read(block, eos);
        });
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

bool SpillSortLocalState::is_blockable() const {
    return _shared_state->is_spilled;
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
    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = _shared_state->in_mem_shared_state,
                               .shared_state_map = {},
                               .task_idx = 0};

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

    // Perform final cleanup for local state: delete any merging streams and
    // close shared state. Centralize cleanup so resources are released when
    // the pipeline task finishes.
    auto& local_state = get_local_state(state);
    local_state._current_merging_files.clear();
    local_state._current_merging_readers.clear();
    local_state._merger.reset();

    if (local_state._shared_state) {
        local_state._shared_state->close();
    }

    return _sort_source_operator->close(state);
}

Status SpillSortSourceOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    if (local_state._shared_state->is_spilled) {
        if (!local_state._merger) {
            return local_state.execute_merge_sort_spill_files(state);
        } else {
            RETURN_IF_ERROR(local_state._merger->get_next(block, eos));
        }
    } else {
        RETURN_IF_ERROR(
                _sort_source_operator->get_block(local_state._runtime_state.get(), block, eos));
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace doris