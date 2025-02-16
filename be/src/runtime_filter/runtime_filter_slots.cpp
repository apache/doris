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

#include "runtime_filter/runtime_filter_slots.h"

#include "pipeline/pipeline_task.h"
#include "runtime_filter/role/producer.h"
#include "runtime_filter/runtime_filter_wrapper.h"
#include "util/defer_op.h"

namespace doris {

Status RuntimeFilterSlots::send_filter_size(
        RuntimeState* state, uint64_t hash_table_size,
        std::shared_ptr<pipeline::CountedFinishDependency> dependency) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
    }

    dependency->add(); // add count at start to avoid dependency ready multiple times
    Defer defer {[&]() { dependency->sub(); }}; // remove the initial external add
    for (auto runtime_filter : _runtime_filters) {
        RETURN_IF_ERROR(runtime_filter->send_filter_size(state, hash_table_size, dependency));
    }
    return Status::OK();
}

/**
    Disable meaningless filters, such as filters:
        RF1: col1 in (1, 3, 5)
        RF2: col1 min: 1, max: 5
    We consider RF2 is meaningless, because RF1 has already filtered out all values that RF2 can filter.
*/
Status RuntimeFilterSlots::_disable_meaningless_filters(RuntimeState* state) {
    // process ignore duplicate IN_FILTER
    std::unordered_set<int> has_in_filter;
    for (auto filter : _runtime_filters) {
        filter->disable_meaningless_filters(has_in_filter, true);
    }

    // process ignore filter when it has IN_FILTER on same expr
    for (auto filter : _runtime_filters) {
        filter->disable_meaningless_filters(has_in_filter, false);
    }
    return Status::OK();
}

Status RuntimeFilterSlots::_init_filters(RuntimeState* state, uint64_t local_hash_table_size) {
    // process IN_OR_BLOOM_FILTER's real type
    for (auto filter : _runtime_filters) {
        RETURN_IF_ERROR(filter->init_with_size(local_hash_table_size));
    }
    return Status::OK();
}

void RuntimeFilterSlots::_insert(const vectorized::Block* block, size_t start) {
    SCOPED_TIMER(_runtime_filter_compute_timer);
    for (auto& filter : _runtime_filters) {
        int result_column_id =
                _build_expr_context[filter->expr_order()]->get_last_result_column_id();
        const auto& column = block->get_by_position(result_column_id).column;
        filter->insert_batch(column, start);
    }
}

Status RuntimeFilterSlots::process(
        RuntimeState* state, const vectorized::Block* block,
        std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
    }

    auto wrapper_state = RuntimeFilterWrapper::State::READY;
    if (state->get_task()->wake_up_early()) {
        // partitial ignore rf to make global rf work
        wrapper_state = RuntimeFilterWrapper::State::IGNORED;
    } else if (_should_build_hash_table) {
        uint64_t hash_table_size = block ? block->rows() : 0;
        {
            RETURN_IF_ERROR(_init_filters(state, hash_table_size));
            RETURN_IF_ERROR(_disable_meaningless_filters(state));
        }
        if (hash_table_size > 1) {
            _insert(block, 1);
        }
    }

    for (auto filter : _runtime_filters) {
        filter->set_wrapper_state_and_ready_to_publish(wrapper_state);
    }

    RETURN_IF_ERROR(_publish(state));
    return Status::OK();
}

Status RuntimeFilterSlots::skip_runtime_filters_process(
        RuntimeState* state, std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency) {
    RETURN_IF_ERROR(send_filter_size(state, 0, finish_dependency));
    for (auto filter : _runtime_filters) {
        filter->disable_and_ready_to_publish("skip all rf process");
    }
    RETURN_IF_ERROR(_publish(state));
    _skip_runtime_filters_process = true;
    return Status::OK();
}
} // namespace doris
