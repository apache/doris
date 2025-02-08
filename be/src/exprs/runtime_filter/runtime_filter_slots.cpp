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

#include "exprs/runtime_filter/runtime_filter_slots.h"

#include "pipeline/pipeline_task.h"

namespace doris {

Status RuntimeFilterSlots::send_filter_size(
        RuntimeState* state, uint64_t hash_table_size,
        std::shared_ptr<pipeline::CountedFinishDependency> dependency) {
    if (_runtime_filters_disabled) {
        return Status::OK();
    }
    for (auto runtime_filter : _runtime_filters) {
        if (runtime_filter->need_sync_filter_size()) {
            runtime_filter->set_finish_dependency(dependency);
        }
    }

    // send_filter_size may call dependency->sub(), so we call set_finish_dependency firstly for all rf to avoid dependency set_ready repeatedly
    for (auto runtime_filter : _runtime_filters) {
        if (runtime_filter->need_sync_filter_size()) {
            RETURN_IF_ERROR(runtime_filter->send_filter_size(state, hash_table_size));
        }
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
        if (filter->get_ignored() || filter->get_disabled()) {
            continue;
        }
        if (filter->get_real_type() != RuntimeFilterType::IN_FILTER) {
            continue;
        }
        if (!filter->need_sync_filter_size() &&
            filter->type() == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            continue;
        }
        if (has_in_filter.contains(filter->expr_order())) {
            filter->set_disabled();
            continue;
        }
        has_in_filter.insert(filter->expr_order());
    }

    // process ignore filter when it has IN_FILTER on same expr
    for (auto filter : _runtime_filters) {
        if (filter->get_ignored() || filter->get_disabled()) {
            continue;
        }
        if (filter->get_real_type() == RuntimeFilterType::IN_FILTER ||
            !has_in_filter.contains(filter->expr_order())) {
            continue;
        }
        filter->set_disabled();
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
        if (filter->get_ignored() || filter->get_disabled()) {
            continue;
        }
        filter->insert_batch(column, start);
    }
}

Status RuntimeFilterSlots::process(
        RuntimeState* state, const vectorized::Block* block,
        std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency) {
    if (_runtime_filters_disabled) {
        return Status::OK();
    }
    if (state->get_task()->wake_up_early()) {
        // partitial ignore rf to make global rf work
        for (auto filter : _runtime_filters) {
            filter->set_ignored();
        }
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
    RETURN_IF_ERROR(_publish(state));
    return Status::OK();
}

} // namespace doris
