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

Status RuntimeFilterSlots::init(RuntimeState* state,
                                const std::vector<TRuntimeFilterDesc>& runtime_filter_descs) {
    _runtime_filters.resize(runtime_filter_descs.size());
    for (size_t i = 0; i < runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->register_producer_runtime_filter(
                runtime_filter_descs[i], &_runtime_filters[i], _profile.get()));
    }
    return Status::OK();
}

Status RuntimeFilterSlots::send_filter_size(
        RuntimeState* state, uint64_t hash_table_size,
        std::shared_ptr<pipeline::CountedFinishDependency> dependency) {
    // TODO: dependency is not needed if `_skip_runtime_filters_process` is true
    for (auto runtime_filter : _runtime_filters) {
        RETURN_IF_ERROR(runtime_filter->send_size(
                state, _skip_runtime_filters_process ? 0 : hash_table_size, dependency));
    }
    return Status::OK();
}

Status RuntimeFilterSlots::_init_filters(RuntimeState* state, uint64_t local_hash_table_size) {
    // process IN_OR_BLOOM_FILTER's real type
    for (auto filter : _runtime_filters) {
        RETURN_IF_ERROR(filter->init(local_hash_table_size));
    }
    return Status::OK();
}

void RuntimeFilterSlots::_insert(const vectorized::Block* block, size_t start) {
    SCOPED_TIMER(_runtime_filter_compute_timer);
    for (auto& filter : _runtime_filters) {
        if (!filter->impl()->is_valid()) {
            // Skip building if ignored or disabled.
            continue;
        }
        int result_column_id =
                _build_expr_context[filter->expr_order()]->get_last_result_column_id();
        const auto& column = block->get_by_position(result_column_id).column;
        filter->insert(column, start);
    }
}

Status RuntimeFilterSlots::process(
        RuntimeState* state, const vectorized::Block* block,
        std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency,
        vectorized::SharedHashTableContextPtr& shared_hash_table_ctx) {
    auto wrapper_state = _skip_runtime_filters_process ? RuntimeFilterWrapper::State::DISABLED
                                                       : RuntimeFilterWrapper::State::READY;
    if (state->get_task()->wake_up_early() && !_skip_runtime_filters_process) {
        // Runtime filter is ignored partially which has no effect on correctness.
        wrapper_state = RuntimeFilterWrapper::State::IGNORED;
    } else if (_should_build_hash_table && !_skip_runtime_filters_process) {
        // Hash table is completed and runtime filter has a global size now.
        uint64_t hash_table_size = block ? block->rows() : 0;
        RETURN_IF_ERROR(_init_filters(state, hash_table_size));
        if (hash_table_size > 1) {
            _insert(block, 1);
        }
    }

    for (auto filter : _runtime_filters) {
        if (shared_hash_table_ctx && _should_build_hash_table) {
            filter->copy_to_shared_context(shared_hash_table_ctx);
        } else if (shared_hash_table_ctx) {
            filter->copy_from_shared_context(shared_hash_table_ctx);
        }
        if (_should_build_hash_table) {
            filter->set_wrapper_state_and_ready_to_publish(
                    wrapper_state, _skip_runtime_filters_process ? "skip all rf process" : "");
        } else {
            filter->set_state(RuntimeFilterProducer::State::READY_TO_PUBLISH);
        }
    }

    RETURN_IF_ERROR(_publish(state));
    return Status::OK();
}

} // namespace doris
