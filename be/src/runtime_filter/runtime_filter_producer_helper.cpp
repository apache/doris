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

#include "runtime_filter/runtime_filter_producer_helper.h"

#include "pipeline/pipeline_task.h"
#include "runtime_filter/runtime_filter_wrapper.h"

namespace doris {

void RuntimeFilterProducerHelper::_init_expr(
        const vectorized::VExprContextSPtrs& build_expr_ctxs,
        const std::vector<TRuntimeFilterDesc>& runtime_filter_descs) {
    _filter_expr_contexts.resize(runtime_filter_descs.size());
    for (size_t i = 0; i < runtime_filter_descs.size(); i++) {
        _filter_expr_contexts[i] = build_expr_ctxs[runtime_filter_descs[i].expr_order];
    }
}

Status RuntimeFilterProducerHelper::init(
        RuntimeState* state, const vectorized::VExprContextSPtrs& build_expr_ctxs,
        const std::vector<TRuntimeFilterDesc>& runtime_filter_descs) {
    _producers.resize(runtime_filter_descs.size());
    for (size_t i = 0; i < runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->register_producer_runtime_filter(runtime_filter_descs[i],
                                                                &_producers[i], _profile.get()));
    }
    _init_expr(build_expr_ctxs, runtime_filter_descs);
    return Status::OK();
}

Status RuntimeFilterProducerHelper::send_filter_size(
        RuntimeState* state, uint64_t hash_table_size,
        std::shared_ptr<pipeline::CountedFinishDependency> dependency) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
    }
    for (const auto& filter : _producers) {
        RETURN_IF_ERROR(filter->send_size(state, hash_table_size, dependency));
    }
    return Status::OK();
}

Status RuntimeFilterProducerHelper::_init_filters(RuntimeState* state,
                                                  uint64_t local_hash_table_size) {
    // process IN_OR_BLOOM_FILTER's real type
    for (const auto& filter : _producers) {
        RETURN_IF_ERROR(filter->init(local_hash_table_size));
    }
    return Status::OK();
}

Status RuntimeFilterProducerHelper::_insert(const vectorized::Block* block, size_t start) {
    SCOPED_TIMER(_runtime_filter_compute_timer);
    for (int i = 0; i < _producers.size(); i++) {
        auto filter = _producers[i];
        if (!filter->impl()->is_valid()) {
            // Skip building if ignored or disabled.
            continue;
        }
        int result_column_id = _filter_expr_contexts[i]->get_last_result_column_id();
        const auto& column = block->get_by_position(result_column_id).column;
        RETURN_IF_ERROR(filter->insert(column, start));
    }
    return Status::OK();
}

Status RuntimeFilterProducerHelper::_publish(RuntimeState* state) {
    SCOPED_TIMER(_publish_runtime_filter_timer);
    for (const auto& filter : _producers) {
        RETURN_IF_ERROR(filter->publish(state, _should_build_hash_table));
    }
    return Status::OK();
}

Status RuntimeFilterProducerHelper::process(
        RuntimeState* state, const vectorized::Block* block,
        std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency,
        vectorized::SharedHashTableContextPtr& shared_hash_table_ctx) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
    }

    bool wake_up_early = state->get_task()->wake_up_early();
    // Runtime filter is ignored partially which has no effect on correctness.
    auto wrapper_state = wake_up_early ? RuntimeFilterWrapper::State::IGNORED
                                       : RuntimeFilterWrapper::State::READY;
    if (_should_build_hash_table && !wake_up_early) {
        // Hash table is completed and runtime filter has a global size now.
        uint64_t hash_table_size = block ? block->rows() : 0;
        RETURN_IF_ERROR(_init_filters(state, hash_table_size));
        if (hash_table_size > 1) {
            RETURN_IF_ERROR(_insert(block, 1));
        }
    }

    for (const auto& filter : _producers) {
        if (shared_hash_table_ctx && !wake_up_early) {
            if (_should_build_hash_table) {
                filter->copy_to_shared_context(shared_hash_table_ctx);
            } else {
                filter->copy_from_shared_context(shared_hash_table_ctx);
            }
        }
        filter->set_wrapper_state_and_ready_to_publish(wrapper_state);
    }

    RETURN_IF_ERROR(_publish(state));
    return Status::OK();
}

Status RuntimeFilterProducerHelper::skip_process(RuntimeState* state) {
    RETURN_IF_ERROR(send_filter_size(state, 0, nullptr));

    for (const auto& filter : _producers) {
        filter->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::DISABLED,
                                                       "skip all rf process");
    }

    RETURN_IF_ERROR(_publish(state));
    _skip_runtime_filters_process = true;
    return Status::OK();
}

} // namespace doris
