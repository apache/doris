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
#include "common/compile_check_begin.h"
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
        int result_column_id = _filter_expr_contexts[i]->get_last_result_column_id();
        DCHECK_NE(result_column_id, -1);
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

Status RuntimeFilterProducerHelper::build(
        RuntimeState* state, const vectorized::Block* block, bool use_shared_table,
        std::map<int, std::shared_ptr<RuntimeFilterWrapper>>& runtime_filters) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
    }

    if (_should_build_hash_table) {
        // Hash table is completed and runtime filter has a global size now.
        uint64_t hash_table_size = block ? block->rows() : 0;
        RETURN_IF_ERROR(_init_filters(state, hash_table_size));
        if (hash_table_size > 1) {
            constexpr int HASH_JOIN_INSERT_OFFSET = 1; // the first row is mocked on hash join sink
            RETURN_IF_ERROR(_insert(block, HASH_JOIN_INSERT_OFFSET));
        }
    }

    for (const auto& filter : _producers) {
        if (use_shared_table) {
            DCHECK(_is_broadcast_join);
            if (_should_build_hash_table) {
                DCHECK(!runtime_filters.contains(filter->wrapper()->filter_id()));
                runtime_filters[filter->wrapper()->filter_id()] = filter->wrapper();
            } else {
                DCHECK(runtime_filters.contains(filter->wrapper()->filter_id()));
                filter->set_wrapper(runtime_filters[filter->wrapper()->filter_id()]);
            }
        }
        filter->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    }
    return Status::OK();
}

Status RuntimeFilterProducerHelper::terminate(RuntimeState* state) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
    }

    for (const auto& filter : _producers) {
        filter->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::IGNORED);
    }

    RETURN_IF_ERROR(_publish(state));
    return Status::OK();
}

Status RuntimeFilterProducerHelper::publish(RuntimeState* state) {
    if (_skip_runtime_filters_process) {
        return Status::OK();
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

    RETURN_IF_ERROR(publish(state));
    _skip_runtime_filters_process = true;
    _profile->add_info_string("SkipProcess", "True");
    return Status::OK();
}

} // namespace doris
