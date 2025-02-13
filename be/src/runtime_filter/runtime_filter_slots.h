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

#pragma once

#include "common/status.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime_filter/runtime_filter.h"
#include "runtime_filter/runtime_filter_producer.h"
#include "vec/core/block.h" // IWYU pragma: keep
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/shared_hash_table_controller.h"

namespace doris {
// this class used in hash join node
class RuntimeFilterSlots {
public:
    RuntimeFilterSlots(const vectorized::VExprContextSPtrs& build_expr_ctxs,
                       RuntimeProfile* profile,
                       const std::vector<std::shared_ptr<RuntimeFilterProducer>>& runtime_filters,
                       bool should_build_hash_table)
            : _build_expr_context(build_expr_ctxs),
              _runtime_filters(runtime_filters),
              _should_build_hash_table(should_build_hash_table),
              _runtime_filters_profile(new RuntimeProfile("RuntimeFilterSlots")) {
        profile->add_child(_runtime_filters_profile.get(), true, nullptr);
        _publish_runtime_filter_timer =
                ADD_TIMER_WITH_LEVEL(_runtime_filters_profile, "PublishTime", 1);
        _runtime_filter_compute_timer =
                ADD_TIMER_WITH_LEVEL(_runtime_filters_profile, "BuildTime", 1);
    }

    Status send_filter_size(RuntimeState* state, uint64_t hash_table_size,
                            std::shared_ptr<pipeline::CountedFinishDependency> dependency);

    Status disable_all_filters(
            RuntimeState* state,
            std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency) {
        RETURN_IF_ERROR(send_filter_size(state, 0, finish_dependency));
        for (auto filter : _runtime_filters) {
            filter->set_disabled();
        }
        RETURN_IF_ERROR(_publish(state));
        _runtime_filters_disabled = true;
        return Status::OK();
    }

    Status process(RuntimeState* state, const vectorized::Block* block,
                   std::shared_ptr<pipeline::CountedFinishDependency> finish_dependency);

    void copy_to_shared_context(vectorized::SharedHashTableContextPtr& context) {
        for (auto& filter : _runtime_filters) {
            context->runtime_filters[filter->filter_id()] = filter->get_shared_context_ref();
        }
    }

    Status copy_from_shared_context(vectorized::SharedHashTableContextPtr& context) {
        for (auto& filter : _runtime_filters) {
            auto filter_id = filter->filter_id();
            auto ret = context->runtime_filters.find(filter_id);
            if (ret == context->runtime_filters.end()) {
                return Status::Aborted("invalid runtime filter id: {}", filter_id);
            }
            filter->get_shared_context_ref() = ret->second;
        }
        return Status::OK();
    }

protected:
    Status _disable_meaningless_filters(RuntimeState* state);
    Status _init_filters(RuntimeState* state, uint64_t local_hash_table_size);
    void _insert(const vectorized::Block* block, size_t start);
    Status _publish(RuntimeState* state) {
        if (_runtime_filters_disabled) {
            return Status::OK();
        }
        SCOPED_TIMER(_publish_runtime_filter_timer);
        for (auto& filter : _runtime_filters) {
            RETURN_IF_ERROR(filter->publish(state, !_should_build_hash_table));
        }
        return Status::OK();
    }

    const std::vector<std::shared_ptr<vectorized::VExprContext>>& _build_expr_context;
    std::vector<std::shared_ptr<RuntimeFilterProducer>> _runtime_filters;
    bool _should_build_hash_table;

    RuntimeProfile::Counter* _publish_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* _runtime_filter_compute_timer = nullptr;
    std::unique_ptr<RuntimeProfile> _runtime_filters_profile;

    bool _runtime_filters_disabled = false;
};

} // namespace doris
