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

#include "exec/runtime_filter/runtime_filter_consumer_helper.h"

#include <algorithm>
#include <atomic>

#include "common/check.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_profile.h"

namespace doris {
RuntimeFilterConsumerHelper::RuntimeFilterConsumerHelper(
        const std::vector<TRuntimeFilterDesc>& runtime_filters)
        : _runtime_filter_descs(runtime_filters) {}

Status RuntimeFilterConsumerHelper::init(RuntimeState* state, bool need_local_merge,
                                         int32_t node_id, int32_t operator_id,
                                         std::vector<std::shared_ptr<Dependency>>& dependencies,
                                         const std::string& name) {
    for (const auto& desc : _runtime_filter_descs) {
        std::shared_ptr<RuntimeFilterConsumer> filter;
        RETURN_IF_ERROR(
                state->register_consumer_runtime_filter(desc, need_local_merge, node_id, &filter));
        _consumers.emplace_back(filter);
    }

    dependencies.resize(_runtime_filter_descs.size());
    std::vector<std::shared_ptr<RuntimeFilterTimer>> runtime_filter_timers(
            _runtime_filter_descs.size());
    std::vector<std::shared_ptr<Dependency>> local_dependencies;
    for (size_t i = 0; i < _consumers.size(); ++i) {
        dependencies[i] = std::make_shared<Dependency>(operator_id, node_id, name);
        runtime_filter_timers[i] = _consumers[i]->create_filter_timer(dependencies[i]);
        if (!_consumers[i]->has_remote_target()) {
            local_dependencies.emplace_back(dependencies[i]);
        }
    }

    // The gloabl runtime filter timer need set local runtime filter dependencies.
    // start to wait before the local runtime filter ready
    for (size_t i = 0; i < _consumers.size(); ++i) {
        if (_consumers[i]->has_remote_target()) {
            runtime_filter_timers[i]->set_local_runtime_filter_dependencies(local_dependencies);
        }
    }

    if (!runtime_filter_timers.empty()) {
        ExecEnv::GetInstance()->runtime_filter_timer_queue()->push_filter_timer(
                std::move(runtime_filter_timers));
    }
    return Status::OK();
}

Status RuntimeFilterConsumerHelper::acquire_runtime_filter(RuntimeState* state,
                                                           VExprContextSPtrs& conjuncts,
                                                           const RowDescriptor& row_descriptor) {
    SCOPED_TIMER(_acquire_runtime_filter_timer.get());
    std::vector<int32_t> late_filter_ids;
    std::vector<RuntimeFilterExprPtr> vexprs;
    for (size_t i = 0; i < _consumers.size(); ++i) {
        const auto& consumer = _consumers[i];
        RETURN_IF_ERROR(consumer->acquire_expr(vexprs));
        if (!consumer->is_applied()) {
            late_filter_ids.emplace_back(_runtime_filter_descs[i].filter_id);
        }
    }

    RETURN_IF_ERROR(_append_rf_into_conjuncts(state, vexprs, conjuncts, row_descriptor));

    DORIS_CHECK(_late_runtime_filter_container == nullptr);
    _late_runtime_filter_container = std::make_shared<LateRuntimeFilterContainer>(late_filter_ids);
    _is_all_rf_applied = late_filter_ids.empty();
    return Status::OK();
}

Status RuntimeFilterConsumerHelper::_append_rf_into_conjuncts(
        RuntimeState* state, const std::vector<RuntimeFilterExprPtr>& vexprs,
        VExprContextSPtrs& conjuncts, const RowDescriptor& row_descriptor) {
    if (vexprs.empty()) {
        return Status::OK();
    }

    for (const auto& expr : vexprs) {
        VExprContextSPtr conjunct = VExprContext::create_shared(expr);
        RETURN_IF_ERROR(conjunct->prepare(state, row_descriptor));
        RETURN_IF_ERROR(conjunct->open(state));
        conjuncts.emplace_back(conjunct);
    }

    return Status::OK();
}

void RuntimeFilterConsumerHelper::_publish_late_runtime_filter(
        int32_t filter_id, std::shared_ptr<const LateRuntimeFilterExprGroup> expr_group) {
    DORIS_CHECK(_late_runtime_filter_container != nullptr);
    auto& filters = _late_runtime_filter_container->filters;
    auto entry = std::ranges::find(filters, filter_id, &LateRuntimeFilterEntry::filter_id);
    DORIS_CHECK(entry != filters.end());
    DORIS_CHECK(!entry->valid.load(std::memory_order_relaxed));
    DORIS_CHECK(expr_group != nullptr);
    DORIS_CHECK(!expr_group->empty());

    entry->expr = std::move(expr_group);
    entry->valid.store(true, std::memory_order_release);
    _late_runtime_filter_container->arrived_cnt.fetch_add(1, std::memory_order_release);
}

Status RuntimeFilterConsumerHelper::try_append_late_arrival_runtime_filter(
        RuntimeState* state, const RowDescriptor& row_descriptor, int& arrived_rf_num,
        VExprContextSPtrs& arrived_conjuncts, StorageFilterChecker storage_filter_checker) {
    if (_is_all_rf_applied) {
        arrived_rf_num = cast_set<int>(_runtime_filter_descs.size());
        return Status::OK();
    }

    // This method will be called in scanner thread.
    // So need to add lock
    std::unique_lock l(_rf_locks);
    if (_is_all_rf_applied) {
        arrived_rf_num = cast_set<int>(_runtime_filter_descs.size());
        return Status::OK();
    }

    // 1. Check if are runtime filter ready but not applied.
    int current_arrived_rf_num = 0;
    for (size_t i = 0; i < _consumers.size(); ++i) {
        std::vector<RuntimeFilterExprPtr> exprs;
        const auto& consumer = _consumers[i];
        RETURN_IF_ERROR(consumer->acquire_expr(exprs));
        current_arrived_rf_num += consumer->is_applied();

        if (exprs.empty()) {
            continue;
        }

        VExprContextSPtrs new_conjuncts;
        RETURN_IF_ERROR(_append_rf_into_conjuncts(state, exprs, new_conjuncts, row_descriptor));

        if (std::ranges::all_of(exprs, [&](const auto& expr) {
                return storage_filter_checker(expr->get_impl());
            })) {
            // ScanLocalState may execute its copy during partition pruning while SegmentIterators
            // concurrently clone the published group. Keep the published contexts as clone-only
            // sources with independent FunctionContexts.
            auto storage_expr_group = std::make_shared<LateRuntimeFilterExprGroup>();
            storage_expr_group->reserve(new_conjuncts.size());
            for (const auto& expr_context : new_conjuncts) {
                VExprContextSPtr storage_expr_context;
                RETURN_IF_ERROR(expr_context->clone(state, storage_expr_context));
                storage_expr_group->emplace_back(std::move(storage_expr_context));
            }

            _publish_late_runtime_filter(_runtime_filter_descs[i].filter_id,
                                         std::move(storage_expr_group));
        }
        arrived_conjuncts.insert(arrived_conjuncts.end(), new_conjuncts.begin(),
                                 new_conjuncts.end());
    }

    if (current_arrived_rf_num == _runtime_filter_descs.size()) {
        _is_all_rf_applied = true;
    }
    arrived_rf_num = current_arrived_rf_num;
    return Status::OK();
}

void RuntimeFilterConsumerHelper::collect_realtime_profile(
        RuntimeProfile* parent_operator_profile) {
    std::ignore = parent_operator_profile->add_counter("RuntimeFilterInfo", TUnit::NONE,
                                                       RuntimeProfile::ROOT_COUNTER, 1);
    RuntimeProfile::Counter* c = parent_operator_profile->add_counter(
            "AcquireRuntimeFilter", TUnit::TIME_NS, "RuntimeFilterInfo", 2);
    c->update(_acquire_runtime_filter_timer->value());
    c = parent_operator_profile->add_counter("PublishedLateRuntimeFilters", TUnit::UNIT,
                                             "RuntimeFilterInfo", 2);
    if (_late_runtime_filter_container != nullptr) {
        c->update(_late_runtime_filter_container->arrived_cnt.load(std::memory_order_relaxed));
    }

    for (const auto& consumer : _consumers) {
        consumer->collect_realtime_profile(parent_operator_profile);
    }
}

} // namespace doris
