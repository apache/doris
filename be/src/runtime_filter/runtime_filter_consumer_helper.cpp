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

#include "runtime_filter/runtime_filter_consumer_helper.h"

#include "runtime_filter/runtime_filter_consumer.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
RuntimeFilterConsumerHelper::RuntimeFilterConsumerHelper(
        const std::vector<TRuntimeFilterDesc>& runtime_filters)
        : _runtime_filter_descs(runtime_filters) {}

Status RuntimeFilterConsumerHelper::init(
        RuntimeState* state, bool need_local_merge, int32_t node_id, int32_t operator_id,
        std::vector<std::shared_ptr<pipeline::Dependency>>& dependencies, const std::string& name) {
    for (const auto& desc : _runtime_filter_descs) {
        std::shared_ptr<RuntimeFilterConsumer> filter;
        RETURN_IF_ERROR(
                state->register_consumer_runtime_filter(desc, need_local_merge, node_id, &filter));
        _consumers.emplace_back(filter);
    }

    dependencies.resize(_runtime_filter_descs.size());
    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> runtime_filter_timers(
            _runtime_filter_descs.size());
    std::vector<std::shared_ptr<pipeline::Dependency>> local_dependencies;
    for (size_t i = 0; i < _consumers.size(); ++i) {
        dependencies[i] = std::make_shared<pipeline::Dependency>(operator_id, node_id, name);
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
                                                           vectorized::VExprContextSPtrs& conjuncts,
                                                           const RowDescriptor& row_descriptor) {
    SCOPED_TIMER(_acquire_runtime_filter_timer.get());
    std::vector<vectorized::VRuntimeFilterPtr> vexprs;
    for (const auto& consumer : _consumers) {
        RETURN_IF_ERROR(consumer->acquire_expr(vexprs));
        if (!consumer->is_applied()) {
            _is_all_rf_applied = false;
        }
    }
    RETURN_IF_ERROR(_append_rf_into_conjuncts(state, vexprs, conjuncts, row_descriptor));
    return Status::OK();
}

Status RuntimeFilterConsumerHelper::_append_rf_into_conjuncts(
        RuntimeState* state, const std::vector<vectorized::VRuntimeFilterPtr>& vexprs,
        vectorized::VExprContextSPtrs& conjuncts, const RowDescriptor& row_descriptor) {
    if (vexprs.empty()) {
        return Status::OK();
    }

    for (const auto& expr : vexprs) {
        vectorized::VExprContextSPtr conjunct = vectorized::VExprContext::create_shared(expr);
        RETURN_IF_ERROR(conjunct->prepare(state, row_descriptor));
        RETURN_IF_ERROR(conjunct->open(state));
        conjuncts.emplace_back(conjunct);
    }

    return Status::OK();
}

Status RuntimeFilterConsumerHelper::try_append_late_arrival_runtime_filter(
        RuntimeState* state, int* arrived_rf_num, vectorized::VExprContextSPtrs& conjuncts,
        const RowDescriptor& row_descriptor) {
    if (_is_all_rf_applied) {
        *arrived_rf_num = cast_set<int>(_runtime_filter_descs.size());
        return Status::OK();
    }

    // This method will be called in scanner thread.
    // So need to add lock
    std::unique_lock l(_rf_locks);
    if (_is_all_rf_applied) {
        *arrived_rf_num = cast_set<int>(_runtime_filter_descs.size());
        return Status::OK();
    }

    // 1. Check if are runtime filter ready but not applied.
    std::vector<vectorized::VRuntimeFilterPtr> exprs;
    int current_arrived_rf_num = 0;
    for (const auto& consumer : _consumers) {
        RETURN_IF_ERROR(consumer->acquire_expr(exprs));
        current_arrived_rf_num += consumer->is_applied();
    }
    // 2. Append unapplied runtime filters to _conjuncts
    if (!exprs.empty()) {
        RETURN_IF_ERROR(_append_rf_into_conjuncts(state, exprs, conjuncts, row_descriptor));
    }
    if (current_arrived_rf_num == _runtime_filter_descs.size()) {
        _is_all_rf_applied = true;
    }

    *arrived_rf_num = current_arrived_rf_num;
    return Status::OK();
}

void RuntimeFilterConsumerHelper::collect_realtime_profile(
        RuntimeProfile* parent_operator_profile) {
    std::ignore = parent_operator_profile->add_counter("RuntimeFilterInfo", TUnit::NONE,
                                                       RuntimeProfile::ROOT_COUNTER, 1);
    RuntimeProfile::Counter* c = parent_operator_profile->add_counter(
            "AcquireRuntimeFilter", TUnit::TIME_NS, "RuntimeFilterInfo", 2);
    c->update(_acquire_runtime_filter_timer->value());

    for (const auto& consumer : _consumers) {
        consumer->collect_realtime_profile(parent_operator_profile);
    }
}

} // namespace doris