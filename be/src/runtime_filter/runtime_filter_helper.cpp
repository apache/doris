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

#include "runtime_filter/runtime_filter_helper.h"

#include "pipeline/pipeline_task.h"
#include "runtime_filter/runtime_filter_consumer.h"

namespace doris::pipeline {

RuntimeFilterHelper::RuntimeFilterHelper(const int32_t _node_id,
                                         const std::vector<TRuntimeFilterDesc>& runtime_filters,
                                         const RowDescriptor& row_descriptor,
                                         vectorized::VExprContextSPtrs& conjuncts)
        : _node_id(_node_id),
          _runtime_filter_descs(runtime_filters),
          _row_descriptor_ref(row_descriptor),
          _conjuncts_ref(conjuncts) {
    _blocked_by_rf = std::make_shared<std::atomic_bool>(false);
}

Status RuntimeFilterHelper::init(RuntimeState* state, RuntimeProfile* profile,
                                 bool need_local_merge) {
    _state = state;
    RETURN_IF_ERROR(_register_runtime_filter(need_local_merge));
    _init_profile(profile);
    return Status::OK();
}

void RuntimeFilterHelper::_init_profile(RuntimeProfile* profile) {
    fmt::memory_buffer buffer;
    for (auto& consumer : _consumers) {
        consumer->init_profile(profile);
        fmt::format_to(buffer, "{}, ", consumer->debug_string());
    }
    profile->add_info_string("RuntimeFilters: ", to_string(buffer));
    _acquire_runtime_filter_timer = ADD_TIMER(profile, "AcquireRuntimeFilterTime");
}

Status RuntimeFilterHelper::_register_runtime_filter(bool need_local_merge) {
    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ready_flag.reserve(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        std::shared_ptr<RuntimeFilterConsumer> filter;
        RETURN_IF_ERROR(_state->register_consumer_runtime_filter(
                _runtime_filter_descs[i], need_local_merge, _node_id, &filter));
        _consumers.emplace_back(filter);
        _runtime_filter_ready_flag.emplace_back(false);
    }
    return Status::OK();
}

void RuntimeFilterHelper::init_runtime_filter_dependency(
        std::vector<std::shared_ptr<pipeline::RuntimeFilterDependency>>&
                runtime_filter_dependencies,
        const int id, const int node_id, const std::string& name) {
    runtime_filter_dependencies.resize(_runtime_filter_descs.size());
    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> runtime_filter_timers(
            _runtime_filter_descs.size());
    std::vector<std::shared_ptr<pipeline::RuntimeFilterDependency>>
            local_runtime_filter_dependencies;

    for (size_t i = 0; i < _consumers.size(); ++i) {
        runtime_filter_dependencies[i] = std::make_shared<pipeline::RuntimeFilterDependency>(
                id, node_id, name, _consumers[i].get());
        runtime_filter_timers[i] =
                _consumers[i]->create_filter_timer(runtime_filter_dependencies[i]);
        if (_consumers[i]->has_local_target()) {
            local_runtime_filter_dependencies.emplace_back(runtime_filter_dependencies[i]);
        } else {
            // The gloabl runtime filter timer need set local runtime filter dependencies.
            // start to wait before the local runtime filter ready
            runtime_filter_timers[i]->set_local_runtime_filter_dependencies(
                    local_runtime_filter_dependencies);
        }
    }

    if (!runtime_filter_timers.empty()) {
        ExecEnv::GetInstance()->runtime_filter_timer_queue()->push_filter_timer(
                std::move(runtime_filter_timers));
    }
}

Status RuntimeFilterHelper::acquire_runtime_filter() {
    SCOPED_TIMER(_acquire_runtime_filter_timer);
    std::vector<vectorized::VRuntimeFilterPtr> vexprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        if (_consumers[i]->is_ready()) {
            // Runtime filter has been applied in open phase.
            RETURN_IF_ERROR(_consumers[i]->apply_ready_expr(_probe_ctxs, vexprs, false));
        }

        _consumers[i]->update_state();

        if (!_consumers[i]->is_applied()) {
            _is_all_rf_applied = false;
        }
    }
    RETURN_IF_ERROR(_append_rf_into_conjuncts(vexprs));
    return Status::OK();
}

Status RuntimeFilterHelper::_append_rf_into_conjuncts(
        const std::vector<vectorized::VRuntimeFilterPtr>& vexprs) {
    if (vexprs.empty()) {
        return Status::OK();
    }

    for (const auto& expr : vexprs) {
        vectorized::VExprContextSPtr conjunct = vectorized::VExprContext::create_shared(expr);
        RETURN_IF_ERROR(conjunct->prepare(_state, _row_descriptor_ref));
        RETURN_IF_ERROR(conjunct->open(_state));
        _conjuncts_ref.emplace_back(conjunct);
    }

    return Status::OK();
}

Status RuntimeFilterHelper::try_append_late_arrival_runtime_filter(int* arrived_rf_num) {
    if (_is_all_rf_applied) {
        *arrived_rf_num = _runtime_filter_descs.size();
        return Status::OK();
    }

    // This method will be called in scanner thread.
    // So need to add lock
    std::unique_lock l(_rf_locks);
    if (_is_all_rf_applied) {
        *arrived_rf_num = _runtime_filter_descs.size();
        return Status::OK();
    }

    // 1. Check if are runtime filter ready but not applied.
    std::vector<vectorized::VRuntimeFilterPtr> exprs;
    int current_arrived_rf_num = 0;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        if (_consumers[i]->is_applied()) {
            ++current_arrived_rf_num;
            continue;
        } else if (_consumers[i]->is_ready()) {
            RETURN_IF_ERROR(_consumers[i]->apply_ready_expr(_probe_ctxs, exprs, true));
            ++current_arrived_rf_num;
        }
    }
    // 2. Append unapplied runtime filters to _conjuncts
    if (!exprs.empty()) {
        RETURN_IF_ERROR(_append_rf_into_conjuncts(exprs));
    }
    if (current_arrived_rf_num == _runtime_filter_descs.size()) {
        _is_all_rf_applied = true;
    }

    *arrived_rf_num = current_arrived_rf_num;
    return Status::OK();
}

} // namespace doris::pipeline