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

#include "pipeline/common/runtime_filter_consumer.h"

#include "pipeline/pipeline_task.h"

namespace doris::pipeline {

RuntimeFilterConsumer::RuntimeFilterConsumer(const int32_t filter_id,
                                             const std::vector<TRuntimeFilterDesc>& runtime_filters,
                                             const RowDescriptor& row_descriptor,
                                             vectorized::VExprContextSPtrs& conjuncts)
        : _filter_id(filter_id),
          _runtime_filter_descs(runtime_filters),
          _row_descriptor_ref(row_descriptor),
          _conjuncts_ref(conjuncts) {
    _blocked_by_rf = std::make_shared<std::atomic_bool>(false);
}

Status RuntimeFilterConsumer::init(RuntimeState* state, bool need_local_merge) {
    _state = state;
    RETURN_IF_ERROR(_register_runtime_filter(need_local_merge));
    return Status::OK();
}

void RuntimeFilterConsumer::_init_profile(RuntimeProfile* profile) {
    fmt::memory_buffer buffer;
    for (auto& rf_ctx : _runtime_filter_ctxs) {
        rf_ctx.runtime_filter->init_profile(profile);
        fmt::format_to(buffer, "{}, ", rf_ctx.runtime_filter->debug_string());
    }
    profile->add_info_string("RuntimeFilters: ", to_string(buffer));
}

Status RuntimeFilterConsumer::_register_runtime_filter(bool need_local_merge) {
    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ctxs.reserve(filter_size);
    _runtime_filter_ready_flag.reserve(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        RETURN_IF_ERROR(_state->register_consumer_runtime_filter(filter_desc, need_local_merge,
                                                                 _filter_id, &runtime_filter));
        _runtime_filter_ctxs.emplace_back(runtime_filter);
        _runtime_filter_ready_flag.emplace_back(false);
    }
    return Status::OK();
}

void RuntimeFilterConsumer::init_runtime_filter_dependency(
        std::vector<std::shared_ptr<pipeline::RuntimeFilterDependency>>&
                runtime_filter_dependencies,
        const int id, const int node_id, const std::string& name) {
    runtime_filter_dependencies.resize(_runtime_filter_descs.size());
    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> runtime_filter_timers(
            _runtime_filter_descs.size());
    std::vector<std::shared_ptr<pipeline::RuntimeFilterDependency>>
            local_runtime_filter_dependencies;

    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        runtime_filter_dependencies[i] = std::make_shared<pipeline::RuntimeFilterDependency>(
                id, node_id, name, runtime_filter);
        _runtime_filter_ctxs[i].runtime_filter_dependency = runtime_filter_dependencies[i].get();
        runtime_filter_timers[i] = std::make_shared<pipeline::RuntimeFilterTimer>(
                runtime_filter->registration_time(), runtime_filter->wait_time_ms(),
                runtime_filter_dependencies[i]);
        runtime_filter->set_filter_timer(runtime_filter_timers[i]);
        if (runtime_filter->has_local_target()) {
            local_runtime_filter_dependencies.emplace_back(runtime_filter_dependencies[i]);
        }
    }

    // The gloabl runtime filter timer need set local runtime filter dependencies.
    // start to wait before the local runtime filter ready
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        if (!runtime_filter->has_local_target()) {
            runtime_filter_timers[i]->set_local_runtime_filter_dependencies(
                    local_runtime_filter_dependencies);
        }
    }
    if (!runtime_filter_timers.empty()) {
        ExecEnv::GetInstance()->runtime_filter_timer_queue()->push_filter_timer(
                std::move(runtime_filter_timers));
    }
}

Status RuntimeFilterConsumer::_acquire_runtime_filter(bool pipeline_x) {
    SCOPED_TIMER(_acquire_runtime_filter_timer);
    std::vector<vectorized::VRuntimeFilterPtr> vexprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        if (pipeline_x) {
            runtime_filter->update_state();
            if (runtime_filter->is_ready() && !_runtime_filter_ctxs[i].apply_mark) {
                // Runtime filter has been applied in open phase.
                RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(_probe_ctxs, vexprs, false));
                _runtime_filter_ctxs[i].apply_mark = true;
            } else if (!_runtime_filter_ctxs[i].apply_mark) {
                // Runtime filter is timeout.
                _is_all_rf_applied = false;
            }
        } else {
            bool ready = runtime_filter->is_ready();
            if (!ready) {
                ready = runtime_filter->await();
            }
            if (ready && !_runtime_filter_ctxs[i].apply_mark) {
                RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(_probe_ctxs, vexprs, false));
                _runtime_filter_ctxs[i].apply_mark = true;
            } else if (runtime_filter->current_state() == RuntimeFilterState::NOT_READY &&
                       !_runtime_filter_ctxs[i].apply_mark) {
                *_blocked_by_rf = true;
            } else if (!_runtime_filter_ctxs[i].apply_mark) {
                DCHECK(runtime_filter->current_state() != RuntimeFilterState::NOT_READY);
                _is_all_rf_applied = false;
            }
        }
    }
    RETURN_IF_ERROR(_append_rf_into_conjuncts(vexprs));
    if (!pipeline_x && *_blocked_by_rf) {
        return Status::WaitForRf("Runtime filters are neither not ready nor timeout");
    }

    return Status::OK();
}

Status RuntimeFilterConsumer::_append_rf_into_conjuncts(
        const std::vector<vectorized::VRuntimeFilterPtr>& vexprs) {
    if (vexprs.empty()) {
        return Status::OK();
    }

    for (const auto& expr : vexprs) {
        vectorized::VExprContextSPtr conjunct = vectorized::VExprContext::create_shared(expr);
        RETURN_IF_ERROR(conjunct->prepare(_state, _row_descriptor_ref));
        RETURN_IF_ERROR(conjunct->open(_state));
        _rf_vexpr_set.insert(expr);
        _conjuncts_ref.emplace_back(conjunct);
    }

    return Status::OK();
}

Status RuntimeFilterConsumer::try_append_late_arrival_runtime_filter(int* arrived_rf_num) {
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
        if (_runtime_filter_ctxs[i].apply_mark) {
            ++current_arrived_rf_num;
            continue;
        } else if (_runtime_filter_ctxs[i].runtime_filter->is_ready()) {
            RETURN_IF_ERROR(_runtime_filter_ctxs[i].runtime_filter->get_push_expr_ctxs(
                    _probe_ctxs, exprs, true));
            ++current_arrived_rf_num;
            _runtime_filter_ctxs[i].apply_mark = true;
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

void RuntimeFilterConsumer::_prepare_rf_timer(RuntimeProfile* profile) {
    _acquire_runtime_filter_timer = ADD_TIMER(profile, "AcquireRuntimeFilterTime");
}

} // namespace doris::pipeline