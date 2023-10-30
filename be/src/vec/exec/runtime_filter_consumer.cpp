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

#include "vec/exec/runtime_filter_consumer.h"

namespace doris::vectorized {

RuntimeFilterConsumer::RuntimeFilterConsumer(const int32_t filter_id,
                                             const std::vector<TRuntimeFilterDesc>& runtime_filters,
                                             const RowDescriptor& row_descriptor,
                                             VExprContextSPtrs& conjuncts)
        : _filter_id(filter_id),
          _runtime_filter_descs(runtime_filters),
          _row_descriptor_ref(row_descriptor),
          _conjuncts_ref(conjuncts) {}

Status RuntimeFilterConsumer::init(RuntimeState* state) {
    _state = state;
    RETURN_IF_ERROR(_register_runtime_filter());
    return Status::OK();
}

void RuntimeFilterConsumer::_init_profile(RuntimeProfile* profile) {
    std::stringstream ss;
    for (auto& rf_ctx : _runtime_filter_ctxs) {
        rf_ctx.runtime_filter->init_profile(profile);
        ss << rf_ctx.runtime_filter->get_name() << ", ";
    }
    profile->add_info_string("RuntimeFilters: ", ss.str());
}

Status RuntimeFilterConsumer::_register_runtime_filter() {
    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ctxs.reserve(filter_size);
    _runtime_filter_ready_flag.reserve(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        if (filter_desc.__isset.opt_remote_rf && filter_desc.opt_remote_rf) {
            DCHECK(filter_desc.type == TRuntimeFilterType::BLOOM && filter_desc.has_remote_targets);
            // Optimize merging phase iff:
            // 1. All BE and FE has been upgraded (e.g. opt_remote_rf)
            // 2. This filter is bloom filter (only bloom filter should be used for merging)
            RETURN_IF_ERROR(_state->get_query_ctx()->runtime_filter_mgr()->register_consumer_filter(
                    filter_desc, _state->query_options(), _filter_id, false));
            RETURN_IF_ERROR(_state->get_query_ctx()->runtime_filter_mgr()->get_consume_filter(
                    filter_desc.filter_id, _filter_id, &runtime_filter));
        } else {
            RETURN_IF_ERROR(_state->runtime_filter_mgr()->register_consumer_filter(
                    filter_desc, _state->query_options(), _filter_id, false));
            RETURN_IF_ERROR(_state->runtime_filter_mgr()->get_consume_filter(
                    filter_desc.filter_id, _filter_id, &runtime_filter));
        }
        _runtime_filter_ctxs.emplace_back(runtime_filter);
        _runtime_filter_ready_flag.emplace_back(false);
    }
    return Status::OK();
}

bool RuntimeFilterConsumer::runtime_filters_are_ready_or_timeout() {
    if (!_blocked_by_rf) {
        return true;
    }
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        if (!runtime_filter->is_ready_or_timeout()) {
            return false;
        }
    }
    _blocked_by_rf = false;
    return true;
}

void RuntimeFilterConsumer::init_runtime_filter_dependency(
        doris::pipeline::RuntimeFilterDependency* _runtime_filter_dependency) {
    _runtime_filter_dependency->set_runtime_filters_are_ready_or_timeout(
            [this]() { runtime_filters_are_ready_or_timeout(); });
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        _runtime_filter_dependency->add_filters(runtime_filter);
    }
}

Status RuntimeFilterConsumer::_acquire_runtime_filter() {
    SCOPED_TIMER(_acquire_runtime_filter_timer);
    VExprSPtrs vexprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready && !_runtime_filter_ctxs[i].apply_mark) {
            RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(_probe_ctxs, vexprs, false));
            _runtime_filter_ctxs[i].apply_mark = true;
        } else if (runtime_filter->current_state() == RuntimeFilterState::NOT_READY &&
                   !_runtime_filter_ctxs[i].apply_mark) {
            _blocked_by_rf = true;
        } else if (!_runtime_filter_ctxs[i].apply_mark) {
            DCHECK(runtime_filter->current_state() != RuntimeFilterState::NOT_READY);
            _is_all_rf_applied = false;
        }
    }
    RETURN_IF_ERROR(_append_rf_into_conjuncts(vexprs));
    if (_blocked_by_rf) {
        return Status::WaitForRf("Runtime filters are neither not ready nor timeout");
    }

    return Status::OK();
}

Status RuntimeFilterConsumer::_append_rf_into_conjuncts(const VExprSPtrs& vexprs) {
    if (vexprs.empty()) {
        return Status::OK();
    }

    for (auto& expr : vexprs) {
        VExprContextSPtr conjunct = VExprContext::create_shared(expr);
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
    VExprSPtrs exprs;
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

} // namespace doris::vectorized