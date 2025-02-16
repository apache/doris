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

#include "runtime_filter/runtime_filter.h"

namespace doris {

class RuntimeFilterConsumer : public RuntimeFilter {
public:
    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         int node_id, std::shared_ptr<RuntimeFilterConsumer>* res) {
        *res = std::shared_ptr<RuntimeFilterConsumer>(
                new RuntimeFilterConsumer(state, desc, node_id));
        return (*res)->_init_with_desc(desc, &state->get_query_ctx()->query_options());
    }

    int node_id() const { return _node_id; }

    Status apply_ready_expr(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                            std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                            bool is_late_arrival);

    std::string formatted_state() const;

    void init_profile(RuntimeProfile* parent_profile);

    void signal();

    void update_filter(const RuntimeFilter* other, int64_t merge_time, int64_t start_apply,
                       uint64_t local_merge_time);

    std::shared_ptr<pipeline::RuntimeFilterTimer> create_filter_timer(
            std::shared_ptr<pipeline::RuntimeFilterDependency> dependencie);

    void update_state();

    std::string debug_string() const {
        return fmt::format("RuntimeFilterConsumer: ({})", _debug_string());
    }

    void update_runtime_filter_type_to_profile(uint64_t local_merge_time) {
        _profile->add_info_string("RealRuntimeFilterType", to_string(_wrapper->get_real_type()));
        _profile->add_info_string("LocalMergeTime",
                                  std::to_string((double)local_merge_time / NANOS_PER_SEC) + " s");
    }

    bool is_ready() { return _rf_state == State::READY; }
    bool is_applied() { return _rf_state == State::APPLIED; }

    enum class State {
        IGNORED,
        DISABLED,
        NOT_READY,
        READY,
        TIMEOUT,
        APPLIED,
        LATE_APPLIED,
    };

private:
    RuntimeFilterConsumer(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                          int node_id)
            : RuntimeFilter(state, desc),
              _node_id(node_id),
              _probe_expr(desc->planId_to_target_expr.find(node_id)->second),
              _profile(new RuntimeProfile(fmt::format("RuntimeFilter: (id = {}, type = {})",
                                                      desc->filter_id,
                                                      to_string(_runtime_filter_type)))),
              _registration_time(MonotonicMillis()),
              _rf_state(State::NOT_READY) {
        // If bitmap filter is not applied, it will cause the query result to be incorrect
        bool wait_infinitely = _state->get_query_ctx()->runtime_filter_wait_infinitely() ||
                               _runtime_filter_type == RuntimeFilterType::BITMAP_FILTER;
        _rf_wait_time_ms = wait_infinitely ? _state->get_query_ctx()->execution_timeout() * 1000
                                           : _state->get_query_ctx()->runtime_filter_wait_time_ms();
    }

    static std::string _to_string(const State& state) {
        switch (state) {
        case State::IGNORED:
            return "IGNORED";
        case State::DISABLED:
            return "DISABLED";
        case State::NOT_READY:
            return "NOT_READY";
        case State::READY:
            return "READY";
        case State::TIMEOUT:
            return "TIMEOUT";
        case State::APPLIED:
            return "APPLIED";
        case State::LATE_APPLIED:
            return "LATE_APPLIED";
        default:
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid State {}",
                                   int(state));
        }
    }

    int _node_id;

    TExpr _probe_expr;

    std::mutex _inner_mutex;

    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> _filter_timer;

    std::atomic<bool> _profile_init = false;
    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _wait_timer = nullptr;

    int32_t _rf_wait_time_ms;
    const int64_t _registration_time;

    std::atomic<State> _rf_state;

    friend class RuntimeFilterProducer;
};

} // namespace doris
