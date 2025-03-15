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

#include <string>

#include "pipeline/dependency.h"
#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter.h"
#include "util/runtime_profile.h"

namespace doris {
// Work on ScanNode or MultiCastDataStreamSource, RuntimeFilterConsumerHelper will manage all RuntimeFilterConsumer
// Used to create VRuntimeFilterWrapper to filter data
class RuntimeFilterConsumer : public RuntimeFilter {
public:
    // NOT_READY (-> TIMEOUT) -> READY -> APPLIED
    enum class State {
        NOT_READY, // The initial state of consumer
        READY,     // The consumer will switch to this state when it is signaled.
        TIMEOUT, // After a consumer has not been ready for a long time(_rf_wait_time_ms), it will be transferred to this state
        APPLIED, // The consumer will switch to this state after the expression is acquired
    };

    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         int node_id, std::shared_ptr<RuntimeFilterConsumer>* res,
                         RuntimeProfile* parent_profile) {
        *res = std::shared_ptr<RuntimeFilterConsumer>(
                new RuntimeFilterConsumer(state, desc, node_id, parent_profile));
        RETURN_IF_ERROR((*res)->_init_with_desc(desc, &state->get_query_ctx()->query_options()));
        (*res)->_profile->add_info_string("Info", ((*res)->debug_string()));
        return Status::OK();
    }

    // Published by producer.
    void signal(RuntimeFilter* other);

    std::shared_ptr<pipeline::RuntimeFilterTimer> create_filter_timer(
            std::shared_ptr<pipeline::RuntimeFilterDependency> dependencies);

    // Called after `State` is ready (e.g. signaled)
    Status acquire_expr(std::vector<vectorized::VRuntimeFilterPtr>& push_exprs);

    std::string debug_string() const override {
        return fmt::format("Consumer: ({}, state: {})", _debug_string(), to_string(_rf_state));
    }

    bool is_applied() { return _rf_state == State::APPLIED; }

    static std::string to_string(const State& state) {
        switch (state) {
        case State::NOT_READY:
            return "NOT_READY";
        case State::READY:
            return "READY";
        case State::TIMEOUT:
            return "TIMEOUT";
        case State::APPLIED:
            return "APPLIED";
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid State {}", int(state));
        }
    }

private:
    RuntimeFilterConsumer(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                          int node_id, RuntimeProfile* parent_profile)
            : RuntimeFilter(state, desc),
              _probe_expr(desc->planId_to_target_expr.find(node_id)->second),
              _profile(new RuntimeProfile(fmt::format("RF{}", desc->filter_id))),
              _storage_profile(new RuntimeProfile(fmt::format("Storage", desc->filter_id))),
              _execution_profile(new RuntimeProfile(fmt::format("Execution", desc->filter_id))),
              _registration_time(MonotonicMillis()),
              _rf_state(State::NOT_READY) {
        // If bitmap filter is not applied, it will cause the query result to be incorrect
        bool wait_infinitely = _state->get_query_ctx()->runtime_filter_wait_infinitely() ||
                               _runtime_filter_type == RuntimeFilterType::BITMAP_FILTER;
        _rf_wait_time_ms = wait_infinitely ? _state->get_query_ctx()->execution_timeout() * 1000
                                           : _state->get_query_ctx()->runtime_filter_wait_time_ms();
        _profile->add_info_string("TimeoutLimit", std::to_string(_rf_wait_time_ms) + "ms");

        parent_profile->add_child(_profile.get(), true, nullptr);
        _profile->add_child(_storage_profile.get(), true, nullptr);
        _profile->add_child(_execution_profile.get(), true, nullptr);
        _wait_timer = ADD_TIMER(_profile, "WaitTime");

        DorisMetrics::instance()->runtime_filter_consumer_num->increment(1);
    }

    Status _apply_ready_expr(std::vector<vectorized::VRuntimeFilterPtr>& push_exprs);

    Status _get_push_exprs(std::vector<vectorized::VRuntimeFilterPtr>& container,
                           const TExpr& probe_expr);

    void _check_state(std::vector<State> assumed_states) {
        if (!check_state_impl<RuntimeFilterConsumer>(_rf_state, assumed_states)) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "consumer meet invalid state, {}, assumed_states is {}", debug_string(),
                            states_to_string<RuntimeFilterConsumer>(assumed_states));
        }
    }

    void _set_state(State rf_state) {
        _rf_state = rf_state;
        _profile->add_info_string("Info", debug_string());
    }

    TExpr _probe_expr;

    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> _filter_timer;

    std::unique_ptr<RuntimeProfile> _profile;
    std::unique_ptr<RuntimeProfile> _storage_profile;   // for storage layer stats
    std::unique_ptr<RuntimeProfile> _execution_profile; // for execution layer stats
    RuntimeProfile::Counter* _wait_timer = nullptr;

    int32_t _rf_wait_time_ms;
    const int64_t _registration_time;

    std::atomic<State> _rf_state;

    friend class RuntimeFilterProducer;
};

} // namespace doris
