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

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <string>

#include "pipeline/dependency.h"
#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
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

    static Status create(const QueryContext* query_ctx, const TRuntimeFilterDesc* desc, int node_id,
                         std::shared_ptr<RuntimeFilterConsumer>* res) {
        *res = std::shared_ptr<RuntimeFilterConsumer>(
                new RuntimeFilterConsumer(query_ctx, desc, node_id));
        RETURN_IF_ERROR((*res)->_init_with_desc(desc, &query_ctx->query_options()));
        return Status::OK();
    }

    // Published by producer.
    void signal(RuntimeFilter* other);

    std::shared_ptr<pipeline::RuntimeFilterTimer> create_filter_timer(
            std::shared_ptr<pipeline::Dependency> dependencies);

    // Called after `State` is ready (e.g. signaled)
    Status acquire_expr(std::vector<vectorized::VRuntimeFilterPtr>& push_exprs);

    std::string debug_string() override {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        return fmt::format("Consumer: ({}, state: {}, reached_timeout: {}, timeout_limit: {}ms)",
                           _debug_string(), to_string(_rf_state),
                           _reached_timeout ? "true" : "false", std::to_string(_rf_wait_time_ms));
    }

    bool is_applied() const { return _rf_state == State::APPLIED; }

    // Called by RuntimeFilterConsumerHelper
    void collect_realtime_profile(RuntimeProfile* parent_operator_profile);

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
    RuntimeFilterConsumer(const QueryContext* query_ctx, const TRuntimeFilterDesc* desc,
                          int node_id)
            : RuntimeFilter(desc),
              _probe_expr(desc->planId_to_target_expr.find(node_id)->second),
              _registration_time(MonotonicMillis()),
              _rf_state(State::NOT_READY) {
        // If bitmap filter is not applied, it will cause the query result to be incorrect
        bool wait_infinitely = query_ctx->runtime_filter_wait_infinitely() ||
                               _runtime_filter_type == RuntimeFilterType::BITMAP_FILTER;
        _rf_wait_time_ms = wait_infinitely ? query_ctx->execution_timeout() * 1000
                                           : query_ctx->runtime_filter_wait_time_ms();
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

    void _set_state(State rf_state, std::shared_ptr<RuntimeFilterWrapper> other = nullptr) {
        if (rf_state == State::TIMEOUT) {
            DorisMetrics::instance()->runtime_filter_consumer_timeout_num->increment(1);
            _reached_timeout = true;
            if (_rf_state != State::NOT_READY) {
                // reach timeout but do not change State::ready to State::timeout
                return;
            }
        } else if (rf_state == State::READY) {
            DorisMetrics::instance()->runtime_filter_consumer_ready_num->increment(1);
            DorisMetrics::instance()->runtime_filter_consumer_wait_ready_ms->increment(
                    MonotonicMillis() - _registration_time);
            _wrapper = other;
            _check_wrapper_state(
                    {RuntimeFilterWrapper::State::DISABLED, RuntimeFilterWrapper::State::READY});
            _check_state({State::NOT_READY, State::TIMEOUT});
        }
        _rf_state = rf_state;
    }

    TExpr _probe_expr;

    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> _filter_timer;

    std::shared_ptr<RuntimeProfile::Counter> _wait_timer =
            std::make_shared<RuntimeProfile::Counter>(TUnit::TIME_NS, 0);
    //_rf_filter is used to record the number of rows filtered by the runtime filter.
    //It aggregates the filtering statistics from both the Storage and Execution.
    // Counter will be shared by RuntimeFilterConsumer & VRuntimeFilterWrapper
    // OperatorLocalState's close method will collect the statistics from RuntimeFilterConsumer
    // VRuntimeFilterWrapper will update the statistics.
    std::shared_ptr<RuntimeProfile::Counter> _rf_filter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0, 1);
    std::shared_ptr<RuntimeProfile::Counter> _rf_input =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0, 1);
    std::shared_ptr<RuntimeProfile::Counter> _always_true_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0, 1);

    int32_t _rf_wait_time_ms;
    const int64_t _registration_time;

    std::atomic<State> _rf_state;

    bool _reached_timeout = false;

    friend class RuntimeFilterProducer;
};
#include "common/compile_check_end.h"
} // namespace doris
