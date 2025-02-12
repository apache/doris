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

#include "runtime_filter/runtime_filter_consumer.h"

#include <mutex>

#include "exprs/bitmapfilter_predicate.h"

namespace doris {

Status RuntimeFilterConsumer::_apply_ready_expr(
        std::list<vectorized::VExprContextSPtr>& probe_ctxs,
        std::vector<vectorized::VRuntimeFilterPtr>& push_exprs) {
    _check_state({State::READY, State::APPLIED});
    _rf_state = State::APPLIED;
    _profile->add_info_string("Info", debug_string());

    if (_wrapper->get_state() != RuntimeFilterWrapper::State::READY) {
        DCHECK(_wrapper->get_state() == RuntimeFilterWrapper::State::DISABLED ||
               _wrapper->get_state() == RuntimeFilterWrapper::State::IGNORED);
        return Status::OK();
    }

    auto origin_size = push_exprs.size();
    RETURN_IF_ERROR(_wrapper->get_push_exprs(probe_ctxs, push_exprs, _probe_expr));
    // The runtime filter is pushed down, adding filtering information.
    auto* expr_filtered_rows_counter = ADD_COUNTER(_profile, "ExprFilteredRows", TUnit::UNIT);
    auto* expr_input_rows_counter = ADD_COUNTER(_profile, "ExprInputRows", TUnit::UNIT);
    auto* always_true_counter = ADD_COUNTER(_profile, "AlwaysTruePassRows", TUnit::UNIT);
    for (auto i = origin_size; i < push_exprs.size(); i++) {
        push_exprs[i]->attach_profile_counter(expr_filtered_rows_counter, expr_input_rows_counter,
                                              always_true_counter);
    }
    return Status::OK();
}

Status RuntimeFilterConsumer::acquire_expr(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                                           std::vector<vectorized::VRuntimeFilterPtr>& push_exprs) {
    std::unique_lock lock(_inner_mutex);
    if (_rf_state == State::READY || _rf_state == State::APPLIED) {
        RETURN_IF_ERROR(_apply_ready_expr(probe_ctxs, push_exprs));
    }
    if (_rf_state != State::APPLIED && _rf_state != State::TIMEOUT) {
        DCHECK(MonotonicMillis() - _registration_time >= _rf_wait_time_ms);
        COUNTER_SET(_wait_timer,
                    int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
        _check_state({State::NOT_READY});
        _rf_state = State::TIMEOUT;
    }
    return Status::OK();
}

void RuntimeFilterConsumer::signal(RuntimeFilter* other) {
    COUNTER_SET(_wait_timer, int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
    _check_state({State::NOT_READY, State::TIMEOUT});
    _rf_state = State::READY;
    _wrapper = other->_wrapper;
    if (!_filter_timer.empty()) {
        for (auto& timer : _filter_timer) {
            timer->call_ready();
        }
    }

    if (_wrapper->get_real_type() == RuntimeFilterType::IN_FILTER) {
        _profile->add_info_string("InFilterSize", std::to_string(_wrapper->get_in_filter_size()));
        _profile->add_info_string("MaxInNum", std::to_string(_wrapper->_max_in_num));
    }
    if (_wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
        auto bitmap_filter = _wrapper->get_bitmap_filter();
        _profile->add_info_string("BitmapSize", std::to_string(bitmap_filter->size()));
        _profile->add_info_string("IsNotIn", bitmap_filter->is_not_in() ? "true" : "false");
    }
    if (_wrapper->get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
        _profile->add_info_string("BloomFilterSize",
                                  std::to_string(_wrapper->get_bloom_filter_size()));
    }
}

std::shared_ptr<pipeline::RuntimeFilterTimer> RuntimeFilterConsumer::create_filter_timer(
        std::shared_ptr<pipeline::RuntimeFilterDependency> dependencie) {
    auto timer = std::make_shared<pipeline::RuntimeFilterTimer>(_registration_time,
                                                                _rf_wait_time_ms, dependencie);
    std::unique_lock lock(_inner_mutex);
    _filter_timer.push_back(timer);
    return timer;
}

} // namespace doris
