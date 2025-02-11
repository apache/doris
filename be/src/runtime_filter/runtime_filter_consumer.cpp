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

#include "exprs/bitmapfilter_predicate.h"

namespace doris {

Status RuntimeFilterConsumer::apply_ready_expr(
        std::list<vectorized::VExprContextSPtr>& probe_ctxs,
        std::vector<vectorized::VRuntimeFilterPtr>& push_exprs, bool is_late_arrival) {
    auto origin_size = push_exprs.size();
    if (_wrapper->get_state() == RuntimeFilterWrapper::State::READY) {
        RETURN_IF_ERROR(_wrapper->get_push_exprs(probe_ctxs, push_exprs, _probe_expr));
    }
    _profile->add_info_string("Info", formatted_state());
    // The runtime filter is pushed down, adding filtering information.
    auto* expr_filtered_rows_counter = ADD_COUNTER(_profile, "ExprFilteredRows", TUnit::UNIT);
    auto* expr_input_rows_counter = ADD_COUNTER(_profile, "ExprInputRows", TUnit::UNIT);
    auto* always_true_counter = ADD_COUNTER(_profile, "AlwaysTruePassRows", TUnit::UNIT);
    for (auto i = origin_size; i < push_exprs.size(); i++) {
        push_exprs[i]->attach_profile_counter(expr_filtered_rows_counter, expr_input_rows_counter,
                                              always_true_counter);
    }
    if (is_late_arrival) {
        _rf_state = State::LATE_APPLIED;
    } else {
        _rf_state = State::APPLIED;
    }
    return Status::OK();
}

std::string RuntimeFilterConsumer::formatted_state() const {
    return fmt::format(
            "[Id = {}, State = {}, HasRemoteTarget = {}, "
            "HasLocalTarget = {}, Wrapper = [{}], WaitTimeMS = {}]",
            filter_id(), _to_string(_rf_state), _has_remote_target, _has_local_target,
            _wrapper->debug_string(), _rf_wait_time_ms);
}

void RuntimeFilterConsumer::init_profile(RuntimeProfile* parent_profile) {
    if (_profile_init) {
        parent_profile->add_child(_profile.get(), true, nullptr);
    } else {
        _profile_init = true;
        parent_profile->add_child(_profile.get(), true, nullptr);
        _profile->add_info_string("Info", formatted_state());
        _wait_timer = ADD_TIMER(_profile, "WaitTime");
    }
}

void RuntimeFilterConsumer::signal() {
    COUNTER_SET(_wait_timer, int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
    _rf_state = State::READY;
    if (!_filter_timer.empty()) {
        for (auto& timer : _filter_timer) {
            timer->call_ready();
        }
    }

    if (_wrapper->get_real_type() == RuntimeFilterType::IN_FILTER) {
        _profile->add_info_string("InFilterSize", std::to_string(_wrapper->get_in_filter_size()));
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

void RuntimeFilterConsumer::update_filter(const RuntimeFilter* other, int64_t merge_time,
                                          int64_t start_apply, uint64_t local_merge_time) {
    _profile->add_info_string("UpdateTime",
                              std::to_string(MonotonicMillis() - start_apply) + " ms");
    _profile->add_info_string("MergeTime", std::to_string(merge_time) + " ms");
    _wrapper = other->_wrapper;
    update_runtime_filter_type_to_profile(local_merge_time);
    signal();
}

std::shared_ptr<pipeline::RuntimeFilterTimer> RuntimeFilterConsumer::create_filter_timer(
        std::shared_ptr<pipeline::RuntimeFilterDependency> dependencie) {
    auto timer = std::make_shared<pipeline::RuntimeFilterTimer>(_registration_time,
                                                                _rf_wait_time_ms, dependencie);
    std::unique_lock lock(_inner_mutex);
    _filter_timer.push_back(timer);
    return timer;
}

void RuntimeFilterConsumer::update_state() {
    auto execution_timeout = _state->get_query_ctx()->execution_timeout() * 1000;
    auto runtime_filter_wait_time_ms = _state->get_query_ctx()->runtime_filter_wait_time_ms();
    // bitmap filter is precise filter and only filter once, so it must be applied.
    int64_t wait_times_ms = _runtime_filter_type == RuntimeFilterType::BITMAP_FILTER
                                    ? execution_timeout
                                    : runtime_filter_wait_time_ms;

    if (!is_applied()) {
        DCHECK(MonotonicMillis() - _registration_time >= wait_times_ms);
        COUNTER_SET(_wait_timer,
                    int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
        _rf_state = State::TIMEOUT;
    }
}

} // namespace doris
