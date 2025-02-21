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

#include "pipeline/dependency.h"
#include "runtime/query_context.h"
#include "runtime_filter/role/runtime_filter.h"
#include "vec/runtime/shared_hash_table_controller.h"

namespace doris {

/**
 * init -> send_size -> insert -> publish
 */
class RuntimeFilterProducer : public RuntimeFilter {
public:
    using Callback = std::function<void()>;
    enum class State {
        WAITING_FOR_SEND_SIZE = 0,
        WAITING_FOR_SYNCED_SIZE = 1,
        WAITING_FOR_DATA = 2,
        READY_TO_PUBLISH = 3,
        PUBLISHED = 4
    };
    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterProducer>* res,
                         RuntimeProfile* parent_profile) {
        *res = std::shared_ptr<RuntimeFilterProducer>(
                new RuntimeFilterProducer(state, desc, parent_profile));
        RETURN_IF_ERROR((*res)->_init_with_desc(desc, &state->get_query_ctx()->query_options()));
        bool need_sync_filter_size =
                (*res)->_wrapper->build_bf_by_runtime_size() && !(*res)->_is_broadcast_join;
        (*res)->_rf_state =
                need_sync_filter_size ? State::WAITING_FOR_SEND_SIZE : State::WAITING_FOR_DATA;
        (*res)->_profile->add_info_string("Info", ((*res)->debug_string()));
        return Status::OK();
    }

    Status init(size_t local_size);
    Status send_size(RuntimeState* state, uint64_t local_filter_size,
                     const std::shared_ptr<pipeline::CountedFinishDependency>& dependency);
    // insert data to build filter
    void insert(vectorized::ColumnPtr column, size_t start) {
        if (_rf_state == State::READY_TO_PUBLISH || _rf_state == State::PUBLISHED) {
            DCHECK(!_wrapper->is_valid());
            return;
        }
        _check_state({State::WAITING_FOR_DATA});
        _wrapper->insert_batch(column, start);
    }
    Status publish(RuntimeState* state, bool build_hash_table);
    std::string debug_string() const override {
        return fmt::format("Producer: ({}, state: {}, dependency: {}, synced_size: {})",
                           _debug_string(), to_string(_rf_state),
                           _dependency ? _dependency->debug_string() : "none", _synced_size);
    }

    void with_callback(Callback& callback) { _callback.emplace_back(callback); }
    int expr_order() const { return _expr_order; }
    void set_synced_size(uint64_t global_size);
    void set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State state,
                                                std::string reason = "") {
        if (set_state(State::READY_TO_PUBLISH)) {
            _wrapper->set_state(state, reason);
        }
    }

    static std::string to_string(const State& state) {
        switch (state) {
        case State::WAITING_FOR_SEND_SIZE:
            return "WAITING_FOR_SEND_SIZE";
        case State::WAITING_FOR_SYNCED_SIZE:
            return "WAITING_FOR_SYNCED_SIZE";
        case State::WAITING_FOR_DATA:
            return "WAITING_FOR_DATA";
        case State::READY_TO_PUBLISH:
            return "READY_TO_PUBLISH";
        case State::PUBLISHED:
            return "PUBLISHED";
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid state {}", int(state));
        }
    }

    void copy_to_shared_context(vectorized::SharedHashTableContextPtr& context) {
        context->runtime_filters[_wrapper->filter_id()] = _wrapper;
    }
    void copy_from_shared_context(vectorized::SharedHashTableContextPtr& context) {
        _wrapper = context->runtime_filters[_wrapper->filter_id()];
    }

    bool set_state(State state) {
        if (_rf_state == State::PUBLISHED ||
            (state != State::PUBLISHED && _rf_state == State::READY_TO_PUBLISH)) {
            return false;
        }
        _rf_state = state;
        _profile->add_info_string("Info", debug_string());
        return true;
    }

private:
    RuntimeFilterProducer(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                          RuntimeProfile* parent_profile)
            : RuntimeFilter(state, desc),
              _is_broadcast_join(desc->is_broadcast_join),
              _expr_order(desc->expr_order),
              _profile(new RuntimeProfile(fmt::format("RF{}", desc->filter_id))) {
        if (parent_profile) { //tmp filter for mgr has no profile
            parent_profile->add_child(_profile.get(), true, nullptr);
        }
    }

    Status _send_to_remote_targets(RuntimeState* state, RuntimeFilter* merger_filter);
    Status _send_to_local_targets(RuntimeFilter* merger_filter, bool global);

    void _check_state(std::vector<State> assumed_states) {
        if (!check_state_impl<RuntimeFilterProducer>(_rf_state, assumed_states)) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "producer meet invalid state, {}, assumed_states is {}", debug_string(),
                            states_to_string<RuntimeFilterProducer>(assumed_states));
        }
    }

    const bool _is_broadcast_join;
    const int _expr_order;

    int64_t _synced_size = -1;
    std::shared_ptr<pipeline::CountedFinishDependency> _dependency;

    std::atomic<State> _rf_state;
    std::unique_ptr<RuntimeProfile> _profile;
    std::vector<Callback> _callback;
};

} // namespace doris
