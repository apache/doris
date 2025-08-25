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

#include <mutex>

#include "pipeline/dependency.h"
#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
// Work on (hash/corss) join build sink node, RuntimeFilterProducerHelper will manage all RuntimeFilterProducer
// Used to generate specific predicate and publish it to consumer/merger
/**
 * send_size -> init -> insert -> publish
 */
class RuntimeFilterProducer : public RuntimeFilter {
public:
    // WAITING_FOR_SEND_SIZE -> WAITING_FOR_SYNCED_SIZE -> WAITING_FOR_DATA -> READY_TO_PUBLISH -> PUBLISHED
    enum class State {
        WAITING_FOR_SEND_SIZE =
                0, // If the rf needs to synchronize the global rf size, it is initialized to this state
        WAITING_FOR_SYNCED_SIZE =
                1, // The rf in the WAITING_FOR_SEND_SIZE state will be set to this state after the rf size is sent.
        WAITING_FOR_DATA =
                2, // rf that do not need to be synchronized in the global rf size will be initialized to this state, or WAITING_FOR_SYNCED_SIZE will also be converted to this state after receiving the global rf size.
        READY_TO_PUBLISH =
                3, // rf will be converted to this state when there are actually available filters after an insert, or it will be converted directly to this state when it is disabled/ignore for some reason during the process.
        PUBLISHED = 4 // Publish is complete, entering the final state of rf
    };

    static Status create(const QueryContext* query_ctx, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterProducer>* res) {
        *res = std::shared_ptr<RuntimeFilterProducer>(new RuntimeFilterProducer(query_ctx, desc));
        RETURN_IF_ERROR((*res)->_init_with_desc(desc, &query_ctx->query_options()));
        return Status::OK();
    }

    Status init(size_t local_size);

    Status send_size(RuntimeState* state, uint64_t local_filter_size);

    void latch_dependency(const std::shared_ptr<pipeline::CountedFinishDependency>& dependency);

    // insert data to build filter
    Status insert(vectorized::ColumnPtr column, size_t start) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        if (!_wrapper->is_valid() || _rf_state == State::READY_TO_PUBLISH ||
            _rf_state == State::PUBLISHED) {
            return Status::OK();
        }
        _check_state({State::WAITING_FOR_DATA});
        return _wrapper->insert(column, start);
    }

    Status publish(RuntimeState* state, bool build_hash_table);

    std::string debug_string() override {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        auto result =
                fmt::format("Producer: ({}, state: {}", _debug_string(), to_string(_rf_state));
        if (_need_sync_filter_size) {
            result += fmt::format(", dependency: {}, synced_size: {}",
                                  _dependency ? _dependency->debug_string() : "none", _synced_size);
        }
        return result + ")";
    }

    void set_synced_size(uint64_t global_size);

    void set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State state,
                                                std::string reason = "") {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        if (_rf_state == State::PUBLISHED || _rf_state == State::READY_TO_PUBLISH) {
            return;
        }
        _wrapper->set_state(state, reason); // set wrapper firstly to pass set_synced_size's check
        set_state(State::READY_TO_PUBLISH);
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

    bool set_state(State state) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        if (_rf_state == State::PUBLISHED ||
            (state != State::PUBLISHED && _rf_state == State::READY_TO_PUBLISH)) {
            return false;
        }
        _rf_state = state;
        return true;
    }

    std::shared_ptr<RuntimeFilterWrapper> wrapper() {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        return _wrapper;
    }
    void set_wrapper(std::shared_ptr<RuntimeFilterWrapper> wrapper) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        _wrapper = wrapper;
    }

private:
    RuntimeFilterProducer(const QueryContext* query_ctx, const TRuntimeFilterDesc* desc)
            : RuntimeFilter(desc), _is_broadcast_join(desc->is_broadcast_join) {}

    Status _send_to_remote_targets(RuntimeState* state, RuntimeFilter* merger_filter);
    Status _send_to_local_targets(RuntimeState* state, RuntimeFilter* merger_filter, bool global);

    void _check_state(std::vector<State> assumed_states) {
        if (!check_state_impl<RuntimeFilterProducer>(_rf_state, assumed_states)) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "producer meet invalid state, {}, assumed_states is {}", debug_string(),
                            states_to_string<RuntimeFilterProducer>(assumed_states));
        }
    }

    Status _init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options) override {
        RETURN_IF_ERROR(RuntimeFilter::_init_with_desc(desc, options));
        _need_sync_filter_size = _wrapper->build_bf_by_runtime_size() && !_is_broadcast_join;
        _rf_state = _need_sync_filter_size ? State::WAITING_FOR_SEND_SIZE : State::WAITING_FOR_DATA;
        return Status::OK();
    }

    const bool _is_broadcast_join;
    bool _need_sync_filter_size = false;

    int64_t _synced_size = -1;
    std::shared_ptr<pipeline::CountedFinishDependency> _dependency;

    std::atomic<State> _rf_state;
};
#include "common/compile_check_end.h"
} // namespace doris
