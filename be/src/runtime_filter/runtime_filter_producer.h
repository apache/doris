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

class RuntimeFilterProducer : public RuntimeFilter {
public:
    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterProducer>* res) {
        *res = std::shared_ptr<RuntimeFilterProducer>(new RuntimeFilterProducer(state, desc));
        RETURN_IF_ERROR((*res)->_init_with_desc(desc, &state->get_query_ctx()->query_options()));
        (*res)->_rf_state = (*res)->need_sync_filter_size() ? State::WAITING_FOR_SEND_SIZE
                                                            : State::WAITING_FOR_DATA;
        return Status::OK();
    }

    // insert data to build filter
    void insert_batch(vectorized::ColumnPtr column, size_t start) {
        _wrapper->insert_batch(column, start);
    }

    int expr_order() const { return _expr_order; }

    bool need_sync_filter_size() {
        return _wrapper->get_build_bf_cardinality() && !_is_broadcast_join;
    }

    Status init_with_size(size_t local_size);

    Status send_filter_size(RuntimeState* state, uint64_t local_filter_size);

    Status publish(RuntimeState* state, bool publish_local);

    void set_synced_size(uint64_t global_size);

    void set_finish_dependency(
            const std::shared_ptr<pipeline::CountedFinishDependency>& dependency);

    int64_t get_synced_size() const {
        if (_synced_size == -1 || !_dependency) {
            throw Exception(doris::ErrorCode::INTERNAL_ERROR,
                            "sync filter size meet error, filter: {}", debug_string());
        }
        return _synced_size;
    }

    std::shared_ptr<RuntimeFilterWrapper>& get_wrapper_ref() { return _wrapper; }

    std::string debug_string() const {
        return fmt::format(
                "RuntimeFilterProducer: ({}, state: {}, dependency: {}, synced_size: {}]",
                _debug_string(), _to_string(_rf_state),
                _dependency ? _dependency->debug_string() : "none", _synced_size);
    }

    enum class State {
        WAITING_FOR_SEND_SIZE = 0,
        WAITING_FOR_SYNCED_SIZE = 1,
        WAITING_FOR_DATA = 2,
        READY_TO_PUBLISH = 3,
        PUBLISHED = 4
    };

    void set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State state) {
        DCHECK(state == RuntimeFilterWrapper::State::IGNORED ||
               state == RuntimeFilterWrapper::State::DISABLED);
        _wrapper->set_state(state);
        _rf_state = State::READY_TO_PUBLISH;
    }

    bool is_ready_to_publish_or_published() {
        return _rf_state == State::READY_TO_PUBLISH || _rf_state == State::PUBLISHED;
    }

private:
    RuntimeFilterProducer(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc)
            : RuntimeFilter(state, desc),
              _is_broadcast_join(desc->is_broadcast_join),
              _expr_order(desc->expr_order) {}

    Status _send_to_remote_targets(RuntimeState* state, RuntimeFilter* filter,
                                   uint64_t local_merge_time);
    Status _send_to_local_targets(std::shared_ptr<RuntimeFilterWrapper> wrapper, bool global,
                                  uint64_t local_merge_time);

    static std::string _to_string(const State& state) {
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
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid state {}",
                                   int(state));
        }
    }

    bool _is_broadcast_join;
    int _expr_order;

    int64_t _synced_size = -1;
    std::shared_ptr<pipeline::CountedFinishDependency> _dependency;

    std::atomic<State> _rf_state;
};

} // namespace doris
