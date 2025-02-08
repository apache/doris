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

#include "exprs/runtime_filter/runtime_filter.h"

namespace doris {

class RuntimeFilterProducer : public RuntimeFilter {
public:
    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterProducer>* res) {
        *res = std::shared_ptr<RuntimeFilterProducer>(new RuntimeFilterProducer(state, desc));
        return (*res)->_init_with_desc(desc, &state->get_query_ctx()->query_options());
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

    RuntimeFilterContextSPtr& get_shared_context_ref() { return _wrapper->_context; }

    std::string debug_string() const {
        return fmt::format("RuntimeFilterProducer: ({}, dependency: {}, synced_size: {}]",
                           _debug_string(), _dependency ? _dependency->debug_string() : "none",
                           _synced_size);
    }

private:
    RuntimeFilterProducer(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc)
            : RuntimeFilter(state, desc),
              _is_broadcast_join(desc->is_broadcast_join),
              _expr_order(desc->expr_order) {}

    Status _send_to_remote_targets(RuntimeState* state, RuntimeFilter* filter,
                                   uint64_t local_merge_time);
    Status _send_to_local_targets(std::shared_ptr<RuntimePredicateWrapper> wrapper, bool global,
                                  uint64_t local_merge_time);

    bool _is_broadcast_join;
    int _expr_order;

    int64_t _synced_size = -1;
    std::shared_ptr<pipeline::CountedFinishDependency> _dependency;
};

} // namespace doris
