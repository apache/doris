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

#include <gen_cpp/PaloInternalService_types.h>

#include "common/status.h"
#include "exprs/runtime_filter/runtime_filter_definitions.h"
#include "exprs/runtime_filter/runtime_filter_wrapper.h"
#include "exprs/runtime_filter/utils.h"
#include "pipeline/dependency.h"
#include "runtime/query_context.h"

namespace doris {
class RuntimePredicateWrapper;
class RuntimeProfile;

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class RuntimeFilter {
public:
    RuntimeFilterType type() const { return _runtime_filter_type; }

    bool has_local_target() const { return _has_local_target; }

    RuntimeFilterState current_state() const { return _rf_state.load(std::memory_order_acquire); }

    void set_ignored();

    bool get_ignored();

    void set_disabled();
    bool get_disabled() const;

    RuntimeFilterType get_real_type();

    int filter_id() const { return _wrapper->_filter_id; }

    template <class T>
    Status assign_data_into_wrapper(const T& request, butil::IOBufAsZeroCopyInputStream* data) {
        return _wrapper->assign_data(request, data);
    }

    template <class T>
    Status serialize(T* request, void** data, int* len) {
        auto real_runtime_filter_type = _wrapper->get_real_type();

        request->set_filter_type(get_type(real_runtime_filter_type));
        request->set_contain_null(_wrapper->contain_null());

        if (real_runtime_filter_type == RuntimeFilterType::IN_FILTER) {
            auto in_filter = request->mutable_in_filter();
            _to_protobuf(in_filter);
        } else if (real_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER) {
            _wrapper->get_bloom_filter_desc((char**)data, len);
            DCHECK(data != nullptr);
            request->mutable_bloom_filter()->set_filter_length(*len);
            request->mutable_bloom_filter()->set_always_true(false);
        } else if (real_runtime_filter_type == RuntimeFilterType::MINMAX_FILTER ||
                   real_runtime_filter_type == RuntimeFilterType::MIN_FILTER ||
                   real_runtime_filter_type == RuntimeFilterType::MAX_FILTER) {
            auto minmax_filter = request->mutable_minmax_filter();
            _to_protobuf(minmax_filter);
        } else {
            return Status::InternalError("not implemented !");
        }
        return Status::OK();
    }

protected:
    RuntimeFilter(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc)
            : _state(state),
              _has_remote_target(desc->has_remote_targets),
              _has_local_target(desc->has_local_targets),
              _rf_state(RuntimeFilterState::NOT_READY),
              _runtime_filter_type(get_runtime_filter_type(desc)) {
        DCHECK_NE(_has_remote_target, _has_local_target);
    }

    Status _init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options);

    // serialize _wrapper to protobuf
    void _to_protobuf(PInFilter* filter);
    void _to_protobuf(PMinMaxFilter* filter);

    Status _push_to_remote(RuntimeState* state, const TNetworkAddress* addr,
                           uint64_t local_merge_time);

    std::string _debug_string() const;

    RuntimeFilterParamsContext* _state = nullptr;
    // _wrapper is a runtime filter function wrapper
    std::shared_ptr<RuntimePredicateWrapper> _wrapper;

    // will apply to remote node
    bool _has_remote_target;
    // will apply to local node
    bool _has_local_target;
    // filter is ready for consumer
    std::atomic<RuntimeFilterState> _rf_state;

    // runtime filter type
    RuntimeFilterType _runtime_filter_type;

    friend class RuntimeFilterProducer;
    friend class RuntimeFilterConsumer;
    friend class RuntimeFilterMerger;
};

} // namespace doris
