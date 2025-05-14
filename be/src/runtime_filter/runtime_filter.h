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

#include "common/exception.h"
#include "common/status.h"
#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "runtime_filter/runtime_filter_wrapper.h"
#include "runtime_filter/utils.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeFilterWrapper;
class RuntimeProfile;

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class RuntimeFilter {
public:
    virtual ~RuntimeFilter() = default;

    RuntimeFilterType type() const { return _runtime_filter_type; }

    bool has_remote_target() const { return _has_remote_target; }

    template <class T>
    Status assign(const T& request, butil::IOBufAsZeroCopyInputStream* data) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        return _wrapper->assign(request, data);
    }

    template <class T>
    Status serialize(T* request, void** data, int* len) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        auto real_runtime_filter_type = _wrapper->get_real_type();

        request->set_filter_type(get_type(real_runtime_filter_type));
        request->set_filter_id(_wrapper->filter_id());

        auto state = _wrapper->get_state();
        if (state != RuntimeFilterWrapper::State::READY) {
            request->set_disabled(state == RuntimeFilterWrapper::State::DISABLED);
            return Status::OK();
        }

        request->set_contain_null(_wrapper->contain_null());

        if (real_runtime_filter_type == RuntimeFilterType::IN_FILTER) {
            auto in_filter = request->mutable_in_filter();
            RETURN_IF_ERROR(_to_protobuf(in_filter));
        } else if (real_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER) {
            DCHECK(data != nullptr);
            RETURN_IF_ERROR(_to_protobuf(request->mutable_bloom_filter(), (char**)data, len));
        } else if (real_runtime_filter_type == RuntimeFilterType::MINMAX_FILTER ||
                   real_runtime_filter_type == RuntimeFilterType::MIN_FILTER ||
                   real_runtime_filter_type == RuntimeFilterType::MAX_FILTER) {
            auto minmax_filter = request->mutable_minmax_filter();
            RETURN_IF_ERROR(_to_protobuf(minmax_filter));
        } else {
            return Status::InternalError("not implemented !");
        }
        return Status::OK();
    }

    virtual std::string debug_string() = 0;

protected:
    RuntimeFilter(const TRuntimeFilterDesc* desc)
            : _has_remote_target(desc->has_remote_targets),
              _runtime_filter_type(get_runtime_filter_type(desc)) {
        DCHECK_NE(desc->has_remote_targets, desc->has_local_targets);
    }

    virtual Status _init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options);

    template <typename T>
    Status _to_protobuf(T* filter) {
        return _wrapper->to_protobuf(filter);
    }
    Status _to_protobuf(PBloomFilter* filter, char** data, int* filter_length) {
        return _wrapper->to_protobuf(filter, data, filter_length);
    }

    Status _push_to_remote(RuntimeState* state, const TNetworkAddress* addr);

    std::string _debug_string() const;

    void _check_wrapper_state(const std::vector<RuntimeFilterWrapper::State>& assumed_states);

    // _wrapper is a runtime filter function wrapper
    std::shared_ptr<RuntimeFilterWrapper> _wrapper;

    // will apply to remote node
    bool _has_remote_target = false;

    // runtime filter type
    RuntimeFilterType _runtime_filter_type = RuntimeFilterType::UNKNOWN_FILTER;

    friend class RuntimeFilterProducer;
    friend class RuntimeFilterConsumer;
    friend class RuntimeFilterMerger;

    std::recursive_mutex _rmtx; // lock all member function of runtime filter producer/consumer
};
#include "common/compile_check_end.h"
} // namespace doris
