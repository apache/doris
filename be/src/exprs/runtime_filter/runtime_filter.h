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
#include "vec/exprs/vexpr_fwd.h"

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
    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         const RuntimeFilterRole role, int node_id,
                         std::shared_ptr<RuntimeFilter>* res);

    RuntimeFilterContextSPtr& get_shared_context_ref();

    // insert data to build filter
    void insert_batch(vectorized::ColumnPtr column, size_t start);

    // publish filter
    // push filter to remote node or push down it to scan_node
    Status publish(RuntimeState* state, bool publish_local);

    Status send_filter_size(RuntimeState* state, uint64_t local_filter_size);

    RuntimeFilterType type() const { return _runtime_filter_type; }

    Status get_push_expr_ctxs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                              std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                              bool is_late_arrival);

    bool has_local_target() const { return _has_local_target; }

    RuntimeFilterState current_state() const { return _rf_state.load(std::memory_order_acquire); }

    int expr_order() const { return _expr_order; }

    void update_state();
    // this function will be called if a runtime filter sent by rpc
    // it will notify all wait threads
    void signal();

    Status merge_from(const RuntimeFilter* other);

    Status init_with_size(size_t local_size);

    void update_filter(const RuntimeFilter* other, int64_t merge_time, int64_t start_apply,
                       uint64_t local_merge_time);

    void set_ignored();

    bool get_ignored();

    void set_disabled();
    bool get_disabled() const;

    RuntimeFilterType get_real_type();

    bool need_sync_filter_size();

    // async push runtimefilter to remote node
    Status push_to_remote(RuntimeState* state, const TNetworkAddress* addr,
                          uint64_t local_merge_time);

    void init_profile(RuntimeProfile* parent_profile);

    std::string debug_string() const;

    void update_runtime_filter_type_to_profile(uint64_t local_merge_time);

    int filter_id() const { return _filter_id; }

    std::shared_ptr<pipeline::RuntimeFilterTimer> create_filter_timer(
            std::shared_ptr<pipeline::RuntimeFilterDependency> dependencie);

    std::string formatted_state() const;

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

private:
    RuntimeFilter(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                  RuntimeFilterRole role)
            : _state(state),
              _filter_id(desc->filter_id),
              _is_broadcast_join(desc->is_broadcast_join),
              _has_remote_target(desc->has_remote_targets),
              _has_local_target(desc->has_local_targets),
              _rf_state(RuntimeFilterState::NOT_READY),
              _role(role),
              _expr_order(desc->expr_order),
              _registration_time(MonotonicMillis()),
              _runtime_filter_type(get_runtime_filter_type(desc)),
              _profile(new RuntimeProfile(fmt::format("RuntimeFilter: (id = {}, type = {})",
                                                      _filter_id,
                                                      to_string(_runtime_filter_type)))) {
        // If bitmap filter is not applied, it will cause the query result to be incorrect
        bool wait_infinitely = _state->get_query_ctx()->runtime_filter_wait_infinitely() ||
                               _runtime_filter_type == RuntimeFilterType::BITMAP_FILTER;
        _rf_wait_time_ms = wait_infinitely ? _state->get_query_ctx()->execution_timeout() * 1000
                                           : _state->get_query_ctx()->runtime_filter_wait_time_ms();
    }

    Status _init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options,
                           int node_id = -1);

    // serialize _wrapper to protobuf
    void _to_protobuf(PInFilter* filter);
    void _to_protobuf(PMinMaxFilter* filter);

    void _set_push_down(bool push_down) { _is_push_down = push_down; }

    RuntimeFilterParamsContext* _state = nullptr;
    // _wrapper is a runtime filter function wrapper
    std::shared_ptr<RuntimePredicateWrapper> _wrapper;
    // runtime filter id
    int _filter_id;
    // Specific types BoardCast or Shuffle
    bool _is_broadcast_join;
    // will apply to remote node
    bool _has_remote_target;
    // will apply to local node
    bool _has_local_target;
    // filter is ready for consumer
    std::atomic<RuntimeFilterState> _rf_state;
    // role consumer or producer
    RuntimeFilterRole _role;
    // expr index
    int _expr_order;
    // used for await or signal
    std::mutex _inner_mutex;
    std::condition_variable _inner_cv;
    bool _is_push_down = false;
    TExpr _probe_expr;

    /// Time in ms (from MonotonicMillis()), that the filter was registered.
    const int64_t _registration_time;
    int32_t _rf_wait_time_ms;

    std::atomic<bool> _profile_init = false;
    // runtime filter type
    RuntimeFilterType _runtime_filter_type;
    // parent profile
    // only effect on consumer
    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _wait_timer = nullptr;

    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> _filter_timer;

    int64_t _synced_size = -1;
    std::shared_ptr<pipeline::CountedFinishDependency> _dependency;
};

} // namespace doris
