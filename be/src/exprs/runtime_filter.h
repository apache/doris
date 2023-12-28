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

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vdatetime_value.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {
class ObjectPool;
class RuntimePredicateWrapper;
class PPublishFilterRequest;
class PPublishFilterRequestV2;
class PMergeFilterRequest;
class TRuntimeFilterDesc;
class RowDescriptor;
class PInFilter;
class PMinMaxFilter;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;
class TNetworkAddress;
class TQueryOptions;

namespace vectorized {
class VExpr;
class VExprContext;
struct SharedRuntimeFilterContext;
} // namespace vectorized

namespace pipeline {
class RuntimeFilterTimer;
} // namespace pipeline

enum class RuntimeFilterType {
    UNKNOWN_FILTER = -1,
    IN_FILTER = 0,
    MINMAX_FILTER = 1,
    BLOOM_FILTER = 2,
    IN_OR_BLOOM_FILTER = 3,
    BITMAP_FILTER = 4,
    MIN_FILTER = 5, // only min
    MAX_FILTER = 6  // only max
};

static RuntimeFilterType get_runtime_filter_type(const TRuntimeFilterDesc* desc) {
    switch (desc->type) {
    case TRuntimeFilterType::BLOOM: {
        return RuntimeFilterType::BLOOM_FILTER;
    }
    case TRuntimeFilterType::MIN_MAX: {
        if (desc->__isset.min_max_type) {
            if (desc->min_max_type == TMinMaxRuntimeFilterType::MIN) {
                return RuntimeFilterType::MIN_FILTER;
            } else if (desc->min_max_type == TMinMaxRuntimeFilterType::MAX) {
                return RuntimeFilterType::MAX_FILTER;
            }
        }
        return RuntimeFilterType::MINMAX_FILTER;
    }
    case TRuntimeFilterType::IN: {
        return RuntimeFilterType::IN_FILTER;
    }
    case TRuntimeFilterType::IN_OR_BLOOM: {
        return RuntimeFilterType::IN_OR_BLOOM_FILTER;
    }
    case TRuntimeFilterType::BITMAP: {
        return RuntimeFilterType::BITMAP_FILTER;
    }
    default: {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid runtime filter type!");
    }
    }
}

enum class RuntimeFilterRole { PRODUCER = 0, CONSUMER = 1 };

struct RuntimeFilterParams {
    RuntimeFilterParams()
            : filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              bloom_filter_size(-1),
              max_in_num(0),
              filter_id(0),
              bitmap_filter_not_in(false) {}

    RuntimeFilterType filter_type;
    PrimitiveType column_return_type;
    // used in bloom filter
    int64_t bloom_filter_size;
    int32_t max_in_num;
    int32_t filter_id;
    bool bitmap_filter_not_in;
    bool build_bf_exactly;
};
struct FilterFuncBase {
public:
    void set_filter_id(int filter_id) {
        if (_filter_id == -1) {
            _filter_id = filter_id;
        }
    }

    [[nodiscard]] int get_filter_id() const { return _filter_id; }

private:
    int _filter_id = -1;
};
struct UpdateRuntimeFilterParams {
    UpdateRuntimeFilterParams(const PPublishFilterRequest* req,
                              butil::IOBufAsZeroCopyInputStream* data_stream, ObjectPool* obj_pool)
            : request(req), data(data_stream), pool(obj_pool) {}
    const PPublishFilterRequest* request = nullptr;
    butil::IOBufAsZeroCopyInputStream* data = nullptr;
    ObjectPool* pool = nullptr;
};

struct UpdateRuntimeFilterParamsV2 {
    UpdateRuntimeFilterParamsV2(const PPublishFilterRequestV2* req,
                                butil::IOBufAsZeroCopyInputStream* data_stream,
                                ObjectPool* obj_pool)
            : request(req), data(data_stream), pool(obj_pool) {}
    const PPublishFilterRequestV2* request;
    butil::IOBufAsZeroCopyInputStream* data;
    ObjectPool* pool = nullptr;
};

struct MergeRuntimeFilterParams {
    MergeRuntimeFilterParams(const PMergeFilterRequest* req,
                             butil::IOBufAsZeroCopyInputStream* data_stream)
            : request(req), data(data_stream) {}
    const PMergeFilterRequest* request = nullptr;
    butil::IOBufAsZeroCopyInputStream* data = nullptr;
};

enum RuntimeFilterState {
    READY,
    NOT_READY,
    TIME_OUT,
};

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class IRuntimeFilter {
public:
    IRuntimeFilter(RuntimeFilterParamsContext* state, ObjectPool* pool,
                   const TRuntimeFilterDesc* desc, bool is_global = false, int parallel_tasks = -1)
            : _state(state),
              _pool(pool),
              _filter_id(desc->filter_id),
              _is_broadcast_join(true),
              _has_remote_target(false),
              _has_local_target(false),
              _rf_state(RuntimeFilterState::NOT_READY),
              _rf_state_atomic(RuntimeFilterState::NOT_READY),
              _role(RuntimeFilterRole::PRODUCER),
              _expr_order(-1),
              _always_true(false),
              _is_ignored(false),
              registration_time_(MonotonicMillis()),
              _wait_infinitely(_state->runtime_filter_wait_infinitely),
              _rf_wait_time_ms(_state->runtime_filter_wait_time_ms),
              _enable_pipeline_exec(_state->enable_pipeline_exec),
              _runtime_filter_type(get_runtime_filter_type(desc)),
              _name(fmt::format("RuntimeFilter: (id = {}, type = {})", _filter_id,
                                to_string(_runtime_filter_type))),
              _profile(new RuntimeProfile(_name)),
              _is_global(is_global),
              _parallel_build_tasks(parallel_tasks) {}

    ~IRuntimeFilter() = default;

    static Status create(RuntimeFilterParamsContext* state, ObjectPool* pool,
                         const TRuntimeFilterDesc* desc, const TQueryOptions* query_options,
                         const RuntimeFilterRole role, int node_id, IRuntimeFilter** res,
                         bool build_bf_exactly = false, bool is_global = false,
                         int parallel_tasks = 0);

    void copy_to_shared_context(vectorized::SharedRuntimeFilterContext& context);
    Status copy_from_shared_context(vectorized::SharedRuntimeFilterContext& context);

    void copy_from_other(IRuntimeFilter* other);

    // insert data to build filter
    void insert_batch(vectorized::ColumnPtr column, size_t start);

    // publish filter
    // push filter to remote node or push down it to scan_node
    Status publish();

    RuntimeFilterType type() const { return _runtime_filter_type; }

    Status get_push_expr_ctxs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                              std::vector<vectorized::VExprSPtr>& push_exprs, bool is_late_arrival);

    bool is_broadcast_join() const { return _is_broadcast_join; }

    bool has_remote_target() const { return _has_remote_target; }

    bool is_ready() const {
        return (!_enable_pipeline_exec && _rf_state == RuntimeFilterState::READY) ||
               (_enable_pipeline_exec &&
                _rf_state_atomic.load(std::memory_order_acquire) == RuntimeFilterState::READY);
    }
    RuntimeFilterState current_state() const {
        return _enable_pipeline_exec ? _rf_state_atomic.load(std::memory_order_acquire) : _rf_state;
    }
    bool is_ready_or_timeout();

    bool is_producer() const { return _role == RuntimeFilterRole::PRODUCER; }
    bool is_consumer() const { return _role == RuntimeFilterRole::CONSUMER; }
    void set_role(const RuntimeFilterRole role) { _role = role; }
    int expr_order() const { return _expr_order; }

    // only used for consumer
    // if filter is not ready for filter data scan_node
    // will wait util it ready or timeout
    // This function will wait at most config::runtime_filter_shuffle_wait_time_ms
    // if return true , filter is ready to use
    bool await();
    // this function will be called if a runtime filter sent by rpc
    // it will notify all wait threads
    void signal();

    // init filter with desc
    Status init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options,
                          int node_id = -1, bool build_bf_exactly = false);

    BloomFilterFuncBase* get_bloomfilter() const;

    // serialize _wrapper to protobuf
    Status serialize(PMergeFilterRequest* request, void** data, int* len);
    Status serialize(PPublishFilterRequest* request, void** data = nullptr, int* len = nullptr);
    Status serialize(PPublishFilterRequestV2* request, void** data = nullptr, int* len = nullptr);

    Status merge_from(const RuntimePredicateWrapper* wrapper);

    // for ut
    static Status create_wrapper(RuntimeFilterParamsContext* state,
                                 const MergeRuntimeFilterParams* param, ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    static Status create_wrapper(RuntimeFilterParamsContext* state,
                                 const UpdateRuntimeFilterParams* param, ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    static Status create_wrapper(RuntimeFilterParamsContext* state,
                                 const UpdateRuntimeFilterParamsV2* param, ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    void change_to_bloom_filter();
    Status init_bloom_filter(const size_t build_bf_cardinality);
    Status update_filter(const UpdateRuntimeFilterParams* param);
    Status update_filter(const UpdateRuntimeFilterParamsV2* param, int64_t start_apply);

    void set_ignored() { _is_ignored = true; }

    void set_ignored_msg(std::string& msg) { _ignored_msg = msg; }

    // for ut
    bool is_bloomfilter();

    bool is_finish_rpc();

    Status join_rpc();

    // async push runtimefilter to remote node
    Status push_to_remote(RuntimeFilterParamsContext* state, const TNetworkAddress* addr,
                          bool opt_remote_rf);

    void init_profile(RuntimeProfile* parent_profile);

    std::string& get_name() { return _name; }

    void update_runtime_filter_type_to_profile();

    int filter_id() const { return _filter_id; }

    static std::string to_string(RuntimeFilterType type) {
        switch (type) {
        case RuntimeFilterType::IN_FILTER: {
            return std::string("in");
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            return std::string("bloomfilter");
        }
        case RuntimeFilterType::MIN_FILTER: {
            return std::string("only_min");
        }
        case RuntimeFilterType::MAX_FILTER: {
            return std::string("only_max");
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            return std::string("minmax");
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            return std::string("in_or_bloomfilter");
        }
        case RuntimeFilterType::BITMAP_FILTER: {
            return std::string("bitmapfilter");
        }
        default:
            return std::string("UNKNOWN");
        }
    }

    // For pipelineX & Producer
    int32_t wait_time_ms() const {
        int32_t res = 0;
        if (wait_infinitely()) {
            res = _state->execution_timeout;
            // Convert to ms
            res *= 1000;
        } else {
            res = _rf_wait_time_ms;
        }
        return res;
    }

    bool wait_infinitely() const;

    int64_t registration_time() const { return registration_time_; }

    void set_filter_timer(std::shared_ptr<pipeline::RuntimeFilterTimer>);

    Status merge_local_filter(RuntimePredicateWrapper* wrapper, int* merged_num);

protected:
    // serialize _wrapper to protobuf
    void to_protobuf(PInFilter* filter);
    void to_protobuf(PMinMaxFilter* filter);

    template <class T>
    Status serialize_impl(T* request, void** data, int* len);

    template <class T>
    static Status _create_wrapper(RuntimeFilterParamsContext* state, const T* param,
                                  ObjectPool* pool,
                                  std::unique_ptr<RuntimePredicateWrapper>* wrapper);

    void _set_push_down() { _is_push_down = true; }

    std::string _format_status() {
        return fmt::format(
                "[IsPushDown = {}, RuntimeFilterState = {}, IsIgnored = {}, HasRemoteTarget = {}, "
                "HasLocalTarget = {}]",
                _is_push_down, _get_explain_state_string(), _is_ignored, _has_remote_target,
                _has_local_target);
    }

    std::string _get_explain_state_string() {
        if (_enable_pipeline_exec) {
            return _rf_state_atomic.load(std::memory_order_acquire) == RuntimeFilterState::READY
                           ? "READY"
                   : _rf_state_atomic.load(std::memory_order_acquire) ==
                                   RuntimeFilterState::TIME_OUT
                           ? "TIME_OUT"
                           : "NOT_READY";
        } else {
            return _rf_state == RuntimeFilterState::READY      ? "READY"
                   : _rf_state == RuntimeFilterState::TIME_OUT ? "TIME_OUT"
                                                               : "NOT_READY";
        }
    }

    RuntimeFilterParamsContext* _state = nullptr;
    ObjectPool* _pool = nullptr;
    // _wrapper is a runtime filter function wrapper
    // _wrapper should alloc from _pool
    RuntimePredicateWrapper* _wrapper = nullptr;
    // runtime filter id
    int _filter_id;
    // Specific types BoardCast or Shuffle
    bool _is_broadcast_join;
    // will apply to remote node
    bool _has_remote_target;
    // will apply to local node
    bool _has_local_target;
    // filter is ready for consumer
    RuntimeFilterState _rf_state;
    std::atomic<RuntimeFilterState> _rf_state_atomic;
    // role consumer or producer
    RuntimeFilterRole _role;
    // expr index
    int _expr_order;
    // used for await or signal
    std::mutex _inner_mutex;
    std::condition_variable _inner_cv;

    bool _is_push_down = false;

    // if set always_true = true
    // this filter won't filter any data
    bool _always_true;

    TExpr _probe_expr;

    // Indicate whether runtime filter expr has been ignored
    bool _is_ignored;
    std::string _ignored_msg;

    struct RPCContext;

    std::shared_ptr<RPCContext> _rpc_context;

    /// Time in ms (from MonotonicMillis()), that the filter was registered.
    const int64_t registration_time_;
    /// runtime filter wait time will be ignored if wait_infinitely is true
    const bool _wait_infinitely;
    const int32_t _rf_wait_time_ms;

    const bool _enable_pipeline_exec;

    std::atomic<bool> _profile_init = false;
    // runtime filter type
    RuntimeFilterType _runtime_filter_type;
    std::string _name;
    // parent profile
    // only effect on consumer
    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _merge_local_rf_timer = nullptr;
    bool _opt_remote_rf;
    // `_is_global` indicates whether this runtime filter is global on this BE.
    // All runtime filters should be merged on each BE if it is global.
    // This is improvement for pipelineX.
    const bool _is_global = false;
    std::mutex _local_merge_mutex;
    // There are `_parallel_build_tasks` pipeline tasks to build runtime filter.
    // We should call `signal` once all runtime filters are done and merged to one
    // (e.g. `_merged_rf_num` is equal to `_parallel_build_tasks`).
    int _merged_rf_num = 0;
    const int _parallel_build_tasks = -1;

    std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>> _filter_timer;
};

// avoid expose RuntimePredicateWrapper
class RuntimeFilterWrapperHolder {
public:
    using WrapperPtr = std::unique_ptr<RuntimePredicateWrapper>;
    RuntimeFilterWrapperHolder();
    ~RuntimeFilterWrapperHolder();
    WrapperPtr* getHandle() { return &_wrapper; }

private:
    WrapperPtr _wrapper;
};
} // namespace doris
