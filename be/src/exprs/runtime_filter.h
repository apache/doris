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

#include "exprs/expr_context.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {
class Predicate;
class ObjectPool;
class ExprContext;
class RuntimeState;
class RuntimePredicateWrapper;
class MemTracker;
class TupleRow;
class PPublishFilterRequest;
class PMergeFilterRequest;
class TRuntimeFilterDesc;
class RowDescriptor;
class PInFilter;
class PMinMaxFilter;
class HashJoinNode;
class RuntimeProfile;
class BloomFilterFuncBase;

namespace vectorized {
class VExpr;
class VExprContext;
} // namespace vectorized

enum class RuntimeFilterType {
    UNKNOWN_FILTER = -1,
    IN_FILTER = 0,
    MINMAX_FILTER = 1,
    BLOOM_FILTER = 2,
    IN_OR_BLOOM_FILTER = 3
};

inline std::string to_string(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::IN_FILTER: {
        return std::string("in");
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        return std::string("bloomfilter");
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        return std::string("minmax");
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        return std::string("in_or_bloomfilter");
    }
    default:
        return std::string("UNKNOWN");
    }
}

enum class RuntimeFilterRole { PRODUCER = 0, CONSUMER = 1 };

struct RuntimeFilterParams {
    RuntimeFilterParams()
            : filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              bloom_filter_size(-1),
              max_in_num(0),
              filter_id(0),
              fragment_instance_id(0, 0) {}

    RuntimeFilterType filter_type;
    PrimitiveType column_return_type;
    // used in bloom filter
    int64_t bloom_filter_size;
    int32_t max_in_num;
    int32_t filter_id;
    UniqueId fragment_instance_id;
};

struct UpdateRuntimeFilterParams {
    UpdateRuntimeFilterParams(const PPublishFilterRequest* req,
                              butil::IOBufAsZeroCopyInputStream* data_stream, ObjectPool* obj_pool)
            : request(req), data(data_stream), pool(obj_pool) {}
    const PPublishFilterRequest* request;
    butil::IOBufAsZeroCopyInputStream* data;
    ObjectPool* pool;
};

struct MergeRuntimeFilterParams {
    MergeRuntimeFilterParams(const PMergeFilterRequest* req,
                             butil::IOBufAsZeroCopyInputStream* data_stream)
            : request(req), data(data_stream) {}
    const PMergeFilterRequest* request;
    butil::IOBufAsZeroCopyInputStream* data;
};

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class IRuntimeFilter {
public:
    IRuntimeFilter(RuntimeState* state, ObjectPool* pool)
            : _state(state),
              _pool(pool),
              _runtime_filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              _filter_id(-1),
              _is_broadcast_join(true),
              _has_remote_target(false),
              _has_local_target(false),
              _is_ready(false),
              _role(RuntimeFilterRole::PRODUCER),
              _expr_order(-1),
              _always_true(false),
              _probe_ctx(nullptr),
              _is_ignored(false),
              registration_time_(MonotonicMillis()) {}

    ~IRuntimeFilter() = default;

    static Status create(RuntimeState* state, ObjectPool* pool, const TRuntimeFilterDesc* desc,
                         const TQueryOptions* query_options, const RuntimeFilterRole role,
                         int node_id, IRuntimeFilter** res);

    Status apply_from_other(IRuntimeFilter* other);

    // insert data to build filter
    // only used for producer
    void insert(const void* data);
    void insert(const StringRef& data);
    void insert_batch(vectorized::ColumnPtr column, const std::vector<int>& rows);

    // publish filter
    // push filter to remote node or push down it to scan_node
    Status publish();

    void publish_finally();

    RuntimeFilterType type() const { return _runtime_filter_type; }

    // get push down expr context
    // This function can only be called once
    // _wrapper's function will be clear
    // only consumer could call this
    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs);

    Status get_push_expr_ctxs(std::vector<vectorized::VExpr*>* push_vexprs);

    // This function is used by UT and producer
    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs, ExprContext* probe_ctx);

    // This function can be called multiple times
    Status get_prepared_context(std::vector<ExprContext*>* push_expr_ctxs,
                                const RowDescriptor& desc);

    Status get_prepared_vexprs(std::vector<doris::vectorized::VExpr*>* push_vexprs,
                               const RowDescriptor& desc);

    bool is_broadcast_join() const { return _is_broadcast_join; }

    bool has_remote_target() const { return _has_remote_target; }

    bool is_ready() const { return _is_ready; }

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
    // it will nodify all wait threads
    void signal();

    // init filter with desc
    Status init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options,
                          UniqueId fragment_id, int node_id = -1);

    BloomFilterFuncBase* get_bloomfilter() const;

    // serialize _wrapper to protobuf
    Status serialize(PMergeFilterRequest* request, void** data, int* len);
    Status serialize(PPublishFilterRequest* request, void** data = nullptr, int* len = nullptr);

    Status merge_from(const RuntimePredicateWrapper* wrapper);

    // for ut
    const RuntimePredicateWrapper* get_wrapper();
    static Status create_wrapper(RuntimeState* state, const MergeRuntimeFilterParams* param,
                                 ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    static Status create_wrapper(RuntimeState* state, const UpdateRuntimeFilterParams* param,
                                 ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    void change_to_bloom_filter();
    Status update_filter(const UpdateRuntimeFilterParams* param);

    void set_ignored() { _is_ignored = true; }

    // for ut
    bool is_ignored() const { return _is_ignored; }

    void set_ignored_msg(std::string& msg) { _ignored_msg = msg; }

    // for ut
    bool is_bloomfilter();

    // consumer should call before released
    Status consumer_close();

    // async push runtimefilter to remote node
    Status push_to_remote(RuntimeState* state, const TNetworkAddress* addr);
    Status join_rpc();

    void init_profile(RuntimeProfile* parent_profile);

    void update_runtime_filter_type_to_profile();

    void set_push_down_profile();

    void ready_for_publish();

    static bool enable_use_batch(int be_exec_version, PrimitiveType type) {
        return be_exec_version > 0 && (is_int_or_bool(type) || is_float_or_double(type));
    }

    int filter_id() const { return _filter_id; }

protected:
    // serialize _wrapper to protobuf
    void to_protobuf(PInFilter* filter);
    void to_protobuf(PMinMaxFilter* filter);

    template <class T>
    Status serialize_impl(T* request, void** data, int* len);

    template <class T>
    static Status _create_wrapper(RuntimeState* state, const T* param, ObjectPool* pool,
                                  std::unique_ptr<RuntimePredicateWrapper>* wrapper);

    RuntimeState* _state;
    ObjectPool* _pool;
    // _wrapper is a runtime filter function wrapper
    // _wrapper should alloc from _pool
    RuntimePredicateWrapper* _wrapper = nullptr;
    // runtime filter type
    RuntimeFilterType _runtime_filter_type;
    // runtime filter id
    int _filter_id;
    // Specific types BoardCast or Shuffle
    bool _is_broadcast_join;
    // will apply to remote node
    bool _has_remote_target;
    // will apply to local node
    bool _has_local_target;
    // filter is ready for consumer
    bool _is_ready;
    // role consumer or producer
    RuntimeFilterRole _role;
    // expr index
    int _expr_order;
    // used for await or signal
    std::mutex _inner_mutex;
    std::condition_variable _inner_cv;

    // if set always_true = true
    // this filter won't filter any data
    bool _always_true;

    // build expr_context
    // ExprContext* _build_ctx;
    // probe expr_context
    // it only used in consumer to generate runtime_filter expr_context
    // we don't have to prepare it or close it
    ExprContext* _probe_ctx;
    doris::vectorized::VExprContext* _vprobe_ctx;

    // Indicate whether runtime filter expr has been ignored
    bool _is_ignored;
    std::string _ignored_msg;

    // some runtime filter will generate
    // multiple contexts such as minmax filter
    // these context is called prepared by this,
    // consumer_close should be called before release
    std::vector<ExprContext*> _push_down_ctxs;
    std::vector<doris::vectorized::VExpr*> _push_down_vexprs;

    struct rpc_context;
    std::shared_ptr<rpc_context> _rpc_context;

    // parent profile
    // only effect on consumer
    std::unique_ptr<RuntimeProfile> _profile;
    // unix millis
    RuntimeProfile::Counter* _await_time_cost = nullptr;
    RuntimeProfile::Counter* _effect_time_cost = nullptr;
    std::unique_ptr<ScopedTimer<MonotonicStopWatch>> _effect_timer;

    /// Time in ms (from MonotonicMillis()), that the filter was registered.
    const int64_t registration_time_;
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
