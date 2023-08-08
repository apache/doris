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
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/lock.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
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

enum class RuntimeFilterType {
    UNKNOWN_FILTER = -1,
    IN_FILTER = 0,
    MINMAX_FILTER = 1,
    BLOOM_FILTER = 2,
    IN_OR_BLOOM_FILTER = 3,
    BITMAP_FILTER = 4
};

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

struct UpdateRuntimeFilterParams {
    UpdateRuntimeFilterParams(const PPublishFilterRequest* req,
                              butil::IOBufAsZeroCopyInputStream* data_stream, ObjectPool* obj_pool)
            : request(req), data(data_stream), pool(obj_pool) {}
    const PPublishFilterRequest* request;
    butil::IOBufAsZeroCopyInputStream* data;
    ObjectPool* pool;
};

struct UpdateRuntimeFilterParamsV2 {
    UpdateRuntimeFilterParamsV2(const PPublishFilterRequestV2* req,
                                butil::IOBufAsZeroCopyInputStream* data_stream,
                                ObjectPool* obj_pool)
            : request(req), data(data_stream), pool(obj_pool) {}
    const PPublishFilterRequestV2* request;
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
    IRuntimeFilter(RuntimeState* state, ObjectPool* pool)
            : _state(state),
              _pool(pool),
              _runtime_filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              _filter_id(-1),
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
              _enable_pipeline_exec(_state->enable_pipeline_exec()) {}

    IRuntimeFilter(QueryContext* query_ctx, ObjectPool* pool)
            : _query_ctx(query_ctx),
              _pool(pool),
              _runtime_filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              _filter_id(-1),
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
              _enable_pipeline_exec(query_ctx->enable_pipeline_exec()) {}

    ~IRuntimeFilter() = default;

    static Status create(RuntimeState* state, ObjectPool* pool, const TRuntimeFilterDesc* desc,
                         const TQueryOptions* query_options, const RuntimeFilterRole role,
                         int node_id, IRuntimeFilter** res, bool build_bf_exactly = false);

    static Status create(QueryContext* query_ctx, ObjectPool* pool, const TRuntimeFilterDesc* desc,
                         const TQueryOptions* query_options, const RuntimeFilterRole role,
                         int node_id, IRuntimeFilter** res, bool build_bf_exactly = false);

    void copy_to_shared_context(vectorized::SharedRuntimeFilterContext& context);
    Status copy_from_shared_context(vectorized::SharedRuntimeFilterContext& context);

    void copy_from_other(IRuntimeFilter* other);

    // insert data to build filter
    // only used for producer
    void insert(const void* data);
    void insert(const StringRef& data);
    void insert_batch(vectorized::ColumnPtr column, const std::vector<int>& rows);

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
    // it will nodify all wait threads
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
    static Status create_wrapper(RuntimeState* state, const MergeRuntimeFilterParams* param,
                                 ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    static Status create_wrapper(RuntimeState* state, const UpdateRuntimeFilterParams* param,
                                 ObjectPool* pool,
                                 std::unique_ptr<RuntimePredicateWrapper>* wrapper);
    static Status create_wrapper(QueryContext* query_ctx, const UpdateRuntimeFilterParamsV2* param,
                                 ObjectPool* pool,
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
    Status push_to_remote(RuntimeState* state, const TNetworkAddress* addr, bool opt_remote_rf);

    void init_profile(RuntimeProfile* parent_profile);

    std::string& get_name() { return _name; }

    void update_runtime_filter_type_to_profile();

    static bool enable_use_batch(bool use_batch, PrimitiveType type) {
        return use_batch && (is_int_or_bool(type) || is_float_or_double(type));
    }

    int filter_id() const { return _filter_id; }

    static std::string to_string(RuntimeFilterType type) {
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
        case RuntimeFilterType::BITMAP_FILTER: {
            return std::string("bitmapfilter");
        }
        default:
            return std::string("UNKNOWN");
        }
    }

protected:
    // serialize _wrapper to protobuf
    void to_protobuf(PInFilter* filter);
    void to_protobuf(PMinMaxFilter* filter);

    template <class T>
    Status serialize_impl(T* request, void** data, int* len);

    template <class T>
    static Status _create_wrapper(RuntimeState* state, const T* param, ObjectPool* pool,
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

    RuntimeState* _state = nullptr;
    QueryContext* _query_ctx = nullptr;
    ObjectPool* _pool;
    // _wrapper is a runtime filter function wrapper
    // _wrapper should alloc from _pool
    RuntimePredicateWrapper* _wrapper;
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
    RuntimeFilterState _rf_state;
    std::atomic<RuntimeFilterState> _rf_state_atomic;
    // role consumer or producer
    RuntimeFilterRole _role;
    // expr index
    int _expr_order;
    // used for await or signal
    Mutex _inner_mutex;
    ConditionVariable _inner_cv;

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

    // parent profile
    // only effect on consumer
    std::unique_ptr<RuntimeProfile> _profile;

    /// Time in ms (from MonotonicMillis()), that the filter was registered.
    const int64_t registration_time_;

    const bool _enable_pipeline_exec;

    bool _profile_init = false;
    std::mutex _profile_mutex;
    std::string _name;
    bool _opt_remote_rf;
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

// copied from expr.h since it is only used in runtime filter

template <PrimitiveType T>
Status create_texpr_literal_node(const void* data, TExprNode* node, int precision = 0,
                                 int scale = 0) {
    if constexpr (T == TYPE_BOOLEAN) {
        auto origin_value = reinterpret_cast<const bool*>(data);
        TBoolLiteral boolLiteral;
        (*node).__set_node_type(TExprNodeType::BOOL_LITERAL);
        boolLiteral.__set_value(*origin_value);
        (*node).__set_bool_literal(boolLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    } else if constexpr (T == TYPE_TINYINT) {
        auto origin_value = reinterpret_cast<const int8_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_TINYINT));
    } else if constexpr (T == TYPE_SMALLINT) {
        auto origin_value = reinterpret_cast<const int16_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_SMALLINT));
    } else if constexpr (T == TYPE_INT) {
        auto origin_value = reinterpret_cast<const int32_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_INT));
    } else if constexpr (T == TYPE_BIGINT) {
        auto origin_value = reinterpret_cast<const int64_t*>(data);
        (*node).__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral intLiteral;
        intLiteral.__set_value(*origin_value);
        (*node).__set_int_literal(intLiteral);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_BIGINT));
    } else if constexpr (T == TYPE_LARGEINT) {
        auto origin_value = reinterpret_cast<const int128_t*>(data);
        (*node).__set_node_type(TExprNodeType::LARGE_INT_LITERAL);
        TLargeIntLiteral large_int_literal;
        large_int_literal.__set_value(LargeIntValue::to_string(*origin_value));
        (*node).__set_large_int_literal(large_int_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_LARGEINT));
    } else if constexpr ((T == TYPE_DATE) || (T == TYPE_DATETIME) || (T == TYPE_TIME)) {
        auto origin_value = reinterpret_cast<const vectorized::VecDateTimeValue*>(data);
        TDateLiteral date_literal;
        char convert_buffer[30];
        origin_value->to_string(convert_buffer);
        date_literal.__set_value(convert_buffer);
        (*node).__set_date_literal(date_literal);
        (*node).__set_node_type(TExprNodeType::DATE_LITERAL);
        if (origin_value->type() == TimeType::TIME_DATE) {
            (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATE));
        } else if (origin_value->type() == TimeType::TIME_DATETIME) {
            (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATETIME));
        } else if (origin_value->type() == TimeType::TIME_TIME) {
            (*node).__set_type(create_type_desc(PrimitiveType::TYPE_TIME));
        }
    } else if constexpr (T == TYPE_DATEV2) {
        auto origin_value =
                reinterpret_cast<const vectorized::DateV2Value<vectorized::DateV2ValueType>*>(data);
        TDateLiteral date_literal;
        char convert_buffer[30];
        origin_value->to_string(convert_buffer);
        date_literal.__set_value(convert_buffer);
        (*node).__set_date_literal(date_literal);
        (*node).__set_node_type(TExprNodeType::DATE_LITERAL);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATEV2));
    } else if constexpr (T == TYPE_DATETIMEV2) {
        auto origin_value =
                reinterpret_cast<const vectorized::DateV2Value<vectorized::DateTimeV2ValueType>*>(
                        data);
        TDateLiteral date_literal;
        char convert_buffer[30];
        origin_value->to_string(convert_buffer);
        date_literal.__set_value(convert_buffer);
        (*node).__set_date_literal(date_literal);
        (*node).__set_node_type(TExprNodeType::DATE_LITERAL);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DATETIMEV2));
    } else if constexpr (T == TYPE_DECIMALV2) {
        auto origin_value = reinterpret_cast<const DecimalV2Value*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string());
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMALV2, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL32) {
        auto origin_value = reinterpret_cast<const vectorized::Decimal<int32_t>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string(scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL32, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL64) {
        auto origin_value = reinterpret_cast<const vectorized::Decimal<int64_t>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string(scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL64, precision, scale));
    } else if constexpr (T == TYPE_DECIMAL128I) {
        auto origin_value = reinterpret_cast<const vectorized::Decimal<int128_t>*>(data);
        (*node).__set_node_type(TExprNodeType::DECIMAL_LITERAL);
        TDecimalLiteral decimal_literal;
        decimal_literal.__set_value(origin_value->to_string(scale));
        (*node).__set_decimal_literal(decimal_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DECIMAL128I, precision, scale));
    } else if constexpr (T == TYPE_FLOAT) {
        auto origin_value = reinterpret_cast<const float*>(data);
        (*node).__set_node_type(TExprNodeType::FLOAT_LITERAL);
        TFloatLiteral float_literal;
        float_literal.__set_value(*origin_value);
        (*node).__set_float_literal(float_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_FLOAT));
    } else if constexpr (T == TYPE_DOUBLE) {
        auto origin_value = reinterpret_cast<const double*>(data);
        (*node).__set_node_type(TExprNodeType::FLOAT_LITERAL);
        TFloatLiteral float_literal;
        float_literal.__set_value(*origin_value);
        (*node).__set_float_literal(float_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_DOUBLE));
    } else if constexpr ((T == TYPE_STRING) || (T == TYPE_CHAR) || (T == TYPE_VARCHAR)) {
        auto origin_value = reinterpret_cast<const StringRef*>(data);
        (*node).__set_node_type(TExprNodeType::STRING_LITERAL);
        TStringLiteral string_literal;
        string_literal.__set_value(origin_value->to_string());
        (*node).__set_string_literal(string_literal);
        (*node).__set_type(create_type_desc(PrimitiveType::TYPE_STRING));
    } else {
        return Status::InvalidArgument("Invalid argument type!");
    }
    return Status::OK();
}

} // namespace doris
