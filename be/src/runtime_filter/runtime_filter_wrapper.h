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

#include "common/status.h"
#include "exprs/bloom_filter_func.h"
#include "runtime/runtime_state.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "runtime_filter/utils.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {

// This class is a wrapper of runtime predicate function
class RuntimeFilterWrapper {
public:
    enum class State { UNINITED, INITED, READY, IGNORED, DISABLED };

    RuntimeFilterWrapper(const RuntimeFilterParams* params);
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimeFilterWrapper(PrimitiveType column_type, RuntimeFilterType type, uint32_t filter_id,
                         State state)
            : _column_return_type(column_type),
              _filter_type(type),
              _filter_id(filter_id),
              _state(state) {}

    Status change_to_bloom_filter();

    Status init_bloom_filter(const size_t build_bf_cardinality);

    bool get_build_bf_cardinality() const;

    void insert_to_bloom_filter(BloomFilterFuncBase* bloom_filter) const;

    BloomFilterFuncBase* get_bloomfilter() const { return _bloom_filter_func.get(); }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start);

    void insert_batch(const vectorized::ColumnPtr& column, size_t start) {
        if (get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
            bitmap_filter_insert_batch(column, start);
        } else {
            insert_fixed_len(column, start);
        }
    }

    void bitmap_filter_insert_batch(const vectorized::ColumnPtr column, size_t start);

    RuntimeFilterType get_real_type() const {
        if (_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            if (_hybrid_set) {
                return RuntimeFilterType::IN_FILTER;
            }
            return RuntimeFilterType::BLOOM_FILTER;
        }
        return _filter_type;
    }

    size_t get_bloom_filter_size() const;

    Status get_push_exprs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                          std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                          const TExpr& probe_expr);

    Status merge(const RuntimeFilterWrapper* wrapper);

    Status assign(const PInFilter& in_filter, bool contain_null);

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PBloomFilter& bloom_filter, butil::IOBufAsZeroCopyInputStream* data,
                  bool contain_null);

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PMinMaxFilter& minmax_filter, bool contain_null);

    void get_bloom_filter_desc(char** data, int* filter_length);

    PrimitiveType column_type() { return _column_return_type; }

    bool is_bloomfilter() const { return get_real_type() == RuntimeFilterType::BLOOM_FILTER; }

    bool contain_null() const;

    void batch_assign(const PInFilter& filter,
                      void (*assign_func)(std::shared_ptr<HybridSetBase>& _hybrid_set,
                                          PColumnValue&));

    size_t get_in_filter_size() const;

    std::shared_ptr<BitmapFilterFuncBase> get_bitmap_filter() const { return _bitmap_filter_func; }

    friend class RuntimeFilter;
    friend class RuntimeFilterProducer;
    friend class RuntimeFilterConsumer;
    friend class RuntimeFilterMerger;

    void set_filter_id(int id);

    template <class T>
    Status assign_data(const T& request, butil::IOBufAsZeroCopyInputStream* data) {
        PFilterType filter_type = request.filter_type();

        if (request.has_disabled() && request.disabled()) {
            _state = State::DISABLED;
            return Status::OK();
        }

        if (request.has_ignored() && request.ignored()) {
            _state = State::IGNORED;
            return Status::OK();
        }

        _state = State::READY;

        switch (filter_type) {
        case PFilterType::IN_FILTER: {
            DCHECK(request.has_in_filter());
            return assign(request.in_filter(), request.contain_null());
        }
        case PFilterType::BLOOM_FILTER: {
            DCHECK(request.has_bloom_filter());
            _hybrid_set.reset(); // change in_or_bloom filter to bloom filter
            return assign(request.bloom_filter(), data, request.contain_null());
        }
        case PFilterType::MIN_FILTER:
        case PFilterType::MAX_FILTER:
        case PFilterType::MINMAX_FILTER: {
            DCHECK(request.has_minmax_filter());
            return assign(request.minmax_filter(), request.contain_null());
        }
        default:
            return Status::InternalError("unknown filter type {}", int(filter_type));
        }
    }

    std::string debug_string() const {
        return fmt::format(
                "filter_id: {}, state: {}, type: {}({}), build_bf_cardinality: {}, error_msg: "
                "[{}]",
                _filter_id, _to_string(_state), to_string(_filter_type), to_string(get_real_type()),
                get_build_bf_cardinality(), _err_msg);
    }

    void set_state(State state) {
        _state = state;
        if (state == State::DISABLED || state == State::IGNORED) {
            _minmax_func.reset();
            _hybrid_set.reset();
            _bloom_filter_func.reset();
            _bitmap_filter_func.reset();
        }
    }

    State get_state() const { return _state; }

private:
    static std::string _to_string(const State& state) {
        switch (state) {
        case State::IGNORED:
            return "IGNORED";
        case State::DISABLED:
            return "DISABLED";
        case State::UNINITED:
            return "UNINITED";
        case State::READY:
            return "READY";
        case State::INITED:
            return "INITED";
        default:
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid State {}",
                                   int(state));
        }
    }

    // When a runtime filter received from remote and it is a bloom filter, _column_return_type will be invalid.
    PrimitiveType _column_return_type; // column type
    RuntimeFilterType _filter_type;
    int32_t _max_in_num;
    uint32_t _filter_id;

    std::shared_ptr<MinMaxFuncBase> _minmax_func;
    std::shared_ptr<HybridSetBase> _hybrid_set;
    std::shared_ptr<BloomFilterFuncBase> _bloom_filter_func;
    std::shared_ptr<BitmapFilterFuncBase> _bitmap_filter_func;
    State _state;
    std::string _err_msg;

    friend class SyncSizeClosure;
};

} // namespace doris
