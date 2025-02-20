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
#include "runtime/runtime_state.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "runtime_filter/utils.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class BloomFilterFuncBase;
namespace vectorized {
class VRuntimeFilterWrapper;
using VRuntimeFilterPtr = std::shared_ptr<VRuntimeFilterWrapper>;
} // namespace vectorized

// This class is a wrapper of runtime predicate function
class RuntimeFilterWrapper {
public:
    enum class State { UNINITED, READY, IGNORED, DISABLED };

    RuntimeFilterWrapper(const RuntimeFilterParams* params);
    RuntimeFilterWrapper(PrimitiveType column_type, RuntimeFilterType type, uint32_t filter_id,
                         State state)
            : _column_return_type(column_type),
              _filter_type(type),
              _filter_id(filter_id),
              _state(state) {}

    Status change_to_bloom_filter();

    int filter_id() const { return _filter_id; }

    int max_in_num() const { return _max_in_num; }

    bool build_bf_by_runtime_size() const;

    Status init_bloom_filter(const size_t runtime_size);

    void insert_to_bloom_filter(BloomFilterFuncBase* bloom_filter) const;

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

    bool contain_null() const;

    void batch_assign(const PInFilter& filter,
                      void (*assign_func)(std::shared_ptr<HybridSetBase>& _hybrid_set,
                                          PColumnValue&));

    friend class RuntimeFilter;

    template <class T>
    Status assign_data(const T& request, butil::IOBufAsZeroCopyInputStream* data) {
        PFilterType filter_type = request.filter_type();

        if (request.has_disabled() && request.disabled()) {
            disable("get disabled from remote");
            return Status::OK();
        }

        if (request.has_ignored() && request.ignored()) {
            set_state(State::IGNORED);
            return Status::OK();
        }

        set_state(State::READY);

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

    std::string debug_string() const;

    void set_state(State state) {
        DCHECK(state != State::DISABLED);
        if (_state == State::DISABLED) {
            return;
        }

        _state = state;
    }

    void disable(std::string reason) {
        _state = State::DISABLED;
        _disabled_reason = reason;
    }

    State get_state() const { return _state; }

    void check_state(std::vector<State> assumed_states) const {
        if (!check_state_impl<RuntimeFilterWrapper>(_state, assumed_states)) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "producer meet invalid state, {}, assumed_states is {}", debug_string(),
                            states_to_string<RuntimeFilterWrapper>(assumed_states));
        }
    }

    static std::string to_string(const State& state) {
        switch (state) {
        case State::IGNORED:
            return "IGNORED";
        case State::DISABLED:
            return "DISABLED";
        case State::UNINITED:
            return "UNINITED";
        case State::READY:
            return "READY";
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid State {}", int(state));
        }
    }

    void _to_protobuf(PInFilter* filter);

    void _to_protobuf(PMinMaxFilter* filter);

private:
    // When a runtime filter received from remote and it is a bloom filter, _column_return_type will be invalid.
    PrimitiveType _column_return_type; // column type
    RuntimeFilterType _filter_type;
    int32_t _max_in_num;
    uint32_t _filter_id;

    std::shared_ptr<MinMaxFuncBase> _minmax_func;
    std::shared_ptr<HybridSetBase> _hybrid_set;
    std::shared_ptr<BloomFilterFuncBase> _bloom_filter_func;
    std::shared_ptr<BitmapFilterFuncBase> _bitmap_filter_func;
    std::atomic<State> _state;
    std::string _disabled_reason;
};

} // namespace doris
