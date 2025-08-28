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

#include <butil/iobuf.h>

#include "common/status.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "runtime_filter/utils.h"
#include "vec/columns/column.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"
class BloomFilterFuncBase;
namespace vectorized {
class VRuntimeFilterWrapper;
using VRuntimeFilterPtr = std::shared_ptr<VRuntimeFilterWrapper>;
} // namespace vectorized

// This class is a wrapper of runtime predicate function
class RuntimeFilterWrapper {
public:
    enum class State {
        UNINITED, // Initial state, filter is not available at this state
        READY,    // After filter obtains insert data, go to this state
        DISABLED // This state indicates that the rf is deprecated, used in cases such as reach max_in_num / join spill / meet rpc error
    };

    // Called by consumer / producer
    RuntimeFilterWrapper(const RuntimeFilterParams* params);
    // Called by merger
    RuntimeFilterWrapper(PrimitiveType column_type, RuntimeFilterType type, uint32_t filter_id,
                         State state, int max_in_num = 0)
            : _column_return_type(column_type),
              _filter_type(type),
              _filter_id(filter_id),
              _max_in_num(max_in_num),
              _state(state) {}

    Status init(const size_t runtime_size);
    Status insert(const vectorized::ColumnPtr& column, size_t start);
    Status merge(const RuntimeFilterWrapper* wrapper);
    template <class T>
    Status assign(const T& request, butil::IOBufAsZeroCopyInputStream* data);

    bool is_valid() const { return _state != State::DISABLED; }
    int filter_id() const { return _filter_id; }
    bool build_bf_by_runtime_size() const;

    RuntimeFilterType get_real_type() const {
        if (_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            if (_hybrid_set) {
                return RuntimeFilterType::IN_FILTER;
            }
            return RuntimeFilterType::BLOOM_FILTER;
        }
        return _filter_type;
    }

    std::shared_ptr<MinMaxFuncBase> minmax_func() const { return _minmax_func; }
    std::shared_ptr<HybridSetBase> hybrid_set() const { return _hybrid_set; }
    std::shared_ptr<BloomFilterFuncBase> bloom_filter_func() const { return _bloom_filter_func; }
    std::shared_ptr<BitmapFilterFuncBase> bitmap_filter_func() const { return _bitmap_filter_func; }

    Status to_protobuf(PInFilter* filter);
    Status to_protobuf(PMinMaxFilter* filter);
    Status to_protobuf(PBloomFilter* filter, char** data, int* filter_length);

    PrimitiveType column_type() const { return _column_return_type; }

    bool contain_null() const;

    std::string debug_string() const;

    // set_state may called in SyncSizeClosure's rpc thread
    // so we need to make all modifyied member variables are atomic
    void set_state(State state, std::string reason = "") {
        if (_state == State::DISABLED) {
            return;
        }
        _state = state;
        if (!reason.empty()) {
            _reason.update(Status::Aborted(reason));
        }
    }
    State get_state() const { return _state; }
    void check_state(std::vector<State> assumed_states) const {
        if (!check_state_impl<RuntimeFilterWrapper>(_state, assumed_states)) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "wrapper meet invalid state, {}, assumed_states is {}", debug_string(),
                            states_to_string<RuntimeFilterWrapper>(assumed_states));
        }
    }
    static std::string to_string(const State& state) {
        switch (state) {
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

private:
    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status _assign(const PInFilter& in_filter, bool contain_null);
    Status _assign(const PBloomFilter& bloom_filter, butil::IOBufAsZeroCopyInputStream* data,
                   bool contain_null);
    Status _assign(const PMinMaxFilter& minmax_filter, bool contain_null);
    Status _change_to_bloom_filter();
    // When a runtime filter received from remote and it is a bloom filter, _column_return_type will be invalid.
    const PrimitiveType _column_return_type; // column type
    const RuntimeFilterType _filter_type;
    const uint32_t _filter_id;
    const int32_t _max_in_num;

    std::shared_ptr<MinMaxFuncBase> _minmax_func;
    std::shared_ptr<HybridSetBase> _hybrid_set;
    std::shared_ptr<BloomFilterFuncBase> _bloom_filter_func;
    std::shared_ptr<BitmapFilterFuncBase> _bitmap_filter_func;

    // Wrapper is the core structure of runtime filter. If filter is local, wrapper may be shared
    // by producer and consumer. To avoid read-write conflict, we need a rwlock to ensure operations
    // on state is thread-safe.
    std::atomic<State> _state;
    AtomicStatus _reason;
};
#include "common/compile_check_end.h"
} // namespace doris
