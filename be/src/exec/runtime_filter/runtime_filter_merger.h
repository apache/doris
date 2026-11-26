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

#include <algorithm>

#include "exec/runtime_filter/runtime_filter.h"
#include "exec/runtime_filter/runtime_filter_definitions.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"

namespace doris {
// The merger is divided into local merger and global merger
// Which are used to merge backend level rf and global rf respectively.
// The local merger will also be used to handle local shuffle situations
// Merger will merge multipe predicate into one predicate
class RuntimeFilterMerger : public RuntimeFilter {
public:
    enum class State {
        WAITING_FOR_PRODUCT, // Still waiting to collect the status of the product
        READY // Collecting all products(_received_producer_num == _expected_producer_num) will transfer to this state, and filter is already available
    };

    static Status create(const QueryContext* query_ctx, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterMerger>* res) {
        *res = std::shared_ptr<RuntimeFilterMerger>(new RuntimeFilterMerger(query_ctx, desc));
        VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(desc->src_expr, build_ctx));
        (*res)->_wrapper = std::make_shared<RuntimeFilterWrapper>(
                build_ctx->root()->data_type()->get_primitive_type(), (*res)->_runtime_filter_type,
                desc->filter_id, RuntimeFilterWrapper::State::UNINITED);
        return Status::OK();
    }

    std::string debug_string() override {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        return fmt::format(
                "Merger: ({}, expected_producer_num: {}, received_producer_num: {}, "
                "received_rf_size_num: {}, received_sum_size: {})",
                _debug_string(), _expected_producer_num, _received_producer_num,
                _received_rf_size_num, _received_sum_size);
    }

    // Keep a private copy of the first input: later merges must not change a producer wrapper
    // which a local consumer may already be probing. A disabled input disables the result.
    // Returns true only for the call that makes the merger ready.
    Status merge_from(const RuntimeFilter* other, bool* ready) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        *ready = false;
        if (_expected_producer_num <= _received_producer_num) {
            return Status::InternalError(
                    "runtime filter merger input product more than expected, {}", debug_string());
        }
        Status status;
        if (_wrapper->get_state() == RuntimeFilterWrapper::State::UNINITED) {
            // For example, consider a join whose probe side has two scan targets:
            //   SELECT p.k FROM build_table b JOIN
            //     (SELECT k FROM local_probe UNION ALL SELECT k FROM merged_probe) p ON b.k = p.k;
            // Depending on the physical plan, the same RF can be routed as follows:
            //   producer 1 RF {5} -----> Scan(local_probe)  [direct, no merge]
            //           |-- copy --> merger <-- producer 2 RF {6}
            //                         |
            //                         +----> Scan(merged_probe) [merged: {5, 6}]
            // The merger must clone producer 1's wrapper: adopting it would let the
            // later merge change the direct scan's filter from {5} to {5, 6}.
            status = other->_wrapper->clone(&_wrapper);
        } else {
            status = _wrapper->merge(other->_wrapper.get());
        }
        RETURN_IF_ERROR(status);
        // A failed copy or merge must not count as an arrival: otherwise later inputs could
        // publish a filter that does not contain the failed producer's values.
        ++_received_producer_num;
        *ready = _received_producer_num == _expected_producer_num;
        if (*ready) {
            _rf_state = State::READY;
        }
        return Status::OK();
    }

    // Only raise the expected producer count. RuntimeFilterMgr may compute the
    // count under its own lock and apply it after releasing that lock, so
    // concurrent registrations can update the merger out of order.
    void increase_expected_producer_num(int num) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        if (_received_producer_num > 0 || _received_rf_size_num > 0) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "runtime filter merger set expected producer after receive data, {}",
                            debug_string());
        }
        _expected_producer_num = std::max(_expected_producer_num, num);
    }

    int get_expected_producer_num() {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        return _expected_producer_num;
    }

    bool add_rf_size(uint64_t size) {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        _received_rf_size_num++;
        if (_expected_producer_num < _received_rf_size_num) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "runtime filter merger input product size more than expected, {}",
                            debug_string());
        }
        _received_sum_size += size;
        return (_received_rf_size_num == _expected_producer_num);
    }

    uint64_t get_received_sum_size() {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        return _received_sum_size;
    }

    bool ready() const { return _rf_state == State::READY; }

    void set_wrapper_state_and_ready_to_apply(RuntimeFilterWrapper::State state,
                                              std::string reason = "") {
        std::unique_lock<std::recursive_mutex> l(_rmtx);
        if (_rf_state == State::READY) {
            return;
        }
        _wrapper->set_state(state, reason);
        _rf_state = State::READY;
    }

private:
    RuntimeFilterMerger(const QueryContext* query_ctx, const TRuntimeFilterDesc* desc)
            : RuntimeFilter(desc), _rf_state(State::WAITING_FOR_PRODUCT) {}

    std::atomic<State> _rf_state;

    int _expected_producer_num = 0;
    int _received_producer_num = 0;

    uint64_t _received_sum_size = 0;
    int _received_rf_size_num = 0;

    friend class RuntimeFilterProducer;
};
} // namespace doris
