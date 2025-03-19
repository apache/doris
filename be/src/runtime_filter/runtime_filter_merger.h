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

#include "runtime_filter/runtime_filter.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
#include "common/compile_check_begin.h"
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

    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterMerger>* res,
                         RuntimeProfile* parent_profile) {
        *res = std::shared_ptr<RuntimeFilterMerger>(
                new RuntimeFilterMerger(state, desc, parent_profile));
        vectorized::VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(desc->src_expr, build_ctx));
        (*res)->_wrapper = std::make_shared<RuntimeFilterWrapper>(
                build_ctx->root()->type().type, (*res)->_runtime_filter_type, desc->filter_id,
                RuntimeFilterWrapper::State::IGNORED);
        return Status::OK();
    }

    std::string debug_string() const override {
        return fmt::format(
                "Merger: ({}, expected_producer_num: {}, received_producer_num: {}, "
                "received_rf_size_num: {}, received_sum_size: {})",
                _debug_string(), _expected_producer_num, _received_producer_num,
                _received_rf_size_num, _received_sum_size);
    }

    // If input is a disabled predicate, the final result is a disabled predicate.
    // If input is a ignored predicate, then we will skip this predicate.
    // If all inputs are ignored predicate, the final result is a ignored predicate.
    Status merge_from(const RuntimeFilter* other) {
        _received_producer_num++;
        if (_expected_producer_num < _received_producer_num) {
            return Status::InternalError(
                    "runtime filter merger input product more than expected, {}", debug_string());
        }
        if (_received_producer_num == _expected_producer_num) {
            _rf_state = State::READY;
        }
        if (_wrapper->get_state() == RuntimeFilterWrapper::State::IGNORED) {
            _wrapper = other->_wrapper;
            _profile->add_info_string("Info", debug_string());
            return Status::OK();
        }
        auto st = _wrapper->merge(other->_wrapper.get());
        _profile->add_info_string("Info", debug_string());
        return st;
    }

    void set_expected_producer_num(int num) {
        DCHECK_EQ(_received_producer_num, 0);
        DCHECK_EQ(_received_rf_size_num, 0);
        _expected_producer_num = num;
        _profile->add_info_string("Info", debug_string());
    }

    bool add_rf_size(uint64_t size) {
        _received_rf_size_num++;
        if (_expected_producer_num < _received_rf_size_num) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "runtime filter merger input product size more than expected, {}",
                            debug_string());
        }
        _received_sum_size += size;
        _profile->add_info_string("Info", debug_string());
        return (_received_rf_size_num == _expected_producer_num);
    }

    uint64_t get_received_sum_size() const { return _received_sum_size; }

    bool ready() const { return _rf_state == State::READY; }

private:
    RuntimeFilterMerger(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                        RuntimeProfile* parent_profile)
            : RuntimeFilter(state, desc),
              _rf_state(State::WAITING_FOR_PRODUCT),
              _profile(new RuntimeProfile(fmt::format("RF{}", desc->filter_id))) {
        parent_profile->add_child(_profile.get(), true, nullptr);
        _profile->add_info_string("Info", debug_string());
    }

    std::atomic<State> _rf_state;

    int _expected_producer_num = 0;
    int _received_producer_num = 0;

    uint64_t _received_sum_size = 0;
    int _received_rf_size_num = 0;

    std::unique_ptr<RuntimeProfile> _profile;

    friend class RuntimeFilterProducer;
};
#include "common/compile_check_end.h"
} // namespace doris
