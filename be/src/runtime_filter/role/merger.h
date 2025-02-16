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

#include "runtime_filter/role/runtime_filter.h"
#include "runtime_filter/runtime_filter_definitions.h"

namespace doris {

class RuntimeFilterMerger : public RuntimeFilter {
public:
    static Status create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                         std::shared_ptr<RuntimeFilterMerger>* res) {
        *res = std::shared_ptr<RuntimeFilterMerger>(new RuntimeFilterMerger(state, desc));
        vectorized::VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(desc->src_expr, build_ctx));
        (*res)->_wrapper = std::make_shared<RuntimeFilterWrapper>(
                build_ctx->root()->type().type, (*res)->_runtime_filter_type, desc->filter_id,
                RuntimeFilterWrapper::State::IGNORED);
        return Status::OK();
    }

    std::string debug_string() const {
        return fmt::format("RuntimeFilterConsumer: ({})", _debug_string());
    }

    Status merge_from(const RuntimeFilter* other) {
        if (_wrapper->get_state() == RuntimeFilterWrapper::State::IGNORED) {
            _wrapper = other->_wrapper;
            return Status::OK();
        }
        return _wrapper->merge(other->_wrapper.get());
    }

    enum class State {
        PUBLISHED,
        WAITING_FOR_PRODUCT,
    };

private:
    RuntimeFilterMerger(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc)
            : RuntimeFilter(state, desc), _rf_state(State::WAITING_FOR_PRODUCT) {}

    static std::string _to_string(const State& state) {
        switch (state) {
        case State::PUBLISHED:
            return "PUBLISHED";
        case State::WAITING_FOR_PRODUCT:
            return "WAITING_FOR_PRODUCT";
        default:
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid State {}",
                                   int(state));
        }
    }

    std::atomic<State> _rf_state;

    friend class RuntimeFilterProducer;
};

} // namespace doris
