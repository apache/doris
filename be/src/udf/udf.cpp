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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/udf/udf.cpp
// and modified by Doris

#include "udf/udf.h"

#include <iostream>
#include <utility>

// Be careful what this includes since this needs to be linked into the UDF's
// binary. For example, it would be unfortunate if they had a random dependency
// on libhdfs.
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "vec/common/string_ref.h"

namespace doris {

static const int MAX_WARNINGS = 1000;

std::unique_ptr<doris::FunctionContext> FunctionContext::create_context(
        RuntimeState* state, const doris::TypeDescriptor& return_type,
        const std::vector<doris::TypeDescriptor>& arg_types) {
    auto ctx = std::unique_ptr<doris::FunctionContext>(new doris::FunctionContext());
    ctx->_state = state;
    ctx->_return_type = return_type;
    ctx->_arg_types = arg_types;
    ctx->_num_warnings = 0;
    ctx->_thread_local_fn_state = nullptr;
    ctx->_fragment_local_fn_state = nullptr;
    return ctx;
}

void FunctionContext::set_constant_cols(
        const std::vector<std::shared_ptr<doris::ColumnPtrWrapper>>& constant_cols) {
    _constant_cols = constant_cols;
}

std::unique_ptr<FunctionContext> FunctionContext::clone() {
    auto new_context = create_context(_state, _return_type, _arg_types);
    new_context->_constant_cols = _constant_cols;
    new_context->_fragment_local_fn_state = _fragment_local_fn_state;
    new_context->_check_overflow_for_decimal = _check_overflow_for_decimal;
    new_context->_string_as_jsonb_string = _string_as_jsonb_string;
    new_context->_jsonb_string_as_string = _jsonb_string_as_string;
    return new_context;
}

void FunctionContext::set_function_state(FunctionStateScope scope, std::shared_ptr<void> ptr) {
    switch (scope) {
    case THREAD_LOCAL:
        _thread_local_fn_state = std::move(ptr);
        break;
    case FRAGMENT_LOCAL:
        _fragment_local_fn_state = std::move(ptr);
        break;
    default:
        std::stringstream ss;
        ss << "Unknown FunctionStateScope: " << scope;
        set_error(ss.str().c_str());
    }
}

void FunctionContext::set_error(const char* error_msg) {
    if (_error_msg.empty()) {
        _error_msg = error_msg;
        std::stringstream ss;
        ss << "UDF ERROR: " << error_msg;

        if (_state != nullptr) {
            _state->set_process_status(ss.str());
        }
    }
}

bool FunctionContext::add_warning(const char* warning_msg) {
    if (_num_warnings++ >= MAX_WARNINGS) {
        return false;
    }

    std::stringstream ss;
    ss << "UDF WARNING: " << warning_msg;

    if (_state != nullptr) {
        return _state->log_error(ss.str());
    } else {
        std::cerr << ss.str() << std::endl;
        return true;
    }
}

const doris::TypeDescriptor* FunctionContext::get_arg_type(int arg_idx) const {
    if (arg_idx < 0 || arg_idx >= _arg_types.size()) {
        return nullptr;
    }
    return &_arg_types[arg_idx];
}

bool FunctionContext::is_col_constant(int i) const {
    if (i < 0 || i >= _constant_cols.size()) {
        return false;
    }
    return _constant_cols[i] != nullptr;
}

doris::ColumnPtrWrapper* FunctionContext::get_constant_col(int i) const {
    if (i < 0 || i >= _constant_cols.size()) {
        return nullptr;
    }
    return _constant_cols[i].get();
}

int FunctionContext::get_num_args() const {
    return _arg_types.size();
}

const doris::TypeDescriptor& FunctionContext::get_return_type() const {
    return _return_type;
}

void* FunctionContext::get_function_state(FunctionStateScope scope) const {
    switch (scope) {
    case THREAD_LOCAL:
        return _thread_local_fn_state.get();
    case FRAGMENT_LOCAL:
        return _fragment_local_fn_state.get();
    default:
        // TODO: signal error somehow
        return nullptr;
    }
}

StringRef FunctionContext::create_temp_string_val(int64_t len) {
    _string_result.resize(len);
    return StringRef((uint8_t*)_string_result.c_str(), len);
}

} // namespace doris
