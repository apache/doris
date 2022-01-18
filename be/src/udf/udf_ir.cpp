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

#include "udf/udf_internal.h"

namespace doris_udf {
bool FunctionContext::is_arg_constant(int i) const {
    if (i < 0 || i >= _impl->_constant_args.size()) {
        return false;
    }
    return _impl->_constant_args[i] != nullptr;
}

bool FunctionContext::is_col_constant(int i) const {
    if (i < 0 || i >= _impl->_constant_cols.size()) {
        return false;
    }
    return _impl->_constant_cols[i] != nullptr;
}

AnyVal* FunctionContext::get_constant_arg(int i) const {
    if (i < 0 || i >= _impl->_constant_args.size()) {
        return nullptr;
    }
    return _impl->_constant_args[i];
}

doris::ColumnPtrWrapper* FunctionContext::get_constant_col(int i) const {
    if (i < 0 || i >= _impl->_constant_cols.size()) {
        return nullptr;
    }
    return _impl->_constant_cols[i];
}

int FunctionContext::get_num_args() const {
    return _impl->_arg_types.size();
}

int FunctionContext::get_num_constant_args() const {
    return _impl->_constant_args.size();
}

const FunctionContext::TypeDesc& FunctionContext::get_return_type() const {
    return _impl->_return_type;
}

void* FunctionContext::get_function_state(FunctionStateScope scope) const {
    // assert(!_impl->_closed);
    switch (scope) {
    case THREAD_LOCAL:
        return _impl->_thread_local_fn_state;
        break;
    case FRAGMENT_LOCAL:
        return _impl->_fragment_local_fn_state;
        break;
    default:
        // TODO: signal error somehow
        return nullptr;
    }
}

} // namespace doris_udf
