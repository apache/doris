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

#include "testutil/function_utils.h"

#include <vector>

#include "runtime/mem_pool.h"
#include "udf/udf_internal.h"

namespace doris {

FunctionUtils::FunctionUtils() {
    doris_udf::FunctionContext::TypeDesc return_type;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    _memory_pool = new MemPool("function util");
    _fn_ctx = FunctionContextImpl::create_context(_state, _memory_pool, return_type, arg_types, 0,
                                                  false);
}
FunctionUtils::FunctionUtils(RuntimeState* state) {
    _state = state;
    doris_udf::FunctionContext::TypeDesc return_type;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    _memory_pool = new MemPool("function util");
    _fn_ctx = FunctionContextImpl::create_context(_state, _memory_pool, return_type, arg_types, 0,
                                                  false);
}

FunctionUtils::FunctionUtils(const doris_udf::FunctionContext::TypeDesc& return_type,
                             const std::vector<doris_udf::FunctionContext::TypeDesc>& arg_types,
                             int varargs_buffer_size) {
    _memory_pool = new MemPool("function util");
    _fn_ctx = FunctionContextImpl::create_context(_state, _memory_pool, return_type, arg_types,
                                                  varargs_buffer_size, false);
}

FunctionUtils::~FunctionUtils() {
    _fn_ctx->impl()->close();
    delete _fn_ctx;
    delete _memory_pool;
}

} // namespace doris
