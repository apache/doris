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
#include "runtime/mem_tracker.h"
#include "udf/udf_internal.h"
#include "udf/udf.h"

namespace doris {

FunctionUtils::FunctionUtils() {
    doris_udf::FunctionContext::TypeDesc return_type;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    _mem_tracker = new MemTracker();
    _memory_pool = new MemPool(_mem_tracker);
    _fn_ctx = FunctionContextImpl::create_context(
        _state, _memory_pool, return_type, arg_types, 0, false);
}

FunctionUtils::~FunctionUtils() {
    _fn_ctx->impl()->close();
    delete _fn_ctx;
    delete _memory_pool;
    delete _mem_tracker;
}

}
