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

#include <gen_cpp/PaloInternalService_types.h>

#include <vector>

#include "runtime/runtime_state.h"
#include "udf/udf.h"

namespace doris {

FunctionUtils::FunctionUtils() {
    TQueryGlobals globals;
    globals.__set_now_string("2019-08-06 01:38:57");
    globals.__set_timestamp_ms(1565026737805);
    globals.__set_time_zone("Asia/Shanghai");
    _state = RuntimeState::create_unique(globals).release();
    doris::TypeDescriptor return_type;
    std::vector<doris::TypeDescriptor> arg_types;
    _fn_ctx = FunctionContext::create_context(_state, return_type, arg_types);
}

FunctionUtils::FunctionUtils(const doris::TypeDescriptor& return_type,
                             const std::vector<doris::TypeDescriptor>& arg_types,
                             int varargs_buffer_size) {
    TQueryGlobals globals;
    globals.__set_now_string("2019-08-06 01:38:57");
    globals.__set_timestamp_ms(1565026737805);
    globals.__set_time_zone("Asia/Shanghai");
    _state = RuntimeState::create_unique(globals).release();
    _fn_ctx = FunctionContext::create_context(_state, return_type, arg_types);
}

FunctionUtils::~FunctionUtils() {
    if (_state) {
        delete _state;
    }
}

} // namespace doris
