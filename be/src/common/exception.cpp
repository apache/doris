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

#include "common/exception.h"

#include "common/config.h"
#include "util/stack_util.h"
namespace doris {

Exception::Exception(int code, const std::string_view& msg) {
    _code = code;
    _err_msg = std::make_unique<ErrMsg>();
    _err_msg->_msg = msg;
    if (ErrorCode::error_states[abs(code)].stacktrace) {
        _err_msg->_stack = get_stack_trace();
    }
#ifdef BE_TEST
    // BE UT TEST exceptions thrown during execution cannot be caught
    // and the error `C++ exception with description "argument not found" thrown in the test body` will be printed.
    std::cout << "Exception: " << code << ", " << msg << ", " << get_stack_trace() << std::endl;
#endif
    if (config::exit_on_exception) {
        LOG(FATAL) << "[ExitOnException] error code: " << code << ", message: " << msg;
    }
}
} // namespace doris