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

Exception::Exception(int code, const std::string_view msg) {
    _code = code;
    _err_msg = std::make_unique<ErrMsg>();
    _err_msg->_msg = msg;
    _err_msg->_stack = get_stack_trace();
    if (config::exit_on_exception) {
        LOG(FATAL) << "[ExitOnException] error code: " << code << ", message: " << msg;
    }
}

Exception::Exception(const Exception& nested, int code, const std::string_view msg) {
    _code = code;
    _err_msg = std::make_unique<ErrMsg>();
    _err_msg->_msg = msg;
    _err_msg->_stack = get_stack_trace();
    _nested_excption = std::make_unique<Exception>();
    _nested_excption->_code = nested._code;
    _nested_excption->_err_msg = std::make_unique<ErrMsg>();
    _nested_excption->_err_msg->_msg = nested._err_msg->_msg;
    _nested_excption->_err_msg->_stack = nested._err_msg->_stack;

    if (config::exit_on_exception) {
        LOG(FATAL) << "[ExitOnException] error code: " << code << ", message: " << msg;
    }
}

} // namespace doris