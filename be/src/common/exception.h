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

#include <fmt/format.h>
#include <gen_cpp/Status_types.h>
#include <stdint.h>

#include <exception>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include "common/status.h"

namespace doris {

class Exception : public std::exception {
public:
    Exception() : _code(ErrorCode::OK) {}
    Exception(int code, const std::string_view msg);
    // add nested exception as first param, or the template may could not find
    // the correct method for ...args
    Exception(const Exception& nested, int code, const std::string_view msg);

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    Exception(int code, const std::string_view fmt, Args&&... args)
            : Exception(code, fmt::format(fmt, std::forward<Args>(args)...)) {}

    int code() const { return _code; }

    std::string to_string() const;

private:
    int _code;
    struct ErrMsg {
        std::string _msg;
        std::string _stack;
    };
    std::unique_ptr<ErrMsg> _err_msg;
    std::unique_ptr<Exception> _nested_excption;
};

inline std::string Exception::to_string() const {
    std::stringstream ostr;
    ostr << "[E" << _code << "] ";
    ostr << (_err_msg ? _err_msg->_msg : "");
    if (_err_msg && !_err_msg->_stack.empty()) {
        ostr << '\n' << _err_msg->_stack;
    }
    if (_nested_excption != nullptr) {
        ostr << '\n' << "Caused by:" << _nested_excption->to_string();
    }
    return ostr.str();
}

} // namespace doris