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

#include "vec/common/exception.h"

#include <cxxabi.h>
#include <string.h>

#include <filesystem>
#include <iostream>
#include <string>
#include <typeinfo>

namespace doris::vectorized {

AbstractException::AbstractException(int code) : _pNested(0), _code(code) {}

AbstractException::AbstractException(const std::string& msg, int code)
        : _msg(msg), _pNested(0), _code(code) {}

AbstractException::AbstractException(const std::string& msg, const std::string& arg, int code)
        : _msg(msg), _pNested(0), _code(code) {
    if (!arg.empty()) {
        _msg.append(": ");
        _msg.append(arg);
    }
}

AbstractException::AbstractException(const std::string& msg, const AbstractException& nested,
                                     int code)
        : _msg(msg), _pNested(nested.clone()), _code(code) {}

AbstractException::AbstractException(const AbstractException& exc)
        : std::exception(exc), _msg(exc._msg), _code(exc._code) {
    _pNested = exc._pNested ? exc._pNested->clone() : 0;
}

AbstractException::~AbstractException() throw() {
    delete _pNested;
}

AbstractException& AbstractException::operator=(const AbstractException& exc) {
    if (&exc != this) {
        AbstractException* newPNested = exc._pNested ? exc._pNested->clone() : 0;
        delete _pNested;
        _msg = exc._msg;
        _pNested = newPNested;
        _code = exc._code;
    }
    return *this;
}

const char* AbstractException::name() const throw() {
    return "Exception";
}

const char* AbstractException::className() const throw() {
    return typeid(*this).name();
}

const char* AbstractException::what() const throw() {
    return name();
}

std::string AbstractException::displayText() const {
    std::string txt = name();
    if (!_msg.empty()) {
        txt.append(": ");
        txt.append(_msg);
    }
    return txt;
}

void AbstractException::extendedMessage(const std::string& arg) {
    if (!arg.empty()) {
        if (!_msg.empty()) _msg.append(": ");
        _msg.append(arg);
    }
}

AbstractException* AbstractException::clone() const {
    return new AbstractException(*this);
}

void AbstractException::rethrow() const {
    throw *this;
}

//TODO: use fmt
std::string errnoToString(int code, int e) {
    const size_t buf_size = 128;
    char buf[buf_size];
    return "errno: " + std::to_string(e) +
           ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf)));
}

void throwFromErrno(const std::string& s, int code, int e) {
    throw ErrnoException(s + ", " + errnoToString(code, e), code, e);
}

void throwFromErrnoWithPath(const std::string& s, const std::string& path, int code,
                            int the_errno) {
    throw ErrnoException(s + ", " + errnoToString(code, the_errno), code, the_errno, path);
}

void tryLogCurrentException(const char* log_name, const std::string& start_of_message) {
    // tryLogCurrentException(&Logger::get(log_name), start_of_message);
    std::cout << "[TODO] should use glog here :" << start_of_message << std::endl;
}

std::string getExtraExceptionInfo(const std::exception& e) {
    std::string msg;
    return msg;
}

std::string getCurrentExceptionMessage(bool with_stacktrace,
                                       bool check_embedded_stacktrace /*= false*/,
                                       bool with_extra_info /*= true*/) {
    std::stringstream stream;

    try {
        throw;
    } catch (const Exception& e) {
        stream << getExceptionMessage(e, with_stacktrace, check_embedded_stacktrace)
               << (with_extra_info ? getExtraExceptionInfo(e) : "") << " (version "
               << "VERSION_STRING"
               << "VERSION_OFFICIAL"
               << ")";
    } catch (const AbstractException& e) {
        try {
            stream << "Poco::Exception. Code: " << TStatusCode::VEC_EXCEPTION
                   << ", e.code() = " << e.code() << ", e.displayText() = " << e.displayText()
                   << (with_extra_info ? getExtraExceptionInfo(e) : "") << " (version "
                   << "VERSION_STRING"
                   << "VERSION_OFFICIAL";
        } catch (...) {
        }
    } catch (const std::exception& e) {
        try {
        } catch (...) {
        }
    } catch (...) {
        try {
        } catch (...) {
        }
    }

    return stream.str();
}

std::string getExceptionMessage(const Exception& e, bool with_stacktrace,
                                bool check_embedded_stacktrace) {
    std::stringstream stream;

    try {
        std::string text = e.displayText();

        bool has_embedded_stack_trace = false;
        if (check_embedded_stacktrace) {
            auto embedded_stack_trace_pos = text.find("Stack trace");
            has_embedded_stack_trace = embedded_stack_trace_pos != std::string::npos;
            if (!with_stacktrace && has_embedded_stack_trace) {
                text.resize(embedded_stack_trace_pos);
            }
        }

        stream << "Code: " << e.code() << ", e.displayText() = " << text;

        if (with_stacktrace && !has_embedded_stack_trace)
            stream << ", Stack trace:\n\n" << e.getStackTrace().value();
    } catch (...) {
    }

    return stream.str();
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace) {
    try {
        std::rethrow_exception(std::move(e));
    } catch (...) {
        return getCurrentExceptionMessage(with_stacktrace);
    }
}

} // namespace  doris::vectorized
