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

#include <exception>
#include <utility>

#include "common/logging.h"
#include "util/stack_util.h"

namespace doris {

// This class is used to defer a function when this object is deconstruct
// A Better Defer operator #5576
// for C++17
// Defer defer {[]{ call something }};
//
// for C++11
// auto op = [] {};
// Defer<decltype<op>> (op);
// Defer runs a closure in its destructor. By default destructors must not
// let exceptions escape during stack unwinding (that would call
// std::terminate). This implementation tries to strike a balance:
// - If the destructor is running while there is an active exception
//   (std::uncaught_exceptions() > 0), we call the closure but catch and
//   swallow any exception to avoid terminate.
// - If there is no active exception, we invoke the closure directly and
//   allow any exception to propagate to the caller. To permit propagation
//   we declare the destructor noexcept(false).
template <class T>
class Defer {
public:
    Defer(T& closure) : _closure(closure) {}
    Defer(T&& closure) : _closure(std::move(closure)) {}
    // Allow throwing when there is no active exception. If we are currently
    // unwinding (std::uncaught_exceptions() > 0), swallow exceptions from
    // the closure to prevent std::terminate.
    ~Defer() noexcept(false) {
        if (std::uncaught_exceptions() > 0) {
            try {
                _closure();
            } catch (...) {
                // swallow: cannot safely rethrow during stack unwind
                // Log the exception for debugging. Try to get std::exception::what()
                try {
                    throw;
                } catch (const std::exception& e) {
                    LOG(WARNING) << "Exception swallowed in Defer destructor during unwind: "
                                 << e.what() << ", stack trace:\n"
                                 << get_stack_trace();
                } catch (...) {
                    LOG(WARNING) << "Unknown exception swallowed in Defer destructor during unwind"
                                 << ", stack trace:\n"
                                 << get_stack_trace();
                }
            }
        } else {
            // No active exception: let any exception escape to caller.
            _closure();
        }
    }

private:
    T _closure;
};

// Nested use Defer, variable name concat line number
#define DEFER_CONCAT(n, ...) const auto defer##n = doris::Defer([&]() { __VA_ARGS__; })
#define DEFER_FWD(n, ...) DEFER_CONCAT(n, __VA_ARGS__)
#define DEFER(...) DEFER_FWD(__LINE__, __VA_ARGS__)

} // namespace doris
