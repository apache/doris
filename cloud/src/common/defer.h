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

#include <type_traits>
#include <utility>

namespace doris::cloud {

// A simple RAII class to defer the execution of a function until the end of the
// scope.
template <typename Fn>
requires std::is_invocable_v<Fn>
class DeferFn {
public:
    DeferFn(Fn &&fn) : fn_(std::move(fn)) {}
    DeferFn(const DeferFn &) = delete;
    DeferFn &operator=(const DeferFn &) = delete;
    ~DeferFn() { fn_(); }

private:
    Fn fn_;
};

} // namespace doris::cloud

// A macro to create a DeferFn object that will execute the given function
// when it goes out of scope. This is useful for cleanup tasks or finalization
// actions that should always run, regardless of how the scope is exited (e.g.
// normal return, exception thrown, etc.).
//
// Usage:
//   DORIS_CLOUD_DEFER {
//       // Code to execute at the end of the scope
//   };
//
#define DORIS_CLOUD_DEFER_IMPL(line, counter)                                  \
    ::doris::cloud::DeferFn defer_fn_##line##_##counter = [&]()
#define DORIS_CLOUD_DEFER_EXPAND(line, counter)                                \
    DORIS_CLOUD_DEFER_IMPL(line, counter)
#define DORIS_CLOUD_DEFER DORIS_CLOUD_DEFER_EXPAND(__LINE__, __COUNTER__)

// A macro to create a DeferFn object that will execute the given function
// with additional parameters when it goes out of scope. This is useful for
// cleanup tasks or finalization actions that should always run, regardless of
// how the scope is exited (e.g. normal return, exception thrown, etc.).
//
// Usage:
//   DORIS_CLOUD_DEFER_COPY(param1, param2) {
//       // Code to execute at the end of the scope, using param1 and param2
//   };
//
// Note: The parameters are captured by copy, so they can be used safely, for
// example,
//   void foo(int &a, int &b) {
//       DORIS_CLOUD_DEFER_COPY(a, b) mutable {
//          a += 1;
//          b *= 2;
//       };
//   }
//
//   int x = 1, y = 2;
//   foo(x, y);
//   assert(x == 1 && y == 2);
//
// The captured parameters are passed by value, so they modifications inside the
// deferred function do not affect the original variables outside the scope, or
// the modifications after the definition of the defer function will not affect
// the captured values.
#define DORIS_CLOUD_DEFER_COPY_IMPL(line, counter, ...)                        \
    ::doris::cloud::DeferFn defer_fn_##line##_##counter = [&, __VA_ARGS__ ]()
#define DORIS_CLOUD_DEFER_COPY_EXPAND(line, counter, ...)                      \
    DORIS_CLOUD_DEFER_COPY_IMPL(line, counter, __VA_ARGS__)
#define DORIS_CLOUD_DEFER_COPY(...)                                            \
    DORIS_CLOUD_DEFER_COPY_EXPAND(__LINE__, __COUNTER__, __VA_ARGS__)
