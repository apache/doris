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

#include <limits>
#include <variant>

namespace doris::vectorized {

template <typename LoopType, LoopType start, LoopType end, template <LoopType> typename Reducer>
struct constexpr_loop_match {
    template <typename... TArgs>
    static void run(LoopType target, TArgs&&... args) {
        if constexpr (start <= end) {
            if (start == target) {
                Reducer<start>::run(std::forward<TArgs>(args)...);
            } else {
                constexpr_loop_match<LoopType, start + 1, end, Reducer>::run(
                        target, std::forward<TArgs>(args)...);
            }
        }
    }
};

template <int start, int end, template <int> typename Reducer>
using constexpr_int_match = constexpr_loop_match<int, start, end, Reducer>;

std::variant<std::false_type, std::true_type> inline make_bool_variant(bool condition) {
    if (condition) {
        return std::true_type {};
    } else {
        return std::false_type {};
    }
}

template <typename... Callables>
struct Overload : Callables... {
    using Callables::operator()...;
};

template <typename... Callables>
Overload(Callables&&... callables) -> Overload<Callables...>;

} // namespace  doris::vectorized
