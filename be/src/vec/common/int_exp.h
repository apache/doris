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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/IntExp.h
// and modified by Doris

#pragma once

#include <cstdint>
#include <limits>
#include <utility>

namespace exp_details {

// compile-time exp(v, n) by linear recursion
template <typename T, T v, std::size_t n>
constexpr inline const T exp = T(v) * exp<T, v, n - 1>;

template <typename T, T v>
constexpr inline const T exp<T, v, 0> = 1;

// compile-time exponentiation table { exp(v, I) ... }
template <typename T, T v, std::size_t... I>
constexpr inline const T exp_table[] = {exp<T, v, I>...};

// get value from compile-time exponentiation table by a (maybe) runtime offset
template <typename T, T v, std::size_t... I>
constexpr T get_exp_helper(std::size_t x, std::index_sequence<I...>) {
    return exp_table<T, v, I...>[x];
}

// get_exp_helper with table { exp(v, 0), exp(v, 1) ... exp(v, N - 1) }
template <typename T, T v, std::size_t N>
constexpr T get_exp(std::size_t x) {
    return get_exp_helper<T, v>(x, std::make_index_sequence<N> {});
}

} // namespace exp_details

/// On overflow, the function returns unspecified value.

inline uint64_t int_exp2(int x) {
    return 1ULL << x;
}

inline uint64_t int_exp10(int x) {
    if (x < 0) return 0;
    if (x > 19) return std::numeric_limits<uint64_t>::max();

    return exp_details::get_exp<uint64_t, 10, 20>(x);
}

namespace common {

inline std::int32_t exp10_i32(int x) {
    return exp_details::get_exp<std::int32_t, 10, 10>(x);
}

inline std::int64_t exp10_i64(int x) {
    return exp_details::get_exp<std::int64_t, 10, 19>(x);
}

inline __int128 exp10_i128(int x) {
    return exp_details::get_exp<__int128, 10, 39>(x);
}

} // namespace common
