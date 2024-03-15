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

#include "vec/core/wide_integer.h"

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

inline uint64_t int_exp10(int x) {
    if (x < 0) {
        return 0;
    }
    if (x > 19) {
        return std::numeric_limits<uint64_t>::max();
    }

    return exp_details::get_exp<uint64_t, 10, 20>(x);
}

namespace common {

constexpr inline int exp10_i32(int x) {
    if (x < 0) {
        return 0;
    }
    if (x > 9) {
        return std::numeric_limits<int>::max();
    }

    constexpr int values[] = {1,      10,      100,      1000,      10000,
                              100000, 1000000, 10000000, 100000000, 1000000000};
    return values[x];
}

constexpr inline int64_t exp10_i64(int x) {
    if (x < 0) {
        return 0;
    }
    if (x > 18) {
        return std::numeric_limits<int64_t>::max();
    }

    constexpr int64_t values[] = {1LL,
                                  10LL,
                                  100LL,
                                  1000LL,
                                  10000LL,
                                  100000LL,
                                  1000000LL,
                                  10000000LL,
                                  100000000LL,
                                  1000000000LL,
                                  10000000000LL,
                                  100000000000LL,
                                  1000000000000LL,
                                  10000000000000LL,
                                  100000000000000LL,
                                  1000000000000000LL,
                                  10000000000000000LL,
                                  100000000000000000LL,
                                  1000000000000000000LL};
    return values[x];
}

constexpr inline __int128 exp10_i128(int x) {
    if (x < 0) {
        return 0;
    }
    if (x > 38) {
        return std::numeric_limits<__int128>::max();
    }

    constexpr __int128 values[] = {
            static_cast<__int128>(1LL),
            static_cast<__int128>(10LL),
            static_cast<__int128>(100LL),
            static_cast<__int128>(1000LL),
            static_cast<__int128>(10000LL),
            static_cast<__int128>(100000LL),
            static_cast<__int128>(1000000LL),
            static_cast<__int128>(10000000LL),
            static_cast<__int128>(100000000LL),
            static_cast<__int128>(1000000000LL),
            static_cast<__int128>(10000000000LL),
            static_cast<__int128>(100000000000LL),
            static_cast<__int128>(1000000000000LL),
            static_cast<__int128>(10000000000000LL),
            static_cast<__int128>(100000000000000LL),
            static_cast<__int128>(1000000000000000LL),
            static_cast<__int128>(10000000000000000LL),
            static_cast<__int128>(100000000000000000LL),
            static_cast<__int128>(1000000000000000000LL),
            static_cast<__int128>(1000000000000000000LL) * 10LL,
            static_cast<__int128>(1000000000000000000LL) * 100LL,
            static_cast<__int128>(1000000000000000000LL) * 1000LL,
            static_cast<__int128>(1000000000000000000LL) * 10000LL,
            static_cast<__int128>(1000000000000000000LL) * 100000LL,
            static_cast<__int128>(1000000000000000000LL) * 1000000LL,
            static_cast<__int128>(1000000000000000000LL) * 10000000LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000LL,
            static_cast<__int128>(1000000000000000000LL) * 1000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 10000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 1000000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 10000000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 1000000000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 10000000000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 10LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 100LL,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL};
    return values[x];
}

inline wide::Int256 exp10_i256(int x) {
    if (x < 0) {
        return 0;
    }
    if (x > 76) {
        return std::numeric_limits<wide::Int256>::max();
    }

    static constexpr wide::Int256 i10e18 {1000000000000000000ll};
    static const wide::Int256 values[] = {
            static_cast<wide::Int256>(1ll),
            static_cast<wide::Int256>(10ll),
            static_cast<wide::Int256>(100ll),
            static_cast<wide::Int256>(1000ll),
            static_cast<wide::Int256>(10000ll),
            static_cast<wide::Int256>(100000ll),
            static_cast<wide::Int256>(1000000ll),
            static_cast<wide::Int256>(10000000ll),
            static_cast<wide::Int256>(100000000ll),
            static_cast<wide::Int256>(1000000000ll),
            static_cast<wide::Int256>(10000000000ll),
            static_cast<wide::Int256>(100000000000ll),
            static_cast<wide::Int256>(1000000000000ll),
            static_cast<wide::Int256>(10000000000000ll),
            static_cast<wide::Int256>(100000000000000ll),
            static_cast<wide::Int256>(1000000000000000ll),
            static_cast<wide::Int256>(10000000000000000ll),
            static_cast<wide::Int256>(100000000000000000ll),
            i10e18,
            i10e18 * 10ll,
            i10e18 * 100ll,
            i10e18 * 1000ll,
            i10e18 * 10000ll,
            i10e18 * 100000ll,
            i10e18 * 1000000ll,
            i10e18 * 10000000ll,
            i10e18 * 100000000ll,
            i10e18 * 1000000000ll,
            i10e18 * 10000000000ll,
            i10e18 * 100000000000ll,
            i10e18 * 1000000000000ll,
            i10e18 * 10000000000000ll,
            i10e18 * 100000000000000ll,
            i10e18 * 1000000000000000ll,
            i10e18 * 10000000000000000ll,
            i10e18 * 100000000000000000ll,
            i10e18 * 100000000000000000ll * 10ll,
            i10e18 * 100000000000000000ll * 100ll,
            i10e18 * 100000000000000000ll * 1000ll,
            i10e18 * 100000000000000000ll * 10000ll,
            i10e18 * 100000000000000000ll * 100000ll,
            i10e18 * 100000000000000000ll * 1000000ll,
            i10e18 * 100000000000000000ll * 10000000ll,
            i10e18 * 100000000000000000ll * 100000000ll,
            i10e18 * 100000000000000000ll * 1000000000ll,
            i10e18 * 100000000000000000ll * 10000000000ll,
            i10e18 * 100000000000000000ll * 100000000000ll,
            i10e18 * 100000000000000000ll * 1000000000000ll,
            i10e18 * 100000000000000000ll * 10000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000ll,
            i10e18 * 100000000000000000ll * 1000000000000000ll,
            i10e18 * 100000000000000000ll * 10000000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 10ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 1000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 10000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 100ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 1000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 100000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 1000000ll,
            i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll *
                    10000000ll,
    };
    return values[x];
}

constexpr inline int max_i32(int digit_count) {
    if (digit_count < 0) {
        return 0;
    }
    if (digit_count > 9) {
        return std::numeric_limits<int>::max();
    }
    constexpr int values[] = {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999};
    return values[digit_count];
}

constexpr inline int64_t max_i64(int digit_count) {
    if (digit_count < 0) {
        return 0;
    }
    if (digit_count > 18) {
        return std::numeric_limits<int64_t>::max();
    }

    constexpr int64_t values[] = {1LL,
                                  9LL,
                                  99LL,
                                  999LL,
                                  9999LL,
                                  99999LL,
                                  999999LL,
                                  9999999LL,
                                  99999999LL,
                                  999999999LL,
                                  9999999999LL,
                                  99999999999LL,
                                  999999999999LL,
                                  9999999999999LL,
                                  99999999999999LL,
                                  999999999999999LL,
                                  9999999999999999LL,
                                  99999999999999999LL,
                                  999999999999999999LL};
    return values[digit_count];
}

constexpr inline __int128 max_i128(int digit_count) {
    DCHECK(digit_count > 0);
    constexpr __int128 values[] = {
            static_cast<__int128>(0LL),
            static_cast<__int128>(9LL),
            static_cast<__int128>(99LL),
            static_cast<__int128>(999LL),
            static_cast<__int128>(9999LL),
            static_cast<__int128>(99999LL),
            static_cast<__int128>(999999LL),
            static_cast<__int128>(9999999LL),
            static_cast<__int128>(99999999LL),
            static_cast<__int128>(999999999LL),
            static_cast<__int128>(9999999999LL),
            static_cast<__int128>(99999999999LL),
            static_cast<__int128>(999999999999LL),
            static_cast<__int128>(9999999999999LL),
            static_cast<__int128>(99999999999999LL),
            static_cast<__int128>(999999999999999LL),
            static_cast<__int128>(9999999999999999LL),
            static_cast<__int128>(99999999999999999LL),
            static_cast<__int128>(999999999999999999LL),
            static_cast<__int128>(1000000000000000000LL) * 10LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 1000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 10000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 1000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 10000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 1000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 10000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 1000000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 10000000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 1000000000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 10000000000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 10LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 100LL - 1,
            static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL - 1};
    return values[digit_count];
}

inline wide::Int256 max_i256(int digit_count) {
    if (digit_count < 0) {
        return 0;
    }
    if (digit_count > 76) {
        return std::numeric_limits<wide::Int256>::max();
    }

    static constexpr wide::Int256 i10e18 {1000000000000000000LL};
    static const wide::Int256 values[] = {
            static_cast<wide::Int256>(0LL),
            static_cast<wide::Int256>(9LL),
            static_cast<wide::Int256>(99LL),
            static_cast<wide::Int256>(999LL),
            static_cast<wide::Int256>(9999LL),
            static_cast<wide::Int256>(99999LL),
            static_cast<wide::Int256>(999999LL),
            static_cast<wide::Int256>(9999999LL),
            static_cast<wide::Int256>(99999999LL),
            static_cast<wide::Int256>(999999999LL),
            static_cast<wide::Int256>(9999999999LL),
            static_cast<wide::Int256>(99999999999LL),
            static_cast<wide::Int256>(999999999999LL),
            static_cast<wide::Int256>(9999999999999LL),
            static_cast<wide::Int256>(99999999999999LL),
            static_cast<wide::Int256>(999999999999999LL),
            static_cast<wide::Int256>(9999999999999999LL),
            static_cast<wide::Int256>(99999999999999999LL),
            i10e18 - 1,
            i10e18 * 10LL - 1,
            i10e18 * 100LL - 1,
            i10e18 * 1000LL - 1,
            i10e18 * 10000LL - 1,
            i10e18 * 100000LL - 1,
            i10e18 * 1000000LL - 1,
            i10e18 * 10000000LL - 1,
            i10e18 * 100000000LL - 1,
            i10e18 * 1000000000LL - 1,
            i10e18 * 10000000000LL - 1,
            i10e18 * 100000000000LL - 1,
            i10e18 * 1000000000000LL - 1,
            i10e18 * 10000000000000LL - 1,
            i10e18 * 100000000000000LL - 1,
            i10e18 * 1000000000000000LL - 1,
            i10e18 * 10000000000000000LL - 1,
            i10e18 * 100000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 10LL - 1,
            i10e18 * 100000000000000000LL * 100LL - 1,
            i10e18 * 100000000000000000LL * 1000LL - 1,
            i10e18 * 100000000000000000LL * 10000LL - 1,
            i10e18 * 100000000000000000LL * 100000LL - 1,
            i10e18 * 100000000000000000LL * 1000000LL - 1,
            i10e18 * 100000000000000000LL * 10000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000LL - 1,
            i10e18 * 100000000000000000LL * 1000000000LL - 1,
            i10e18 * 100000000000000000LL * 10000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000LL - 1,
            i10e18 * 100000000000000000LL * 1000000000000LL - 1,
            i10e18 * 100000000000000000LL * 10000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000LL - 1,
            i10e18 * 100000000000000000LL * 1000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 10000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 10LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 1000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 10000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 1000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 10000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 1000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 10000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 1000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 10000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 1000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 10000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL * 10LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL * 100LL - 1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL * 1000LL -
                    1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL * 10000LL -
                    1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL * 100000LL -
                    1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL *
                            1000000LL -
                    1,
            i10e18 * 100000000000000000LL * 100000000000000000LL * 100000000000000000LL *
                            10000000LL -
                    1,
    };
    return values[digit_count];
}
} // namespace common
