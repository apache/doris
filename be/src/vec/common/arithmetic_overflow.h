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

namespace common {
template <typename T>
inline bool add_overflow(T x, T y, T& res) {
    return __builtin_add_overflow(x, y, &res);
}

template <>
inline bool add_overflow(int x, int y, int& res) {
    return __builtin_sadd_overflow(x, y, &res);
}

template <>
inline bool add_overflow(long x, long y, long& res) {
    return __builtin_saddl_overflow(x, y, &res);
}

template <>
inline bool add_overflow(long long x, long long y, long long& res) {
    return __builtin_saddll_overflow(x, y, &res);
}

template <>
inline bool add_overflow(__int128 x, __int128 y, __int128& res) {
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 =
            (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    res = x + y;
    return (y > 0 && x > max_int128 - y) || (y < 0 && x < min_int128 - y);
}

template <typename T>
inline bool sub_overflow(T x, T y, T& res) {
    return __builtin_sub_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(int x, int y, int& res) {
    return __builtin_ssub_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(long x, long y, long& res) {
    return __builtin_ssubl_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(long long x, long long y, long long& res) {
    return __builtin_ssubll_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(__int128 x, __int128 y, __int128& res) {
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 =
            (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    res = x - y;
    return (y < 0 && x > max_int128 + y) || (y > 0 && x < min_int128 + y);
}

template <typename T>
inline bool mul_overflow(T x, T y, T& res) {
    return __builtin_mul_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(int x, int y, int& res) {
    return __builtin_smul_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(long x, long y, long& res) {
    return __builtin_smull_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(long long x, long long y, long long& res) {
    return __builtin_smulll_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(__int128 x, __int128 y, __int128& res) {
    res = static_cast<unsigned __int128>(x) *
          static_cast<unsigned __int128>(y); /// Avoid signed integer overflow.
    if (!x || !y) return false;

    unsigned __int128 a = (x > 0) ? x : -x;
    unsigned __int128 b = (y > 0) ? y : -y;
    return (a * b) / b != a;
}
} // namespace common
