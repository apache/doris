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
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/extended_types.h
// and modified by Doris
#pragma once

#include <type_traits>

#include "wide_integer.h"

using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;

static_assert(sizeof(Int256) == 32);
static_assert(sizeof(UInt256) == 32);

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <>
struct is_signed<Int256> {
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned // NOLINT(readability-identifier-naming)
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;

template <class T>
concept is_integer =
        std::is_integral_v<T> || std::is_same_v<T, Int256> || std::is_same_v<T, UInt256>;

namespace std {
template <>
struct make_unsigned<Int256> {
    using type = UInt256;
};
template <>
struct make_unsigned<UInt256> {
    using type = UInt256;
};

template <typename T>
using make_unsigned_t = typename make_unsigned<T>::type;

template <>
struct make_signed<Int256> {
    using type = Int256;
};
template <>
struct make_signed<UInt256> {
    using type = Int256;
};

template <typename T>
using make_signed_t = typename make_signed<T>::type;
} // namespace std