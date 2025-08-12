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

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
namespace wide {

using Int128 = wide::integer<128, signed>;
using UInt128 = wide::integer<128, unsigned>;
using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;

static_assert(sizeof(wide::Int256) == 32);
static_assert(sizeof(wide::UInt256) == 32);
} // namespace wide
template <typename T>
struct MakeUnsigned {
    using type = std::make_unsigned_t<T>;
};
template <size_t Bits, typename Signed>
struct MakeUnsigned<wide::integer<Bits, Signed>> {
    using type = wide::integer<Bits, unsigned>;
};
template <typename T>
using MakeUnsignedT = typename MakeUnsigned<T>::type;

template <typename T>
struct MakeSigned {
    using type = std::make_signed_t<T>;
};
template <size_t Bits, typename Signed>
struct MakeSigned<wide::integer<Bits, Signed>> {
    using type = wide::integer<Bits, signed>;
};
template <typename T>
using MakeSignedT = typename MakeSigned<T>::type;

template <typename T>
struct IsSigned {
    static constexpr bool value = std::is_signed<T>::value;
};
template <>
struct IsSigned<wide::Int128> {
    static constexpr bool value = true;
};
template <>
struct IsSigned<wide::Int256> {
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool IsSignedV = IsSigned<T>::value;

template <typename T>
struct IsUnSigned {
    static constexpr bool value = std::is_unsigned<T>::value;
};
template <>
struct IsUnSigned<wide::UInt128> {
    static constexpr bool value = true;
};
template <>
struct IsUnSigned<wide::UInt256> {
    static constexpr bool value = true;
};
template <typename T>
inline constexpr bool IsUnsignedV = IsUnSigned<T>::value;

template <typename T>
struct IsIntegral {
    static constexpr bool value = std::is_integral<T>::value || wide::IsWideInteger<T>::value;
};
template <typename T>
inline constexpr bool IsIntegralV = IsIntegral<T>::value;

// Operator +, -, /, *, % aren't implemented, so it's not an arithmetic type
template <typename T>
struct IsArithmetic {
    static constexpr bool value = std::is_arithmetic<T>::value || wide::IsWideInteger<T>::value;
};

template <typename T>
inline constexpr bool IsArithmeticV = IsArithmetic<T>::value;
