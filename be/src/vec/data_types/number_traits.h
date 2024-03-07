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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/NumberTraits.h
// and modified by Doris

#pragma once

#include <climits>
#include <type_traits>

#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/core/wide_integer.h"

namespace doris::vectorized {

/** Allows get the result type of the functions +, -, *, /, %, intDiv (integer division).
  * The rules are different from those used in C++.
  */

namespace NumberTraits {

struct Error {};

constexpr size_t max(size_t x, size_t y) {
    return x > y ? x : y;
}

constexpr size_t min(size_t x, size_t y) {
    return x < y ? x : y;
}

// only largeint as argument should return 16
constexpr size_t next_size(size_t size) {
    return size > 8 ? 16 : min(size * 2, 8);
}

template <bool is_signed, bool is_floating, size_t size>
struct Construct {
    using Type = Error;
};

template <>
struct Construct<false, false, 1> {
    using Type = Int16;
};
template <>
struct Construct<false, false, 2> {
    using Type = Int32;
};
template <>
struct Construct<false, false, 4> {
    using Type = Int64;
};
template <>
struct Construct<false, false, 8> {
    using Type = Int128;
};
template <>
struct Construct<false, false, 16> {
    using Type = Int128;
};
template <>
struct Construct<false, false, 32> {
    using Type = wide::Int256;
};
template <>
struct Construct<false, true, 1> {
    using Type = Float32;
};
template <>
struct Construct<false, true, 2> {
    using Type = Float32;
};
template <>
struct Construct<false, true, 4> {
    using Type = Float32;
};
template <>
struct Construct<false, true, 8> {
    using Type = Float64;
};
template <>
struct Construct<true, false, 1> {
    using Type = Int8;
};
template <>
struct Construct<true, false, 2> {
    using Type = Int16;
};
template <>
struct Construct<true, false, 4> {
    using Type = Int32;
};
template <>
struct Construct<true, false, 8> {
    using Type = Int64;
};
template <>
struct Construct<true, false, 16> {
    using Type = Int128;
};
template <>
struct Construct<true, false, 32> {
    using Type = wide::Int256;
};
template <>
struct Construct<true, true, 1> {
    using Type = Float32;
};
template <>
struct Construct<true, true, 2> {
    using Type = Float32;
};
template <>
struct Construct<true, true, 4> {
    using Type = Float32;
};
template <>
struct Construct<true, true, 8> {
    using Type = Float64;
};

template <>
struct Construct<true, true, 16> {
    using Type = Float64;
};

/** The result of addition or multiplication is calculated according to the following rules:
    * - if one of the arguments is floating-point, the result is a floating point, otherwise - the whole;
    * - if one of the arguments is signed, the result is signed, otherwise it is unsigned;
    * - the result contains more bits (not only meaningful) than the maximum in the arguments
    *   (for example, UInt8 + Int32 = Int64).
    */
template <typename A, typename B>
struct ResultOfAdditionMultiplication {
    using Type = typename Construct<std::is_signed_v<A> || std::is_signed_v<B>,
                                    std::is_floating_point_v<A> || std::is_floating_point_v<B>,
                                    next_size(max(sizeof(A), sizeof(B)))>::Type;
};

template <typename A, typename B>
struct ResultOfSubtraction {
    using Type =
            typename Construct<true, std::is_floating_point_v<A> || std::is_floating_point_v<B>,
                               next_size(max(sizeof(A), sizeof(B)))>::Type;
};

/** When dividing, you always get a floating-point number.
    */
template <typename A, typename B>
struct ResultOfFloatingPointDivision {
    using Type = std::conditional_t<IsDecimalNumber<A>, A,
                                    std::conditional_t<IsDecimalNumber<B>, B, Float64>>;
};

/** For integer division, we get a number with the same number of bits as in divisible.
    */
template <typename A, typename B>
struct ResultOfIntegerDivision {
    using Type =
            typename Construct<std::is_signed_v<A> || std::is_signed_v<B>, false, sizeof(A)>::Type;
};

/** Division with remainder you get a number with the same number of bits as in divisor.
    */
template <typename A, typename B>
struct ResultOfModulo {
    using Type = typename Construct<std::is_signed_v<A> || std::is_signed_v<B>,
                                    std::is_floating_point_v<A> || std::is_floating_point_v<B>,
                                    max(sizeof(A), sizeof(B))>::Type;
};

template <typename A>
struct ResultOfAbs {
    using Type = typename Construct<false, std::is_floating_point_v<A>, sizeof(A)>::Type;
};

/** For bitwise operations, an integer is obtained with number of bits is equal to the maximum of the arguments.
    */
template <typename A, typename B>
struct ResultOfBit {
    using Type = typename Construct<std::is_signed_v<A> || std::is_signed_v<B>, false,
                                    std::is_floating_point_v<A> || std::is_floating_point_v<B>
                                            ? 8
                                            : max(sizeof(A), sizeof(B))>::Type;
};

template <typename A>
struct ResultOfBitNot {
    using Type = typename Construct<std::is_signed_v<A>, false, sizeof(A)>::Type;
};

template <typename A, typename B>
struct BinaryOperatorTraits {
    using ColumnVectorA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColumnVectorB = std::conditional_t<IsDecimalNumber<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColumnVectorA::Container;
    using ArrayB = typename ColumnVectorB::Container;
    using ArrayNull = PaddedPODArray<UInt8>;
};

template <typename T>
/// Returns the maximum ascii string length for this type.
/// e.g. the max/min int8_t has 3 characters.
int max_ascii_len() {
    return 0;
}

template <>
inline int max_ascii_len<uint8_t>() {
    return 3;
}

template <>
inline int max_ascii_len<uint16_t>() {
    return 5;
}

template <>
inline int max_ascii_len<uint32_t>() {
    return 10;
}

template <>
inline int max_ascii_len<uint64_t>() {
    return 20;
}

template <>
inline int max_ascii_len<int8_t>() {
    return 3;
}

template <>
inline int max_ascii_len<int16_t>() {
    return 5;
}

template <>
inline int max_ascii_len<int32_t>() {
    return 10;
}

template <>
inline int max_ascii_len<int64_t>() {
    return 19;
}

template <>
inline int max_ascii_len<__int128>() {
    return 39;
}

template <>
inline int max_ascii_len<wide::Int256>() {
    return 77;
}

template <>
inline int max_ascii_len<float>() {
    return INT_MAX;
}

template <>
inline int max_ascii_len<double>() {
    return INT_MAX;
}
} // namespace NumberTraits

} // namespace doris::vectorized
