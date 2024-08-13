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
///////////////////////////////////////////////////////////////
//  Distributed under the Boost Software License, Version 1.0.
//  (See at http://www.boost.org/LICENSE_1_0.txt)
///////////////////////////////////////////////////////////////

/*  Divide and multiply
 *
 *
 * Copyright (c) 2008
 * Evan Teran
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose and without fee is hereby granted, provided
 * that the above copyright notice appears in all copies and that both the
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the same name not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission. We make no representations about the
 * suitability this software for any purpose. It is provided "as is"
 * without express or implied warranty.
 */
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/wide_integer.h
// and modified by Doris
#pragma once

#include <cstdint>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <type_traits>

// NOLINTBEGIN(*)

namespace wide {
template <size_t Bits, typename Signed>
class integer;
}

namespace std {

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
struct common_type<wide::integer<Bits, Signed>, wide::integer<Bits2, Signed2>>;

template <size_t Bits, typename Signed, typename Arithmetic>
struct common_type<wide::integer<Bits, Signed>, Arithmetic>;

template <typename Arithmetic, size_t Bits, typename Signed>
struct common_type<Arithmetic, wide::integer<Bits, Signed>>;

} // namespace std

namespace wide {

template <size_t Bits, typename Signed>
class integer {
public:
    using base_type = uint64_t;
    using signed_base_type = int64_t;

    // ctors
    constexpr integer() noexcept = default;

    template <typename T>
    constexpr integer(T rhs) noexcept;

    template <typename T>
    constexpr integer(std::initializer_list<T> il) noexcept;

    // assignment
    template <size_t Bits2, typename Signed2>
    constexpr integer<Bits, Signed>& operator=(const integer<Bits2, Signed2>& rhs) noexcept;

    template <typename Arithmetic>
    constexpr integer<Bits, Signed>& operator=(Arithmetic rhs) noexcept;

    template <typename Arithmetic>
    constexpr integer<Bits, Signed>& operator*=(const Arithmetic& rhs);

    template <typename Arithmetic>
    constexpr integer<Bits, Signed>& operator/=(const Arithmetic& rhs);

    template <typename Arithmetic>
    constexpr integer<Bits, Signed>& operator+=(const Arithmetic& rhs) noexcept(
            std::is_same_v<Signed, unsigned>);

    template <typename Arithmetic>
    constexpr integer<Bits, Signed>& operator-=(const Arithmetic& rhs) noexcept(
            std::is_same_v<Signed, unsigned>);

    template <typename Integral>
    constexpr integer<Bits, Signed>& operator%=(const Integral& rhs);

    template <typename Integral>
    constexpr integer<Bits, Signed>& operator&=(const Integral& rhs) noexcept;

    template <typename Integral>
    constexpr integer<Bits, Signed>& operator|=(const Integral& rhs) noexcept;

    template <typename Integral>
    constexpr integer<Bits, Signed>& operator^=(const Integral& rhs) noexcept;

    constexpr integer<Bits, Signed>& operator<<=(int n) noexcept;
    constexpr integer<Bits, Signed>& operator>>=(int n) noexcept;

    constexpr integer<Bits, Signed>& operator++() noexcept(std::is_same_v<Signed, unsigned>);
    constexpr integer<Bits, Signed> operator++(int) noexcept(std::is_same_v<Signed, unsigned>);
    constexpr integer<Bits, Signed>& operator--() noexcept(std::is_same_v<Signed, unsigned>);
    constexpr integer<Bits, Signed> operator--(int) noexcept(std::is_same_v<Signed, unsigned>);

    // observers

    constexpr explicit operator bool() const noexcept;

    template <typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>, T>>
    constexpr operator T() const noexcept;

    constexpr operator long double() const noexcept;
    constexpr operator double() const noexcept;
    constexpr operator float() const noexcept;

    std::string to_hex_string() const {
        std::ostringstream os;
        os << std::setw(16) << std::setfill('0') << std::hex;
        for (size_t i = 0; i < _impl::item_count; i++) {
            os << items[i];
        }
        return os.str();
    }

    struct _impl;

    base_type items[_impl::item_count];

    uint64_t low() const {
        static_assert(_impl::item_count == 2);
        return items[0];
    }
    uint64_t high() const {
        static_assert(_impl::item_count == 2);
        return items[1];
    }

private:
    template <size_t Bits2, typename Signed2>
    friend class integer;

    friend class std::numeric_limits<integer<Bits, signed>>;
    friend class std::numeric_limits<integer<Bits, unsigned>>;
};

using UInt128 = integer<128, unsigned>;
using Int256 = integer<256, signed>;
using UInt256 = integer<256, unsigned>;

template <typename T>
static constexpr bool ArithmeticConcept() noexcept;

template <class T1, class T2>
using _only_arithmetic =
        typename std::enable_if<ArithmeticConcept<T1>() && ArithmeticConcept<T2>()>::type;

template <typename T>
static constexpr bool IntegralConcept() noexcept;

template <class T, class T2>
using _only_integer = typename std::enable_if<IntegralConcept<T>() && IntegralConcept<T2>()>::type;

// Unary operators
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator~(const integer<Bits, Signed>& lhs) noexcept;

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator-(const integer<Bits, Signed>& lhs) noexcept(
        std::is_same_v<Signed, unsigned>);

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator+(const integer<Bits, Signed>& lhs) noexcept(
        std::is_same_v<Signed, unsigned>);

// Binary operators
template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator*(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator*(const Arithmetic& rhs,
                                                                const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator/(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator/(const Arithmetic& rhs,
                                                                const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator+(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator+(const Arithmetic& rhs,
                                                                const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator-(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator-(const Arithmetic& rhs,
                                                                const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator%(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Integral, typename Integral2, class = _only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator%(const Integral& rhs,
                                                            const Integral2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator&(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Integral, typename Integral2, class = _only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator&(const Integral& rhs,
                                                            const Integral2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator|(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Integral, typename Integral2, class = _only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator|(const Integral& rhs,
                                                            const Integral2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator^(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Integral, typename Integral2, class = _only_integer<Integral, Integral2>>
std::common_type_t<Integral, Integral2> constexpr operator^(const Integral& rhs,
                                                            const Integral2& lhs);

// TODO: Integral
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator<<(const integer<Bits, Signed>& lhs, int n) noexcept;

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator>>(const integer<Bits, Signed>& lhs, int n) noexcept;

template <size_t Bits, typename Signed, typename Int,
          typename = std::enable_if_t<!std::is_same_v<Int, int>>>
constexpr integer<Bits, Signed> operator<<(const integer<Bits, Signed>& lhs, Int n) noexcept {
    return lhs << int(n);
}
template <size_t Bits, typename Signed, typename Int,
          typename = std::enable_if_t<!std::is_same_v<Int, int>>>
constexpr integer<Bits, Signed> operator>>(const integer<Bits, Signed>& lhs, Int n) noexcept {
    return lhs >> int(n);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator<(const Arithmetic& rhs, const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator>(const Arithmetic& rhs, const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<=(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator<=(const Arithmetic& rhs, const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>=(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator>=(const Arithmetic& rhs, const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator==(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator==(const Arithmetic& rhs, const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator!=(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr bool operator!=(const Arithmetic& rhs, const Arithmetic2& lhs);

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr auto operator<=>(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs);
template <typename Arithmetic, typename Arithmetic2,
          class = _only_arithmetic<Arithmetic, Arithmetic2>>
constexpr auto operator<=>(const Arithmetic& rhs, const Arithmetic2& lhs);

} // namespace wide

// NOLINTEND(*)

#include "wide_integer_impl.h"
