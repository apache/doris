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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/formatIPv6.cpp
// and modified by Doris

#include "vec/common/format_ip.h"

#include <algorithm>
#include <array>

namespace doris::vectorized {

/** Further we want to generate constexpr array of strings with sizes from sequence of unsigned ints [0..N)
 *  in order to use this arrey for fast conversion of unsigned integers to strings
 */
namespace detail {
template <unsigned... digits>
struct ToChars {
    static const char value[];
    static const size_t size;
};

template <unsigned... digits>
constexpr char ToChars<digits...>::value[] = {('0' + digits)..., 0};

template <unsigned... digits>
constexpr size_t ToChars<digits...>::size = sizeof...(digits);

template <unsigned rem, unsigned... digits>
struct Decompose : Decompose<rem / 10, rem % 10, digits...> {};

template <unsigned... digits>
struct Decompose<0, digits...> : ToChars<digits...> {};

template <>
struct Decompose<0> : ToChars<0> {};

template <unsigned num>
struct NumToString : Decompose<num> {};

template <class T, T... ints>
consteval std::array<std::pair<const char*, size_t>, sizeof...(ints)> str_make_array_impl(
        std::integer_sequence<T, ints...>) {
    return std::array<std::pair<const char*, size_t>, sizeof...(ints)> {
            std::pair<const char*, size_t> {NumToString<ints>::value, NumToString<ints>::size}...};
}
} // namespace detail

/** str_make_array<N>() - generates static array of std::pair<const char *, size_t> for numbers [0..N), where:
 *      first - null-terminated string representing number
 *      second - size of the string as would returned by strlen()
 */
template <size_t N>
consteval std::array<std::pair<const char*, size_t>, N> str_make_array() {
    return detail::str_make_array_impl(std::make_integer_sequence<int, N> {});
}

/// This will generate static array of pair<const char *, size_t> for [0..255] at compile time
extern constexpr std::array<std::pair<const char*, size_t>, 256> one_byte_to_string_lookup_table =
        str_make_array<256>();

} // namespace doris::vectorized
