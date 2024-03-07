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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Commom/StringUtils/StringUtils.h
// and modified by Doris

#pragma once

#include <cstddef>
#include <cstring>
#include <string>

namespace doris::vectorized::detail {
bool starts_with(const std::string& s, const char* prefix, size_t prefix_size);
bool ends_with(const std::string& s, const char* suffix, size_t suffix_size);
} // namespace doris::vectorized::detail

inline bool starts_with(const std::string& s, const std::string& prefix) {
    return doris::vectorized::detail::starts_with(s, prefix.data(), prefix.size());
}

inline bool ends_with(const std::string& s, const std::string& suffix) {
    return doris::vectorized::detail::ends_with(s, suffix.data(), suffix.size());
}

/// With GCC, strlen is evaluated compile time if we pass it a constant
/// string that is known at compile time.
inline bool starts_with(const std::string& s, const char* prefix) {
    return doris::vectorized::detail::starts_with(s, prefix, strlen(prefix));
}

inline bool ends_with(const std::string& s, const char* suffix) {
    return doris::vectorized::detail::ends_with(s, suffix, strlen(suffix));
}

/// Given an integer, return the adequate suffix for
/// printing an ordinal number.
template <typename T>
std::string get_ordinal_suffix(T n) {
    static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>,
                  "Unsigned integer value required");

    const auto last_digit = n % 10;

    if ((last_digit < 1 || last_digit > 3) || ((n > 10) && (((n / 10) % 10) == 1))) return "th";

    switch (last_digit) {
    case 1:
        return "st";
    case 2:
        return "nd";
    case 3:
        return "rd";
    default:
        return "th";
    }
}

/// More efficient than libc, because doesn't respect locale. But for some functions table implementation could be better.

inline bool is_ascii(char c) {
    return static_cast<unsigned char>(c) < 0x80;
}

inline bool is_alpha_ascii(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

inline bool is_numeric_ascii(char c) {
    /// This is faster than
    /// return UInt8(UInt8(c) - UInt8('0')) < UInt8(10);
    /// on Intel CPUs when compiled by gcc 8.
    return (c >= '0' && c <= '9');
}

inline bool is_hex_digit(char c) {
    return is_numeric_ascii(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

inline bool is_alpha_numeric_ascii(char c) {
    return is_alpha_ascii(c) || is_numeric_ascii(c);
}

inline bool is_word_char_ascii(char c) {
    return is_alpha_numeric_ascii(c) || c == '_';
}

inline bool is_valid_identifier_begin(char c) {
    return is_alpha_ascii(c) || c == '_';
}

inline bool is_whitespace_ascii(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

inline bool is_control_ascii(char c) {
    return static_cast<unsigned char>(c) <= 31;
}

/// Works assuming is_alpha_ascii.
inline char to_lower_if_alpha_ascii(char c) {
    return c | 0x20;
}

inline char to_upper_if_alpha_ascii(char c) {
    return c & (~0x20);
}

inline char alternate_case_if_alpha_ascii(char c) {
    return c ^ 0x20;
}

inline bool equals_case_insensitive(char a, char b) {
    return a == b || (is_alpha_ascii(a) && alternate_case_if_alpha_ascii(a) == b);
}

template <typename F>
std::string trim(const std::string& str, F&& predicate) {
    size_t cut_front = 0;
    size_t cut_back = 0;
    size_t size = str.size();

    for (size_t i = 0; i < size; ++i) {
        if (predicate(str[i]))
            ++cut_front;
        else
            break;
    }

    if (cut_front == size) return {};

    for (auto it = str.rbegin(); it != str.rend(); ++it) {
        if (predicate(*it))
            ++cut_back;
        else
            break;
    }

    return str.substr(cut_front, size - cut_front - cut_back);
}
