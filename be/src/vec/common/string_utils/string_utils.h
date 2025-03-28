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

#include "common/compiler_util.h"

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
    // \t, \n, \v, \f, \r are 9~13, respectively.
    return UNLIKELY(c == ' ' || (c >= 9 && c <= 13));
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

// trim leading and trailing ascii whitespaces
template <typename T>
inline const char* trim_ascii_whitespaces(const char* s, T& len) {
    while (len > 0 && is_whitespace_ascii(*s)) {
        ++s;
        --len;
    }
    while (len > 0 && is_whitespace_ascii(s[len - 1])) {
        --len;
    }
    return s;
}
