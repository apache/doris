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

inline bool is_alpha_numeric_ascii(char c) {
    return is_alpha_ascii(c) || is_numeric_ascii(c);
}

inline bool is_word_char_ascii(char c) {
    return is_alpha_numeric_ascii(c) || c == '_';
}

inline bool is_valid_identifier_begin(char c) {
    return is_alpha_ascii(c) || c == '_';
}

inline bool is_non_alnum(char c) {
    return !is_alpha_numeric_ascii(c);
}

inline bool is_tz_name_part_ascii(char c) {
    return is_alpha_ascii(c) || c == '_';
}

inline bool is_slash_ascii(char c) {
    return c == '/';
}

// Our own definition of "isspace" that optimize on the ' ' branch.
inline bool is_whitespace_ascii(char c) {
    return LIKELY(c == ' ') ||
           UNLIKELY(c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r');
}

inline bool is_not_whitespace_ascii(char c) {
    return !is_whitespace_ascii(c);
}
