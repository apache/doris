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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/hex.h
// and modified by Doris

#pragma once
#include <cstring>
#include <string>

#include "vec/core/types.h"

namespace doris::vectorized {

/// Maps 0..15 to 0..9A..F or 0..9a..f correspondingly.

extern const char* const hex_digit_to_char_uppercase_table;
extern const char* const hex_digit_to_char_lowercase_table;

inline char hex_digit_uppercase(unsigned char c) {
    return hex_digit_to_char_uppercase_table[c];
}

inline char hex_digit_lowercase(unsigned char c) {
    return hex_digit_to_char_lowercase_table[c];
}

/// Maps 0..255 to 00..FF or 00..ff correspondingly

extern const char* const hex_byte_to_char_uppercase_table;
extern const char* const hex_byte_to_char_lowercase_table;

inline void write_hex_byte_uppercase(UInt8 byte, void* out) {
    memcpy(out, &hex_byte_to_char_uppercase_table[static_cast<size_t>(byte) * 2], 2);
}

inline void write_hex_byte_lowercase(UInt8 byte, void* out) {
    memcpy(out, &hex_byte_to_char_lowercase_table[static_cast<size_t>(byte) * 2], 2);
}

extern const char* const bin_byte_to_char_table;

inline void write_bin_byte(UInt8 byte, void* out) {
    memcpy(out, &bin_byte_to_char_table[static_cast<size_t>(byte) * 8], 8);
}

/// Produces hex representation of an unsigned int with leading zeros (for checksums)
template <typename TUInt>
void write_hex_uint_impl(TUInt uint_, char* out, const char* const table) {
    union {
        TUInt value;
        UInt8 uint8[sizeof(TUInt)];
    };

    value = uint_;

    /// Use little endian
    for (size_t i = 0; i < sizeof(TUInt); ++i) {
        memcpy(out + i * 2, &table[static_cast<size_t>(uint8[sizeof(TUInt) - 1 - i]) * 2], 2);
    }
}

template <typename TUInt>
void write_hex_uint_uppercase(TUInt uint_, char* out) {
    write_hex_uint_impl(uint_, out, hex_byte_to_char_uppercase_table);
}

template <typename TUInt>
void write_hex_uint_lowercase(TUInt uint_, char* out) {
    write_hex_uint_impl(uint_, out, hex_byte_to_char_lowercase_table);
}

template <typename TUInt>
std::string get_hex_uint_uppercase(TUInt uint_) {
    std::string res(sizeof(TUInt) * 2, '\0');
    write_hex_uint_uppercase(uint_, res.data());
    return res;
}

template <typename TUInt>
std::string get_hex_uint_lowercase(TUInt uint_) {
    std::string res(sizeof(TUInt) * 2, '\0');
    write_hex_uint_lowercase(uint_, res.data());
    return res;
}

/// Maps 0..9, A..F, a..f to 0..15. Other chars are mapped to implementation specific value.

extern const char* const hex_char_to_digit_table;

inline UInt8 unhex(char c) {
    return (uint8_t)hex_char_to_digit_table[static_cast<UInt8>((uint8_t)c)];
}

inline UInt8 unhex2(const char* data) {
    return static_cast<UInt8>(unhex(data[0])) * 0x10 + static_cast<UInt8>(unhex(data[1]));
}

inline UInt16 unhex4(const char* data) {
    return static_cast<UInt16>(unhex(data[0])) * 0x1000 +
           static_cast<UInt16>(unhex(data[1])) * 0x100 +
           static_cast<UInt16>(unhex(data[2])) * 0x10 + static_cast<UInt16>(unhex(data[3]));
}

template <typename TUInt>
TUInt unhex_uint(const char* data) {
    TUInt res = TUInt(0);
    if constexpr ((sizeof(TUInt) <= 8) || ((sizeof(TUInt) % 8) != 0)) {
        for (size_t i = 0; i < sizeof(TUInt) * 2; ++i, ++data) {
            res <<= 4;
            res += unhex(*data);
        }
    } else {
        for (size_t i = 0; i < sizeof(TUInt) / 8; ++i, data += 16) {
            res <<= TUInt(64);
            res += TUInt(unhex_uint<UInt64>(data));
        }
    }
    return res;
}
} // namespace doris::vectorized