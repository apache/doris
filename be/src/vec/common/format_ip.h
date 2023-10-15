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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/formatIPv6.h
// and modified by Doris

#pragma once

#include <vec/common/string_utils/string_utils.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <utility>

constexpr size_t IPV4_BINARY_LENGTH = 4;
constexpr size_t IPV4_MAX_TEXT_LENGTH = 15;       /// Does not count tail zero byte.
constexpr size_t IPV4_MIN_NUM_VALUE = 0;          //num value of '0.0.0.0'
constexpr size_t IPV4_MAX_NUM_VALUE = 4294967295; //num value of '255.255.255.255'

namespace doris::vectorized {

extern const std::array<std::pair<const char*, size_t>, 256> one_byte_to_string_lookup_table;

/** Format 4-byte binary sequesnce as IPv4 text: 'aaa.bbb.ccc.ddd',
  * expects in out to be in BE-format, that is 0x7f000001 => "127.0.0.1".
  *
  * Any number of the tail bytes can be masked with given mask string.
  *
  * Assumptions:
  *     src is IPV4_BINARY_LENGTH long,
  *     dst is IPV4_MAX_TEXT_LENGTH long,
  *     mask_tail_octets <= IPV4_BINARY_LENGTH
  *     mask_string is NON-NULL, if mask_tail_octets > 0.
  *
  * Examples:
  *     formatIPv4(&0x7f000001, dst, mask_tail_octets = 0, nullptr);
  *         > dst == "127.0.0.1"
  *     formatIPv4(&0x7f000001, dst, mask_tail_octets = 1, "xxx");
  *         > dst == "127.0.0.xxx"
  *     formatIPv4(&0x7f000001, dst, mask_tail_octets = 1, "0");
  *         > dst == "127.0.0.0"
  */
inline void formatIPv4(const unsigned char* src, size_t src_size, char*& dst,
                       uint8_t mask_tail_octets = 0, const char* mask_string = "xxx") {
    const size_t mask_length = mask_string ? strlen(mask_string) : 0;
    const size_t limit = std::min(IPV4_BINARY_LENGTH, IPV4_BINARY_LENGTH - mask_tail_octets);
    const size_t padding = std::min(4 - src_size, limit);
    for (size_t octet = 0; octet < padding; ++octet) {
        *dst++ = '0';
        *dst++ = '.';
    }

    for (size_t octet = 4 - src_size; octet < limit; ++octet) {
        uint8_t value = 0;
        if constexpr (std::endian::native == std::endian::little)
            value = static_cast<uint8_t>(src[IPV4_BINARY_LENGTH - octet - 1]);
        else
            value = static_cast<uint8_t>(src[octet]);
        const uint8_t len = one_byte_to_string_lookup_table[value].second;
        const char* str = one_byte_to_string_lookup_table[value].first;

        memcpy(dst, str, len);
        dst += len;

        *dst++ = '.';
    }

    for (size_t mask = 0; mask < mask_tail_octets; ++mask) {
        memcpy(dst, mask_string, mask_length);
        dst += mask_length;

        *dst++ = '.';
    }

    dst--;
}

inline void formatIPv4(const unsigned char* src, char*& dst, uint8_t mask_tail_octets = 0,
                       const char* mask_string = "xxx") {
    formatIPv4(src, 4, dst, mask_tail_octets, mask_string);
}

/** Unsafe (no bounds-checking for src nor dst), optimized version of parsing IPv4 string.
 *
 * Parses the input string `src` and stores binary host-endian value into buffer pointed by `dst`,
 * which should be long enough.
 * That is "127.0.0.1" becomes 0x7f000001.
 *
 * In case of failure doesn't modify buffer pointed by `dst`.
 *
 * WARNING - this function is adapted to work with ReadBuffer, where src is the position reference (ReadBuffer::position())
 *           and eof is the ReadBuffer::eof() - therefore algorithm below does not rely on buffer's continuity.
 *           To parse strings use overloads below.
 *
 * @param src         - iterator (reference to pointer) over input string - warning - continuity is not guaranteed.
 * @param eof         - function returning true if iterator riched the end - warning - can break iterator's continuity.
 * @param dst         - where to put output bytes, expected to be non-null and at IPV4_BINARY_LENGTH-long.
 * @param first_octet - preparsed first octet
 * @return            - true if parsed successfully, false otherwise.
 */
template <typename T, typename EOFfunction>
    requires(std::is_same<typename std::remove_cv<T>::type, char>::value)
inline bool parseIPv4(T*& src, EOFfunction eof, unsigned char* dst, int32_t first_octet = -1) {
    if (src == nullptr || first_octet > 255) return false;

    uint32_t result = 0;
    int offset = 24;
    if (first_octet >= 0) {
        result |= first_octet << offset;
        offset -= 8;
    }

    for (; true; offset -= 8, ++src) {
        if (eof()) return false;

        uint32_t value = 0;
        size_t len = 0;
        while (is_numeric_ascii(*src) && len <= 3) {
            value = value * 10 + (*src - '0');
            ++len;
            ++src;
            if (eof()) break;
        }
        if (len == 0 || value > 255 || (offset > 0 && (eof() || *src != '.'))) return false;
        result |= value << offset;

        if (offset == 0) break;
    }

    memcpy(dst, &result, sizeof(result));
    return true;
}

/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char* parseIPv4(const char* src, const char* end, unsigned char* dst) {
    if (parseIPv4(
                src, [&src, end]() { return src == end; }, dst))
        return src;
    return nullptr;
}

/// returns true if whole buffer was parsed successfully
inline bool parseIPv4whole(const char* src, const char* end, unsigned char* dst) {
    return parseIPv4(src, end, dst) == end;
}

/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char* parseIPv4(const char* src, unsigned char* dst) {
    if (parseIPv4(
                src, []() { return false; }, dst))
        return src;
    return nullptr;
}

/// returns true if whole null-terminated string was parsed successfully
inline bool parseIPv4whole(const char* src, unsigned char* dst) {
    const char* end = parseIPv4(src, dst);
    return end != nullptr && *end == '\0';
}

} // namespace doris::vectorized
