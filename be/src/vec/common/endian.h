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

#pragma once

#include "olap/uint24.h"
#include "vec/common/unaligned.h"
#include "vec/core/extended_types.h"

namespace doris {
#include "common/compile_check_begin.h"
inline uint64_t gbswap_64(uint64_t host_int) {
#if defined(__GNUC__) && defined(__x86_64__) && !defined(__APPLE__)
    // Adapted from /usr/include/byteswap.h.  Not available on Mac.
    if (__builtin_constant_p(host_int)) {
        return __bswap_constant_64(host_int);
    } else {
        uint64_t result;
        __asm__("bswap %0" : "=r"(result) : "0"(host_int));
        return result;
    }
#elif defined(bswap_64)
    return bswap_64(host_int);
#else
    return static_cast<uint64_t>(bswap_32(static_cast<uint32_t>(host_int >> 32))) |
           (static_cast<uint64_t>(bswap_32(static_cast<uint32_t>(host_int))) << 32);
#endif // bswap_64
}

inline unsigned __int128 gbswap_128(unsigned __int128 host_int) {
    return static_cast<unsigned __int128>(bswap_64(static_cast<uint64_t>(host_int >> 64))) |
           (static_cast<unsigned __int128>(bswap_64(static_cast<uint64_t>(host_int))) << 64);
}

inline wide::UInt256 gbswap_256(wide::UInt256 host_int) {
    wide::UInt256 result {gbswap_64(host_int.items[3]), gbswap_64(host_int.items[2]),
                          gbswap_64(host_int.items[1]), gbswap_64(host_int.items[0])};
    return result;
}

// Swap bytes of a 24-bit value.
inline uint32_t bswap_24(uint32_t x) {
    return uint32_t((x & 0x0000ffULL) << 16) | ((x & 0x00ff00ULL)) | ((x & 0xff0000ULL) >> 16);
}

// use std::byteswap after doris enable cpp23
template <typename T>
T byte_swap(T x) {
    if constexpr (sizeof(T) == sizeof(wide::Int256)) {
        return gbswap_256(x);
    } else if constexpr (sizeof(T) == sizeof(__int128)) {
        return gbswap_128(x);
    } else if constexpr (sizeof(T) == sizeof(int64_t)) {
        return bswap_64(x);
    } else if constexpr (sizeof(T) == sizeof(int32_t)) {
        return bswap_32(x);
    } else if constexpr (sizeof(T) == sizeof(doris::uint24_t)) {
        return bswap_24(x);
    } else if constexpr (sizeof(T) == sizeof(int16_t)) {
        return bswap_16(x);
    } else {
        static_assert(sizeof(T) == 1, "Unsupported type size for byte_swap");
        return x; // No byte swap needed for unsupported types
    }
}

template <std::endian target, typename T>
T to_endian(T value) {
    if constexpr (std::endian::native == target) {
        return value; // No swap needed
    } else {
        static_assert(std::endian::native == std::endian::big ||
                              std::endian::native == std::endian::little,
                      "Unsupported endianness");
        return byte_swap(value);
    }
}

// Utilities to convert numbers between the current hosts's native byte
// order and little-endian byte order
//
// Load/Store methods are alignment safe
class LittleEndian {
public:
    // Functions to do unaligned loads and stores in little-endian order.
    static uint16_t Load16(const void* p) {
        return to_endian<std::endian::little>(unaligned_load<uint16_t>(p));
    }

    static void Store16(void* p, uint16_t v) {
        unaligned_store<uint16_t>(p, to_endian<std::endian::little>(v));
    }

    static uint32_t Load32(const void* p) {
        return to_endian<std::endian::little>(unaligned_load<uint32_t>(p));
    }

    static void Store32(void* p, uint32_t v) {
        unaligned_store<uint32_t>(p, to_endian<std::endian::little>(v));
    }

    static uint64_t Load64(const void* p) {
        return to_endian<std::endian::little>(unaligned_load<uint64_t>(p));
    }

    static void Store64(void* p, uint64_t v) {
        unaligned_store<uint64_t>(p, to_endian<std::endian::little>(v));
    }
};

// Utilities to convert numbers between the current hosts's native byte
// order and big-endian byte order (same as network byte order)
//
// Load/Store methods are alignment safe
class BigEndian {
public:
    // Functions to do unaligned loads and stores in little-endian order.
    static uint16_t Load16(const void* p) {
        return to_endian<std::endian::big>(unaligned_load<uint16_t>(p));
    }

    static void Store16(void* p, uint16_t v) {
        unaligned_store<uint16_t>(p, to_endian<std::endian::big>(v));
    }

    static uint32_t Load32(const void* p) {
        return to_endian<std::endian::big>(unaligned_load<uint32_t>(p));
    }

    static void Store32(void* p, uint32_t v) {
        unaligned_store<uint32_t>(p, to_endian<std::endian::big>(v));
    }

    static uint64_t Load64(const void* p) {
        return to_endian<std::endian::big>(unaligned_load<uint64_t>(p));
    }

    static void Store64(void* p, uint64_t v) {
        unaligned_store<uint64_t>(p, to_endian<std::endian::big>(v));
    }
}; // BigEndian
#include "common/compile_check_end.h"
} // namespace doris