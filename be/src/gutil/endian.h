// Copyright 2005 Google Inc.
//
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
//
// ---
//
//
// Utility functions that depend on bytesex. We define htonll and ntohll,
// as well as "Google" versions of all the standards: ghtonl, ghtons, and
// so on. These functions do exactly the same as their standard variants,
// but don't require including the dangerous netinet/in.h.
//
// Buffer routines will copy to and from buffers without causing
// a bus error when the architecture requires different byte alignments

#pragma once

#include <assert.h>

#include "olap/uint24.h"
#include "vec/core/wide_integer.h"

// Portable handling of unaligned loads, stores, and copies.
// On some platforms, like ARM, the copy functions can be more efficient
// then a load and a store.

#if defined(__i386) || defined(ARCH_ATHLON) || defined(__x86_64__) || defined(_ARCH_PPC)

// x86 and x86-64 can perform unaligned loads/stores directly;
// modern PowerPC hardware can also do unaligned integer loads and stores;
// but note: the FPU still sends unaligned loads and stores to a trap handler!

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16_t*>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32_t*>(_p))
#define UNALIGNED_LOAD64(_p) (*reinterpret_cast<const uint64_t*>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16_t*>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32_t*>(_p) = (_val))
#define UNALIGNED_STORE64(_p, _val) (*reinterpret_cast<uint64_t*>(_p) = (_val))

#elif defined(__arm__) && !defined(__ARM_ARCH_5__) && !defined(__ARM_ARCH_5T__) &&               \
        !defined(__ARM_ARCH_5TE__) && !defined(__ARM_ARCH_5TEJ__) && !defined(__ARM_ARCH_6__) && \
        !defined(__ARM_ARCH_6J__) && !defined(__ARM_ARCH_6K__) && !defined(__ARM_ARCH_6Z__) &&   \
        !defined(__ARM_ARCH_6ZK__) && !defined(__ARM_ARCH_6T2__)

// ARMv7 and newer support native unaligned accesses, but only of 16-bit
// and 32-bit values (not 64-bit); older versions either raise a fatal signal,
// do an unaligned read and rotate the words around a bit, or do the reads very
// slowly (trip through kernel mode). There's no simple #define that says just
// “ARMv7 or higher”, so we have to filter away all ARMv5 and ARMv6
// sub-architectures. Newer gcc (>= 4.6) set an __ARM_FEATURE_ALIGNED #define,
// so in time, maybe we can move on to that.
//
// This is a mess, but there's not much we can do about it.

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16_t*>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32_t*>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16_t*>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32_t*>(_p) = (_val))

// TODO(user): NEON supports unaligned 64-bit loads and stores.
// See if that would be more efficient on platforms supporting it,
// at least for copies.

inline uint64_t UNALIGNED_LOAD64(const void* p) {
    uint64_t t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline void UNALIGNED_STORE64(void* p, uint64_t v) {
    memcpy(p, &v, sizeof v);
}

#else

#define NEED_ALIGNED_LOADS

// These functions are provided for architectures that don't support
// unaligned loads and stores.

inline uint16_t UNALIGNED_LOAD16(const void* p) {
    uint16_t t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline uint32_t UNALIGNED_LOAD32(const void* p) {
    uint32_t t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline uint64_t UNALIGNED_LOAD64(const void* p) {
    uint64_t t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline void UNALIGNED_STORE16(void* p, uint16_t v) {
    memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE32(void* p, uint32_t v) {
    memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE64(void* p, uint64_t v) {
    memcpy(p, &v, sizeof v);
}

#endif

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
    return ((x & 0x0000ffULL) << 16) | ((x & 0x00ff00ULL)) | ((x & 0xff0000ULL) >> 16);
}

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
        return to_endian<std::endian::little>(UNALIGNED_LOAD16(p));
    }

    static void Store16(void* p, uint16_t v) {
        UNALIGNED_STORE16(p, to_endian<std::endian::little>(v));
    }

    static uint32_t Load32(const void* p) {
        return to_endian<std::endian::little>(UNALIGNED_LOAD32(p));
    }

    static void Store32(void* p, uint32_t v) {
        UNALIGNED_STORE32(p, to_endian<std::endian::little>(v));
    }

    static uint64_t Load64(const void* p) {
        return to_endian<std::endian::little>(UNALIGNED_LOAD64(p));
    }

    static void Store64(void* p, uint64_t v) {
        UNALIGNED_STORE64(p, to_endian<std::endian::little>(v));
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
        return to_endian<std::endian::big>(UNALIGNED_LOAD16(p));
    }

    static void Store16(void* p, uint16_t v) {
        UNALIGNED_STORE16(p, to_endian<std::endian::big>(v));
    }

    static uint32_t Load32(const void* p) {
        return to_endian<std::endian::big>(UNALIGNED_LOAD32(p));
    }

    static void Store32(void* p, uint32_t v) {
        UNALIGNED_STORE32(p, to_endian<std::endian::big>(v));
    }

    static uint64_t Load64(const void* p) {
        return to_endian<std::endian::big>(UNALIGNED_LOAD64(p));
    }

    static void Store64(void* p, uint64_t v) {
        UNALIGNED_STORE64(p, to_endian<std::endian::big>(v));
    }
}; // BigEndian

// Network byte order is big-endian
typedef BigEndian NetworkByteOrder;
