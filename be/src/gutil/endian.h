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

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

// Definitions for ntohl etc. that don't require us to include
// netinet/in.h. We wrap bswap_32 and bswap_16 in functions rather
// than just #defining them because in debug mode, gcc doesn't
// correctly handle the (rather involved) definitions of bswap_32.
// gcc guarantees that inline functions are as fast as macros, so
// this isn't a performance hit.
inline uint16_t ghtons(uint16_t x) {
    return bswap_16(x);
}
inline uint32_t ghtonl(uint32_t x) {
    return bswap_32(x);
}
inline uint64_t ghtonll(uint64_t x) {
    return gbswap_64(x);
}

#else

// These definitions are simpler on big-endian machines
// These are functions instead of macros to avoid self-assignment warnings
// on calls such as "i = ghtnol(i);".  This also provides type checking.
inline uint16_t ghtons(uint16_t x) {
    return x;
}
inline uint32_t ghtonl(uint32_t x) {
    return x;
}
inline uint64_t ghtonll(uint64_t x) {
    return x;
}

#endif // bytesex

// ntoh* and hton* are the same thing for any size and bytesex,
// since the function is an involution, i.e., its own inverse.
#if !defined(__APPLE__)
// This one is safe to take as it's an extension
#define htonll(x) ghtonll(x)
#define ntohll(x) htonll(x)
#endif

// Utilities to convert numbers between the current hosts's native byte
// order and little-endian byte order
//
// Load/Store methods are alignment safe
class LittleEndian {
public:
    // Conversion functions.
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

    static uint16_t FromHost16(uint16_t x) { return x; }
    static uint16_t ToHost16(uint16_t x) { return x; }

    static uint32_t FromHost32(uint32_t x) { return x; }
    static uint32_t ToHost32(uint32_t x) { return x; }

    static uint64_t FromHost64(uint64_t x) { return x; }
    static uint64_t ToHost64(uint64_t x) { return x; }

    static unsigned __int128 FromHost128(unsigned __int128 x) { return x; }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return x; }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return x; }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return x; }

    static bool IsLittleEndian() { return true; }

#else

    static uint16_t FromHost16(uint16_t x) { return bswap_16(x); }
    static uint16_t ToHost16(uint16_t x) { return bswap_16(x); }

    static uint32_t FromHost32(uint32_t x) { return bswap_32(x); }
    static uint32_t ToHost32(uint32_t x) { return bswap_32(x); }

    static uint64_t FromHost64(uint64_t x) { return gbswap_64(x); }
    static uint64_t ToHost64(uint64_t x) { return gbswap_64(x); }

    static unsigned __int128 FromHost128(unsigned __int128 x) { return gbswap_128(x); }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return gbswap_128(x); }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return gbswap_256(x); }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return gbswap_256(x); }

    static bool IsLittleEndian() { return false; }

#endif /* ENDIAN */

    // Functions to do unaligned loads and stores in little-endian order.
    static uint16_t Load16(const void* p) { return ToHost16(UNALIGNED_LOAD16(p)); }

    static void Store16(void* p, uint16_t v) { UNALIGNED_STORE16(p, FromHost16(v)); }

    static uint32_t Load32(const void* p) { return ToHost32(UNALIGNED_LOAD32(p)); }

    static void Store32(void* p, uint32_t v) { UNALIGNED_STORE32(p, FromHost32(v)); }

    static uint64_t Load64(const void* p) { return ToHost64(UNALIGNED_LOAD64(p)); }

    // Build a uint64_t from 1-8 bytes.
    // 8 * len least significant bits are loaded from the memory with
    // LittleEndian order. The 64 - 8 * len most significant bits are
    // set all to 0.
    // In latex-friendly words, this function returns:
    //     $\sum_{i=0}^{len-1} p[i] 256^{i}$, where p[i] is unsigned.
    //
    // This function is equivalent with:
    // uint64_t val = 0;
    // memcpy(&val, p, len);
    // return ToHost64(val);
    // TODO(user): write a small benchmark and benchmark the speed
    // of a memcpy based approach.
    //
    // For speed reasons this function does not work for len == 0.
    // The caller needs to guarantee that 1 <= len <= 8.
    static uint64_t Load64VariableLength(const void* const p, int len) {
        assert(len >= 1 && len <= 8);
        const char* const buf = static_cast<const char*>(p);
        uint64_t val = 0;
        --len;
        do {
            val = (val << 8) | buf[len];
            // (--len >= 0) is about 10 % faster than (len--) in some benchmarks.
        } while (--len >= 0);
        // No ToHost64(...) needed. The bytes are accessed in little-endian manner
        // on every architecture.
        return val;
    }

    static void Store64(void* p, uint64_t v) { UNALIGNED_STORE64(p, FromHost64(v)); }
};

// Utilities to convert numbers between the current hosts's native byte
// order and big-endian byte order (same as network byte order)
//
// Load/Store methods are alignment safe
class BigEndian {
public:
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

    static uint16_t FromHost16(uint16_t x) { return bswap_16(x); }
    static uint16_t ToHost16(uint16_t x) { return bswap_16(x); }

    static uint32_t FromHost24(uint32_t x) { return bswap_24(x); }
    static uint32_t ToHost24(uint32_t x) { return bswap_24(x); }

    static uint32_t FromHost32(uint32_t x) { return bswap_32(x); }
    static uint32_t ToHost32(uint32_t x) { return bswap_32(x); }

    static uint64_t FromHost64(uint64_t x) { return gbswap_64(x); }
    static uint64_t ToHost64(uint64_t x) { return gbswap_64(x); }

    static unsigned __int128 FromHost128(unsigned __int128 x) { return gbswap_128(x); }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return gbswap_128(x); }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return gbswap_256(x); }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return gbswap_256(x); }

    static bool IsLittleEndian() { return true; }

#else

    static uint16_t FromHost16(uint16_t x) { return x; }
    static uint16_t ToHost16(uint16_t x) { return x; }

    static uint32_t FromHost24(uint32_t x) { return x; }
    static uint32_t ToHost24(uint32_t x) { return x; }

    static uint32_t FromHost32(uint32_t x) { return x; }
    static uint32_t ToHost32(uint32_t x) { return x; }

    static uint64_t FromHost64(uint64_t x) { return x; }
    static uint64_t ToHost64(uint64_t x) { return x; }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return x; }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return x; }

    static bool IsLittleEndian() { return false; }

#endif /* ENDIAN */
    // Functions to do unaligned loads and stores in little-endian order.
    static uint16_t Load16(const void* p) { return ToHost16(UNALIGNED_LOAD16(p)); }

    static void Store16(void* p, uint16_t v) { UNALIGNED_STORE16(p, FromHost16(v)); }

    static uint32_t Load32(const void* p) { return ToHost32(UNALIGNED_LOAD32(p)); }

    static void Store32(void* p, uint32_t v) { UNALIGNED_STORE32(p, FromHost32(v)); }

    static uint64_t Load64(const void* p) { return ToHost64(UNALIGNED_LOAD64(p)); }

    // Build a uint64_t from 1-8 bytes.
    // 8 * len least significant bits are loaded from the memory with
    // BigEndian order. The 64 - 8 * len most significant bits are
    // set all to 0.
    // In latex-friendly words, this function returns:
    //     $\sum_{i=0}^{len-1} p[i] 256^{i}$, where p[i] is unsigned.
    //
    // This function is equivalent with:
    // uint64_t val = 0;
    // memcpy(&val, p, len);
    // return ToHost64(val);
    // TODO(user): write a small benchmark and benchmark the speed
    // of a memcpy based approach.
    //
    // For speed reasons this function does not work for len == 0.
    // The caller needs to guarantee that 1 <= len <= 8.
    static uint64_t Load64VariableLength(const void* const p, int len) {
        assert(len >= 1 && len <= 8);
        uint64_t val = Load64(p);
        uint64_t mask = 0;
        --len;
        do {
            mask = (mask << 8) | 0xff;
            // (--len >= 0) is about 10 % faster than (len--) in some benchmarks.
        } while (--len >= 0);
        return val & mask;
    }

    static void Store64(void* p, uint64_t v) { UNALIGNED_STORE64(p, FromHost64(v)); }
}; // BigEndian

// Network byte order is big-endian
typedef BigEndian NetworkByteOrder;
