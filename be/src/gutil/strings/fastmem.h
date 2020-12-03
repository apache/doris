// Copyright 2008 Google Inc. All Rights Reserved.
//
// Fast memory copying and comparison routines.
//   strings::fastmemcmp_inlined() replaces memcmp()
//   strings::memcpy_inlined() replaces memcpy()
//   strings::memeq(a, b, n) replaces memcmp(a, b, n) == 0
//
// strings::*_inlined() routines are inline versions of the
// routines exported by this module.  Sometimes using the inlined
// versions is faster.  Measure before using the inlined versions.
//
// Performance measurement:
//   strings::fastmemcmp_inlined
//     Analysis: memcmp, fastmemcmp_inlined, fastmemcmp
//     2012-01-30

#ifndef STRINGS_FASTMEM_H_
#define STRINGS_FASTMEM_H_

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "gutil/integral_types.h"
#include "gutil/port.h"

namespace strings {

// Return true if the n bytes at a equal the n bytes at b.
// The regions are allowed to overlap.
//
// The performance is similar to the performance memcmp(), but faster for
// moderately-sized inputs, or inputs that share a common prefix and differ
// somewhere in their last 8 bytes. Further optimizations can be added later
// if it makes sense to do so.
inline bool memeq(const void* a_v, const void* b_v, size_t n) {
    const uint8_t* a = reinterpret_cast<const uint8_t*>(a_v);
    const uint8_t* b = reinterpret_cast<const uint8_t*>(b_v);

    size_t n_rounded_down = n & ~static_cast<size_t>(7);
    if (PREDICT_FALSE(n_rounded_down == 0)) { // n <= 7
        return memcmp(a, b, n) == 0;
    }
    // n >= 8
    uint64 u = UNALIGNED_LOAD64(a) ^ UNALIGNED_LOAD64(b);
    uint64 v = UNALIGNED_LOAD64(a + n - 8) ^ UNALIGNED_LOAD64(b + n - 8);
    if ((u | v) != 0) { // The first or last 8 bytes differ.
        return false;
    }
    a += 8;
    b += 8;
    n = n_rounded_down - 8;
    if (n > 128) {
        // As of 2012, memcmp on x86-64 uses a big unrolled loop with SSE2
        // instructions, and while we could try to do something faster, it
        // doesn't seem worth pursuing.
        return memcmp(a, b, n) == 0;
    }
    for (; n >= 16; n -= 16) {
        uint64 x = UNALIGNED_LOAD64(a) ^ UNALIGNED_LOAD64(b);
        uint64 y = UNALIGNED_LOAD64(a + 8) ^ UNALIGNED_LOAD64(b + 8);
        if ((x | y) != 0) {
            return false;
        }
        a += 16;
        b += 16;
    }
    // n must be 0 or 8 now because it was a multiple of 8 at the top of the loop.
    return n == 0 || UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b);
}

inline int fastmemcmp_inlined(const void* a_void, const void* b_void, size_t n) {
    const uint8_t* a = reinterpret_cast<const uint8_t*>(a_void);
    const uint8_t* b = reinterpret_cast<const uint8_t*>(b_void);

    if (n >= 64) {
        return memcmp(a, b, n);
    }
    const void* a_limit = a + n;
    const size_t sizeof_uint64 = sizeof(uint64); // NOLINT(runtime/sizeof)
    while (a + sizeof_uint64 <= a_limit && UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b)) {
        a += sizeof_uint64;
        b += sizeof_uint64;
    }
    const size_t sizeof_uint32 = sizeof(uint32); // NOLINT(runtime/sizeof)
    if (a + sizeof_uint32 <= a_limit && UNALIGNED_LOAD32(a) == UNALIGNED_LOAD32(b)) {
        a += sizeof_uint32;
        b += sizeof_uint32;
    }
    while (a < a_limit) {
        int d = static_cast<uint32>(*a++) - static_cast<uint32>(*b++);
        if (d) return d;
    }
    return 0;
}

// The standard memcpy operation is slow for variable small sizes.
// This implementation inlines the optimal realization for sizes 1 to 16.
// To avoid code bloat don't use it in case of not performance-critical spots,
// nor when you don't expect very frequent values of size <= 16.
inline void memcpy_inlined(void* dst, const void* src, size_t size) {
    // Compiler inlines code with minimal amount of data movement when third
    // parameter of memcpy is a constant.
    switch (size) {
    case 1:
        memcpy(dst, src, 1);
        break;
    case 2:
        memcpy(dst, src, 2);
        break;
    case 3:
        memcpy(dst, src, 3);
        break;
    case 4:
        memcpy(dst, src, 4);
        break;
    case 5:
        memcpy(dst, src, 5);
        break;
    case 6:
        memcpy(dst, src, 6);
        break;
    case 7:
        memcpy(dst, src, 7);
        break;
    case 8:
        memcpy(dst, src, 8);
        break;
    case 9:
        memcpy(dst, src, 9);
        break;
    case 10:
        memcpy(dst, src, 10);
        break;
    case 11:
        memcpy(dst, src, 11);
        break;
    case 12:
        memcpy(dst, src, 12);
        break;
    case 13:
        memcpy(dst, src, 13);
        break;
    case 14:
        memcpy(dst, src, 14);
        break;
    case 15:
        memcpy(dst, src, 15);
        break;
    case 16:
        memcpy(dst, src, 16);
        break;
    default:
        memcpy(dst, src, size);
        break;
    }
}

} // namespace strings

#endif // STRINGS_FASTMEM_H_
