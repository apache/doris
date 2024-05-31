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

#pragma once
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "common/compiler_util.h"
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

ALWAYS_INLINE inline void memcpy_inlined(void* __restrict _dst, const void* __restrict _src,
                                         size_t size) {
    auto dst = static_cast<uint8_t*>(_dst);
    auto src = static_cast<const uint8_t*>(_src);

[[maybe_unused]]tail:
    /// Small sizes and tails after the loop for large sizes.
    /// The order of branches is important but in fact the optimal order depends on the distribution of sizes in your application.
    /// This order of branches is from the disassembly of glibc's code.
    /// We copy chunks of possibly uneven size with two overlapping movs.
    /// Example: to copy 5 bytes [0, 1, 2, 3, 4] we will copy tail [1, 2, 3, 4] first and then head [0, 1, 2, 3].
    if (size <= 16) {
        if (size >= 8) {
            /// Chunks of 8..16 bytes.
            __builtin_memcpy(dst + size - 8, src + size - 8, 8);
            __builtin_memcpy(dst, src, 8);
        } else if (size >= 4) {
            /// Chunks of 4..7 bytes.
            __builtin_memcpy(dst + size - 4, src + size - 4, 4);
            __builtin_memcpy(dst, src, 4);
        } else if (size >= 2) {
            /// Chunks of 2..3 bytes.
            __builtin_memcpy(dst + size - 2, src + size - 2, 2);
            __builtin_memcpy(dst, src, 2);
        } else if (size >= 1) {
            /// A single byte.
            *dst = *src;
        }
        /// No bytes remaining.
    } else {
#ifdef __AVX2__
        if (size <= 256) {
            if (size <= 32) {
                __builtin_memcpy(dst, src, 8);
                __builtin_memcpy(dst + 8, src + 8, 8);
                size -= 16;
                dst += 16;
                src += 16;
                goto tail;
            }

            /// Then we will copy every 16 bytes from the beginning in a loop.
            /// The last loop iteration will possibly overwrite some part of already copied last 32 bytes.
            /// This is Ok, similar to the code for small sizes above.
            while (size > 32) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst),
                                    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)));
                dst += 32;
                src += 32;
                size -= 32;
            }

            _mm256_storeu_si256(
                    reinterpret_cast<__m256i*>(dst + size - 32),
                    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + size - 32)));
        } else {
            if (size >= 512 * 1024 && size <= 2048 * 1024) {
                asm volatile("rep movsb"
                             : "=D"(dst), "=S"(src), "=c"(size)
                             : "0"(dst), "1"(src), "2"(size)
                             : "memory");
            } else {
                size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

                if (padding > 0) {
                    __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), head);
                    dst += padding;
                    src += padding;
                    size -= padding;
                }

                /// Aligned unrolled copy. We will use half of available AVX registers.
                /// It's not possible to have both src and dst aligned.
                /// So, we will use aligned stores and unaligned loads.
                __m256i c0, c1, c2, c3, c4, c5, c6, c7;

                while (size >= 256) {
                    c0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
                    c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32));
                    c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 64));
                    c3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 96));
                    c4 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 128));
                    c5 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 160));
                    c6 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 192));
                    c7 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 224));
                    src += 256;

                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst)), c0);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 32)), c1);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 64)), c2);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 96)), c3);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 128)), c4);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 160)), c5);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 192)), c6);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 224)), c7);
                    dst += 256;

                    size -= 256;
                }

                goto tail;
            }
        }
#else
        memcpy(dst, src, size);
#endif
    }
}

} // namespace strings
