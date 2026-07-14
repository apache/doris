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

#include "storage/index/snii/encoding/crc32c.h"

// T21 test-seam implementation. Production crc32c()/crc32c_extend() (in the header)
// delegate to the bundled Google crc32c thirdparty, which already runs a
// runtime-dispatched, hardware-accelerated and interleaved CRC32C. The reference
// sub-paths defined here -- portable slice-by-8, serial SSE4.2 hardware, and the
// 3-way interleaved SSE4.2 hardware algorithm T21 specifies -- exist ONLY so that
// unit tests can prove, byte-for-byte across all sizes and alignments, that the
// production path equals the canonical CRC32C (same bit-reflected Castagnoli
// polynomial) and that the hardware path is engaged. All of them are compiled out
// of release builds by the BE_TEST gate, so production pays nothing: no extra code,
// no static slice-by-8 table, no startup CPUID probe. Every function here is a pure
// function with no shared mutable state, so it is trivially thread-safe.
#ifdef BE_TEST

#include <array>
#include <cstddef>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#define SNII_CRC32C_X86 1
#include <cpuid.h>     // __get_cpuid, bit_SSE4_2
#include <nmmintrin.h> // _mm_crc32_u8/u32/u64 (SSE4.2)
#endif

namespace doris::snii {
namespace {

// Bit-reflected Castagnoli polynomial (CRC32C / iSCSI). Identical to the constant
// the removed in-tree implementation used, so every reference path below yields
// the same on-disk checksum value as the production library.
constexpr uint32_t kPoly = 0x82F63B78U;

// Below this length the 3-way path's lane setup and GF(2) shift-combine outweigh
// the throughput win, so crc32c_hw3 falls back to the serial hardware path. Small
// buffers (inline prx windows, small pod_ref regions) therefore never pay the
// combine cost. Exposed to tests via crc32c_interleave_threshold().
constexpr size_t kInterleaveThreshold = 1024;

// Builds the slice-by-8 lookup tables. Column 0 is the classic byte table; each
// successive column folds in one more byte of look-ahead, letting the inner loop
// consume 8 bytes per iteration with 8 table reads + XORs instead of 8 dependent
// shift/lookup steps. The checksum value is identical to the byte-at-a-time loop.
std::array<std::array<uint32_t, 256>, 8> make_slice8_table() {
    std::array<std::array<uint32_t, 256>, 8> t {};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
        }
        t[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = t[0][i];
        for (int s = 1; s < 8; ++s) {
            c = t[0][c & 0xFF] ^ (c >> 8);
            t[s][i] = c;
        }
    }
    return t;
}

const std::array<std::array<uint32_t, 256>, 8> kSlice8 = make_slice8_table();

inline uint32_t load_le32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

// Pure software slice-by-8 (used as the portable path and the hardware fallback).
// Operates on the raw (pre/post-inversion applied by the callers) CRC register.
uint32_t crc32c_slice8(uint32_t crc, const uint8_t* p, size_t n) {
    while (n >= 8) {
        crc ^= load_le32(p);
        const uint32_t hi = load_le32(p + 4);
        crc = kSlice8[7][crc & 0xFF] ^ kSlice8[6][(crc >> 8) & 0xFF] ^
              kSlice8[5][(crc >> 16) & 0xFF] ^ kSlice8[4][crc >> 24] ^ kSlice8[3][hi & 0xFF] ^
              kSlice8[2][(hi >> 8) & 0xFF] ^ kSlice8[1][(hi >> 16) & 0xFF] ^ kSlice8[0][hi >> 24];
        p += 8;
        n -= 8;
    }
    while (n--) {
        crc = kSlice8[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

#if SNII_CRC32C_X86
// Serial hardware CRC32C via the SSE4.2 crc32 instruction. The intrinsics operate
// on the same bit-reflected Castagnoli polynomial as the tables, so the result is
// byte-identical. This TU is compiled without -msse4.2, so gate the intrinsics
// behind a function-level target attribute and a runtime CPUID check. This is the
// authoritative serial hardware path that crc32c_hw3 reuses for each lane and tail.
__attribute__((target("sse4.2"))) uint32_t crc32c_hw_serial(uint32_t crc, const uint8_t* p,
                                                            size_t n) {
    while (n >= 8) {
        uint64_t v;
        std::memcpy(&v, p, sizeof(v)); // unaligned-safe; x86 folds to a plain load
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
        p += 8;
        n -= 8;
    }
    if (n >= 4) {
        crc = _mm_crc32_u32(crc, load_le32(p));
        p += 4;
        n -= 4;
    }
    while (n--) {
        crc = _mm_crc32_u8(crc, *p++);
    }
    return crc;
}

// GF(2) 32x32 bit-matrix helpers (zlib crc32_combine style). A matrix column mat[i]
// is the image of the i-th unit CRC register; gf2_matrix_times sums (XORs) the
// columns selected by the set bits of vec.
uint32_t gf2_matrix_times(const uint32_t* mat, uint32_t vec) {
    uint32_t sum = 0;
    while (vec != 0) {
        if (vec & 1) {
            sum ^= *mat;
        }
        vec >>= 1;
        ++mat;
    }
    return sum;
}

void gf2_matrix_square(uint32_t* square, const uint32_t* mat) {
    for (int n = 0; n < 32; ++n) {
        square[n] = gf2_matrix_times(mat, mat[n]);
    }
}

// crc32c_shift(crc, bytes): advance the raw CRC32C register as if `bytes` zero
// bytes were appended, i.e. crc . x^(8*bytes) mod P in the bit-reflected domain.
// This is the linear operator the 3-way combine applies to a lane's partial CRC so
// it lines up with the following lanes -- a pure table/matrix computation with no
// PCLMULQDQ, hence no extra CPUID gate. bytes == 0 returns crc unchanged.
uint32_t crc32c_shift(uint32_t crc, size_t bytes) {
    uint32_t even[32]; // operator for 2^k zero bits, doubled each round
    uint32_t odd[32];  // operator for 2^(k-1) zero bits

    // odd = operator for a single zero bit: column 0 is the polynomial, columns
    // 1..31 shift the register right by one (bit i maps to bit i-1).
    odd[0] = kPoly;
    uint32_t row = 1;
    for (int n = 1; n < 32; ++n) {
        odd[n] = row;
        row <<= 1;
    }
    gf2_matrix_square(even, odd); // even = two zero bits
    gf2_matrix_square(odd, even); // odd  = four zero bits

    size_t len = bytes;
    do {
        gf2_matrix_square(even, odd); // first pass: even = one zero byte (8 bits)
        if (len & 1) {
            crc = gf2_matrix_times(even, crc);
        }
        len >>= 1;
        if (len == 0) {
            break;
        }
        gf2_matrix_square(odd, even);
        if (len & 1) {
            crc = gf2_matrix_times(odd, crc);
        }
        len >>= 1;
    } while (len != 0);
    return crc;
}

// 3-way interleaved hardware CRC32C (rocksdb/folly/Intel style). Splits the buffer
// into three equal, 8-byte-aligned lanes processed by independent _mm_crc32_u64
// accumulators (breaking the ~3-cycle loop-carried dependency of the serial path),
// then stitches them with the GF(2) shift-combine and finishes the remainder
// serially. Byte-identical to crc32c_hw_serial / crc32c_slice8. Below the
// threshold it defers to the serial path so small buffers skip the combine cost.
uint32_t crc32c_hw3(uint32_t crc, const uint8_t* p, size_t n) {
    if (n < kInterleaveThreshold) {
        return crc32c_hw_serial(crc, p, n);
    }
    // 8-byte-aligned lane length keeps every lane on the u64 fast path. n >= 1024
    // guarantees L >= 336 > 0, so 3L <= n and the tail (n - 3L) is well defined.
    const size_t lane = (n / 3) & ~static_cast<size_t>(7);
    const uint32_t crc_a = crc32c_hw_serial(crc, p, lane);          // seeded lane
    const uint32_t crc_b = crc32c_hw_serial(0, p + lane, lane);     // raw lane
    const uint32_t crc_c = crc32c_hw_serial(0, p + 2 * lane, lane); // raw lane
    // crc(seed, A||B) == shift(crc(seed, A), |B|) ^ crc(0, B), applied twice.
    uint32_t comb = crc32c_shift(crc_a, lane) ^ crc_b;
    comb = crc32c_shift(comb, lane) ^ crc_c;
    return crc32c_hw_serial(comb, p + 3 * lane, n - 3 * lane); // serial tail
}

bool detect_sse42() {
    unsigned int eax = 0, ebx = 0, ecx = 0, edx = 0;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    return (ecx & bit_SSE4_2) != 0;
}

const bool kHasSse42 = detect_sse42();
#endif // SNII_CRC32C_X86

} // namespace

namespace detail {

// Portable software path. Always available; the canonical scalar reference for
// every other path and for the on-disk checksum value.
uint32_t crc32c_slice8_extend(uint32_t crc, Slice data) {
    crc = ~crc;
    crc = crc32c_slice8(crc, data.data(), data.size());
    return ~crc;
}

// Serial hardware path (falls back to slice8 without SSE4.2).
uint32_t crc32c_hw_serial_extend(uint32_t crc, Slice data) {
    crc = ~crc;
#if SNII_CRC32C_X86
    if (kHasSse42) {
        crc = crc32c_hw_serial(crc, data.data(), data.size());
        return ~crc;
    }
#endif
    crc = crc32c_slice8(crc, data.data(), data.size());
    return ~crc;
}

// 3-way interleaved hardware path (falls back to slice8 without SSE4.2; internally
// falls back to the serial path below the interleave threshold).
uint32_t crc32c_hw3_extend(uint32_t crc, Slice data) {
    crc = ~crc;
#if SNII_CRC32C_X86
    if (kHasSse42) {
        crc = crc32c_hw3(crc, data.data(), data.size());
        return ~crc;
    }
#endif
    crc = crc32c_slice8(crc, data.data(), data.size());
    return ~crc;
}

size_t crc32c_interleave_threshold() {
    return kInterleaveThreshold;
}

bool crc32c_has_hw() {
#if SNII_CRC32C_X86
    return kHasSse42;
#else
    return false;
#endif
}

} // namespace detail
} // namespace doris::snii

#endif // BE_TEST
