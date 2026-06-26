#include "snii/encoding/crc32c.h"

#include <array>
#include <cstddef>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#define SNII_CRC32C_X86 1
#include <cpuid.h>
#include <nmmintrin.h> // _mm_crc32_u8/u32/u64 (SSE4.2)
#endif

namespace snii {
namespace {

// Bit-reflected Castagnoli polynomial (CRC32C / iSCSI).
constexpr uint32_t kPoly = 0x82F63B78u;

// Builds the slice-by-8 lookup tables. Column 0 is the classic byte table; each
// successive column folds in one more byte of look-ahead, letting the inner loop
// consume 8 bytes per iteration with 8 table reads + XORs instead of 8 dependent
// shift/lookup steps. The checksum value is identical to the byte-at-a-time loop.
std::array<std::array<uint32_t, 256>, 8> make_slice8_table() {
    std::array<std::array<uint32_t, 256>, 8> t {};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
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
// Hardware CRC32C via the SSE4.2 crc32 instruction. The intrinsics operate on the
// same bit-reflected Castagnoli polynomial as the tables, so the result is
// byte-identical. This TU is compiled without -msse4.2, so gate the intrinsics
// behind a function-level target attribute and a runtime CPUID check.
__attribute__((target("sse4.2"))) uint32_t crc32c_hw(uint32_t crc, const uint8_t* p, size_t n) {
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
    while (n--) crc = _mm_crc32_u8(crc, *p++);
    return crc;
}

bool detect_sse42() {
    unsigned eax = 0, ebx = 0, ecx = 0, edx = 0;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) return false;
    return (ecx & bit_SSE4_2) != 0;
}

const bool kHasSse42 = detect_sse42();
#endif

} // namespace

uint32_t crc32c_extend(uint32_t crc, Slice data) {
    const uint8_t* p = data.data();
    const size_t n = data.size();
    crc = ~crc;
#if SNII_CRC32C_X86
    if (kHasSse42) {
        crc = crc32c_hw(crc, p, n);
        return ~crc;
    }
#endif
    crc = crc32c_slice8(crc, p, n);
    return ~crc;
}

} // namespace snii
