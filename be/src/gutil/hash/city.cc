// Copyright 2010 Google Inc. All Rights Reserved.
// Authors: gpike@google.com (Geoff Pike), jyrki@google.com (Jyrki Alakuijala)
//
// This file provides CityHash64() and related functions.
//
// The externally visible functions follow the naming conventions of
// hash.h, where the size of the output is part of the name.  For
// example, CityHash64 returns a 64-bit hash.  The internal helpers do
// not have the return type in their name, but instead have names like
// HashLenXX or HashLenXXtoYY, where XX and YY are input string lengths.
//
// Most of the constants and tricks here were copied from murmur.cc or
// hash.h, or discovered by trial and error.  It's probably possible to further
// optimize the code here by writing a program that systematically explores
// more of the space of possible hash functions, or by using SIMD instructions.

#include "gutil/hash/city.h"

// IWYU pragma: no_include <pstl/glue_algorithm_defs.h>

#include <sys/types.h>

#include <algorithm>
#include <iterator>

using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <utility>

using std::make_pair;
using std::pair;

#include "common/logging.h"
#include "gutil/endian.h"
#include "gutil/integral_types.h"
#include "gutil/port.h"

namespace util_hash {

// Some primes between 2^63 and 2^64 for various uses.
static const uint64 k0 = 0xa5b85c5e198ed849ULL;
static const uint64 k1 = 0x8d58ac26afe12e47ULL;
static const uint64 k2 = 0xc47b6e9e3a970ed3ULL;
static const uint64 k3 = 0xc70f6907e782aa0bULL;

// Bitwise right rotate.  Normally this will compile to a single
// instruction, especially if the shift is a manifest constant.
static uint64 Rotate(uint64 val, int shift) {
    DCHECK_GE(shift, 0);
    DCHECK_LE(shift, 63);
    // Avoid shifting by 64: doing so yields an undefined result.
    return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

// Equivalent to Rotate(), but requires the second arg to be non-zero.
// On x86-64, and probably others, it's possible for this to compile
// to a single instruction if both args are already in registers.
static uint64 RotateByAtLeast1(uint64 val, int shift) {
    DCHECK_GE(shift, 1);
    DCHECK_LE(shift, 63);
    return (val >> shift) | (val << (64 - shift));
}

static uint64 ShiftMix(uint64 val) {
    return val ^ (val >> 47);
}

uint64 HashLen16(uint64 u, uint64 v) {
    const uint64 kMul = 0xc6a4a7935bd1e995ULL;
    uint64 a = (u ^ v) * kMul;
    a ^= (a >> 47);
    uint64 b = (v ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

static uint64 HashLen0to16(const char* s, size_t len) {
    DCHECK_GE(len, 0);
    DCHECK_LE(len, 16);
    if (len > 8) {
        uint64 a = LittleEndian::Load64(s);
        uint64 b = LittleEndian::Load64(s + len - 8);
        return HashLen16(a, RotateByAtLeast1(b + len, len)) ^ b;
    }
    if (len >= 4) {
        uint64 a = LittleEndian::Load32(s);
        return HashLen16(len + (a << 3), LittleEndian::Load32(s + len - 4));
    }
    if (len > 0) {
        uint8 a = s[0];
        uint8 b = s[len >> 1];
        uint8 c = s[len - 1];
        uint32 y = static_cast<uint32>(a) + (static_cast<uint32>(b) << 8);
        uint32 z = len + (static_cast<uint32>(c) << 2);
        return ShiftMix(y * k2 ^ z * k3) * k2;
    }
    return k2;
}

// This probably works well for 16-byte strings as well, but it may be overkill
// in that case.
static uint64 HashLen17to32(const char* s, size_t len) {
    DCHECK_GE(len, 17);
    DCHECK_LE(len, 32);
    uint64 a = LittleEndian::Load64(s) * k1;
    uint64 b = LittleEndian::Load64(s + 8);
    uint64 c = LittleEndian::Load64(s + len - 8) * k2;
    uint64 d = LittleEndian::Load64(s + len - 16) * k0;
    return HashLen16(Rotate(a - b, 43) + Rotate(c, 30) + d, a + Rotate(b ^ k3, 20) - c + len);
}

// Return a 16-byte hash for 48 bytes.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
// (For more, see the code review discussion of CL 18799087.)
static pair<uint64, uint64> WeakHashLen32WithSeeds(uint64 w, uint64 x, uint64 y, uint64 z, uint64 a,
                                                   uint64 b) {
    a += w;
    b = Rotate(b + a + z, 51);
    uint64 c = a;
    a += x;
    a += y;
    b += Rotate(a, 23);
    return make_pair(a + z, b + c);
}

// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
static pair<uint64, uint64> WeakHashLen32WithSeeds(const char* s, uint64 a, uint64 b) {
    return WeakHashLen32WithSeeds(LittleEndian::Load64(s), LittleEndian::Load64(s + 8),
                                  LittleEndian::Load64(s + 16), LittleEndian::Load64(s + 24), a, b);
}

// Return an 8-byte hash for 33 to 64 bytes.
static uint64 HashLen33to64(const char* s, size_t len) {
    uint64 z = LittleEndian::Load64(s + 24);
    uint64 a = LittleEndian::Load64(s) + (len + LittleEndian::Load64(s + len - 16)) * k0;
    uint64 b = Rotate(a + z, 52);
    uint64 c = Rotate(a, 37);
    a += LittleEndian::Load64(s + 8);
    c += Rotate(a, 7);
    a += LittleEndian::Load64(s + 16);
    uint64 vf = a + z;
    uint64 vs = b + Rotate(a, 31) + c;
    a = LittleEndian::Load64(s + 16) + LittleEndian::Load64(s + len - 32);
    z += LittleEndian::Load64(s + len - 8);
    b = Rotate(a + z, 52);
    c = Rotate(a, 37);
    a += LittleEndian::Load64(s + len - 24);
    c += Rotate(a, 7);
    a += LittleEndian::Load64(s + len - 16);
    uint64 wf = a + z;
    uint64 ws = b + Rotate(a, 31) + c;
    uint64 r = ShiftMix((vf + ws) * k2 + (wf + vs) * k0);
    return ShiftMix(r * k0 + vs) * k2;
}

uint64 CityHash64(const char* s, size_t len) {
    if (len <= 32) {
        if (len <= 16) {
            return HashLen0to16(s, len);
        } else {
            return HashLen17to32(s, len);
        }
    } else if (len <= 64) {
        return HashLen33to64(s, len);
    }

    // For strings over 64 bytes we hash the end first, and then as we
    // loop we keep 56 bytes of state: v, w, x, y, and z.
    uint64 x = LittleEndian::Load64(s + len - 40);
    uint64 y = LittleEndian::Load64(s + len - 16) + LittleEndian::Load64(s + len - 56);
    uint64 z =
            HashLen16(LittleEndian::Load64(s + len - 48) + len, LittleEndian::Load64(s + len - 24));
    pair<uint64, uint64> v = WeakHashLen32WithSeeds(s + len - 64, len, z);
    pair<uint64, uint64> w = WeakHashLen32WithSeeds(s + len - 32, y + k1, x);
    x = x * k1 + LittleEndian::Load64(s);

    // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
    len = (len - 1) & ~static_cast<size_t>(63);
    DCHECK_GT(len, 0);
    DCHECK_EQ(len, len / 64 * 64);
    do {
        x = Rotate(x + y + v.first + LittleEndian::Load64(s + 8), 37) * k1;
        y = Rotate(y + v.second + LittleEndian::Load64(s + 48), 42) * k1;
        x ^= w.second;
        y += v.first + LittleEndian::Load64(s + 40);
        z = Rotate(z + w.first, 33) * k1;
        v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
        w = WeakHashLen32WithSeeds(s + 32, z + w.second, y + LittleEndian::Load64(s + 16));
        std::swap(z, x);
        s += 64;
        len -= 64;
    } while (len != 0);
    return HashLen16(HashLen16(v.first, w.first) + ShiftMix(y) * k1 + z,
                     HashLen16(v.second, w.second) + x);
}

uint64 CityHash64WithSeed(const char* s, size_t len, uint64 seed) {
    return CityHash64WithSeeds(s, len, k2, seed);
}

uint64 CityHash64WithSeeds(const char* s, size_t len, uint64 seed0, uint64 seed1) {
    return HashLen16(CityHash64(s, len) - seed0, seed1);
}
} // namespace util_hash
