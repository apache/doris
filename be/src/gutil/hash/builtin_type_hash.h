// Copyright 2011 Google Inc. All Rights Reserved.
//
// Hash functions for C++ builtin types. These are all of the fundamental
// integral and floating point types in the language as well as pointers. This
// library provides a minimal set of interfaces for hashing these values.

#ifndef UTIL_HASH_BUILTIN_TYPE_HASH_H_
#define UTIL_HASH_BUILTIN_TYPE_HASH_H_

#include <stddef.h>
#include <stdint.h>

#include "gutil/casts.h"
#include "gutil/hash/jenkins_lookup2.h"
#include "gutil/integral_types.h"
#include "gutil/macros.h"

inline uint32 Hash32NumWithSeed(uint32 num, uint32 c) {
    uint32 b = 0x9e3779b9UL; // the golden ratio; an arbitrary value
    mix(num, b, c);
    return c;
}

inline uint64 Hash64NumWithSeed(uint64 num, uint64 c) {
    uint64 b = GG_ULONGLONG(0xe08c1d668b756f82); // more of the golden ratio
    mix(num, b, c);
    return c;
}

// This function hashes pointer sized items and returns a 32b hash,
// convenienty hiding the fact that pointers may be 32b or 64b,
// depending on the architecture.
inline uint32 Hash32PointerWithSeed(const void* p, uint32 seed) {
    uintptr_t pvalue = reinterpret_cast<uintptr_t>(p);
    uint32 h = seed;
    // Hash the pointer 32b at a time.
    for (size_t i = 0; i < sizeof(pvalue); i += 4) {
        h = Hash32NumWithSeed(static_cast<uint32>(pvalue >> (i * 8)), h);
    }
    return h;
}

// ----------------------------------------------------------------------
// Hash64FloatWithSeed
// Hash64DoubleWithSeed
//   Functions for computing a hash value of floating-point numbers.
//   On systems where float and double comply with IEEE 754, these hashes
//   guarantee that if a == b, Hash64FloatWithSeed(a, c) ==
//   Hash64FloatWithSeed(b, c). Note that NaN does not compare equal to
//   itself, so two NaN inputs will not necessarily hash to the same value.
//
//   It is often a mistake to compare floating-point values for equality,
//   since floating-point computations do not produce exact values, due to
//   rounding. If equality comparison doesn't make sense in your situation,
//   hashing almost certainly doesn't make sense either.
//
//   Not guaranteed to return the same value in different builds, or to
//   avoid any reserved values.
// ----------------------------------------------------------------------
inline uint64 Hash64FloatWithSeed(float num, uint64 seed) {
    // +0 and -0 are the only floating point numbers which compare equal but
    // have distinct bitwise representations in IEEE 754. To work around this,
    // we force 0 to be +0.
    if (num == 0) {
        num = 0;
    }
    COMPILE_ASSERT(sizeof(float) == sizeof(uint32), float_has_wrong_size);

    const uint64 kMul = 0xc6a4a7935bd1e995ULL;

    uint64 a = (bit_cast<uint32>(num) + seed) * kMul;
    a ^= (a >> 47);
    a *= kMul;
    a ^= (a >> 47);
    a *= kMul;
    return a;
}

inline uint64 Hash64DoubleWithSeed(double num, uint64 seed) {
    if (num == 0) {
        num = 0;
    }
    COMPILE_ASSERT(sizeof(double) == sizeof(uint64), double_has_wrong_size);

    const uint64 kMul = 0xc6a4a7935bd1e995ULL;

    uint64 a = (bit_cast<uint64>(num) + seed) * kMul;
    a ^= (a >> 47);
    a *= kMul;
    a ^= (a >> 47);
    a *= kMul;
    return a;
}

#endif // UTIL_HASH_BUILTIN_TYPE_HASH_H_
