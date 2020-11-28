// Copyright 2011 Google Inc. All Rights Reserved.
//
// These are the core hashing routines which operate on strings. We define
// strings loosely as a sequence of bytes, and these routines are designed to
// work with the most fundamental representations of a string of bytes.
//
// These routines provide "good" hash functions in terms of both quality and
// speed. Their values can and will change as their implementations change and
// evolve.

#ifndef UTIL_HASH_STRING_HASH_H_
#define UTIL_HASH_STRING_HASH_H_

#include <stddef.h>

#include "gutil/hash/city.h"
#include "gutil/hash/jenkins.h"
#include "gutil/hash/jenkins_lookup2.h"
#include "gutil/integral_types.h"
#include "gutil/port.h"

namespace hash_internal {

// We have some special cases for 64-bit hardware and x86-64 in particular.
// Instead of sprinkling ifdefs through the file, we have one ugly ifdef here.
// Later code can then use "if" instead of "ifdef".
#if defined(__x86_64__)
enum { x86_64 = true, sixty_four_bit = true };
#elif defined(_LP64)
enum { x86_64 = false, sixty_four_bit = true };
#else
enum { x86_64 = false, sixty_four_bit = false };
#endif

// Arbitrary mix constants (pi).
static const uint32 kMix32 = 0x12b9b0a1UL;
static const uint64 kMix64 = GG_ULONGLONG(0x2b992ddfa23249d6);

} // namespace hash_internal

inline size_t HashStringThoroughlyWithSeed(const char* s, size_t len, size_t seed) {
    if (hash_internal::x86_64)
        return static_cast<size_t>(util_hash::CityHash64WithSeed(s, len, seed));

    if (hash_internal::sixty_four_bit)
        return Hash64StringWithSeed(s, static_cast<uint32>(len), seed);

    return static_cast<size_t>(
            Hash32StringWithSeed(s, static_cast<uint32>(len), static_cast<uint32>(seed)));
}

inline size_t HashStringThoroughly(const char* s, size_t len) {
    if (hash_internal::x86_64) return static_cast<size_t>(util_hash::CityHash64(s, len));

    if (hash_internal::sixty_four_bit)
        return Hash64StringWithSeed(s, static_cast<uint32>(len), hash_internal::kMix64);

    return static_cast<size_t>(
            Hash32StringWithSeed(s, static_cast<uint32>(len), hash_internal::kMix32));
}

inline size_t HashStringThoroughlyWithSeeds(const char* s, size_t len, size_t seed0, size_t seed1) {
    if (hash_internal::x86_64) return util_hash::CityHash64WithSeeds(s, len, seed0, seed1);

    if (hash_internal::sixty_four_bit) {
        uint64 a = seed0;
        uint64 b = seed1;
        uint64 c = HashStringThoroughly(s, len);
        mix(a, b, c);
        return c;
    }

    uint32 a = static_cast<uint32>(seed0);
    uint32 b = static_cast<uint32>(seed1);
    uint32 c = static_cast<uint32>(HashStringThoroughly(s, len));
    mix(a, b, c);
    return c;
}

#endif // UTIL_HASH_STRING_HASH_H_
