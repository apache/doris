//
// Copyright (C) 1999 and onwards Google, Inc.
//
//
// This file contains routines for hashing and fingerprinting.
//
// A hash function takes an arbitrary input bitstring (string, char*,
// number) and turns it into a hash value (a fixed-size number) such
// that unequal input values have a high likelihood of generating
// unequal hash values.  A fingerprint is a hash whose design is
// biased towards avoiding hash collisions, possibly at the expense of
// other characteristics such as execution speed.
//
// In general, if you are only using the hash values inside a single
// executable -- you're not writing the values to disk, and you don't
// depend on another instance of your program, running on another
// machine, generating the same hash values as you -- you want to use
// a HASH.  Otherwise, you want to use a FINGERPRINT.
//
// RECOMMENDED HASH FOR STRINGS:    GoodFastHash
//
// It is a functor, so you can use it like this:
//     hash_map<string, xxx, GoodFastHash<string> >
//     hash_set<char *, GoodFastHash<char*> >
//
// RECOMMENDED HASH FOR NUMBERS:    hash<>
//
// Note that this is likely the identity hash, so if your
// numbers are "non-random" (especially in the low bits), another
// choice is better.  You can use it like this:
//     hash_map<int, xxx>
//     hash_set<uint64>
//
// RECOMMENDED HASH FOR POINTERS:    hash<>
//
// This is also likely the identity hash.
//
// RECOMMENDED HASH FOR STRUCTS:    hash<some_fingerprint(struct)>
//
// Take a fingerprint of the struct, and use that as the key.
// For instance: const uint64 hash_data[] = { s.foo, bit_cast<uint64>(s.bar) };
//    uint64 fprint = <fingerprint fn>(reinterpret_cast<const char*>(hash_data),
//                                     sizeof(hash_data));
//    hash_map[fprint] = whatever;
//
// RECOMMENDED FINGERPRINT:         Fingerprint2011
//
// (In util/hash/fingerprint2011.h)
// In particular, do *not* use Fingerprint in new code; it has
// problems with excess collisions.
//
// OTHER HASHES AND FINGERPRINTS:
//
//
// The wiki page also has good advice for when to use a fingerprint vs
// a hash.
//
//
// Note: if your file declares hash_map<string, ...> or
// hash_set<string>, it will use the default hash function,
// hash<string>.  This is not a great choice.  Always provide an
// explicit functor, such as GoodFastHash, as a template argument.
// (Either way, you will need to #include this file to get the
// necessary definition.)
//
// Some of the hash functions below are documented to be fixed
// forever; the rest (whether they're documented as so or not) may
// change over time.  If you require a hash function that does not
// change over time, you should have unittests enforcing this
// property.  We already have several such functions; see
// hash_unittest.cc for the details and unittests.

#ifndef UTIL_HASH_HASH_H_
#define UTIL_HASH_HASH_H_

#include <stddef.h>
#include <stdint.h> // for uintptr_t
#include <string.h>

#include <algorithm>
#include <string>
#include <utility>

#include "gutil/casts.h"
#include "gutil/hash/city.h"
#include "gutil/hash/hash128to64.h"
#include "gutil/hash/jenkins.h"
#include "gutil/hash/jenkins_lookup2.h"
#include "gutil/hash/legacy_hash.h"
#include "gutil/hash/string_hash.h"
#include "gutil/int128.h"
#include "gutil/integral_types.h"
#include "gutil/macros.h"
#include "gutil/port.h"

// ----------------------------------------------------------------------
// Fingerprint()
//   Not recommended for new code.  Instead, use Fingerprint2011(),
//   a higher-quality and faster hash function.  See fingerprint2011.h.
//
//   Fingerprinting a string (or char*) will never return 0 or 1,
//   in case you want a couple of special values.  However,
//   fingerprinting a numeric type may produce 0 or 1.
//
//   The hash mapping of Fingerprint() will never change.
//
//   Note: AVOID USING FINGERPRINT if at all possible.  Use
//   Fingerprint2011 (in fingerprint2011.h) instead.
//   Fingerprint() is susceptible to collisions for even short
//   strings with low edit distance; see
//   Example collisions:
//     "01056/02" vs. "11057/02"
//     "LTA 02" vs. "MTA 12"
//   The same study found only one collision each for CityHash64() and
//   MurmurHash64(), from more than 2^32 inputs, and on medium-length
//   strings with large edit distances.These issues, among others,
//   led to the recommendation that new code should avoid Fingerprint().
// ----------------------------------------------------------------------
extern uint64 FingerprintReferenceImplementation(const char* s, uint32 len);
extern uint64 FingerprintInterleavedImplementation(const char* s, uint32 len);
inline uint64 Fingerprint(const char* s, uint32 len) {
    if (sizeof(s) == 8) { // 64-bit systems have 8-byte pointers.
        // The better choice when we have a decent number of registers.
        return FingerprintInterleavedImplementation(s, len);
    } else {
        return FingerprintReferenceImplementation(s, len);
    }
}

// Routine that combines together the hi/lo part of a fingerprint
// and changes the result appropriately to avoid returning 0/1.
inline uint64 CombineFingerprintHalves(uint32 hi, uint32 lo) {
    uint64 result = (static_cast<uint64>(hi) << 32) | static_cast<uint64>(lo);
    if ((hi == 0) && (lo < 2)) {
        result ^= GG_ULONGLONG(0x130f9bef94a0a928);
    }
    return result;
}

inline uint64 Fingerprint(const std::string& s) {
    return Fingerprint(s.data(), static_cast<uint32>(s.size()));
}
inline uint64 Hash64StringWithSeed(const std::string& s, uint64 c) {
    return Hash64StringWithSeed(s.data(), static_cast<uint32>(s.size()), c);
}
inline uint64 Fingerprint(schar c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(char c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(uint16 c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(int16 c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(uint32 c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(int32 c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(uint64 c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}
inline uint64 Fingerprint(int64 c) {
    return Hash64NumWithSeed(static_cast<uint64>(c), MIX64);
}

// This concatenates two 64-bit fingerprints. It is a convenience function to
// get a fingerprint for a combination of already fingerprinted components.
// It assumes that each input is already a good fingerprint itself.
// Note that this is legacy code and new code should use its replacement
// FingerprintCat2011().
//
// Note that in general it's impossible to construct Fingerprint(str)
// from the fingerprints of substrings of str.  One shouldn't expect
// FingerprintCat(Fingerprint(x), Fingerprint(y)) to indicate
// anything about Fingerprint(StrCat(x, y)).
inline uint64 FingerprintCat(uint64 fp1, uint64 fp2) {
    return Hash64NumWithSeed(fp1, fp2);
}

namespace std {

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct hash<uint128> {
    size_t operator()(const uint128& x) const {
        if (sizeof(&x) == 8) { // 64-bit systems have 8-byte pointers.
            return Hash128to64(x);
        } else {
            uint32 a = static_cast<uint32>(Uint128Low64(x)) + static_cast<uint32>(0x9e3779b9UL);
            uint32 b =
                    static_cast<uint32>(Uint128Low64(x) >> 32) + static_cast<uint32>(0x9e3779b9UL);
            uint32 c = static_cast<uint32>(Uint128High64(x)) + MIX32;
            mix(a, b, c);
            a += static_cast<uint32>(Uint128High64(x) >> 32);
            mix(a, b, c);
            return c;
        }
    }
    static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// Hasher for STL pairs. Requires hashers for both members to be defined
template <class First, class Second>
struct hash<pair<First, Second> > {
    size_t operator()(const pair<First, Second>& p) const {
        size_t h1 = std::hash<First>()(p.first);
        size_t h2 = std::hash<Second>()(p.second);
        // The decision below is at compile time
        return (sizeof(h1) <= sizeof(uint32)) ? Hash32NumWithSeed(h1, h2)
                                              : Hash64NumWithSeed(h1, h2);
    }
    static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

} // namespace std

// If you want an excellent string hash function, and you don't mind if it
// might change when you sync and recompile, please use GoodFastHash<>.
// For most applications, GoodFastHash<> is a good choice, better than
// hash<string> or hash<char*> or similar.  GoodFastHash<> can change
// from time to time and may differ across platforms, and we'll strive
// to keep improving it.
//
// By the way, when deleting the contents of a hash_set of pointers, it is
// unsafe to delete *iterator because the hash function may be called on
// the next iterator advance.  Use STLDeleteContainerPointers().

template <class X>
struct GoodFastHash;

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct GoodFastHash<char*> {
    size_t operator()(const char* s) const { return HashStringThoroughly(s, strlen(s)); }
    // Less than operator for MSVC.
    bool operator()(const char* a, const char* b) const { return strcmp(a, b) < 0; }
    static const size_t bucket_size = 4; // These are required by MSVC
    static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct GoodFastHash<const char*> {
    size_t operator()(const char* s) const { return HashStringThoroughly(s, strlen(s)); }
    // Less than operator for MSVC.
    bool operator()(const char* a, const char* b) const { return strcmp(a, b) < 0; }
    static const size_t bucket_size = 4; // These are required by MSVC
    static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// This intended to be a "good" hash function.  It may change from time to time.
template <class _CharT, class _Traits, class _Alloc>
struct GoodFastHash<std::basic_string<_CharT, _Traits, _Alloc> > {
    size_t operator()(const std::basic_string<_CharT, _Traits, _Alloc>& k) const {
        return HashStringThoroughly(k.data(), k.length() * sizeof(k[0]));
    }
    // Less than operator for MSVC.
    bool operator()(const std::basic_string<_CharT, _Traits, _Alloc>& a,
                    const std::basic_string<_CharT, _Traits, _Alloc>& b) const {
        return a < b;
    }
    static const size_t bucket_size = 4; // These are required by MSVC
    static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// This intended to be a "good" hash function.  It may change from time to time.
template <class _CharT, class _Traits, class _Alloc>
struct GoodFastHash<const std::basic_string<_CharT, _Traits, _Alloc> > {
    size_t operator()(const std::basic_string<_CharT, _Traits, _Alloc>& k) const {
        return HashStringThoroughly(k.data(), k.length() * sizeof(k[0]));
    }
    // Less than operator for MSVC.
    bool operator()(const std::basic_string<_CharT, _Traits, _Alloc>& a,
                    const std::basic_string<_CharT, _Traits, _Alloc>& b) const {
        return a < b;
    }
    static const size_t bucket_size = 4; // These are required by MSVC
    static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

#endif // UTIL_HASH_HASH_H_
