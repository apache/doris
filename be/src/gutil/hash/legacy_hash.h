// Copyright 2011 Google Inc. All Rights Reserved.
//
// This is a library of legacy hashing routines. These routines are still in
// use, but are not encouraged for any new code, and may be removed at some
// point in the future.
//
// New code should use one of the targeted libraries that provide hash
// interfaces for the types needed. See //util/hash/README for details.

#ifndef UTIL_HASH_LEGACY_HASH_H_
#define UTIL_HASH_LEGACY_HASH_H_

#include "gutil/hash/builtin_type_hash.h"
#include "gutil/hash/string_hash.h"
#include "gutil/integral_types.h"

// Hash8, Hash16 and Hash32 are for legacy use only.
typedef uint32 Hash32;
typedef uint16 Hash16;
typedef uint8 Hash8;

const Hash32 kIllegalHash32 = static_cast<Hash32>(0xffffffffUL);
const Hash16 kIllegalHash16 = static_cast<Hash16>(0xffff);

static const uint32 MIX32 = 0x12b9b0a1UL;                     // pi; an arbitrary number
static const uint64 MIX64 = GG_ULONGLONG(0x2b992ddfa23249d6); // more of pi

// ----------------------------------------------------------------------
// HashTo32()
// HashTo16()
//    These functions take various types of input (through operator
//    overloading) and return 32 or 16 bit quantities, respectively.
//    The basic rule of our hashing is: always mix().  Thus, even for
//    char outputs we cast to a uint32 and mix with two arbitrary numbers.
//    HashTo32 never returns kIllegalHash32, and similarity,
//    HashTo16 never returns kIllegalHash16.
//
// Note that these methods avoid returning certain reserved values, while
// the corresponding HashXXStringWithSeed() methods may return any value.
// ----------------------------------------------------------------------

// This macro defines the HashTo32 and HashTo16 versions all in one go.
// It takes the argument list and a command that hashes your number.
// (For 16 we just mod retval before returning it.)  Example:
//    HASH_TO((char c), Hash32NumWithSeed(c, MIX32_1))
// evaluates to
//    uint32 retval;
//    retval = Hash32NumWithSeed(c, MIX32_1);
//    return retval == kIllegalHash32 ? retval-1 : retval;
//

#define HASH_TO(arglist, command)                              \
    inline uint32 HashTo32 arglist {                           \
        uint32 retval = command;                               \
        return retval == kIllegalHash32 ? retval - 1 : retval; \
    }

// This defines:
// HashToXX(char *s, int slen);
// HashToXX(char c);
// etc

HASH_TO((const char* s, uint32 slen), Hash32StringWithSeed(s, slen, MIX32))
HASH_TO((const wchar_t* s, uint32 slen),
        Hash32StringWithSeed(reinterpret_cast<const char*>(s),
                             static_cast<uint32>(sizeof(wchar_t) * slen), MIX32))
HASH_TO((char c), Hash32NumWithSeed(static_cast<uint32>(c), MIX32))
HASH_TO((schar c), Hash32NumWithSeed(static_cast<uint32>(c), MIX32))
HASH_TO((uint16 c), Hash32NumWithSeed(static_cast<uint32>(c), MIX32))
HASH_TO((int16 c), Hash32NumWithSeed(static_cast<uint32>(c), MIX32))
HASH_TO((uint32 c), Hash32NumWithSeed(static_cast<uint32>(c), MIX32))
HASH_TO((int32 c), Hash32NumWithSeed(static_cast<uint32>(c), MIX32))
HASH_TO((uint64 c), static_cast<uint32>(Hash64NumWithSeed(c, MIX64) >> 32))
HASH_TO((int64 c), static_cast<uint32>(Hash64NumWithSeed(c, MIX64) >> 32))

#undef HASH_TO // clean up the macro space

inline uint16 HashTo16(const char* s, uint32 slen) {
    uint16 retval = Hash32StringWithSeed(s, slen, MIX32) >> 16;
    return retval == kIllegalHash16 ? static_cast<uint16>(retval - 1) : retval;
}

#endif // UTIL_HASH_LEGACY_HASH_H_
