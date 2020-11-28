//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef DORIS_BE_SRC_UTIL_MURMUR_HASH3_H
#define DORIS_BE_SRC_UTIL_MURMUR_HASH3_H

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER) && (_MSC_VER < 1600)

typedef unsigned char uint8_t;
typedef unsigned int uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else // defined(_MSC_VER)

#include <stdint.h>

#endif // !defined(_MSC_VER)

//-----------------------------------------------------------------------------

void murmur_hash3_x86_32(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x86_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_64(const void* key, int len, uint64_t seed, void* out);

//-----------------------------------------------------------------------------

#endif // DORIS_BE_SRC_UTIL_MURMUR_HASH3_H
