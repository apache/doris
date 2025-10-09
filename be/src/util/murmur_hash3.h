//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#pragma once

#include <cstdint>
namespace doris {

void murmur_hash3_x86_32(const void* key, int64_t len, uint32_t seed, void* out);

void murmur_hash3_x86_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_128(const void* key, int len, uint32_t seed, void* out);

void murmur_hash3_x64_64(const void* key, int64_t len, uint64_t seed, void* out);

} // namespace doris