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

#ifndef DORIS_BE_SRC_COMMON_UTIL_HASH_UTIL_HPP
#define DORIS_BE_SRC_COMMON_UTIL_HASH_UTIL_HPP

#include "common/compiler_util.h"
#include "common/logging.h"

// For cross compiling with clang, we need to be able to generate an IR file with
// no sse instructions.  Attempting to load a precompiled IR file that contains
// unsupported instructions causes llvm to fail.  We need to use #defines to control
// the code that is built and the runtime checks to control what code is run.
#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif
#include <zlib.h>

#include "gen_cpp/Types_types.h"
#include "util/cpu_info.h"
#include "util/murmur_hash3.h"

namespace doris {

// Utility class to compute hash values.
class HashUtil {
public:
    static uint32_t zlib_crc_hash(const void* data, int32_t bytes, uint32_t hash) {
        return crc32(hash, (const unsigned char*)data, bytes);
    }
#ifdef __SSE4_2__
    // Compute the Crc32 hash for data using SSE4 instructions.  The input hash parameter is
    // the current hash/seed value.
    // This should only be called if SSE is supported.
    // This is ~4x faster than Fnv/Boost Hash.
    // NOTE: DO NOT use this method for checksum! This does not generate the standard CRC32 checksum!
    //       For checksum, use CRC-32C algorithm from crc32c.h
    // NOTE: Any changes made to this function need to be reflected in Codegen::GetHashFn.
    // TODO: crc32 hashes with different seeds do not result in different hash functions.
    // The resulting hashes are correlated.
    static uint32_t crc_hash(const void* data, int32_t bytes, uint32_t hash) {
        if (!CpuInfo::is_supported(CpuInfo::SSE4_2)) {
            return zlib_crc_hash(data, bytes, hash);
        }
        uint32_t words = bytes / sizeof(uint32_t);
        bytes = bytes % sizeof(uint32_t);

        const uint32_t* p = reinterpret_cast<const uint32_t*>(data);

        while (words--) {
            hash = _mm_crc32_u32(hash, *p);
            ++p;
        }

        const uint8_t* s = reinterpret_cast<const uint8_t*>(p);

        while (bytes--) {
            hash = _mm_crc32_u8(hash, *s);
            ++s;
        }

        // The lower half of the CRC hash has has poor uniformity, so swap the halves
        // for anyone who only uses the first several bits of the hash.
        hash = (hash << 16) | (hash >> 16);
        return hash;
    }

    static uint64_t crc_hash64(const void* data, int32_t bytes, uint64_t hash) {
        uint32_t words = bytes / sizeof(uint32_t);
        bytes = bytes % sizeof(uint32_t);

        uint32_t h1 = hash >> 32;
        uint32_t h2 = (hash << 32) >> 32;

        const uint32_t* p = reinterpret_cast<const uint32_t*>(data);
        while (words--) {
            (words & 1) ? (h1 = _mm_crc32_u32(h1, *p)) : (h2 = _mm_crc32_u32(h2, *p));
            ++p;
        }

        const uint8_t* s = reinterpret_cast<const uint8_t*>(p);
        while (bytes--) {
            (bytes & 1) ? (h1 = _mm_crc32_u8(h1, *s)) : (h2 = _mm_crc32_u8(h2, *s));
            ++s;
        }
        union {
            uint64_t u64;
            uint32_t u32[2];
        } converter;
        converter.u64 = hash;

        h1 = (h1 << 16) | (h1 >> 16);
        h2 = (h2 << 16) | (h2 >> 16);
        converter.u32[0] = h1;
        converter.u32[1] = h2;

        return converter.u64;
    }
#else
    static uint32_t crc_hash(const void* data, int32_t bytes, uint32_t hash) {
        return zlib_crc_hash(data, bytes, hash);
    }
#endif

    // refer to https://github.com/apache/commons-codec/blob/master/src/main/java/org/apache/commons/codec/digest/MurmurHash3.java
    static const uint32_t MURMUR3_32_SEED = 104729;

    ALWAYS_INLINE static uint32_t rotl32(uint32_t x, int8_t r) {
        return (x << r) | (x >> (32 - r));
    }

    ALWAYS_INLINE static uint32_t fmix32(uint32_t h) {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

    // modify from https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
    static uint32_t murmur_hash3_32(const void* key, int32_t len, uint32_t seed) {
        const uint8_t* data = (const uint8_t*)key;
        const int nblocks = len / 4;

        uint32_t h1 = seed;

        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;
        const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);

        for (int i = -nblocks; i; i++) {
            uint32_t k1 = blocks[i];

            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = rotl32(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
        uint32_t k1 = 0;
        switch (len & 3) {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        };

        h1 ^= len;
        h1 = fmix32(h1);
        return h1;
    }

    static const int MURMUR_R = 47;

    // Murmur2 hash implementation returning 64-bit hashes.
    static uint64_t murmur_hash2_64(const void* input, int len, uint64_t seed) {
        uint64_t h = seed ^ (len * MURMUR_PRIME);

        const uint64_t* data = reinterpret_cast<const uint64_t*>(input);
        const uint64_t* end = data + (len / sizeof(uint64_t));

        while (data != end) {
            uint64_t k = *data++;
            k *= MURMUR_PRIME;
            k ^= k >> MURMUR_R;
            k *= MURMUR_PRIME;
            h ^= k;
            h *= MURMUR_PRIME;
        }

        const uint8_t* data2 = reinterpret_cast<const uint8_t*>(data);
        switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= MURMUR_PRIME;
        }

        h ^= h >> MURMUR_R;
        h *= MURMUR_PRIME;
        h ^= h >> MURMUR_R;
        return h;
    }

    // default values recommended by http://isthe.com/chongo/tech/comp/fnv/
    static const uint32_t FNV_PRIME = 0x01000193; //   16777619
    static const uint32_t FNV_SEED = 0x811C9DC5;  // 2166136261
    static const uint64_t FNV64_PRIME = 1099511628211UL;
    static const uint64_t FNV64_SEED = 14695981039346656037UL;
    static const uint64_t MURMUR_PRIME = 0xc6a4a7935bd1e995ULL;
    static const uint32_t MURMUR_SEED = 0xadc83b19ULL;
    // Implementation of the Fowler–Noll–Vo hash function.  This is not as performant
    // as boost's hash on int types (2x slower) but has bit entropy.
    // For ints, boost just returns the value of the int which can be pathological.
    // For example, if the data is <1000, 2000, 3000, 4000, ..> and then the mod of 1000
    // is taken on the hash, all values will collide to the same bucket.
    // For string values, Fnv is slightly faster than boost.
    static uint32_t fnv_hash(const void* data, int32_t bytes, uint32_t hash) {
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);

        while (bytes--) {
            hash = (*ptr ^ hash) * FNV_PRIME;
            ++ptr;
        }

        return hash;
    }

    static uint64_t fnv_hash64(const void* data, int32_t bytes, uint64_t hash) {
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);

        while (bytes--) {
            hash = (*ptr ^ hash) * FNV64_PRIME;
            ++ptr;
        }

        return hash;
    }

    // Our hash function is MurmurHash2, 64 bit version.
    // It was modified in order to provide the same result in
    // big and little endian archs (endian neutral).
    static uint64_t murmur_hash64A(const void* key, int32_t len, unsigned int seed) {
        const uint64_t m = MURMUR_PRIME;
        const int r = 47;
        uint64_t h = seed ^ (len * m);
        const uint8_t* data = (const uint8_t*)key;
        const uint8_t* end = data + (len - (len & 7));

        while (data != end) {
            uint64_t k;
#if (BYTE_ORDER == BIG_ENDIAN)
            k = (uint64_t)data[0];
            k |= (uint64_t)data[1] << 8;
            k |= (uint64_t)data[2] << 16;
            k |= (uint64_t)data[3] << 24;
            k |= (uint64_t)data[4] << 32;
            k |= (uint64_t)data[5] << 40;
            k |= (uint64_t)data[6] << 48;
            k |= (uint64_t)data[7] << 56;
#else
            k = *((uint64_t*)data);
#endif

            k *= m;
            k ^= k >> r;
            k *= m;
            h ^= k;
            h *= m;
            data += 8;
        }

        switch (len & 7) {
        case 7:
            h ^= (uint64_t)data[6] << 48;
        case 6:
            h ^= (uint64_t)data[5] << 40;
        case 5:
            h ^= (uint64_t)data[4] << 32;
        case 4:
            h ^= (uint64_t)data[3] << 24;
        case 3:
            h ^= (uint64_t)data[2] << 16;
        case 2:
            h ^= (uint64_t)data[1] << 8;
        case 1:
            h ^= (uint64_t)data[0];
            h *= m;
        };

        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    // Computes the hash value for data.  Will call either CrcHash or FnvHash
    // depending on hardware capabilities.
    // Seed values for different steps of the query execution should use different seeds
    // to prevent accidental key collisions. (See IMPALA-219 for more details).
    static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
#ifdef __SSE4_2__

        if (LIKELY(CpuInfo::is_supported(CpuInfo::SSE4_2))) {
            return crc_hash(data, bytes, seed);
        } else {
            return fnv_hash(data, bytes, seed);
        }

#else
        return fnv_hash(data, bytes, seed);
#endif
    }

    static uint64_t hash64(const void* data, int32_t bytes, uint64_t seed) {
#ifdef _SSE4_2_
        if (LIKELY(CpuInfo::is_supported(CpuInfo::SSE4_2))) {
            return crc_hash64(data, bytes, seed);

        } else {
            uint64_t hash = 0;
            murmur_hash3_x64_64(data, bytes, seed, &hash);
            return hash;
        }
#else
        uint64_t hash = 0;
        murmur_hash3_x64_64(data, bytes, seed, &hash);
        return hash;
#endif
    }
    // hash_combine is the same with boost hash_combine,
    // except replace boost::hash with std::hash 
    template <class T>
    static inline void hash_combine(std::size_t& seed, const T& v) {
        std::hash<T> hasher;
        seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
};

} // namespace doris

namespace std {
template <>
struct hash<doris::TUniqueId> {
    std::size_t operator()(const doris::TUniqueId& id) const {
        std::size_t seed = 0;
        seed = doris::HashUtil::hash(&id.lo, sizeof(id.lo), seed);
        seed = doris::HashUtil::hash(&id.hi, sizeof(id.hi), seed);
        return seed;
    }
};

template <>
struct hash<doris::TNetworkAddress> {
    size_t operator()(const doris::TNetworkAddress& address) const {
        std::size_t seed = 0;
        seed = doris::HashUtil::hash(address.hostname.data(), address.hostname.size(), seed);
        seed = doris::HashUtil::hash(&address.port, 4, seed);
        return seed;
    }
};

#if !defined(IR_COMPILE) && __GNUC__ < 6 && !defined(__clang__)
// Cause this is builtin function
template <>
struct hash<__int128> {
    std::size_t operator()(const __int128& val) const {
        return doris::HashUtil::hash(&val, sizeof(val), 0);
    }
};
#endif

template <>
struct hash<std::pair<doris::TUniqueId, int64_t>> {
    size_t operator()(const std::pair<doris::TUniqueId, int64_t>& pair) const {
        size_t seed = 0;
        seed = doris::HashUtil::hash(&pair.first.lo, sizeof(pair.first.lo), seed);
        seed = doris::HashUtil::hash(&pair.first.hi, sizeof(pair.first.hi), seed);
        seed = doris::HashUtil::hash(&pair.second, sizeof(pair.second), seed);
        return seed;
    }
};

} // namespace std

#endif
