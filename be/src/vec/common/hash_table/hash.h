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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/Hash.h
// and modified by Doris

#pragma once

#include <type_traits>

#include "vec/common/uint128.h"
#include "vec/core/types.h"

/** Hash functions that are better than the trivial function std::hash.
  *
  * Example: when we do aggregation by the visitor ID, the performance increase is more than 5 times.
  * This is because of following reasons:
  * - in Yandex, visitor identifier is an integer that has timestamp with seconds resolution in lower bits;
  * - in typical implementation of standard library, hash function for integers is trivial and just use lower bits;
  * - traffic is non-uniformly distributed across a day;
  * - we are using open-addressing linear probing hash tables that are most critical to hash function quality,
  *   and trivial hash function gives disastrous results.
  */

/** Taken from MurmurHash. This is Murmur finalizer.
  * Faster than int_hash32 when inserting into the hash table UInt64 -> UInt64, where the key is the visitor ID.
  */
inline doris::vectorized::UInt64 int_hash64(doris::vectorized::UInt64 x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

/** CRC32C is not very high-quality as a hash function,
  *  according to avalanche and bit independence tests (see SMHasher software), as well as a small number of bits,
  *  but can behave well when used in hash tables,
  *  due to high speed (latency 3 + 1 clock cycle, throughput 1 clock cycle).
  * Works only with SSE 4.2 support.
  */
#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

inline doris::vectorized::UInt64 int_hash_crc32(doris::vectorized::UInt64 x) {
#ifdef __SSE4_2__
    return _mm_crc32_u64(-1ULL, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(-1U, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return int_hash64(x);
#endif
}

template <typename T>
inline size_t default_hash64(T key) {
    union {
        T in;
        doris::vectorized::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return int_hash64(u.out);
}

template <typename T, typename Enable = void>
struct DefaultHash;

template <typename T>
struct DefaultHash<T, std::enable_if_t<std::is_arithmetic_v<T>>> {
    size_t operator()(T key) const { return default_hash64<T>(key); }
};

template <typename T>
struct HashCRC32;

template <typename T>
inline size_t hash_crc32(T key) {
    union {
        T in;
        doris::vectorized::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return int_hash_crc32(u.out);
}

template <>
inline size_t hash_crc32(doris::vectorized::UInt128 u) {
    return doris::vectorized::UInt128HashCRC32()(u);
}

template <>
inline size_t hash_crc32(doris::vectorized::Int128 u) {
    return doris::vectorized::UInt128HashCRC32()(doris::vectorized::UInt128(
            (u >> 64) & int64_t (-1), u & int64_t(-1)));
}

#define DEFINE_HASH(T)                                                \
    template <>                                                       \
    struct HashCRC32<T> {                                             \
        size_t operator()(T key) const { return hash_crc32<T>(key); } \
    };

DEFINE_HASH(doris::vectorized::UInt8)
DEFINE_HASH(doris::vectorized::UInt16)
DEFINE_HASH(doris::vectorized::UInt32)
DEFINE_HASH(doris::vectorized::UInt64)
DEFINE_HASH(doris::vectorized::UInt128)
DEFINE_HASH(doris::vectorized::Int8)
DEFINE_HASH(doris::vectorized::Int16)
DEFINE_HASH(doris::vectorized::Int32)
DEFINE_HASH(doris::vectorized::Int64)
DEFINE_HASH(doris::vectorized::Int128)
DEFINE_HASH(doris::vectorized::Float32)
DEFINE_HASH(doris::vectorized::Float64)

#undef DEFINE_HASH

template <>
struct HashCRC32<doris::vectorized::UInt256> {
    size_t operator()(const doris::vectorized::UInt256& x) const {
#ifdef __SSE4_2__
        doris::vectorized::UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.a);
        crc = _mm_crc32_u64(crc, x.b);
        crc = _mm_crc32_u64(crc, x.c);
        crc = _mm_crc32_u64(crc, x.d);
        return crc;
#else
        return Hash128to64({Hash128to64({x.a, x.b}), Hash128to64({x.c, x.d})});
#endif
    }
};

/// It is reasonable to use for UInt8, UInt16 with sufficient hash table size.
struct TrivialHash {
    template <typename T>
    size_t operator()(T key) const {
        return key;
    }
};

/** A relatively good non-cryptographic hash function from UInt64 to UInt32.
  * But worse (both in quality and speed) than just cutting int_hash64.
  * Taken from here: http://www.concentric.net/~ttwang/tech/inthash.htm
  *
  * Slightly changed compared to the function by link: shifts to the right are accidentally replaced by a cyclic shift to the right.
  * This change did not affect the smhasher test results.
  *
  * It is recommended to use different salt for different tasks.
  * That was the case that in the database values were sorted by hash (for low-quality pseudo-random spread),
  *  and in another place, in the aggregate function, the same hash was used in the hash table,
  *  as a result, this aggregate function was monstrously slowed due to collisions.
  *
  * NOTE Salting is far from perfect, because it commutes with first steps of calculation.
  *
  * NOTE As mentioned, this function is slower than int_hash64.
  * But occasionally, it is faster, when written in a loop and loop is vectorized.
  */
template <doris::vectorized::UInt64 salt>
inline doris::vectorized::UInt32 int_hash32(doris::vectorized::UInt64 key) {
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return key;
}

/// For containers.
template <typename T, doris::vectorized::UInt64 salt = 0>
struct IntHash32 {
    size_t operator()(const T& key) const { return int_hash32<salt>(key); }
};
