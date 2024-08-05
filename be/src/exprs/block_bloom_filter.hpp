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
// https://github.com/apache/kudu/blob/master/src/kudu/util/block_bloom_filter.h
// and modified by Doris

#pragma once

#include "vec/common/string_ref.h"
#ifdef __AVX2__
#include <immintrin.h>

#include "gutil/macros.h"
#endif
#include "common/status.h"
#include "fmt/format.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {

// https://github.com/apache/kudu/blob/master/src/kudu/util/block_bloom_filter.h
// BlockBloomFilter is modified based on Impala's BlockBloomFilter.
// For general Bloomfilter implementations, this implementation is
// more friendly to CPU Cache, and it is easier to use SIMD instructions to
// speed up the implementation.

// BlockBloomFilter will not store null values, and will always return a false if the input is null.

// Some constants used in hashing. #defined for efficiency reasons.
#define BLOOM_HASH_CONSTANTS                                                                   \
    0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, \
            0x5c6bfb31U

class BlockBloomFilter {
public:
    explicit BlockBloomFilter();
    ~BlockBloomFilter();

    BlockBloomFilter(const BlockBloomFilter&) = delete;
    BlockBloomFilter& operator=(const BlockBloomFilter&) = delete;

    Status init(int log_space_bytes, uint32_t hash_seed);
    // Initialize the BlockBloomFilter from a populated "directory" structure.
    // Useful for initializing the BlockBloomFilter by de-serializing a custom protobuf message.
    Status init_from_directory(int log_space_bytes, butil::IOBufAsZeroCopyInputStream* data,
                               const size_t data_size, bool always_false, uint32_t hash_seed);

    void close();

    // Adds an element to the BloomFilter. The function used to generate 'hash' need not
    // have good uniformity, but it should have low collision probability. For instance, if
    // the set of values is 32-bit ints, the identity function is a valid hash function for
    // this Bloom filter, since the collision probability (the probability that two
    // non-equal values will have the same hash value) is 0.
    void insert(uint32_t hash) noexcept;
    // Same as above with convenience of hashing the key.
    void insert(const StringRef& key) noexcept {
        if (key.data) {
            insert(HashUtil::crc_hash(key.data, key.size, _hash_seed));
        }
    }

#ifdef __AVX2__

    static inline ATTRIBUTE_ALWAYS_INLINE __attribute__((__target__("avx2"))) __m256i make_mark(
            const uint32_t hash) {
        const __m256i ones = _mm256_set1_epi32(1);
        const __m256i rehash = _mm256_setr_epi32(BLOOM_HASH_CONSTANTS);
        // Load hash into a YMM register, repeated eight times
        __m256i hash_data = _mm256_set1_epi32(hash);
        // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
        // odd constants, then keep the 5 most significant bits from each product.
        hash_data = _mm256_mullo_epi32(rehash, hash_data);
        hash_data = _mm256_srli_epi32(hash_data, 27);
        // Use these 5 bits to shift a single bit to a location in each 32-bit lane
        return _mm256_sllv_epi32(ones, hash_data);
    }
#endif
    // Finds an element in the BloomFilter, returning true if it is found and false (with
    // high probability) if it is not.
    ALWAYS_INLINE bool find(uint32_t hash) const noexcept {
        if (_always_false) {
            return false;
        }
        const uint32_t bucket_idx = rehash32to32(hash) & _directory_mask;
#ifdef __AVX2__
        const __m256i mask = make_mark(hash);
        const __m256i bucket = reinterpret_cast<__m256i*>(_directory)[bucket_idx];
        // We should return true if 'bucket' has a one wherever 'mask' does. _mm256_testc_si256
        // takes the negation of its first argument and ands that with its second argument. In
        // our case, the result is zero everywhere iff there is a one in 'bucket' wherever
        // 'mask' is one. testc returns 1 if the result is 0 everywhere and returns 0 otherwise.
        const bool result = _mm256_testc_si256(bucket, mask);
        _mm256_zeroupper();
        return result;
#else
        return bucket_find(bucket_idx, hash);
#endif
    }
    // Same as above with convenience of hashing the key.
    bool find(const StringRef& key) const noexcept {
        if (key.data) {
            return find(HashUtil::crc_hash(key.data, key.size, _hash_seed));
        }
        return false;
    }

#ifdef __ARM_NEON
    void make_find_mask(uint32_t key, uint32x4_t* masks) const noexcept {
        uint32x4_t hash_data_1 = vdupq_n_u32(key);
        uint32x4_t hash_data_2 = vdupq_n_u32(key);

        uint32x4_t rehash_1 = vld1q_u32(&kRehash[0]);
        uint32x4_t rehash_2 = vld1q_u32(&kRehash[4]);

        //  masks[i] = key * kRehash[i];
        hash_data_1 = vmulq_u32(rehash_1, hash_data_1);
        hash_data_2 = vmulq_u32(rehash_2, hash_data_2);
        //  masks[i] = masks[i] >> shift_num;
        hash_data_1 = vshrq_n_u32(hash_data_1, shift_num);
        hash_data_2 = vshrq_n_u32(hash_data_2, shift_num);

        const uint32x4_t ones = vdupq_n_u32(1);

        // masks[i] = 0x1 << masks[i];
        masks[0] = vshlq_u32(ones, reinterpret_cast<int32x4_t>(hash_data_1));
        masks[1] = vshlq_u32(ones, reinterpret_cast<int32x4_t>(hash_data_2));
    }
#else
    void make_find_mask(uint32_t key, uint32_t* masks) const noexcept {
        for (int i = 0; i < kBucketWords; ++i) {
            masks[i] = key * kRehash[i];

            masks[i] = masks[i] >> shift_num;

            masks[i] = 0x1 << masks[i];
        }
    }
#endif

    // Computes the logical OR of this filter with 'other' and stores the result in this
    // filter.
    // Notes:
    // - The directory sizes of the Bloom filters must match.
    // - Or'ing with kAlwaysTrueFilter is disallowed.
    Status merge(const BlockBloomFilter& other);

    // Computes out[i] |= in[i] for the arrays 'in' and 'out' of length 'n' bytes where 'n'
    // is multiple of 32-bytes.
    static Status or_equal_array(size_t n, const uint8_t* __restrict__ in,
                                 uint8_t* __restrict__ out);

    // Returns whether the Bloom filter is empty and hence would return false for all lookups.
    bool always_false() const { return _always_false; }

    // Returns amount of space used in log2 bytes.
    int log_space_bytes() const { return _log_num_buckets + kLogBucketByteSize; }

    // Returns the directory structure. Useful for serializing the BlockBloomFilter to
    // a custom protobuf message.
    Slice directory() const {
        return Slice(reinterpret_cast<const uint8_t*>(_directory), directory_size());
    }

    // Representation of a filter which allows all elements to pass.
    static constexpr BlockBloomFilter* const kAlwaysTrueFilter = nullptr;

private:
    // _always_false is true when the bloom filter hasn't had any elements inserted.
    bool _always_false;

    // The BloomFilter is divided up into Buckets and each Bucket comprises of 8 BucketWords of
    // 4 bytes each.
    static constexpr uint64_t kBucketWords = 8;
    typedef uint32_t BucketWord;

    // log2(number of bits in a BucketWord)
    static constexpr int kLogBucketWordBits = 5;
    static constexpr BucketWord kBucketWordMask = (1 << kLogBucketWordBits) - 1;
    // (>> 27) is equivalent to (mod 32)
    static constexpr auto shift_num = ((1 << kLogBucketWordBits) - kLogBucketWordBits);
    // log2(number of bytes in a bucket)
    static constexpr int kLogBucketByteSize = 5;
    // Bucket size in bytes.
    static constexpr size_t kBucketByteSize = 1UL << kLogBucketByteSize;

    static_assert(
            (1 << kLogBucketWordBits) == std::numeric_limits<BucketWord>::digits,
            "BucketWord must have a bit-width that is be a power of 2, like 64 for uint64_t.");

    typedef BucketWord Bucket[kBucketWords];

    // log_num_buckets_ is the log (base 2) of the number of buckets in the directory.
    int _log_num_buckets;

    // _directory_mask is (1 << log_num_buckets_) - 1. It is precomputed for
    // efficiency reasons.
    uint32_t _directory_mask;

    Bucket* _directory;

    // Seed used with hash algorithm.
    uint32_t _hash_seed;

    // Helper function for public Init() variants.
    Status init_internal(int log_space_bytes, uint32_t hash_seed);

    // Same as Insert(), but skips the CPU check and assumes that AVX2 is not available.
    void insert_no_avx2(uint32_t hash) noexcept;

    // Does the actual work of Insert(). bucket_idx is the index of the bucket to insert
    // into and 'hash' is the value passed to Insert().
    void bucket_insert(uint32_t bucket_idx, uint32_t hash) noexcept;

    bool bucket_find(uint32_t bucket_idx, uint32_t hash) const noexcept;

    // Computes out[i] |= in[i] for the arrays 'in' and 'out' of length 'n' without using AVX2
    // operations.
    static void or_equal_array_no_avx2(size_t n, const uint8_t* __restrict__ in,
                                       uint8_t* __restrict__ out);
    // Helper function for OrEqualArray functions that encapsulates AVX2 v/s non-AVX2 logic to
    // invoke the right function.
    static void or_equal_array_internal(size_t n, const uint8_t* __restrict__ in,
                                        uint8_t* __restrict__ out);

#ifdef __AVX2__
    // Same as Insert(), but skips the CPU check and assumes that AVX2 is available.
    void insert_avx2(uint32_t hash) noexcept __attribute__((__target__("avx2")));

    // A faster SIMD version of BucketInsert().
    void bucket_insert_avx2(uint32_t bucket_idx, uint32_t hash) noexcept
            __attribute__((__target__("avx2")));

    // Computes out[i] |= in[i] for the arrays 'in' and 'out' of length 'n' using AVX2
    // instructions. 'n' must be a multiple of 32.
    static void or_equal_array_avx2(size_t n, const uint8_t* __restrict__ in,
                                    uint8_t* __restrict__ out) __attribute__((target("avx2")));

#endif
    // Size of the internal directory structure in bytes.
    size_t directory_size() const { return 1ULL << log_space_bytes(); }

    // kRehash is used as 8 odd 32-bit unsigned ints.  See Dietzfelbinger et al.'s "A
    // reliable randomized algorithm for the closest-pair problem".
    static constexpr uint32_t kRehash[8] __attribute__((aligned(32))) = {BLOOM_HASH_CONSTANTS};

    // Get 32 more bits of randomness from a 32-bit hash:
    static ALWAYS_INLINE uint32_t rehash32to32(const uint32_t hash) {
        // Constants generated by uuidgen(1) with the -r flag
        static constexpr uint64_t m = 0x7850f11ec6d14889ULL;
        static constexpr uint64_t a = 0x6773610597ca4c63ULL;
        // This is strongly universal hashing following Dietzfelbinger's "Universal hashing
        // and k-wise independent random variables via integer arithmetic without primes". As
        // such, for any two distinct uint32_t's hash1 and hash2, the probability (over the
        // randomness of the constants) that any subset of bit positions of
        // Rehash32to32(hash1) is equal to the same subset of bit positions
        // Rehash32to32(hash2) is minimal.
        return (static_cast<uint64_t>(hash) * m + a) >> 32U;
    }
};

} // namespace doris
