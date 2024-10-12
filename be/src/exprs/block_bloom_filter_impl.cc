
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
// https://github.com/apache/kudu/blob/master/src/kudu/util/block_bloom_filter.cc
// and modified by Doris

#include <butil/iobuf.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <climits> // IWYU pragma: keep
#include <cmath>   // IWYU pragma: keep
#include <cstdlib>
#include <cstring>
#include <string>

#include "common/status.h"
#include "exprs/block_bloom_filter.hpp"
// IWYU pragma: no_include <emmintrin.h>
#include "util/sse_util.hpp"

namespace doris {

constexpr uint32_t BlockBloomFilter::kRehash[8] __attribute__((aligned(32)));
// constexpr data member requires initialization in the class declaration.
// Hence no duplicate initialization in the definition here.
constexpr BlockBloomFilter* const BlockBloomFilter::kAlwaysTrueFilter;

BlockBloomFilter::BlockBloomFilter()
        : _always_false(true),
          _log_num_buckets(0),
          _directory_mask(0),
          _directory(nullptr),
          _hash_seed(0) {}

BlockBloomFilter::~BlockBloomFilter() {
    close();
}

Status BlockBloomFilter::init_internal(const int log_space_bytes, uint32_t hash_seed) {
    // Since log_space_bytes is in bytes, we need to convert it to the number of tiny
    // Bloom filters we will use.
    _log_num_buckets = std::max(1, log_space_bytes - kLogBucketByteSize);
    // Since we use 32 bits in the arguments of Insert() and Find(), _log_num_buckets
    // must be limited.
    if (_log_num_buckets > 32) {
        return Status::InvalidArgument("Bloom filter too large. log_space_bytes: {}",
                                       log_space_bytes);
    }
    // Don't use _log_num_buckets if it will lead to undefined behavior by a shift
    // that is too large.
    _directory_mask = (1ULL << _log_num_buckets) - 1;

    const size_t alloc_size = directory_size();
    close(); // Ensure that any previously allocated memory for directory_ is released.
    DCHECK(_directory == nullptr);
    int rc = posix_memalign((void**)&_directory, 32, alloc_size);
    if (rc != 0) {
        return Status::InternalError("block_bloom_filter alloc fail");
    }

    _hash_seed = hash_seed;
    return Status::OK();
}

Status BlockBloomFilter::init(const int log_space_bytes, uint32_t hash_seed) {
    RETURN_IF_ERROR(init_internal(log_space_bytes, hash_seed));
    DCHECK(_directory);
    memset(_directory, 0, directory_size());
    _always_false = true;
    return Status::OK();
}

Status BlockBloomFilter::init_from_directory(int log_space_bytes,
                                             butil::IOBufAsZeroCopyInputStream* data,
                                             const size_t data_size, bool always_false,
                                             uint32_t hash_seed) {
    RETURN_IF_ERROR(init_internal(log_space_bytes, hash_seed));
    DCHECK(_directory);

    if (directory_size() != data_size) {
        return Status::InvalidArgument(fmt::format(
                "Mismatch in BlockBloomFilter source directory size {} and expected size {}",
                data_size, directory_size()));
    }
    int size = 0;
    char* tmp;
    const void** ptr = (const void**)&tmp;
    char* data_ptr = reinterpret_cast<char*>(_directory);
    while (data->Next(ptr, &size)) {
        memcpy(data_ptr, *ptr, size);
        data_ptr += size;
    }
    _always_false = always_false;
    return Status::OK();
}

void BlockBloomFilter::close() {
    if (_directory != nullptr) {
        free(_directory);
        _directory = nullptr;
    }
}

void BlockBloomFilter::bucket_insert(const uint32_t bucket_idx, const uint32_t hash) noexcept {
    // new_bucket will be all zeros except for eight 1-bits, one in each 32-bit word. It is
    // 16-byte aligned so it can be read as a __m128i using aligned SIMD loads in the second
    // part of this method.
    uint32_t new_bucket[kBucketWords] __attribute__((aligned(16)));
    for (int i = 0; i < kBucketWords; ++i) {
        // Rehash 'hash' and use the top kLogBucketWordBits bits, following Dietzfelbinger.
        new_bucket[i] = (kRehash[i] * hash) >> ((1 << kLogBucketWordBits) - kLogBucketWordBits);
        new_bucket[i] = 1U << new_bucket[i];
    }
    for (int i = 0; i < 2; ++i) {
        __m128i new_bucket_sse = _mm_load_si128(reinterpret_cast<__m128i*>(new_bucket + 4 * i));
        __m128i* existing_bucket =
                reinterpret_cast<__m128i*>(&DCHECK_NOTNULL(_directory)[bucket_idx][4 * i]);
        *existing_bucket = _mm_or_si128(*existing_bucket, new_bucket_sse);
    }
}

bool BlockBloomFilter::bucket_find(const uint32_t bucket_idx, const uint32_t hash) const noexcept {
#if defined(__ARM_NEON)
    uint32x4_t masks[2];

    uint32x4_t directory_1 = vld1q_u32(&_directory[bucket_idx][0]);
    uint32x4_t directory_2 = vld1q_u32(&_directory[bucket_idx][4]);

    make_find_mask(hash, masks);
    // The condition for returning true is that all the bits in _directory[bucket_idx][i] specified by masks[i] are 1.
    // This can be equivalently expressed as all the bits in not( _directory[bucket_idx][i]) specified by masks[i] are 0.
    // vbicq_u32(vec1, vec2) : Result of (vec1 AND NOT vec2)
    // If true is returned, out_1 and out_2 should be all zeros.
    uint32x4_t out_1 = vbicq_u32(masks[0], directory_1);
    uint32x4_t out_2 = vbicq_u32(masks[1], directory_2);

    out_1 = vorrq_u32(out_1, out_2);

    uint32x2_t low = vget_low_u32(out_1);
    uint32x2_t high = vget_high_u32(out_1);
    low = vorr_u32(low, high);
    uint32_t res = vget_lane_u32(low, 0) | vget_lane_u32(low, 1);
    return !(res);
#else
    uint32_t masks[kBucketWords];
    make_find_mask(hash, masks);
    for (int i = 0; i < kBucketWords; ++i) {
        if ((DCHECK_NOTNULL(_directory)[bucket_idx][i] & masks[i]) == 0) {
            return false;
        }
    }
    return true;
#endif
}

void BlockBloomFilter::insert_no_avx2(const uint32_t hash) noexcept {
    _always_false = false;
    const uint32_t bucket_idx = rehash32to32(hash) & _directory_mask;
    bucket_insert(bucket_idx, hash);
}

// To set 8 bits in an 32-byte Bloom filter, we set one bit in each 32-bit uint32_t. This
// is a "split Bloom filter", and it has approximately the same false positive probability
// as standard a Bloom filter; See Mitzenmacher's "Bloom Filters and Such". It also has
// the advantage of requiring fewer random bits: log2(32) * 8 = 5 * 8 = 40 random bits for
// a split Bloom filter, but log2(256) * 8 = 64 random bits for a standard Bloom filter.
void BlockBloomFilter::insert(const uint32_t hash) noexcept {
    _always_false = false;
    const uint32_t bucket_idx = rehash32to32(hash) & _directory_mask;
#ifdef __AVX2__
    bucket_insert_avx2(bucket_idx, hash);
#else
    bucket_insert(bucket_idx, hash);
#endif
}

void BlockBloomFilter::or_equal_array_internal(size_t n, const uint8_t* __restrict__ in,
                                               uint8_t* __restrict__ out) {
#ifdef __AVX2__
    BlockBloomFilter::or_equal_array_avx2(n, in, out);
#else
    BlockBloomFilter::or_equal_array_no_avx2(n, in, out);
#endif
}

Status BlockBloomFilter::or_equal_array(size_t n, const uint8_t* __restrict__ in,
                                        uint8_t* __restrict__ out) {
    if ((n % kBucketByteSize) != 0) {
        return Status::InvalidArgument("Input size {} not a multiple of 32-bytes", n);
    }

    or_equal_array_internal(n, in, out);

    return Status::OK();
}

void BlockBloomFilter::or_equal_array_no_avx2(size_t n, const uint8_t* __restrict__ in,
                                              uint8_t* __restrict__ out) {
#if defined(__SSE4_2__) || defined(__aarch64__)
    // The trivial loop out[i] |= in[i] should auto-vectorize with gcc at -O3, but it is not
    // written in a way that is very friendly to auto-vectorization. Instead, we manually
    // vectorize, increasing the speed by up to 56x.
    const __m128i* simd_in = reinterpret_cast<const __m128i*>(in);
    const __m128i* const simd_in_end = reinterpret_cast<const __m128i*>(in + n);
    __m128i* simd_out = reinterpret_cast<__m128i*>(out);
    // in.directory has a size (in bytes) that is a multiple of 32. Since sizeof(__m128i)
    // == 16, we can do two _mm_or_si128's in each iteration without checking array
    // bounds.
    while (simd_in != simd_in_end) {
        for (int i = 0; i < 2; ++i, ++simd_in, ++simd_out) {
            _mm_storeu_si128(simd_out,
                             _mm_or_si128(_mm_loadu_si128(simd_out), _mm_loadu_si128(simd_in)));
        }
    }
#else
    for (int i = 0; i < n; ++i) {
        out[i] |= in[i];
    }
#endif
}

Status BlockBloomFilter::merge(const BlockBloomFilter& other) {
    // AlwaysTrueFilter is a special case implemented with a nullptr.
    // Hence merge'ing with an AlwaysTrueFilter will result in a Bloom filter that also
    // always returns true which'll require destructing this Bloom filter.
    // Moreover for a reference "other" to be an AlwaysTrueFilter the reference needs
    // to be created from a nullptr and so we get into undefined behavior territory.
    // Comparing AlwaysTrueFilter with "&other" results in a compiler warning for
    // comparing a non-null argument "other" with nullptr [-Wnonnull-compare].
    // For above reasons, guard against it.
    CHECK_NE(kAlwaysTrueFilter, &other);

    if (this == &other) {
        // No op.
        return Status::OK();
    }
    if (directory_size() != other.directory_size()) {
        return Status::InvalidArgument("Directory size don't match. this: {}, other: {}",
                                       directory_size(), other.directory_size());
    }
    if (other.always_false()) {
        // Nothing to do.
        return Status::OK();
    }

    or_equal_array_internal(directory_size(), reinterpret_cast<const uint8_t*>(other._directory),
                            reinterpret_cast<uint8_t*>(_directory));

    _always_false = false;
    return Status::OK();
}

} // namespace doris
