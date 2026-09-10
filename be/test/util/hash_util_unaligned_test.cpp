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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include "util/hash_util.hpp"

namespace doris {

// H2: HashUtil::murmur_hash2_64 loads uint64_t words through
// reinterpret_cast of the caller buffer. Hashing the same content at every
// byte misalignment must yield the reference value (computed from an aligned
// copy). Guards the unaligned_load hardening; UBSan-ready.
TEST(HashUtilUnalignedTest, MurmurHash2FromMisalignedBuffers) {
    std::mt19937_64 rng(20260815);
    for (int len : {1, 7, 8, 9, 15, 16, 17, 64, 100, 1000}) {
        std::vector<uint8_t> backing(len + 16);
        uint8_t* aligned_base =
                reinterpret_cast<uint8_t*>((reinterpret_cast<uintptr_t>(backing.data()) + 7) &
                                           ~7ULL);
        std::vector<uint8_t> content(len);
        for (auto& b : content) {
            b = static_cast<uint8_t>(rng());
        }
        const uint64_t seed = 0x9e3779b97f4a7c15ULL;
        // reference on aligned copy
        memcpy(aligned_base, content.data(), len);
        uint64_t ref = HashUtil::murmur_hash2_64(aligned_base, len, seed);
        for (int offset = 1; offset < 8; ++offset) {
            memcpy(aligned_base + offset, content.data(), len);
            uint64_t got = HashUtil::murmur_hash2_64(aligned_base + offset, len, seed);
            EXPECT_EQ(ref, got) << "len=" << len << " offset=" << offset;
        }
    }
}

// Same guard for HashUtil::crc_hash, which feeds word loads to _mm_crc32_u32
// (sse2neon on aarch64). Reference is computed on the aligned copy.
TEST(HashUtilUnalignedTest, CrcHashFromMisalignedBuffers) {
    std::mt19937_64 rng(20260815);
    for (int len : {1, 7, 8, 9, 15, 16, 17, 64, 100, 1000}) {
        std::vector<uint8_t> backing(len + 16);
        uint8_t* aligned_base =
                reinterpret_cast<uint8_t*>((reinterpret_cast<uintptr_t>(backing.data()) + 7) &
                                           ~7ULL);
        std::vector<uint8_t> content(len);
        for (auto& b : content) {
            b = static_cast<uint8_t>(rng());
        }
        const uint32_t seed = 0x9e3779b9U;
        // reference on aligned copy
        memcpy(aligned_base, content.data(), len);
        uint32_t ref = HashUtil::crc_hash(aligned_base, len, seed);
        for (int offset = 1; offset < 8; ++offset) {
            memcpy(aligned_base + offset, content.data(), len);
            uint32_t got = HashUtil::crc_hash(aligned_base + offset, len, seed);
            EXPECT_EQ(ref, got) << "len=" << len << " offset=" << offset;
        }
    }
}

// Same guard for HashUtil::crc_hash64, the 64-bit variant built on the same
// _mm_crc32_* word loads.
TEST(HashUtilUnalignedTest, CrcHash64FromMisalignedBuffers) {
    std::mt19937_64 rng(20260815);
    for (int len : {1, 7, 8, 9, 15, 16, 17, 64, 100, 1000}) {
        std::vector<uint8_t> backing(len + 16);
        uint8_t* aligned_base =
                reinterpret_cast<uint8_t*>((reinterpret_cast<uintptr_t>(backing.data()) + 7) &
                                           ~7ULL);
        std::vector<uint8_t> content(len);
        for (auto& b : content) {
            b = static_cast<uint8_t>(rng());
        }
        const uint64_t seed = 0x9e3779b97f4a7c15ULL;
        // reference on aligned copy
        memcpy(aligned_base, content.data(), len);
        uint64_t ref = HashUtil::crc_hash64(aligned_base, len, seed);
        for (int offset = 1; offset < 8; ++offset) {
            memcpy(aligned_base + offset, content.data(), len);
            uint64_t got = HashUtil::crc_hash64(aligned_base + offset, len, seed);
            EXPECT_EQ(ref, got) << "len=" << len << " offset=" << offset;
        }
    }
}

} // namespace doris
