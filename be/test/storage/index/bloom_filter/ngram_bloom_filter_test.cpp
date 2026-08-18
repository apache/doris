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

#include "storage/index/bloom_filter/ngram_bloom_filter.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <cstring>
#include <vector>

namespace doris::segment_v2 {

// H1: NGramBloomFilter::init() reads the on-disk bitset through a
// reinterpret_cast<const uint64_t*> of an arbitrarily-offset page buffer.
// Feed it a deliberately misaligned buffer (offset by 1 byte) and verify the
// filter still round-trips correctly. This guards the memcpy-based hardening
// and is UBSan-ready.
TEST(NGramBloomFilterTest, InitFromUnalignedBuffer) {
    constexpr size_t kBfSize = 1024; // bytes
    NGramBloomFilter writer(kBfSize);
    const char* added[] = {"hello", "doris", "arm64", "bloom-filter"};
    for (const char* s : added) {
        writer.add_bytes(s, strlen(s));
    }

    // Copy the serialized bitset into a buffer shifted by 1 byte from an
    // 8-aligned base (vector<uint8_t> storage is not guaranteed aligned).
    std::vector<uint8_t> raw(kBfSize + 16, 0);
    uint8_t* aligned_base =
            reinterpret_cast<uint8_t*>((reinterpret_cast<uintptr_t>(raw.data()) + 7) & ~7ULL);
    memcpy(aligned_base + 1, writer.data(), kBfSize);
    const char* unaligned_buf = reinterpret_cast<const char*>(aligned_base + 1);
    ASSERT_EQ(1, static_cast<int>(reinterpret_cast<uintptr_t>(unaligned_buf) % alignof(uint64_t)));

    NGramBloomFilter reader(kBfSize);
    ASSERT_TRUE(reader.init(unaligned_buf, kBfSize, CITY_HASH_64).ok());

    // Every added entry must be found in the filter read back from the
    // unaligned buffer.
    for (const char* s : added) {
        NGramBloomFilter query(kBfSize);
        query.add_bytes(s, strlen(s));
        EXPECT_TRUE(reader.contains(query)) << "missing added entry: " << s;
    }
    // A never-added entry should (with overwhelming probability) be absent.
    NGramBloomFilter absent(kBfSize);
    const char* not_added = "definitely-not-added-string";
    absent.add_bytes(not_added, strlen(not_added));
    EXPECT_FALSE(reader.contains(absent));
}

// Content read back via init() must be identical to the source filter.
TEST(NGramBloomFilterTest, InitPreservesContent) {
    constexpr size_t kBfSize = 512;
    NGramBloomFilter writer(kBfSize);
    for (int i = 0; i < 100; ++i) {
        std::string s = "key_" + std::to_string(i);
        writer.add_bytes(s.data(), s.size());
    }
    NGramBloomFilter reader(kBfSize);
    ASSERT_TRUE(reader.init(writer.data(), kBfSize, CITY_HASH_64).ok());
    EXPECT_EQ(0, memcmp(writer.data(), reader.data(), kBfSize));
}

} // namespace doris::segment_v2
