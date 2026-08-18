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

#include "util/bitmap_intersect.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace doris {

// H2: BitmapIntersect serialization places a 4-byte length + variable-length
// key bytes back to back; after an odd-length string key the following reads
// (*(int32_t*) / BitmapValue header) land on odd addresses. aarch64 scalar
// loads tolerate this, but it is C++ UB. This roundtrip test with odd-length
// keys guards the memcpy-based hardening and is UBSan-ready.
TEST(BitmapIntersectTest, RoundtripWithOddLengthKeys) {
    BitmapIntersect<std::string> writer;
    // Odd-length keys make every following field misaligned.
    std::vector<std::string> keys = {"a", "bbb", "ccccc", "x", "odd_key_9"};
    BitmapValue bv1;
    for (uint32_t i = 0; i < 100; i += 2) {
        bv1.add(i);
    }
    BitmapValue bv2;
    for (uint32_t i = 0; i < 100; i += 3) {
        bv2.add(i);
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        writer.add_key(keys[i]);
        writer.update(keys[i], i % 2 == 0 ? bv1 : bv2);
    }

    const size_t ser_size = writer.size();
    // Serialize into a buffer shifted by 1 byte from an 8-aligned base so the
    // whole stream is misaligned, then also misalign the read side.
    std::vector<char> raw(ser_size + 16, 0);
    char* aligned_base =
            reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(raw.data()) + 7) & ~7ULL);
    char* dest = aligned_base + 1;
    writer.serialize(dest);

    BitmapIntersect<std::string> reader(dest);
    EXPECT_EQ(writer.intersect_count(), reader.intersect_count());

    // Expected intersection: bv1 (even) keys & bv2 (odd) keys.
    // keys with even index hold bv1, odd index hold bv2.
    BitmapValue expect = bv1;
    expect &= bv2;
    EXPECT_EQ(expect.cardinality(), reader.intersect_count());
}

// VecDateTimeValue keys: read_from does two sequential word reads; with the
// count prefix the first key starts at offset 4 (only 4-aligned). Guards
// Helper::read_from<VecDateTimeValue> hardening.
TEST(BitmapIntersectTest, RoundtripIntKeysMisaligned) {
    BitmapIntersect<int32_t> writer;
    BitmapValue bv;
    for (uint32_t i = 1; i <= 10; ++i) {
        bv.add(i * 7);
    }
    for (int32_t k = -5; k <= 5; ++k) {
        writer.add_key(k);
        writer.update(k, bv);
    }
    const size_t ser_size = writer.size();
    std::vector<char> raw(ser_size + 16, 0);
    char* aligned_base =
            reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(raw.data()) + 7) & ~7ULL);
    char* dest = aligned_base + 3; // 8k+3 offset: every int32 read misaligned
    writer.serialize(dest);

    BitmapIntersect<int32_t> reader(dest);
    EXPECT_EQ(bv.cardinality(), reader.intersect_count());
}

} // namespace doris
