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

#include "vec/common/arena.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>

#include <iostream>

#include "gtest/gtest_pred_impl.h"
#include "util/bit_util.h"

namespace doris {

TEST(ArenaTest, Basic) {
    vectorized::Arena p;
    vectorized::Arena p2;
    vectorized::Arena p3;

    // allocate a total of 24K in 32-byte pieces (for which we only request 25 bytes)
    for (int i = 0; i < 768; ++i) {
        // pads to 32 bytes
        p.alloc(25);
    }
    std::cout << "ArenaTest 111 " << p.size() << ", " << p.remaining_space_in_current_chunk()
              << std::endl;
    // 4096 + 4096 * 2 < 25 * 768 < 4096 + 4096 * 2 + 4096 * 2 * 2
    EXPECT_EQ((4 + 8 + 16) * 1024, p.size());
    EXPECT_EQ(16 * 1024 - 25 * (768 - (4 * 1024 - 15) / 25 - (8 * 1024 - 15) / 25) - 15,
              p.remaining_space_in_current_chunk()); // padding 15 bytes

    // we allocate 10K, for which there isn't enough room in the current chunk,
    // so another one is allocated (32K)
    p.alloc(10 * 1024);
    std::cout << "ArenaTest 222 " << p.size() << ", " << p.remaining_space_in_current_chunk()
              << std::endl;
    EXPECT_EQ((4 + 8 + 16 + 32) * 1024, p.size());
    EXPECT_EQ(32 * 1024 - 10 * 1024 - 15, p.remaining_space_in_current_chunk());

    // we allocate 65K, which doesn't fit into the current chunk or the default
    // size of the next allocated chunk (64K)
    p.alloc(65 * 1024);
    std::cout << "ArenaTest 333 " << p.size() << ", " << p.remaining_space_in_current_chunk()
              << std::endl;
    // (max(65 * 1024 + 15, 32 * 1024 * 2) + 4096 - 1) / 4096 * 4096
    EXPECT_EQ((4 + 8 + 16 + 32) * 1024 + (65 * 1024 + 15 + 4096 - 1) / 4096 * 4096, p.size());
    EXPECT_EQ((65 * 1024 + 15 + 4096 - 1) / 4096 * 4096 - 65 * 1024 - 15,
              p.remaining_space_in_current_chunk());
}

// Maximum allocation size which exceeds 32-bit.
#define LARGE_ALLOC_SIZE (1LL << 32)

TEST(ArenaTest, MaxAllocation) {
    int64_t int_max_rounded = BitUtil::round_up(LARGE_ALLOC_SIZE, 8);
    size_t linear_growth_threshold = 128 * 1024 * 1024;

    // Allocate a single LARGE_ALLOC_SIZE chunk
    {
        vectorized::Arena p1;
        char* ptr = p1.alloc(LARGE_ALLOC_SIZE);
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(4096 + (int_max_rounded + 15 + 4096 - 1) / 4096 * 4096, p1.size());
        EXPECT_EQ((int_max_rounded + 15 + 4096 - 1) / 4096 * 4096 - int_max_rounded - 15,
                  p1.remaining_space_in_current_chunk());
    }

    // Allocate a small chunk (DEFAULT_INITIAL_CHUNK_SIZE) followed by an LARGE_ALLOC_SIZE chunk
    {
        vectorized::Arena p2;
        p2.alloc(8);
        EXPECT_EQ(p2.size(), 4096);
        EXPECT_EQ(p2.remaining_space_in_current_chunk(), 4096 - 15 - 8);
        char* ptr = p2.alloc(LARGE_ALLOC_SIZE);
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(p2.size(), 4096LL + (int_max_rounded + 15 + 4096 - 1) / 4096 * 4096);
    }

    {
        // Allocate three LARGE_ALLOC_SIZE chunks followed by a small chunk followed by another LARGE_ALLOC_SIZE
        // chunk
        vectorized::Arena p3;
        p3.alloc(LARGE_ALLOC_SIZE);
        // Allocates new int_max_rounded * 2 chunk
        // NOTE: exceed MAX_CHUNK_SIZE limit, will not *2
        char* ptr = p3.alloc(LARGE_ALLOC_SIZE);
        EXPECT_TRUE(ptr != nullptr);
        size_t size_after_grow =
                ((LARGE_ALLOC_SIZE + 15 + linear_growth_threshold - 1) / linear_growth_threshold) *
                linear_growth_threshold;
        EXPECT_EQ(4096 + (int_max_rounded + 15 + 4096 - 1) / 4096 * 4096 +
                          (size_after_grow + 4096 - 1) / 4096 * 4096,
                  p3.size());
        // Uses existing int_max_rounded * 2 chunk
        ptr = p3.alloc(LARGE_ALLOC_SIZE);
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(4096 + (int_max_rounded + 15 + 4096 - 1) / 4096 * 4096 +
                          (size_after_grow + 4096 - 1) / 4096 * 4096 * 2,
                  p3.size());
        EXPECT_EQ((size_after_grow + 4096 - 1) / 4096 * 4096 - int_max_rounded - 15,
                  p3.remaining_space_in_current_chunk());
    }
}
} // namespace doris
