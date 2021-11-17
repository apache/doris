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

#include "runtime/mem_pool.h"

#include <gtest/gtest.h>

#include <string>

#include "runtime/mem_tracker.h"
#include "util/logging.h"

namespace doris {

TEST(MemPoolTest, Basic) {
    MemTracker tracker(-1);
    MemPool p(&tracker);
    MemPool p2(&tracker);
    MemPool p3(&tracker);

    // allocate a total of 24K in 32-byte pieces (for which we only request 25 bytes)
    for (int i = 0; i < 768; ++i) {
        // pads to 32 bytes
        p.allocate(25);
    }
    // we handed back 24K, (4, 8 16) first allocate don't need padding
    EXPECT_EQ(24 * 1024 - 3 * 7, p.total_allocated_bytes()); // 32 * 768 == 24 * 1024
    // .. and allocated 28K of chunks (4, 8, 16)
    EXPECT_EQ(28 * 1024, p.total_reserved_bytes());

    // we're passing on the first two chunks, containing 12K of data; we're left with one
    // chunk of 16K containing 12K of data
    p2.acquire_data(&p, true);
    EXPECT_EQ(12 * 1024 - 7, p.total_allocated_bytes());
    EXPECT_EQ(16 * 1024, p.total_reserved_bytes());

    // we allocate 8K, for which there isn't enough room in the current chunk,
    // so another one is allocated (32K)
    p.allocate(8 * 1024);
    EXPECT_EQ((16 + 32) * 1024, p.total_reserved_bytes());

    // we allocate 65K, which doesn't fit into the current chunk or the default
    // size of the next allocated chunk (64K)
    p.allocate(65 * 1024);
    EXPECT_EQ((12 + 8 + 65) * 1024 - 7, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 128) * 1024, p.total_reserved_bytes());

    // Clear() resets allocated data, but doesn't remove any chunks
    p.clear();
    EXPECT_EQ(0, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 128) * 1024, p.total_reserved_bytes());

    // next allocation reuses existing chunks
    p.allocate(1024);
    EXPECT_EQ(1024, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 128) * 1024, p.total_reserved_bytes());

    // ... unless it doesn't fit into any available chunk
    p.allocate(120 * 1024);
    EXPECT_EQ((1 + 120) * 1024, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 128) * 1024, p.total_reserved_bytes());

    // ... Try another chunk that fits into an existing chunk
    p.allocate(33 * 1024);
    EXPECT_EQ((1 + 120 + 33) * 1024, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 128 + 256) * 1024, p.total_reserved_bytes());

    // we're releasing 3 chunks, which get added to p2
    p2.acquire_data(&p, false);
    EXPECT_EQ(0, p.total_allocated_bytes());
    EXPECT_EQ(0, p.total_reserved_bytes());

    p3.acquire_data(&p2, true); // we're keeping the 65k chunk
    EXPECT_EQ(33 * 1024, p2.total_allocated_bytes());
    EXPECT_EQ(256 * 1024, p2.total_reserved_bytes());

    {
        MemPool p4(&tracker);
        p4.exchange_data(&p2);
        EXPECT_EQ(33 * 1024, p4.total_allocated_bytes());
        EXPECT_EQ(256 * 1024, p4.total_reserved_bytes());
    }
}

// Test that we can keep an allocated chunk and a free chunk.
// This case verifies that when chunks are acquired by another memory pool the
// remaining chunks are consistent if there were more than one used chunk and some
// free chunks.
TEST(MemPoolTest, Keep) {
    MemTracker tracker(-1);
    MemPool p(&tracker);
    p.allocate(4 * 1024);
    p.allocate(8 * 1024);
    p.allocate(16 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (4 + 8 + 16) * 1024);
    EXPECT_EQ(p.total_reserved_bytes(), (4 + 8 + 16) * 1024);
    p.clear();
    EXPECT_EQ(p.total_allocated_bytes(), 0);
    EXPECT_EQ(p.total_reserved_bytes(), (4 + 8 + 16) * 1024);
    p.allocate(1 * 1024);
    p.allocate(4 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (1 + 4) * 1024);
    EXPECT_EQ(p.total_reserved_bytes(), (4 + 8 + 16) * 1024);
    MemPool p2(&tracker);
    p2.acquire_data(&p, true);

    {
        p2.exchange_data(&p);
        EXPECT_EQ(4 * 1024, p2.total_allocated_bytes());
        EXPECT_EQ((8 + 16) * 1024, p2.total_reserved_bytes());
        EXPECT_EQ(1 * 1024, p.total_allocated_bytes());
        EXPECT_EQ(4 * 1024, p.total_reserved_bytes());
    }
}

// Maximum allocation size which exceeds 32-bit.
#define LARGE_ALLOC_SIZE (1LL << 32)

TEST(MemPoolTest, MaxAllocation) {
    int64_t int_max_rounded = BitUtil::round_up(LARGE_ALLOC_SIZE, 8);

    // Allocate a single LARGE_ALLOC_SIZE chunk
    MemTracker tracker(-1);
    MemPool p1(&tracker);
    uint8_t* ptr = p1.allocate(LARGE_ALLOC_SIZE);
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(int_max_rounded, p1.total_reserved_bytes());
    EXPECT_EQ(int_max_rounded, p1.total_allocated_bytes());
    p1.free_all();

    // Allocate a small chunk (DEFAULT_INITIAL_CHUNK_SIZE) followed by an LARGE_ALLOC_SIZE chunk
    MemPool p2(&tracker);
    p2.allocate(8);
    EXPECT_EQ(p2.total_reserved_bytes(), 4096);
    EXPECT_EQ(p2.total_allocated_bytes(), 8);
    ptr = p2.allocate(LARGE_ALLOC_SIZE);
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(p2.total_reserved_bytes(), 4096LL + int_max_rounded);
    EXPECT_EQ(p2.total_allocated_bytes(), 8LL + int_max_rounded);
    p2.free_all();

    // Allocate three LARGE_ALLOC_SIZE chunks followed by a small chunk followed by another LARGE_ALLOC_SIZE
    // chunk
    MemPool p3(&tracker);
    p3.allocate(LARGE_ALLOC_SIZE);
    // Allocates new int_max_rounded * 2 chunk
    // NOTE: exceed MAX_CHUNK_SIZE limit, will not *2
    ptr = p3.allocate(LARGE_ALLOC_SIZE);
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(int_max_rounded * 2, p3.total_reserved_bytes());
    EXPECT_EQ(int_max_rounded * 2, p3.total_allocated_bytes());
    // Uses existing int_max_rounded * 2 chunk
    ptr = p3.allocate(LARGE_ALLOC_SIZE);
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(int_max_rounded * 3, p3.total_reserved_bytes());
    EXPECT_EQ(int_max_rounded * 3, p3.total_allocated_bytes());

    // Allocates a new int_max_rounded * 4 chunk
    // NOTE: exceed MAX_CHUNK_SIZE limit, will not *2
#if !defined(ADDRESS_SANITIZER) || (__clang_major__ >= 3 && __clang_minor__ >= 7)
    ptr = p3.allocate(8);
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(int_max_rounded * 3 + 512 * 1024, p3.total_reserved_bytes());
    EXPECT_EQ(int_max_rounded * 3 + 8, p3.total_allocated_bytes());
    // Uses existing int_max_rounded * 4 chunk
    ptr = p3.allocate(LARGE_ALLOC_SIZE);
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(int_max_rounded * 4 + 512 * 1024, p3.total_reserved_bytes());
    EXPECT_EQ(int_max_rounded * 4 + 8, p3.total_allocated_bytes());
#endif
    p3.free_all();
}
} // namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::init_glog("be-test");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
