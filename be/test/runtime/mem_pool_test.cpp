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

#include <string>
#include <gtest/gtest.h>

#include "runtime/mem_pool.h"
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

    // we handed back 24K
    EXPECT_EQ(p.total_allocated_bytes(), 24 * 1024); // 32 * 768 == 24 * 1024
    // .. and allocated 28K of chunks (4, 8, 16)
    EXPECT_EQ(p.get_total_chunk_sizes(), 28 * 1024);

    // we're passing on the first two chunks, containing 12K of data; we're left with one
    // chunk of 16K containing 12K of data
    p2.acquire_data(&p, true);
    EXPECT_EQ(p.total_allocated_bytes(), 12 * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), 16 * 1024);

    // we allocate 8K, for which there isn't enough room in the current chunk,
    // so another one is allocated (32K)
    p.allocate(8 * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (16 + 32) * 1024);

    // we allocate 65K, which doesn't fit into the current chunk or the default
    // size of the next allocated chunk (64K)
    p.allocate(65 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (12 + 8 + 65) * 1024);
    EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (16 + 32 + 65) * 1024);

    // Clear() resets allocated data, but doesn't remove any chunks
    p.clear();
    EXPECT_EQ(p.total_allocated_bytes(), 0);
    EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (16 + 32 + 65) * 1024);

    // next allocation reuses existing chunks
    p.allocate(1024);
    EXPECT_EQ(p.total_allocated_bytes(), 1024);
    EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (16 + 32 + 65) * 1024);

    // ... unless it doesn't fit into any available chunk
    p.allocate(120 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (1 + 120) * 1024);
    EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (130 + 16 + 32 + 65) * 1024);

    // ... Try another chunk that fits into an existing chunk
    p.allocate(33 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (1 + 120 + 33) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (130 + 65 + 16 + 32) * 1024);

    // we're releasing 3 chunks, which get added to p2
    p2.acquire_data(&p, false);
    EXPECT_EQ(p.total_allocated_bytes(), 0);
    EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), 0);

    p3.acquire_data(&p2, true);  // we're keeping the 65k chunk
    EXPECT_EQ(p2.total_allocated_bytes(), 33 * 1024);
    EXPECT_EQ(p2.get_total_chunk_sizes(), 65 * 1024);
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
    EXPECT_EQ(p.get_total_chunk_sizes(), (4 + 8 + 16) * 1024);
    p.clear();
    EXPECT_EQ(p.total_allocated_bytes(), 0);
    EXPECT_EQ(p.get_total_chunk_sizes(), (4 + 8 + 16) * 1024);
    p.allocate(1 * 1024);
    p.allocate(4 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (1 + 4) * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (4 + 8 + 16) * 1024);
    MemPool p2(&tracker);
    p2.acquire_data(&p, true);
    EXPECT_EQ(p.total_allocated_bytes(), 4 * 1024);
    EXPECT_EQ(p.get_total_chunk_sizes(), (8 + 16) * 1024);
    EXPECT_EQ(p2.total_allocated_bytes(), 1 * 1024);
    EXPECT_EQ(p2.get_total_chunk_sizes(), 4 * 1024);
}

// Tests that we can return partial allocations.
TEST(MemPoolTest, ReturnPartial) {
    MemTracker tracker(-1);
    MemPool p(&tracker);
    uint8_t* ptr = p.allocate(1024);
    EXPECT_EQ(p.total_allocated_bytes(), 1024);
    memset(ptr, 0, 1024);
    p.return_partial_allocation(1024);

    uint8_t* ptr2 = p.allocate(1024);
    EXPECT_EQ(p.total_allocated_bytes(), 1024);
    EXPECT_TRUE(ptr == ptr2);
    p.return_partial_allocation(1016);

    ptr2 = p.allocate(1016);
    EXPECT_EQ(p.total_allocated_bytes(), 1024);
    EXPECT_TRUE(ptr2 == ptr + 8);
    p.return_partial_allocation(512);
    memset(ptr2, 1, 1016 - 512);

    uint8_t* ptr3 = p.allocate(512);
    EXPECT_EQ(p.total_allocated_bytes(), 1024);
    EXPECT_TRUE(ptr3 == ptr + 512);
    memset(ptr3, 2, 512);

    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(ptr[i], 0);
    }
    for (int i = 8; i < 512; ++i) {
        EXPECT_EQ(ptr[i], 1);
    }
    for (int i = 512; i < 1024; ++i) {
        EXPECT_EQ(ptr[i], 2);
    }

    p.free_all();
}

TEST(MemPoolTest, Limits) {
    MemTracker limit3(320);
    MemTracker limit1(160, "", &limit3);
    MemTracker limit2(240, "", &limit3);

    MemPool* p1 = new MemPool(&limit1);
    EXPECT_FALSE(limit1.any_limit_exceeded());

    MemPool* p2 = new MemPool(&limit2);
    EXPECT_FALSE(limit2.any_limit_exceeded());

    // p1 exceeds a non-shared limit
    p1->allocate(80);
    EXPECT_FALSE(limit1.limit_exceeded());
    EXPECT_EQ(limit1.consumption(), 80);
    EXPECT_FALSE(limit3.limit_exceeded());
    EXPECT_EQ(limit3.consumption(), 80);

    p1->allocate(88);
    EXPECT_TRUE(limit1.limit_exceeded());
    EXPECT_EQ(limit1.consumption(), 168);
    EXPECT_FALSE(limit3.limit_exceeded());
    EXPECT_EQ(limit3.consumption(), 168);

    // p2 exceeds a shared limit
    p2->allocate(80);
    EXPECT_FALSE(limit2.limit_exceeded());
    EXPECT_EQ(limit2.consumption(), 80);
    EXPECT_FALSE(limit3.limit_exceeded());
    EXPECT_EQ(limit3.consumption(), 248);

    p2->allocate(80);
    EXPECT_FALSE(limit2.limit_exceeded());
    EXPECT_EQ(limit2.consumption(), 160);
    EXPECT_TRUE(limit3.limit_exceeded());
    EXPECT_EQ(limit3.consumption(), 328);

    // deleting pools reduces consumption
    p1->free_all();
    delete p1;
    EXPECT_EQ(limit1.consumption(), 0);
    EXPECT_EQ(limit2.consumption(), 160);
    EXPECT_EQ(limit3.consumption(), 160);

    // allocate 160 bytes from 240 byte limit.
    p2->free_all();
    EXPECT_FALSE(limit2.limit_exceeded());
    uint8_t* result = p2->try_allocate(160);
    DCHECK(result != NULL);

    // Try To allocate another 160 bytes, this should fail.
    result = p2->try_allocate(160);
    DCHECK(result == NULL);

    // Try To allocate 20 bytes, this should succeed. try_allocate() should leave the
    // pool in a functional state..
    result = p2->try_allocate(20);
    DCHECK(result != NULL);

    p2->free_all();
    delete p2;
}

TEST(MemPoolTest, MaxAllocation) {
    int64_t int_max_rounded = BitUtil::round_up(INT_MAX, 8);

    // Allocate a single INT_MAX chunk
    MemTracker tracker(-1);
    MemPool p1(&tracker);
    uint8_t* ptr = p1.allocate(INT_MAX);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_EQ(p1.get_total_chunk_sizes(), int_max_rounded);
    EXPECT_EQ(p1.total_allocated_bytes(), int_max_rounded);
    p1.free_all();

    // Allocate a small chunk (DEFAULT_INITIAL_CHUNK_SIZE) followed by an INT_MAX chunk
    MemPool p2(&tracker);
    p2.allocate(8);
    EXPECT_EQ(p2.get_total_chunk_sizes(), 4096);
    EXPECT_EQ(p2.total_allocated_bytes(), 8);
    ptr = p2.allocate(INT_MAX);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_EQ(p2.get_total_chunk_sizes(), 4096LL + int_max_rounded);
    EXPECT_EQ(p2.total_allocated_bytes(), 8LL + int_max_rounded);
    p2.free_all();

    // Allocate three INT_MAX chunks followed by a small chunk followed by another INT_MAX
    // chunk
    MemPool p3(&tracker);
    p3.allocate(INT_MAX);
    // Allocates new int_max_rounded * 2 chunk
    // NOTE: exceed MAX_CHUNK_SIZE limit, will not *2
    ptr = p3.allocate(INT_MAX);
    EXPECT_TRUE(ptr != NULL);
    // EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 3);
    EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 2);
    EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 2);
    // Uses existing int_max_rounded * 2 chunk
    ptr = p3.allocate(INT_MAX);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 3);
    EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 3);

    // Allocates a new int_max_rounded * 4 chunk
    // NOTE: exceed MAX_CHUNK_SIZE limit, will not *2
#if !defined (ADDRESS_SANITIZER) || (__clang_major__ >= 3 && __clang_minor__ >= 7)
    ptr = p3.allocate(8);
    EXPECT_TRUE(ptr != NULL);
    // EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 7);
    EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 3 + 512 * 1024);
    EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 3 + 8);
    // Uses existing int_max_rounded * 4 chunk
    ptr = p3.allocate(INT_MAX);
    EXPECT_TRUE(ptr != NULL);
    // EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 7);
    EXPECT_EQ(p3.get_total_chunk_sizes(), int_max_rounded * 4 + 512 * 1024);
    EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 4 + 8);
#endif
    p3.free_all();

}
#if 0
#endif
}

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

