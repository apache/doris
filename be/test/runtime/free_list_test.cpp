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

#include "runtime/free_list.hpp"

#include <gtest/gtest.h>

#include <string>

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

TEST(FreeListTest, Basic) {
    MemTracker tracker;
    MemPool pool(&tracker);
    FreeList list;

    int allocated_size;
    uint8_t* free_list_mem = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_EQ(nullptr, free_list_mem);
    EXPECT_EQ(allocated_size, 0);

    uint8_t* mem = pool.allocate(FreeList::min_size());
    EXPECT_TRUE(mem != nullptr);

    list.add(mem, FreeList::min_size());
    free_list_mem = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_EQ(mem, free_list_mem);
    EXPECT_EQ(allocated_size, FreeList::min_size());

    free_list_mem = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_EQ(nullptr, free_list_mem);
    EXPECT_EQ(allocated_size, 0);

    // Make 3 allocations and add them to the free list.
    // Get them all back from the free list, scribbling to the
    // returned memory in between.
    // Attempt a 4th allocation from the free list and make sure
    // we get nullptr.
    // Repeat with the same memory blocks.
    uint8_t* free_list_mem1 = nullptr;
    uint8_t* free_list_mem2 = nullptr;
    uint8_t* free_list_mem3 = nullptr;

    mem = pool.allocate(FreeList::min_size());
    list.add(mem, FreeList::min_size());
    mem = pool.allocate(FreeList::min_size());
    list.add(mem, FreeList::min_size());
    mem = pool.allocate(FreeList::min_size());
    list.add(mem, FreeList::min_size());

    free_list_mem1 = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_TRUE(free_list_mem1 != nullptr);
    EXPECT_EQ(allocated_size, FreeList::min_size());
    bzero(free_list_mem1, FreeList::min_size());

    free_list_mem2 = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_TRUE(free_list_mem2 != nullptr);
    EXPECT_EQ(allocated_size, FreeList::min_size());
    bzero(free_list_mem2, FreeList::min_size());

    free_list_mem3 = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_TRUE(free_list_mem3 != nullptr);
    EXPECT_EQ(allocated_size, FreeList::min_size());
    bzero(free_list_mem3, FreeList::min_size());

    free_list_mem = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_EQ(nullptr, free_list_mem);
    EXPECT_EQ(allocated_size, 0);

    list.add(free_list_mem1, FreeList::min_size());
    list.add(free_list_mem2, FreeList::min_size());
    list.add(free_list_mem3, FreeList::min_size());

    free_list_mem1 = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_TRUE(free_list_mem1 != nullptr);
    EXPECT_EQ(allocated_size, FreeList::min_size());
    bzero(free_list_mem1, FreeList::min_size());

    free_list_mem2 = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_TRUE(free_list_mem2 != nullptr);
    EXPECT_EQ(allocated_size, FreeList::min_size());
    bzero(free_list_mem2, FreeList::min_size());

    free_list_mem3 = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_TRUE(free_list_mem3 != nullptr);
    EXPECT_EQ(allocated_size, FreeList::min_size());
    bzero(free_list_mem3, FreeList::min_size());

    free_list_mem = list.allocate(FreeList::min_size(), &allocated_size);
    EXPECT_EQ(nullptr, free_list_mem);
    EXPECT_EQ(allocated_size, 0);

    // Try some allocations with different sizes
    int size1 = FreeList::min_size();
    int size2 = FreeList::min_size() * 2;
    int size4 = FreeList::min_size() * 4;

    uint8_t* mem1 = pool.allocate(size1);
    uint8_t* mem2 = pool.allocate(size2);
    uint8_t* mem4 = pool.allocate(size4);

    list.add(mem2, size2);
    free_list_mem = list.allocate(size4, &allocated_size);
    EXPECT_EQ(nullptr, free_list_mem);
    EXPECT_EQ(allocated_size, 0);

    free_list_mem = list.allocate(size1, &allocated_size);
    EXPECT_TRUE(free_list_mem != nullptr);
    EXPECT_EQ(allocated_size, size2);
    bzero(free_list_mem, size1);

    free_list_mem = list.allocate(size1, &allocated_size);
    EXPECT_EQ(nullptr, free_list_mem);
    EXPECT_EQ(allocated_size, 0);

    list.add(mem2, size2);
    list.add(mem4, size4);
    list.add(mem1, size1);

    free_list_mem = list.allocate(size4, &allocated_size);
    EXPECT_EQ(mem4, free_list_mem);
    EXPECT_EQ(allocated_size, size4);
    bzero(free_list_mem, size4);

    free_list_mem = list.allocate(size2, &allocated_size);
    EXPECT_EQ(mem2, free_list_mem);
    EXPECT_EQ(allocated_size, size2);
    bzero(free_list_mem, size2);

    free_list_mem = list.allocate(size1, &allocated_size);
    EXPECT_EQ(mem1, free_list_mem);
    EXPECT_EQ(allocated_size, size1);
    bzero(free_list_mem, size1);
}

} // namespace doris
