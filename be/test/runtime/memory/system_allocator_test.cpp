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

#include "runtime/memory/system_allocator.h"

#include <gtest/gtest.h>

#include "common/config.h"

namespace doris {

template <bool use_mmap>
void test_normal() {
    config::use_mmap_allocate_chunk = use_mmap;
    {
        auto ptr = SystemAllocator::allocate(4096);
        EXPECT_NE(nullptr, ptr);
        EXPECT_EQ(0, (uint64_t)ptr % 4096);
        SystemAllocator::free(ptr, 4096);
    }
    {
        auto ptr = SystemAllocator::allocate(100);
        EXPECT_NE(nullptr, ptr);
        EXPECT_EQ(0, (uint64_t)ptr % 4096);
        SystemAllocator::free(ptr, 100);
    }
}

TEST(SystemAllocatorTest, TestNormal) {
    test_normal<true>();
    test_normal<false>();
}

} // namespace doris
