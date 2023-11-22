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

#include "vec/common/allocator.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "vec/common/allocator_fwd.h"

namespace doris {

template <typename T>
void test_allocator(T allocator) {
    auto ptr = allocator.alloc(4096);
    EXPECT_NE(nullptr, ptr);
    ptr = allocator.realloc(ptr, 4096, 4096 * 1024);
    EXPECT_NE(nullptr, ptr);
    allocator.free(ptr, 4096 * 1024);

    ptr = allocator.alloc(100);
    EXPECT_NE(nullptr, ptr);
    ptr = allocator.realloc(ptr, 100, 100 * 1024);
    EXPECT_NE(nullptr, ptr);
    allocator.free(ptr, 100 * 1024);
}

void test_normal() {
    {
        test_allocator(Allocator<false, false, false>());
        test_allocator(Allocator<false, false, true>());
        test_allocator(Allocator<false, true, false>());
        test_allocator(Allocator<false, true, true>());
        test_allocator(Allocator<true, false, false>());
        test_allocator(Allocator<true, false, true>());
        test_allocator(Allocator<true, true, false>());
        test_allocator(Allocator<true, true, true>());
    }
}

TEST(AllocatorTest, TestNormal) {
    test_normal();
}

} // namespace doris
