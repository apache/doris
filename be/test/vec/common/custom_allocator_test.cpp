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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/tests/gtest_pod_array.cpp
// and modified by Doris

#include "vec/common/custom_allocator.h"

#include <gtest/gtest.h>

#include <memory>

namespace doris {
TEST(CustomAllocatorTest, StdAllocatorBasic) {
    using Alloc = CustomStdAllocator<int32_t>;
    Alloc alloc;

    int32_t* buf = alloc.allocate(10);
    for (int i = 0; i < 10; ++i) {
        buf[i] = i;
    }
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(buf[i], i);
    }
    alloc.deallocate(buf, 10);
}

TEST(DorisUniqueBufferPtrTest, Basic) {
    auto buf = make_unique_buffer<int32_t>(10);
    for (int32_t i = 0; i != 10; ++i) {
        buf[i] = i;
    }
    for (int32_t i = 0; i != 10; ++i) {
        ASSERT_EQ(buf[i], i);
    }

    DorisUniqueBufferPtr<int32_t> buf2 = std::move(buf);
    for (int32_t i = 0; i != 10; ++i) {
        ASSERT_EQ(buf2[i], i);
    }

    ASSERT_EQ(buf, nullptr);

    auto* ptr = buf2.release();
    for (int32_t i = 0; i != 10; ++i) {
        ASSERT_EQ(ptr[i], i);
    }

    ASSERT_EQ(buf2, nullptr);
    Allocator<false> {}.free(ptr, 10 * sizeof(int32_t));

    DorisUniqueBufferPtr<int32_t> buf3(100);
    ASSERT_NE(buf3, nullptr);

    buf3.reset();
    ASSERT_EQ(buf3, nullptr);

    DorisUniqueBufferPtr<int32_t>::Deleter deleter(50);
    auto unique_ptr = std::unique_ptr<int32_t[], DorisUniqueBufferPtr<int32_t>::Deleter>(
            static_cast<int32_t*>(deleter.alloc(50 * sizeof(int32_t))), std::move(deleter));

    DorisUniqueBufferPtr<int32_t> buf4(std::move(unique_ptr));
    ASSERT_NE(buf4, nullptr);

    ptr = buf4.get();

    DorisUniqueBufferPtr<int32_t> buf5(std::move(buf4));

    ASSERT_EQ(ptr, buf5.get());
    ASSERT_EQ(ptr, buf5);

    DorisUniqueBufferPtr<int32_t> buf6 = nullptr;
    ASSERT_EQ(buf6, nullptr);
}

}; // namespace doris