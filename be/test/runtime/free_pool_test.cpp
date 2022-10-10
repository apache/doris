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

#include "runtime/free_pool.hpp"

#include <gtest/gtest.h>

#include <memory>

namespace doris {

class FreePoolTest : public testing::Test {
public:
    FreePoolTest() {}

protected:
    virtual void SetUp() override {
        memPool.reset(new MemPool());
        freePool.reset(new FreePool(memPool.get()));
    }
    virtual void TearDown() override {
        memPool.reset(nullptr);
        freePool.reset(nullptr);
    }

protected:
    std::unique_ptr<MemPool> memPool;
    std::unique_ptr<FreePool> freePool;
};

inline uint16_t getFreeListNodeCount(FreePool::FreeListNode* list) {
    auto count = 0;
    auto current = list;
    while (nullptr != current) {
        count++;
        current = current->next;
    }
    return count;
}

TEST_F(FreePoolTest, free) {
    const int64_t size = 16;
    auto ptr = freePool->allocate(size);

    auto node = reinterpret_cast<FreePool::FreeListNode*>(ptr - sizeof(FreePool::FreeListNode));
    auto list = node->list;
    auto beforeCount = getFreeListNodeCount(list);
    freePool->free(ptr);
    auto afterCount = getFreeListNodeCount(list);

    ASSERT_EQ(beforeCount + 1, afterCount);
}
} // namespace doris
