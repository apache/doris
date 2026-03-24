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

#include "cpp/lru_cache.h"

#include <atomic>

#include "gtest/gtest.h"

namespace doris {

class CloudLRUCacheTest : public ::testing::Test {
protected:
    struct TestValue {
        explicit TestValue(int value_) : value(value_) {}
        ~TestValue() { ++deleted_count; }

        int value;
        static inline std::atomic<int> deleted_count = 0;
    };

    void SetUp() override { TestValue::deleted_count.store(0); }
};

TEST_F(CloudLRUCacheTest, CanUseSharedShardedLRUCache) {
    ShardedLRUCache cache("cloud_ut", 2, LRUCacheType::NUMBER, 1, 0, false);

    Cache::Handle* handle = cache.insert(CacheKey("k1"), new TestValue(1), 1, CachePriority::NORMAL,
                                         cache_value_deleter<TestValue>);
    ASSERT_NE(handle, nullptr);
    cache.release(handle);

    handle = cache.lookup(CacheKey("k1"));
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(static_cast<TestValue*>(cache.value(handle))->value, 1);
    cache.release(handle);
}

TEST_F(CloudLRUCacheTest, EvictionUsesConfiguredDeleter) {
    ShardedLRUCache cache("cloud_ut", 2, LRUCacheType::NUMBER, 1, 0, false);

    cache.release(cache.insert(CacheKey("k1"), new TestValue(1), 1, CachePriority::NORMAL,
                               cache_value_deleter<TestValue>));
    cache.release(cache.insert(CacheKey("k2"), new TestValue(2), 1, CachePriority::NORMAL,
                               cache_value_deleter<TestValue>));
    cache.release(cache.insert(CacheKey("k3"), new TestValue(3), 1, CachePriority::NORMAL,
                               cache_value_deleter<TestValue>));

    EXPECT_EQ(TestValue::deleted_count.load(), 1);
    EXPECT_EQ(cache.lookup(CacheKey("k1")), nullptr);
}

} // namespace doris
