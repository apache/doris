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
//
// This file is copied from
// https://github.com/apache/impala/blob/master/be/src/util/lru-multi-cache-test.cc
// and modified by Doris

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "util/lru_multi_cache.inline.h"

struct CollidingKey {
    CollidingKey(const std::string& s) : s(s) {}
    CollidingKey(const char* s) : s(s) {}
    std::string s;
};

bool operator==(const CollidingKey& k1, const CollidingKey& k2) {
    return k1.s == k2.s;
}

template <>
struct std::hash<CollidingKey> {
    size_t operator()(const CollidingKey& k) const noexcept { return 0; }
};

namespace doris {

struct TestType {
    // Testing emplace_and_get with perfect forwarding
    explicit TestType(int i, float) : i(i) {}
    int i;

    // Making sure nothing is being copied or moved
    TestType(const TestType&) = delete;
    TestType(TestType&&) = delete;

    TestType& operator=(const TestType&) = delete;
    TestType& operator=(const TestType&&) = delete;
};

class LruMultiCacheTest : public testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}
};

// Notation for implied state of inner data structure:
// 108 -> 107 -> .... -> 102 | (19) (18) (109)
// LRU list has 108 107 106 105 104 103 102 in this order, 7 elements available
// 19, 18 and 109 are currently in use
// 7 + 3 = 10 total elements in cache

TEST(LruMultiCacheTest, BasicTests) {
    LruMultiCache<std::string, TestType> cache(100);

    const size_t num_of_parallel_keys = 5;
    std::string keys[num_of_parallel_keys] = {"a", "b", "c", "d", "e"};
    int value_bases[num_of_parallel_keys] = {1, 10, 100, 1000, 10000};

    for (size_t i = 0; i < 5; i++) {
        std::string& key = keys[i];
        int& value_base = value_bases[i];

        // {}
        ASSERT_EQ(nullptr, cache.get(key).get());

        auto value = cache.emplace_and_get(key, value_base, 1.0f);

        ASSERT_EQ(nullptr, cache.get(key).get());

        value.release();

        // 1
        value = cache.get(key);

        ASSERT_NE(nullptr, value.get());

        value.release();

        auto value2 = cache.emplace_and_get(key, value_base + 1, 1.0f);

        value = cache.get(key);

        ASSERT_EQ(value_base, value.get()->i);

        value.release();
        value2.release();
        // 2 -> 1
    }

    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(10, cache.number_of_available_objects());

    for (size_t i = 0; i < 5; i++) {
        std::string& key = keys[i];
        int& value_base = value_bases[i];

        // 2 -> 1
        auto value2 = cache.get(key);

        ASSERT_EQ(value_base + 1, value2.get()->i);

        auto value = cache.get(key);

        ASSERT_EQ(value_base, value.get()->i);

        value2.release();
        value.release();

        // 1 -> 2

        value = cache.get(key);

        ASSERT_EQ(value_base, value.get()->i);

        cache.rehash();

        value2 = cache.get(key);

        ASSERT_EQ(value_base + 1, value2.get()->i);

        value.release();
        value2.release();
        // 2 -> 1

        cache.rehash();
    }
}

TEST(LruMultiCacheTest, EvictionTests) {
    const size_t cache_capacity = 10;
    LruMultiCache<std::string, TestType> cache(cache_capacity);
    std::string key = "a";

    for (size_t i = 0; i < cache_capacity; i++) {
        cache.emplace_and_get(key, i, 1.0f).release();
        ASSERT_EQ(i + 1, cache.size());
        ASSERT_EQ(i + 1, cache.number_of_available_objects());
    }

    // 9 -> 8 .... -> 0
    auto value = cache.get(key);

    ASSERT_EQ(9, value.get()->i);
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(9, cache.number_of_available_objects());

    value.release();

    for (size_t i = 0; i < cache_capacity; i++) {
        cache.emplace_and_get(key, 10 + i, 1.0f).release();
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(10, cache.number_of_available_objects());
    }

    // 19 -> 18 .... -> 10
    value = cache.get(key);
    ASSERT_EQ(19, value.get()->i);

    // 18 .... -> 10 | (19)
    auto value2 = cache.get(key);
    ASSERT_EQ(18, value2.get()->i);

    // 17 -> 16 .... -> 10 | (19) (18)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(8, cache.number_of_available_objects());

    cache.rehash();

    for (size_t i = 0; i < cache_capacity; i++) {
        cache.emplace_and_get(key, 100 + i, 1.0f).release();
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(8, cache.number_of_available_objects());
    }

    // 109 -> 108 .... -> 102 | (19) (18)
    auto value3 = cache.get(key);
    ASSERT_EQ(109, value3.get()->i);

    // 108 .... -> 102 | (19) (18) (109)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(7, cache.number_of_available_objects());

    value2.release();

    // 18 -> 108 .... -> 102 | (19) (109)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(8, cache.number_of_available_objects());

    auto value4 = cache.get(key);
    ASSERT_EQ(18, value4.get()->i);

    // 108 .... -> 102 | (19) (109) (18)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(7, cache.number_of_available_objects());

    value.release();
    value3.release();
    value4.release();

    for (size_t i = 0; i < cache_capacity; i++) {
        cache.emplace_and_get(key, 1000 + i, 1.0f).release();
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(10, cache.number_of_available_objects());
    }

    // 1009 .... -> 1000

    std::vector<LruMultiCache<std::string, TestType>::Accessor> values;

    for (size_t i = 0; i < cache_capacity; i++) {
        values.push_back(cache.get(key));
        ASSERT_EQ(1000 + (cache_capacity - 1 - i), values[i].get()->i);
    }

    // {} | (1009) ... (1000)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(0, cache.number_of_available_objects());

    value = cache.emplace_and_get(key, 10000, 1.0f);
    ASSERT_EQ(10000, value.get()->i);

    // {} | (10001) (1009) ... (1000)
    ASSERT_EQ(11, cache.size());
    ASSERT_EQ(0, cache.number_of_available_objects());

    value2 = cache.emplace_and_get(key, 10001, 1.0f);
    ASSERT_EQ(10001, value2.get()->i);

    // {} | (10001) (10000) (1009) ... (1000)
    ASSERT_EQ(12, cache.size());
    ASSERT_EQ(0, cache.number_of_available_objects());

    values[0].release();
    values[1].release();

    // {} | (10001) (10000) (1007) ... (1000)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(0, cache.number_of_available_objects());

    for (size_t i = 2; i < cache_capacity; i++) {
        values[i].release();
    }

    // 1000 -> ... -> 1007 | (10001) (10000)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(8, cache.number_of_available_objects());

    value3 = cache.get(key);
    ASSERT_EQ(1000, value3.get()->i);

    // 1001 -> ... -> 1007 | (10001) (10000) (1000)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(7, cache.number_of_available_objects());

    value2.release();

    // 10001 -> 1001 -> ... -> 1007 | (10000) (1000)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(8, cache.number_of_available_objects());

    value4 = cache.get(key);
    ASSERT_EQ(10001, value4.get()->i);

    // 1001 -> ... -> 1007 | (10001) (10000) (1000)
    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(7, cache.number_of_available_objects());
}

TEST(LruMultiCacheTest, AutoRelease) {
    const size_t cache_capacity = 10;
    LruMultiCache<std::string, TestType> cache(cache_capacity);
    std::string key = "a";

    for (size_t i = 0; i < cache_capacity; i++) {
        {
            auto accessor = cache.emplace_and_get(key, i, 1.0f);
        }
        ASSERT_EQ(i + 1, cache.size());
        ASSERT_EQ(i + 1, cache.number_of_available_objects());
    }

    // 9 -> 8 .... -> 0
    {
        auto value = cache.get(key);

        ASSERT_EQ(9, value.get()->i);
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(9, cache.number_of_available_objects());
    }

    for (size_t i = 0; i < cache_capacity; i++) {
        {
            auto accessor = cache.emplace_and_get(key, 10 + i, 1.0f);
        }
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(10, cache.number_of_available_objects());
    }
}

TEST(LruMultiCacheTest, destroy) {
    const size_t cache_capacity = 10;
    LruMultiCache<std::string, TestType> cache(cache_capacity);
    std::string key = "a";

    for (size_t i = 0; i < cache_capacity; i++) {
        {
            auto accessor = cache.emplace_and_get(key, i, 1.0f);
        }
        ASSERT_EQ(i + 1, cache.size());
        ASSERT_EQ(i + 1, cache.number_of_available_objects());
    }

    // 9 -> 8 .... -> 0
    {
        auto value = cache.get(key);

        ASSERT_EQ(9, value.get()->i);
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(9, cache.number_of_available_objects());

        // 8 -> .... -> 0
        value.destroy();
        ASSERT_EQ(9, cache.size());
        ASSERT_EQ(9, cache.number_of_available_objects());
    }

    ASSERT_EQ(9, cache.size());
    ASSERT_EQ(9, cache.number_of_available_objects());

    for (size_t i = 0; i < cache_capacity; i++) {
        {
            auto accessor = cache.emplace_and_get(key, 10 + i, 1.0f);
        }
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(10, cache.number_of_available_objects());
    }
}

TEST(LruMultiCacheTest, RemoveEmptyList) {
    const size_t cache_capacity = 10;
    LruMultiCache<std::string, TestType> cache(cache_capacity);
    std::string key = "a";

    ASSERT_EQ(0, cache.number_of_keys());

    auto accessor = cache.emplace_and_get(key, 0, 1.0f);
    ASSERT_EQ(1, cache.number_of_keys());

    // Last element is destroyed in "a" list
    accessor.destroy();
    ASSERT_EQ(0, cache.number_of_keys());

    // Removed by eviction

    for (size_t i = 0; i < cache_capacity; i++) {
        cache.emplace_and_get(key, i, 1.0f).release();
        ASSERT_EQ(i + 1, cache.size());
        ASSERT_EQ(i + 1, cache.number_of_available_objects());
    }

    // All in "a" list
    ASSERT_EQ(1, cache.number_of_keys());

    std::string key2 = "b";

    for (size_t i = 0; i < (cache_capacity - 1); i++) {
        cache.emplace_and_get(key2, 10 + i, 1.0f).release();
        ASSERT_EQ(10, cache.size());
        ASSERT_EQ(10, cache.number_of_available_objects());

        // 9-i in "a" list, i+1 in "b" list
        ASSERT_EQ(2, cache.number_of_keys());
    }

    cache.emplace_and_get(key2, 19, 1.0f).release();

    // 10 in "b" list, "a" is removed
    ASSERT_EQ(1, cache.number_of_keys());
}

TEST(LruMultiCacheTest, HashCollision) {
    const size_t cache_capacity = 10;
    LruMultiCache<CollidingKey, TestType> cache(cache_capacity);

    const size_t num_of_parallel_keys = 5;
    CollidingKey keys[num_of_parallel_keys] = {"a", "b", "c", "d", "e"};
    int value_bases[num_of_parallel_keys] = {1, 10, 100, 1000, 10000};

    for (size_t i = 0; i < 5; i++) {
        CollidingKey& key = keys[i];
        int& value_base = value_bases[i];

        // {}
        ASSERT_EQ(nullptr, cache.get(key).get());

        auto value = cache.emplace_and_get(key, value_base, 1.0f);

        ASSERT_EQ(nullptr, cache.get(key).get());

        value.release();

        // 1
        value = cache.get(key);

        ASSERT_NE(nullptr, value.get());

        value.release();

        auto value2 = cache.emplace_and_get(key, value_base + 1, 1.0f);

        value = cache.get(key);

        ASSERT_EQ(value_base, value.get()->i);

        value.release();
        value2.release();
        // 2 -> 1
    }

    ASSERT_EQ(10, cache.size());
    ASSERT_EQ(10, cache.number_of_available_objects());

    for (size_t i = 0; i < 5; i++) {
        CollidingKey& key = keys[i];
        int& value_base = value_bases[i];

        // 2 -> 1
        auto value2 = cache.get(key);

        ASSERT_EQ(value_base + 1, value2.get()->i);

        auto value = cache.get(key);

        ASSERT_EQ(value_base, value.get()->i);

        value2.release();
        value.release();

        // 1 -> 2

        value = cache.get(key);

        ASSERT_EQ(value_base, value.get()->i);

        cache.rehash();

        value2 = cache.get(key);

        ASSERT_EQ(value_base + 1, value2.get()->i);

        value.release();
        value2.release();
        // 2 -> 1

        cache.rehash();
    }
}

} // namespace doris
