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

#include <gtest/gtest.h>

#include "storage/cache/page_cache.h"

namespace doris {
namespace {
struct TestIndexObject : LRUCacheValueBase {
    explicit TestIndexObject(bool* destroyed) : destroyed(destroyed) {}
    ~TestIndexObject() override { *destroyed = true; }
    bool* destroyed;
};
using KeyKind = StoragePageCache::CacheKey::Kind;

TEST(AnnPageCacheTest, NamespacesAndFileIdentity) {
    StoragePageCache cache(1024 * 1024, 50, 0, 1);
    const StoragePageCache::CacheKey key("segment/index", 4096, 128, KeyKind::ANN_IVF_LIST, 32);
    PageCacheHandle handle;
    auto* page = new DataPage(32, cache.mem_tracker(segment_v2::DATA_PAGE));
    page->data()[0] = 'x';
    cache.insert(key, page, &handle, segment_v2::DATA_PAGE);
    PageCacheHandle found;
    ASSERT_TRUE(cache.lookup(key, &found, segment_v2::DATA_PAGE));
    EXPECT_EQ(found.data().data, handle.data().data);
    EXPECT_EQ(found.data().data[0], 'x');
    for (const auto& other :
         {StoragePageCache::CacheKey("segment/index", 4096, 128),
          StoragePageCache::CacheKey("segment/index", 4096, 128, KeyKind::ANN_INDEX),
          StoragePageCache::CacheKey("segment/other", 4096, 128, KeyKind::ANN_IVF_LIST, 32),
          StoragePageCache::CacheKey("segment/index", 8192, 128, KeyKind::ANN_IVF_LIST, 32),
          StoragePageCache::CacheKey("segment/index", 4096, 256, KeyKind::ANN_IVF_LIST, 32),
          StoragePageCache::CacheKey("segment/index", 4096, 128, KeyKind::ANN_IVF_LIST, 64)}) {
        EXPECT_FALSE(cache.lookup(other, &found, segment_v2::DATA_PAGE));
    }
}

TEST(AnnPageCacheTest, EraseRetainsPinnedObjectAndCharge) {
    StoragePageCache cache(1024 * 1024, 0, 0, 1);
    const StoragePageCache::CacheKey key("segment/index", 0, 0, KeyKind::ANN_INDEX);
    bool destroyed = false;
    PageCacheHandle handle;
    cache.insert_object(key, std::make_unique<TestIndexObject>(&destroyed), 4096, 4096, &handle);
    auto* policy = handle.cache();
    EXPECT_EQ(policy->value_mem_consumption(), 4096);
    auto* borrowed = handle.value();
    cache.erase(key, segment_v2::DATA_PAGE);
    PageCacheHandle miss;
    EXPECT_FALSE(cache.lookup(key, &miss, segment_v2::DATA_PAGE));
    EXPECT_EQ(handle.value(), borrowed);
    EXPECT_FALSE(destroyed);
    EXPECT_EQ(policy->value_mem_consumption(), 4096);
    handle = PageCacheHandle();
    EXPECT_TRUE(destroyed);
    EXPECT_EQ(policy->value_mem_consumption(), 0);
}

TEST(AnnPageCacheTest, ObjectChargeDrivesEviction) {
    StoragePageCache cache(8192, 0, 0, 1);
    const StoragePageCache::CacheKey first("first", 0, 0, KeyKind::ANN_INDEX);
    const StoragePageCache::CacheKey second("second", 0, 0, KeyKind::ANN_INDEX);
    bool first_destroyed = false;
    bool second_destroyed = false;
    PageCacheHandle handle;
    cache.insert_object(first, std::make_unique<TestIndexObject>(&first_destroyed), 6000, 6000,
                        &handle);
    handle = PageCacheHandle();
    cache.insert_object(second, std::make_unique<TestIndexObject>(&second_destroyed), 6000, 6000,
                        &handle);
    // DATA_PAGE uses LRU-K: the first insertion under pressure is not admitted.
    EXPECT_FALSE(first_destroyed);
    handle = PageCacheHandle();
    EXPECT_TRUE(second_destroyed);
    second_destroyed = false;
    cache.insert_object(second, std::make_unique<TestIndexObject>(&second_destroyed), 6000, 6000,
                        &handle);
    EXPECT_TRUE(first_destroyed);
    EXPECT_FALSE(second_destroyed);
    EXPECT_EQ(handle.cache()->value_mem_consumption(), 6000);
    PageCacheHandle miss;
    EXPECT_FALSE(cache.lookup(first, &miss, segment_v2::DATA_PAGE));
    cache.erase(second, segment_v2::DATA_PAGE);
    handle = PageCacheHandle();
    EXPECT_TRUE(second_destroyed);
}

TEST(AnnPageCacheTest, ListPagesCanEvictIndexObjectsFromTheSamePool) {
    StoragePageCache cache(8192, 0, 0, 1);
    const StoragePageCache::CacheKey index_key("segment/index", 0, 0, KeyKind::ANN_INDEX);
    const StoragePageCache::CacheKey list_key("segment/index", 8192, 0, KeyKind::ANN_IVF_LIST,
                                              6000);
    bool destroyed = false;
    PageCacheHandle object;
    cache.insert_object(index_key, std::make_unique<TestIndexObject>(&destroyed), 6000, 6000,
                        &object);
    auto* policy = object.cache();
    object = PageCacheHandle();
    PageCacheHandle list;
    // The first miss records a visit; the second admits the list and evicts the object.
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto* page = new DataPage(6000, cache.mem_tracker(segment_v2::DATA_PAGE));
        cache.insert(list_key, page, &list, segment_v2::DATA_PAGE);
        EXPECT_EQ(list.cache(), policy);
        list = PageCacheHandle();
    }
    EXPECT_TRUE(destroyed);
    EXPECT_EQ(policy->value_mem_consumption(), 0); // List bytes use Allocator tracking.
    PageCacheHandle hit;
    EXPECT_TRUE(cache.lookup(list_key, &hit, segment_v2::DATA_PAGE));
    EXPECT_FALSE(cache.lookup(index_key, &object, segment_v2::DATA_PAGE));
    cache.erase(index_key, segment_v2::DATA_PAGE);
}

TEST(AnnPageCacheTest, DisabledDataCacheStillOwnsUntilRelease) {
    StoragePageCache cache(0, 0, 0, 1);
    const StoragePageCache::CacheKey key("index", 0, 0, KeyKind::ANN_INDEX);
    bool destroyed = false;
    PageCacheHandle handle;
    cache.insert_object(key, std::make_unique<TestIndexObject>(&destroyed), 4096, 4096, &handle);
    PageCacheHandle miss;
    EXPECT_FALSE(cache.lookup(key, &miss, segment_v2::DATA_PAGE));
    EXPECT_FALSE(destroyed);
    auto* policy = handle.cache();
    EXPECT_EQ(policy->value_mem_consumption(), 4096);
    handle = PageCacheHandle();
    EXPECT_TRUE(destroyed);
    EXPECT_EQ(policy->value_mem_consumption(), 0);
}
} // namespace
} // namespace doris
