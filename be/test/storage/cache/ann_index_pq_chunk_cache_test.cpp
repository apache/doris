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

#include "storage/cache/ann_index_pq_chunk_cache.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/cache/page_cache.h"

namespace doris {

class AnnIndexPqChunkCacheTest : public testing::Test {
public:
    AnnIndexPqChunkCacheTest() {
        _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                        "PqChunkCacheTest");
    }
    ~AnnIndexPqChunkCacheTest() override = default;

protected:
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

    // Helper: create a DataPage filled with a byte pattern.
    DataPage* make_page(size_t size, uint8_t fill_byte) {
        auto* page = new DataPage(size, _mem_tracker);
        std::memset(page->data(), fill_byte, size);
        return page;
    }
};

// Test basic insert + lookup cycle.
TEST_F(AnnIndexPqChunkCacheTest, InsertAndLookup) {
    // Create a cache with 1MB capacity
    AnnIndexPqChunkCache cache(1024 * 1024, 4);

    StoragePageCache::CacheKey key("test_file", 100000, 0);
    PageCacheHandle handle;

    // Initially should miss
    EXPECT_FALSE(cache.lookup(key, &handle));

    // Insert a 64KB page
    constexpr size_t kPageSize = 64 * 1024;
    auto* page = make_page(kPageSize, 0xAB);
    cache.insert(key, page, &handle);

    // Now lookup should hit
    PageCacheHandle handle2;
    EXPECT_TRUE(cache.lookup(key, &handle2));

    // Verify data is accessible and correct
    Slice data = handle2.data();
    EXPECT_EQ(data.size, kPageSize);
    EXPECT_EQ(static_cast<uint8_t>(data.data[0]), 0xAB);
    EXPECT_EQ(static_cast<uint8_t>(data.data[kPageSize - 1]), 0xAB);
}

// Test that different keys map to different entries.
TEST_F(AnnIndexPqChunkCacheTest, DifferentKeysAreIndependent) {
    AnnIndexPqChunkCache cache(1024 * 1024, 4);

    StoragePageCache::CacheKey key1("file_a", 100000, 0);
    StoragePageCache::CacheKey key2("file_a", 100000, 65536);
    StoragePageCache::CacheKey key3("file_b", 100000, 0);

    PageCacheHandle h1, h2, h3;
    cache.insert(key1, make_page(1024, 0x11), &h1);
    cache.insert(key2, make_page(1024, 0x22), &h2);
    cache.insert(key3, make_page(1024, 0x33), &h3);

    PageCacheHandle lookup_h;

    ASSERT_TRUE(cache.lookup(key1, &lookup_h));
    EXPECT_EQ(static_cast<uint8_t>(lookup_h.data().data[0]), 0x11);

    ASSERT_TRUE(cache.lookup(key2, &lookup_h));
    EXPECT_EQ(static_cast<uint8_t>(lookup_h.data().data[0]), 0x22);

    ASSERT_TRUE(cache.lookup(key3, &lookup_h));
    EXPECT_EQ(static_cast<uint8_t>(lookup_h.data().data[0]), 0x33);
}

// Test that a lookup miss returns false and doesn't crash.
TEST_F(AnnIndexPqChunkCacheTest, LookupMissReturnsFalse) {
    AnnIndexPqChunkCache cache(1024 * 1024, 4);

    StoragePageCache::CacheKey key("nonexistent", 999, 42);
    PageCacheHandle handle;

    EXPECT_FALSE(cache.lookup(key, &handle));
}

// Test global cache factory lifecycle.
TEST_F(AnnIndexPqChunkCacheTest, GlobalCacheLifecycle) {
    std::unique_ptr<AnnIndexPqChunkCache> cache(
            AnnIndexPqChunkCache::create_global_cache(1024 * 1024, 4));
    ASSERT_NE(cache, nullptr);

    {
        StoragePageCache::CacheKey key("singleton_test", 50000, 0);
        PageCacheHandle handle;
        cache->insert(key, make_page(512, 0xFF), &handle);

        PageCacheHandle lookup_h;
        EXPECT_TRUE(cache->lookup(key, &lookup_h));
        EXPECT_EQ(static_cast<uint8_t>(lookup_h.data().data[0]), 0xFF);
    }
}

// Test that mem_tracker is accessible and non-null.
TEST_F(AnnIndexPqChunkCacheTest, MemTrackerAccessible) {
    AnnIndexPqChunkCache cache(1024 * 1024, 4);
    EXPECT_NE(cache.mem_tracker(), nullptr);
}

} // namespace doris
