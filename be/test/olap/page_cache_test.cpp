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

#include "olap/page_cache.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

static int kNumShards = StoragePageCache::kDefaultNumShards;

class StoragePageCacheTest : public testing::Test {
public:
    StoragePageCacheTest() {
        mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                       "StoragePageCacheTest");
    }
    ~StoragePageCacheTest() override = default;

    std::shared_ptr<MemTrackerLimiter> mem_tracker;
};

// All cache space is allocated to data pages
TEST_F(StoragePageCacheTest, data_page_only) {
    StoragePageCache cache(kNumShards * 2048, 0, 0, kNumShards);

    StoragePageCache::CacheKey key("abc", 0, 0);
    StoragePageCache::CacheKey memory_key("mem", 0, 0);

    segment_v2::PageTypePB page_type = segment_v2::DATA_PAGE;

    {
        // insert normal page
        PageCacheHandle handle;
        auto* data = new DataPage(1024, true, page_type);
        cache.insert(key, data, &handle, page_type, false);

        EXPECT_EQ(handle.data().data, data->data());

        auto found = cache.lookup(key, &handle, page_type);
        EXPECT_TRUE(found);
        EXPECT_EQ(data->data(), handle.data().data);
    }

    {
        // insert in_memory page
        PageCacheHandle handle;
        auto* data = new DataPage(1024, true, page_type);
        cache.insert(memory_key, data, &handle, page_type, true);

        EXPECT_EQ(handle.data().data, data->data());

        auto found = cache.lookup(memory_key, &handle, page_type);
        EXPECT_TRUE(found);
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", 0, i);
        PageCacheHandle handle;
        auto* data = new DataPage(1024, true, page_type);
        cache.insert(key, data, &handle, page_type, false);
    }

    // cache miss, different offset
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 0, 1);
        auto found = cache.lookup(miss_key, &handle, page_type);
        EXPECT_FALSE(found);
    }

    // cache miss, different file size
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1, 0);
        auto found = cache.lookup(miss_key, &handle, page_type);
        EXPECT_FALSE(found);
    }

    // cache miss for eliminated key
    {
        PageCacheHandle handle;
        auto found = cache.lookup(key, &handle, page_type);
        EXPECT_FALSE(found);
    }
}

// All cache space is allocated to index pages
TEST_F(StoragePageCacheTest, index_page_only) {
    StoragePageCache cache(kNumShards * 2048, 100, 0, kNumShards);

    StoragePageCache::CacheKey key("abc", 0, 0);
    StoragePageCache::CacheKey memory_key("mem", 0, 0);

    segment_v2::PageTypePB page_type = segment_v2::INDEX_PAGE;

    {
        // insert normal page
        PageCacheHandle handle;
        auto* data = new DataPage(1024, true, page_type);
        cache.insert(key, data, &handle, page_type, false);

        EXPECT_EQ(handle.data().data, data->data());

        auto found = cache.lookup(key, &handle, page_type);
        EXPECT_TRUE(found);
        EXPECT_EQ(data->data(), handle.data().data);
    }

    {
        // insert in_memory page
        PageCacheHandle handle;
        auto* data = new DataPage(1024, true, page_type);
        cache.insert(memory_key, data, &handle, page_type, true);

        EXPECT_EQ(handle.data().data, data->data());

        auto found = cache.lookup(memory_key, &handle, page_type);
        EXPECT_TRUE(found);
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", 0, i);
        PageCacheHandle handle;
        auto* data = new DataPage(1024, true, page_type);
        cache.insert(key, data, &handle, page_type, false);
    }

    // cache miss, different offset
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1, 1);
        auto found = cache.lookup(miss_key, &handle, page_type);
        EXPECT_FALSE(found);
    }

    // cache miss, different file size
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1, 0);
        auto found = cache.lookup(miss_key, &handle, page_type);
        EXPECT_FALSE(found);
    }

    // cache miss for eliminated key
    {
        PageCacheHandle handle;
        auto found = cache.lookup(key, &handle, page_type);
        EXPECT_FALSE(found);
    }
}

// Cache space is allocated by index_page_cache_ratio
TEST_F(StoragePageCacheTest, mixed_pages) {
    StoragePageCache cache(kNumShards * 2048, 10, 0, kNumShards);

    StoragePageCache::CacheKey data_key("data", 0, 0);
    StoragePageCache::CacheKey index_key("index", 0, 0);
    StoragePageCache::CacheKey data_key_mem("data_mem", 0, 0);
    StoragePageCache::CacheKey index_key_mem("index_mem", 0, 0);

    segment_v2::PageTypePB page_type_data = segment_v2::DATA_PAGE;
    segment_v2::PageTypePB page_type_index = segment_v2::INDEX_PAGE;

    {
        // insert both normal pages
        PageCacheHandle data_handle, index_handle;
        auto* data = new DataPage(1024, true, page_type_data);
        auto* index = new DataPage(1024, true, page_type_index);
        cache.insert(data_key, data, &data_handle, page_type_data, false);
        cache.insert(index_key, index, &index_handle, page_type_index, false);

        EXPECT_EQ(data_handle.data().data, data->data());
        EXPECT_EQ(index_handle.data().data, index->data());

        auto found_data = cache.lookup(data_key, &data_handle, page_type_data);
        auto found_index = cache.lookup(index_key, &index_handle, page_type_index);
        EXPECT_TRUE(found_data);
        EXPECT_TRUE(found_index);
        EXPECT_EQ(data->data(), data_handle.data().data);
        EXPECT_EQ(index->data(), index_handle.data().data);
    }

    {
        // insert both in_memory pages
        PageCacheHandle data_handle, index_handle;
        auto* data = new DataPage(1024, true, page_type_data);
        auto* index = new DataPage(1024, true, page_type_index);
        cache.insert(data_key_mem, data, &data_handle, page_type_data, true);
        cache.insert(index_key_mem, index, &index_handle, page_type_index, true);

        EXPECT_EQ(data_handle.data().data, data->data());
        EXPECT_EQ(index_handle.data().data, index->data());

        auto found_data = cache.lookup(data_key_mem, &data_handle, page_type_data);
        auto found_index = cache.lookup(index_key_mem, &index_handle, page_type_index);
        EXPECT_TRUE(found_data);
        EXPECT_TRUE(found_index);
    }

    // put too many page to eliminate first page of both cache
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", 0, i);
        PageCacheHandle handle;
        std::unique_ptr<DataPage> data = std::make_unique<DataPage>(1024, true, page_type_data);
        std::unique_ptr<DataPage> index = std::make_unique<DataPage>(1024, true, page_type_index);
        cache.insert(key, data.release(), &handle, page_type_data, false);
        cache.insert(key, index.release(), &handle, page_type_index, false);
    }

    // cache miss by key
    {
        PageCacheHandle data_handle, index_handle;
        StoragePageCache::CacheKey miss_key("abc", 0, 1);
        auto found_data = cache.lookup(miss_key, &data_handle, page_type_data);
        auto found_index = cache.lookup(miss_key, &index_handle, page_type_index);
        EXPECT_FALSE(found_data);
        EXPECT_FALSE(found_index);
    }

    // cache miss by page type
    {
        PageCacheHandle data_handle, index_handle;
        StoragePageCache::CacheKey miss_key_data("data_miss", 0, 1);
        StoragePageCache::CacheKey miss_key_index("index_miss", 0, 1);
        std::unique_ptr<DataPage> data = std::make_unique<DataPage>(1024, true, page_type_data);
        std::unique_ptr<DataPage> index = std::make_unique<DataPage>(1024, true, page_type_index);
        cache.insert(miss_key_data, data.release(), &data_handle, page_type_data, false);
        cache.insert(miss_key_index, index.release(), &index_handle, page_type_index, false);

        auto found_data = cache.lookup(miss_key_data, &data_handle, page_type_index);
        auto found_index = cache.lookup(miss_key_index, &index_handle, page_type_data);
        EXPECT_FALSE(found_data);
        EXPECT_FALSE(found_index);
    }

    // cache miss for eliminated key
    {
        PageCacheHandle data_handle, index_handle;
        auto found_data = cache.lookup(data_key, &data_handle, page_type_data);
        auto found_index = cache.lookup(index_key, &index_handle, page_type_index);
        EXPECT_FALSE(found_data);
        EXPECT_FALSE(found_index);
    }
}

} // namespace doris
