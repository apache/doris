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

#include <gtest/gtest.h>

namespace doris {

class StoragePageCacheTest : public testing::Test {
public:
    StoragePageCacheTest() {}
    virtual ~StoragePageCacheTest() {}
};

// All cache space is allocated to data pages
TEST(StoragePageCacheTest, data_page_only) {
    StoragePageCache cache(kNumShards * 2048, 0);

    StoragePageCache::CacheKey key("abc", 0);
    StoragePageCache::CacheKey memory_key("mem", 0);

    segment_v2::PageTypePB page_type = segment_v2::DATA_PAGE;

    {
        // insert normal page
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(key, data, &handle, page_type, false);

        EXPECT_EQ(handle.data().data, buf);

        auto found = cache.lookup(key, &handle, page_type);
        EXPECT_TRUE(found);
        EXPECT_EQ(buf, handle.data().data);
    }

    {
        // insert in_memory page
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(memory_key, data, &handle, page_type, true);

        EXPECT_EQ(handle.data().data, buf);

        auto found = cache.lookup(memory_key, &handle, page_type);
        EXPECT_TRUE(found);
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", i);
        PageCacheHandle handle;
        Slice data(new char[1024], 1024);
        cache.insert(key, data, &handle, page_type, false);
    }

    // cache miss
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1);
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
TEST(StoragePageCacheTest, index_page_only) {
    StoragePageCache cache(kNumShards * 2048, 100);

    StoragePageCache::CacheKey key("abc", 0);
    StoragePageCache::CacheKey memory_key("mem", 0);

    segment_v2::PageTypePB page_type = segment_v2::INDEX_PAGE;

    {
        // insert normal page
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(key, data, &handle, page_type, false);

        EXPECT_EQ(handle.data().data, buf);

        auto found = cache.lookup(key, &handle, page_type);
        EXPECT_TRUE(found);
        EXPECT_EQ(buf, handle.data().data);
    }

    {
        // insert in_memory page
        char* buf = new char[1024];
        PageCacheHandle handle;
        Slice data(buf, 1024);
        cache.insert(memory_key, data, &handle, page_type, true);

        EXPECT_EQ(handle.data().data, buf);

        auto found = cache.lookup(memory_key, &handle, page_type);
        EXPECT_TRUE(found);
    }

    // put too many page to eliminate first page
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", i);
        PageCacheHandle handle;
        Slice data(new char[1024], 1024);
        cache.insert(key, data, &handle, page_type, false);
    }

    // cache miss
    {
        PageCacheHandle handle;
        StoragePageCache::CacheKey miss_key("abc", 1);
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
TEST(StoragePageCacheTest, mixed_pages) {
    StoragePageCache cache(kNumShards * 2048, 10);

    StoragePageCache::CacheKey data_key("data", 0);
    StoragePageCache::CacheKey index_key("index", 0);
    StoragePageCache::CacheKey data_key_mem("data_mem", 0);
    StoragePageCache::CacheKey index_key_mem("index_mem", 0);

    segment_v2::PageTypePB page_type_data = segment_v2::DATA_PAGE;
    segment_v2::PageTypePB page_type_index = segment_v2::INDEX_PAGE;

    {
        // insert both normal pages
        char* buf_data = new char[1024];
        char* buf_index = new char[1024];
        PageCacheHandle data_handle, index_handle;
        Slice data(buf_data, 1024), index(buf_index, 1024);
        cache.insert(data_key, data, &data_handle, page_type_data, false);
        cache.insert(index_key, index, &index_handle, page_type_index, false);

        EXPECT_EQ(data_handle.data().data, buf_data);
        EXPECT_EQ(index_handle.data().data, buf_index);

        auto found_data = cache.lookup(data_key, &data_handle, page_type_data);
        auto found_index = cache.lookup(index_key, &index_handle, page_type_index);
        EXPECT_TRUE(found_data);
        EXPECT_TRUE(found_index);
        EXPECT_EQ(buf_data, data_handle.data().data);
        EXPECT_EQ(buf_index, index_handle.data().data);
    }

    {
        // insert both in_memory pages
        char* buf_data = new char[1024];
        char* buf_index = new char[1024];
        PageCacheHandle data_handle, index_handle;
        Slice data(buf_data, 1024), index(buf_index, 1024);
        cache.insert(data_key_mem, data, &data_handle, page_type_data, true);
        cache.insert(index_key_mem, index, &index_handle, page_type_index, true);

        EXPECT_EQ(data_handle.data().data, buf_data);
        EXPECT_EQ(index_handle.data().data, buf_index);

        auto found_data = cache.lookup(data_key_mem, &data_handle, page_type_data);
        auto found_index = cache.lookup(index_key_mem, &index_handle, page_type_index);
        EXPECT_TRUE(found_data);
        EXPECT_TRUE(found_index);
    }

    // put too many page to eliminate first page of both cache
    for (int i = 0; i < 10 * kNumShards; ++i) {
        StoragePageCache::CacheKey key("bcd", i);
        PageCacheHandle handle;
        Slice data(new char[1024], 1024), index(new char[1024], 1024);
        cache.insert(key, data, &handle, page_type_data, false);
        cache.insert(key, index, &handle, page_type_index, false);
    }

    // cache miss by key
    {
        PageCacheHandle data_handle, index_handle;
        StoragePageCache::CacheKey miss_key("abc", 1);
        auto found_data = cache.lookup(miss_key, &data_handle, page_type_data);
        auto found_index = cache.lookup(miss_key, &index_handle, page_type_index);
        EXPECT_FALSE(found_data);
        EXPECT_FALSE(found_index);
    }

    // cache miss by page type
    {
        PageCacheHandle data_handle, index_handle;
        StoragePageCache::CacheKey miss_key_data("data_miss", 1);
        StoragePageCache::CacheKey miss_key_index("index_miss", 1);
        char* buf_data = new char[1024];
        char* buf_index = new char[1024];
        Slice data(buf_data, 1024), index(buf_index, 1024);
        cache.insert(miss_key_data, data, &data_handle, page_type_data, false);
        cache.insert(miss_key_index, index, &index_handle, page_type_index, false);

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
