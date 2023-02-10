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

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "util/file_utils.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {

auto fs = io::global_local_filesystem();

class InvertedIndexSearcherCacheTest : public testing::Test {
public:
    // there is set 1 shard in ShardedLRUCache
    // And the LRUHandle size is about 100B. So the cache size should big enough
    // to run the UT.
    static const int kCacheSize = 1000;
    const std::string kTestDir = "./ut_dir/invertedInvertedIndexSearcherCache::instance()_test";

    void SetUp() override {
        if (FileUtils::check_exist(kTestDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kTestDir).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(kTestDir).ok());
    }
    void TearDown() override {
        if (FileUtils::check_exist(kTestDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kTestDir).ok());
        }
    }
};

TEST_F(InvertedIndexSearcherCacheTest, insert_lookup) {
    InvertedIndexSearcherCache* index_searcher_cache =
            new InvertedIndexSearcherCache(kCacheSize, 1);
    std::string file_name_1 = "test_1.idx";
    std::string file_name_2 = "test_2.idx";

    //insert searcher
    Status status = Status::OK();
    status = index_searcher_cache->insert(fs, kTestDir, file_name_1);
    EXPECT_EQ(Status::OK(), status);

    status = index_searcher_cache->insert(fs, kTestDir, file_name_2);
    EXPECT_EQ(Status::OK(), status);

    OlapReaderStatistics stats;

    // lookup after insert
    {
        // case 1: lookup exist entry
        InvertedIndexCacheHandle inverted_index_cache_handle;
        status = index_searcher_cache->get_index_searcher(fs, kTestDir, file_name_1,
                                                          &inverted_index_cache_handle, &stats);
        EXPECT_EQ(Status::OK(), status);

        auto cache_value_1 =
                (InvertedIndexSearcherCache::CacheValue*)(inverted_index_cache_handle._cache)
                        ->value(inverted_index_cache_handle._handle);
        EXPECT_GE(UnixMillis(), cache_value_1->last_visit_time);

        status = index_searcher_cache->get_index_searcher(fs, kTestDir, file_name_2,
                                                          &inverted_index_cache_handle, &stats);
        EXPECT_EQ(Status::OK(), status);

        auto cache_value_2 =
                (InvertedIndexSearcherCache::CacheValue*)(inverted_index_cache_handle._cache)
                        ->value(inverted_index_cache_handle._handle);
        EXPECT_GE(UnixMillis(), cache_value_2->last_visit_time);
    }

    {
        // case 2: lookup not exist entry
        std::string file_name_not_exist_1 = "test_3.idx";
        std::string file_name_not_exist_2 = "test_4.idx";
        // use cache
        {
            InvertedIndexCacheHandle inverted_index_cache_handle_1;
            status = index_searcher_cache->get_index_searcher(
                    fs, kTestDir, file_name_not_exist_1, &inverted_index_cache_handle_1, &stats);
            EXPECT_EQ(Status::OK(), status);
            EXPECT_FALSE(inverted_index_cache_handle_1.owned);
        }

        // lookup again
        {
            InvertedIndexCacheHandle inverted_index_cache_handle_1;
            status = index_searcher_cache->get_index_searcher(
                    fs, kTestDir, file_name_not_exist_1, &inverted_index_cache_handle_1, &stats);
            EXPECT_EQ(Status::OK(), status);
            EXPECT_FALSE(inverted_index_cache_handle_1.owned);

            auto cache_value_use_cache =
                    (InvertedIndexSearcherCache::CacheValue*)(inverted_index_cache_handle_1._cache)
                            ->value(inverted_index_cache_handle_1._handle);
            EXPECT_LT(UnixMillis(), cache_value_use_cache->last_visit_time);
        }

        // not use cache
        InvertedIndexCacheHandle inverted_index_cache_handle_2;
        status = index_searcher_cache->get_index_searcher(
                fs, kTestDir, file_name_not_exist_2, &inverted_index_cache_handle_2, &stats, false);
        EXPECT_EQ(Status::OK(), status);
        EXPECT_TRUE(inverted_index_cache_handle_2.owned);
        EXPECT_EQ(nullptr, inverted_index_cache_handle_2._cache);
        EXPECT_EQ(nullptr, inverted_index_cache_handle_2._handle);

        status = index_searcher_cache->get_index_searcher(fs, kTestDir, file_name_not_exist_2,
                                                          &inverted_index_cache_handle_2, &stats);
        EXPECT_EQ(Status::OK(), status);
        EXPECT_FALSE(inverted_index_cache_handle_2.owned);
        auto cache_value_use_cache_2 =
                (InvertedIndexSearcherCache::CacheValue*)(inverted_index_cache_handle_2._cache)
                        ->value(inverted_index_cache_handle_2._handle);
        EXPECT_EQ(0, cache_value_use_cache_2->last_visit_time);
    }

    delete index_searcher_cache;
}

TEST_F(InvertedIndexSearcherCacheTest, evict) {
    InvertedIndexSearcherCache* index_searcher_cache =
            new InvertedIndexSearcherCache(kCacheSize, 1);
    // no need evict
    std::string file_name_1 = "test_1.idx";
    InvertedIndexSearcherCache::CacheKey key_1(file_name_1);
    IndexCacheValuePtr cache_value_1 = std::make_unique<InvertedIndexSearcherCache::CacheValue>();
    cache_value_1->size = 200;
    cache_value_1->index_searcher = nullptr;
    cache_value_1->last_visit_time = 10;
    index_searcher_cache->_cache->release(
            index_searcher_cache->_insert(key_1, cache_value_1.release()));

    // should evict {key_1, cache_value_1}
    std::string file_name_2 = "test_2.idx";
    InvertedIndexSearcherCache::CacheKey key_2(file_name_2);
    IndexCacheValuePtr cache_value_2 = std::make_unique<InvertedIndexSearcherCache::CacheValue>();
    cache_value_2->size = 800;
    cache_value_2->index_searcher = nullptr;
    cache_value_2->last_visit_time = 20;
    index_searcher_cache->_cache->release(
            index_searcher_cache->_insert(key_2, cache_value_2.release()));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_1
        EXPECT_FALSE(index_searcher_cache->_lookup(key_1, &cache_handle));
        // lookup key_2
        EXPECT_TRUE(index_searcher_cache->_lookup(key_2, &cache_handle));
    }

    // should evict {key_2, cache_value_2}
    std::string file_name_3 = "test_3.idx";
    InvertedIndexSearcherCache::CacheKey key_3(file_name_3);
    IndexCacheValuePtr cache_value_3 = std::make_unique<InvertedIndexSearcherCache::CacheValue>();
    cache_value_3->size = 400;
    cache_value_3->index_searcher = nullptr;
    cache_value_3->last_visit_time = 30;
    index_searcher_cache->_cache->release(
            index_searcher_cache->_insert(key_3, cache_value_3.release()));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_2
        EXPECT_FALSE(index_searcher_cache->_lookup(key_2, &cache_handle));
        // lookup key_3
        EXPECT_TRUE(index_searcher_cache->_lookup(key_3, &cache_handle));
    }

    // no need evict
    std::string file_name_4 = "test_4.idx";
    InvertedIndexSearcherCache::CacheKey key_4(file_name_4);
    IndexCacheValuePtr cache_value_4 = std::make_unique<InvertedIndexSearcherCache::CacheValue>();
    cache_value_4->size = 100;
    cache_value_4->index_searcher = nullptr;
    cache_value_4->last_visit_time = 40;
    index_searcher_cache->_cache->release(
            index_searcher_cache->_insert(key_4, cache_value_4.release()));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_3
        EXPECT_TRUE(index_searcher_cache->_lookup(key_3, &cache_handle));
        // lookup key_4
        EXPECT_TRUE(index_searcher_cache->_lookup(key_4, &cache_handle));
    }

    delete index_searcher_cache;
}

} // namespace segment_v2
} // namespace doris