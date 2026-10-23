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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <string>
#include <thread>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/index/inverted/clucene/searcher_cache_value.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/olap_common.h"
#include "util/lru_cache.h"
#include "util/time.h"

namespace doris::segment_v2 {

auto fs = io::global_local_filesystem();

class InvertedIndexSearcherCacheTest : public testing::Test {
public:
    // there is set 1 shard in ShardedLRUCache
    // And the LRUHandle size is about 100B. So the cache size should big enough
    // to run the UT.
    static const int kCacheSize = 1000;
    const std::string kTestDir = "./ut_dir/invertedInvertedIndexSearcherCache::instance()_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
};

TEST_F(InvertedIndexSearcherCacheTest, insert_lookup) {
    auto* index_searcher_cache = new InvertedIndexSearcherCache(kCacheSize, 1);
    std::string file_name_1 = "test_1.idx";
    std::string file_name_2 = "test_2.idx";
    {
        auto* cache_value_1 =
                new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 0, UnixMillis());
        InvertedIndexSearcherCache::CacheKey searcher_cache_key1(kTestDir + "/" + file_name_1);
        index_searcher_cache->insert(searcher_cache_key1, cache_value_1);
    }

    {
        auto* cache_value_2 =
                new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 0, UnixMillis());
        InvertedIndexSearcherCache::CacheKey searcher_cache_key2(kTestDir + "/" + file_name_2);
        index_searcher_cache->insert(searcher_cache_key2, cache_value_2);
    }

    // lookup after insert
    {
        // case 1: lookup exist entry
        InvertedIndexCacheHandle inverted_index_cache_handle;
        auto index_file_path = kTestDir + "/" + file_name_1;
        auto searcher_cache_key1 = index_file_path;
        auto status =
                index_searcher_cache->lookup(searcher_cache_key1, &inverted_index_cache_handle);
        EXPECT_TRUE(status);

        auto* cache_value_1 = inverted_index_cache_handle.get_index_cache_value();
        EXPECT_GE(UnixMillis(), cache_value_1->last_visit_time);
    }
    {
        InvertedIndexCacheHandle inverted_index_cache_handle;
        auto index_file_path = kTestDir + "/" + file_name_2;
        auto searcher_cache_key2 = index_file_path;
        auto status =
                index_searcher_cache->lookup(searcher_cache_key2, &inverted_index_cache_handle);
        EXPECT_TRUE(status);

        auto* cache_value_2 = inverted_index_cache_handle.get_index_cache_value();
        EXPECT_GE(UnixMillis(), cache_value_2->last_visit_time);
    }

    {
        // case 2: lookup not exist entry
        // use cache
        {
            std::string file_name_not_exist_1 = "test_3.idx";
            InvertedIndexCacheHandle inverted_index_cache_handle_1;
            auto index_file_path = kTestDir + "/" + file_name_not_exist_1;
            auto searcher_cache_key = index_file_path;
            auto status = index_searcher_cache->lookup(searcher_cache_key,
                                                       &inverted_index_cache_handle_1);
            EXPECT_FALSE(status);
        }

        // lookup again
        {
            std::string file_name_not_exist_1 = "test_4.idx";
            InvertedIndexCacheHandle inverted_index_cache_handle_1;
            auto index_file_path = kTestDir + "/" + file_name_not_exist_1;
            auto searcher_cache_key = index_file_path;
            auto status = index_searcher_cache->lookup(searcher_cache_key,
                                                       &inverted_index_cache_handle_1);
            EXPECT_FALSE(status);
        }
    }

    delete index_searcher_cache;
}

TEST_F(InvertedIndexSearcherCacheTest, evict_by_usage) {
    auto* index_searcher_cache = new InvertedIndexSearcherCache(kCacheSize, 1);
    // no need evict
    std::string file_name_1 = "test_1.idx";
    InvertedIndexSearcherCache::CacheKey key_1(file_name_1);
    auto* cache_value_1 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 200, 10);
    index_searcher_cache->insert(key_1, cache_value_1);

    // should evict {key_1, cache_value_1}
    std::string file_name_2 = "test_2.idx";
    InvertedIndexSearcherCache::CacheKey key_2(file_name_2);
    auto* cache_value_2 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 800, 20);
    index_searcher_cache->insert(key_2, cache_value_2);
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_1
        EXPECT_FALSE(index_searcher_cache->lookup(key_1, &cache_handle));
        // lookup key_2
        EXPECT_TRUE(index_searcher_cache->lookup(key_2, &cache_handle));
    }

    // should evict {key_2, cache_value_2}
    std::string file_name_3 = "test_3.idx";
    InvertedIndexSearcherCache::CacheKey key_3(file_name_3);
    auto* cache_value_3 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 400, 30);
    index_searcher_cache->insert(key_3, cache_value_3);
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_2
        EXPECT_FALSE(index_searcher_cache->lookup(key_2, &cache_handle));
        // lookup key_3
        EXPECT_TRUE(index_searcher_cache->lookup(key_3, &cache_handle));
    }

    // no need evict
    std::string file_name_4 = "test_4.idx";
    InvertedIndexSearcherCache::CacheKey key_4(file_name_4);
    auto* cache_value_4 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 100, 40);
    index_searcher_cache->insert(key_4, cache_value_4);
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_3
        EXPECT_TRUE(index_searcher_cache->lookup(key_3, &cache_handle));
        // lookup key_4
        EXPECT_TRUE(index_searcher_cache->lookup(key_4, &cache_handle));
    }

    delete index_searcher_cache;
}

TEST_F(InvertedIndexSearcherCacheTest, evict_by_element_count_limit) {
    auto* index_searcher_cache = new InvertedIndexSearcherCache(kCacheSize, 1);
    // no need evict
    std::string file_name_1 = "test_1.idx";
    InvertedIndexSearcherCache::CacheKey key_1(file_name_1);
    auto* cache_value_1 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 20, 10);
    index_searcher_cache->insert(key_1, cache_value_1);

    // no need evict
    std::string file_name_2 = "test_2.idx";
    InvertedIndexSearcherCache::CacheKey key_2(file_name_2);
    auto* cache_value_2 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 50, 20);
    index_searcher_cache->insert(key_2, cache_value_2);
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_1
        EXPECT_TRUE(index_searcher_cache->lookup(key_1, &cache_handle));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_2
        EXPECT_TRUE(index_searcher_cache->lookup(key_2, &cache_handle));
    }

    // should evict {key_1, cache_value_1}
    std::string file_name_3 = "test_3.idx";
    InvertedIndexSearcherCache::CacheKey key_3(file_name_3);
    auto* cache_value_3 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 80, 30);
    index_searcher_cache->insert(key_3, cache_value_3);
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_1
        EXPECT_FALSE(index_searcher_cache->lookup(key_1, &cache_handle));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_2
        EXPECT_TRUE(index_searcher_cache->lookup(key_2, &cache_handle));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_3
        EXPECT_TRUE(index_searcher_cache->lookup(key_3, &cache_handle));
    }

    // should evict {key_2, cache_value_2}
    std::string file_name_4 = "test_4.idx";
    InvertedIndexSearcherCache::CacheKey key_4(file_name_4);
    auto* cache_value_4 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 100, 40);
    index_searcher_cache->insert(key_4, cache_value_4);
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_1
        EXPECT_FALSE(index_searcher_cache->lookup(key_1, &cache_handle));
        // lookup key_2
        EXPECT_FALSE(index_searcher_cache->lookup(key_2, &cache_handle));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_3
        EXPECT_TRUE(index_searcher_cache->lookup(key_3, &cache_handle));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_4
        EXPECT_TRUE(index_searcher_cache->lookup(key_4, &cache_handle));
    }

    delete index_searcher_cache;
}

TEST_F(InvertedIndexSearcherCacheTest, remove_element_only_in_table) {
    auto* index_searcher_cache = new InvertedIndexSearcherCache(kCacheSize, 1);
    // no need evict
    std::string file_name_1 = "test_1.idx";
    InvertedIndexSearcherCache::CacheKey key_1(file_name_1);
    auto* cache_value_1 = new CluceneSearcherCacheValue(FulltextIndexSearcherPtr(nullptr), 200, 10);
    index_searcher_cache->insert(key_1, cache_value_1);

    std::string file_name_2 = "test_2.idx";
    InvertedIndexSearcherCache::CacheKey key_2(file_name_2);
    IndexCacheValuePtr cache_value_2 =
            std::make_unique<CluceneSearcherCacheValue>(FulltextIndexSearcherPtr(nullptr), 0, 0);

    // lookup key_1, insert key_2, release key_2, release key_1
    {
        InvertedIndexCacheHandle cache_handle;
        // lookup key_1
        EXPECT_TRUE(index_searcher_cache->lookup(key_1, &cache_handle));

        // insert key_2, and then evict {key_1, cache_value_1}
        cache_value_2->size = 800;
        static_cast<CluceneSearcherCacheValue*>(cache_value_2.get())->searcher =
                FulltextIndexSearcherPtr(nullptr);
        cache_value_2->last_visit_time = 20;
        index_searcher_cache->insert(key_2, cache_value_2.release());

        // lookup key_2, key_2 has removed from table due to cache is full
        {
            InvertedIndexCacheHandle cache_handle_2;
            EXPECT_FALSE(index_searcher_cache->lookup(key_2, &cache_handle_2));
        }
    }

    // lookup key_1 exist
    {
        InvertedIndexCacheHandle cache_handle;
        EXPECT_TRUE(index_searcher_cache->lookup(key_1, &cache_handle));
    }

    // lookup key_2 not exist, then insert into cache, and evict key_1
    {
        InvertedIndexCacheHandle inverted_index_cache_handle;
        cache_value_2 = std::make_unique<CluceneSearcherCacheValue>(
                FulltextIndexSearcherPtr(nullptr), 0, UnixMillis());
        InvertedIndexSearcherCache::CacheKey searcher_cache_key2(kTestDir + "/" + file_name_2);
        index_searcher_cache->insert(searcher_cache_key2, cache_value_2.release());
    }
    // lookup key_2 again
    {
        InvertedIndexCacheHandle inverted_index_cache_handle;
        auto index_file_path = kTestDir + "/" + file_name_2;
        auto searcher_cache_key2 = index_file_path;
        auto status =
                index_searcher_cache->lookup(searcher_cache_key2, &inverted_index_cache_handle);
        EXPECT_TRUE(status);

        auto* cache_value_use_cache = inverted_index_cache_handle.get_index_cache_value();
        EXPECT_GE(UnixMillis(), cache_value_use_cache->last_visit_time);
    }

    delete index_searcher_cache;
}

// The non-NULL norm length sum of a field is computed once per cached searcher, kept apart from
// the other fields' sums, and not kept when computing it fails.
TEST(CluceneSearcherCacheValueTest, NonNullNormLengthSumIsComputedOncePerField) {
    CluceneSearcherCacheValue value(FulltextIndexSearcherPtr(nullptr), 0, 0);
    int computed = 0;
    auto compute = [&computed](uint64_t sum) {
        return [&computed, sum](uint64_t* out) {
            ++computed;
            *out = sum;
            return Status::OK();
        };
    };
    uint64_t sum = 0;
    ASSERT_TRUE(value.non_null_norm_length_sum(L"a", compute(7), &sum).ok());
    EXPECT_EQ(sum, 7);
    ASSERT_TRUE(value.non_null_norm_length_sum(L"a", compute(9), &sum).ok());
    EXPECT_EQ(sum, 7);
    ASSERT_TRUE(value.non_null_norm_length_sum(L"b", compute(9), &sum).ok());
    EXPECT_EQ(sum, 9);
    EXPECT_EQ(computed, 2);

    auto fail = [&computed](uint64_t*) {
        ++computed;
        return Status::Corruption("bad norms");
    };
    EXPECT_FALSE(value.non_null_norm_length_sum(L"c", fail, &sum).ok());
    ASSERT_TRUE(value.non_null_norm_length_sum(L"c", compute(3), &sum).ok());
    EXPECT_EQ(sum, 3);
    EXPECT_EQ(computed, 4);
}

} // namespace doris::segment_v2
