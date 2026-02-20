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

#include <cstring>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "olap/rowset/segment_v2/inverted_index_cache.h"

namespace doris::segment_v2 {

class SearchFunctionQueryCacheTest : public testing::Test {
public:
    static const int kCacheSize = 4096;

    void SetUp() override { _cache = new SearchFunctionQueryCache(kCacheSize, 1); }

    void TearDown() override { delete _cache; }

protected:
    SearchFunctionQueryCache* _cache = nullptr;
};

TEST_F(SearchFunctionQueryCacheTest, insert_and_lookup) {
    auto result_bm = std::make_shared<roaring::Roaring>();
    result_bm->add(1);
    result_bm->add(3);
    result_bm->add(5);

    auto null_bm = std::make_shared<roaring::Roaring>();
    null_bm->add(2);

    SearchFunctionQueryCache::CacheKey key {"segment_0001", "title:hello#title=100;"};

    InvertedIndexQueryCacheHandle handle;
    _cache->insert(key, result_bm, null_bm, &handle);

    // Lookup should succeed
    InvertedIndexQueryCacheHandle lookup_handle;
    EXPECT_TRUE(_cache->lookup(key, &lookup_handle));

    auto* cv = lookup_handle.get_value<SearchFunctionQueryCache::CacheValue>();
    ASSERT_NE(cv, nullptr);
    EXPECT_TRUE(cv->result_bitmap->contains(1));
    EXPECT_TRUE(cv->result_bitmap->contains(3));
    EXPECT_TRUE(cv->result_bitmap->contains(5));
    EXPECT_FALSE(cv->result_bitmap->contains(2));
    EXPECT_EQ(cv->result_bitmap->cardinality(), 3);

    EXPECT_TRUE(cv->null_bitmap->contains(2));
    EXPECT_EQ(cv->null_bitmap->cardinality(), 1);
}

TEST_F(SearchFunctionQueryCacheTest, lookup_miss) {
    SearchFunctionQueryCache::CacheKey key {"segment_0001", "title:hello#title=100;"};

    InvertedIndexQueryCacheHandle handle;
    EXPECT_FALSE(_cache->lookup(key, &handle));
}

TEST_F(SearchFunctionQueryCacheTest, different_keys_independent) {
    auto bm1 = std::make_shared<roaring::Roaring>();
    bm1->add(10);
    auto bm2 = std::make_shared<roaring::Roaring>();
    bm2->add(20);
    auto null_bm = std::make_shared<roaring::Roaring>();

    SearchFunctionQueryCache::CacheKey key1 {"seg_a", "dsl_1"};
    SearchFunctionQueryCache::CacheKey key2 {"seg_a", "dsl_2"};

    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key1, bm1, null_bm, &h);
    }
    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key2, bm2, null_bm, &h);
    }

    // Lookup key1
    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(key1, &h));
        auto* cv = h.get_value<SearchFunctionQueryCache::CacheValue>();
        ASSERT_NE(cv, nullptr);
        EXPECT_TRUE(cv->result_bitmap->contains(10));
        EXPECT_FALSE(cv->result_bitmap->contains(20));
    }

    // Lookup key2
    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(key2, &h));
        auto* cv = h.get_value<SearchFunctionQueryCache::CacheValue>();
        ASSERT_NE(cv, nullptr);
        EXPECT_TRUE(cv->result_bitmap->contains(20));
        EXPECT_FALSE(cv->result_bitmap->contains(10));
    }
}

TEST_F(SearchFunctionQueryCacheTest, null_bitmap_handling) {
    auto result_bm = std::make_shared<roaring::Roaring>();
    result_bm->add(1);

    // Insert with nullptr null_bitmap
    SearchFunctionQueryCache::CacheKey key {"seg_x", "dsl_null"};
    InvertedIndexQueryCacheHandle handle;
    _cache->insert(key, result_bm, nullptr, &handle);

    InvertedIndexQueryCacheHandle lookup_handle;
    EXPECT_TRUE(_cache->lookup(key, &lookup_handle));
    auto* cv = lookup_handle.get_value<SearchFunctionQueryCache::CacheValue>();
    ASSERT_NE(cv, nullptr);
    EXPECT_TRUE(cv->result_bitmap->contains(1));
    EXPECT_EQ(cv->null_bitmap, nullptr);
}

TEST_F(SearchFunctionQueryCacheTest, cache_key_encode) {
    // Length-prefix encoding: 4-byte little-endian prefix length + prefix + signature
    SearchFunctionQueryCache::CacheKey key {"segment_prefix", "dsl_sig"};
    auto encoded = key.encode();
    // Verify prefix_len (14) is encoded as first 4 bytes
    uint32_t prefix_len = 0;
    std::memcpy(&prefix_len, encoded.data(), sizeof(prefix_len));
    EXPECT_EQ(prefix_len, 14);
    EXPECT_EQ(encoded.substr(4, 14), "segment_prefix");
    EXPECT_EQ(encoded.substr(18), "dsl_sig");

    // Keys that would collide under old '#' delimiter don't collide with length-prefix
    SearchFunctionQueryCache::CacheKey keyA {"seg#", "adsl"};
    SearchFunctionQueryCache::CacheKey keyB {"seg", "#adsl"};
    EXPECT_NE(keyA.encode(), keyB.encode());

    // Also verify keys with embedded null bytes don't collide
    SearchFunctionQueryCache::CacheKey keyC {"seg#a", "dsl"};
    SearchFunctionQueryCache::CacheKey keyD {"seg", "a" + std::string("\0", 1) + "dsl"};
    EXPECT_NE(keyC.encode(), keyD.encode());

    SearchFunctionQueryCache::CacheKey key2 {"", "dsl"};
    auto encoded2 = key2.encode();
    uint32_t prefix_len2 = 0;
    std::memcpy(&prefix_len2, encoded2.data(), sizeof(prefix_len2));
    EXPECT_EQ(prefix_len2, 0);
    EXPECT_EQ(encoded2.substr(4), "dsl");
}

TEST_F(SearchFunctionQueryCacheTest, lookup_deep_copy_prevents_cache_pollution) {
    auto result_bm = std::make_shared<roaring::Roaring>();
    result_bm->add(1);
    result_bm->add(2);
    result_bm->add(3);

    auto null_bm = std::make_shared<roaring::Roaring>();
    null_bm->add(3);

    SearchFunctionQueryCache::CacheKey key {"seg_dc", "dsl_dc"};
    InvertedIndexQueryCacheHandle insert_handle;
    _cache->insert(key, result_bm, null_bm, &insert_handle);

    // First lookup: deep-copy and mutate (simulating mask_out_null)
    {
        InvertedIndexQueryCacheHandle h;
        ASSERT_TRUE(_cache->lookup(key, &h));
        auto* cv = h.get_value<SearchFunctionQueryCache::CacheValue>();
        ASSERT_NE(cv, nullptr);

        auto copy_result = std::make_shared<roaring::Roaring>(*cv->result_bitmap);
        auto copy_null =
                cv->null_bitmap ? std::make_shared<roaring::Roaring>(*cv->null_bitmap) : nullptr;
        // Mutate the copy as mask_out_null would
        *copy_result -= *copy_null;
        EXPECT_EQ(copy_result->cardinality(), 2); // {1,2} after removing {3}
    }

    // Second lookup: cached entry must be unmodified
    {
        InvertedIndexQueryCacheHandle h;
        ASSERT_TRUE(_cache->lookup(key, &h));
        auto* cv = h.get_value<SearchFunctionQueryCache::CacheValue>();
        ASSERT_NE(cv, nullptr);
        EXPECT_EQ(cv->result_bitmap->cardinality(), 3); // Still {1,2,3}
        EXPECT_TRUE(cv->result_bitmap->contains(3));
        EXPECT_EQ(cv->null_bitmap->cardinality(), 1);
        EXPECT_TRUE(cv->null_bitmap->contains(3));
    }
}

TEST_F(SearchFunctionQueryCacheTest, insert_deep_copy_isolation) {
    auto bm = std::make_shared<roaring::Roaring>();
    bm->add(10);
    bm->add(20);

    SearchFunctionQueryCache::CacheKey key {"seg_iso", "dsl_iso"};
    InvertedIndexQueryCacheHandle h;
    // Insert a deep copy (as production code does)
    _cache->insert(key, std::make_shared<roaring::Roaring>(*bm), nullptr, &h);

    // Mutate original after insert
    bm->add(999);

    // Cached entry should not contain 999
    InvertedIndexQueryCacheHandle lookup_h;
    ASSERT_TRUE(_cache->lookup(key, &lookup_h));
    auto* cv = lookup_h.get_value<SearchFunctionQueryCache::CacheValue>();
    ASSERT_NE(cv, nullptr);
    EXPECT_FALSE(cv->result_bitmap->contains(999));
    EXPECT_EQ(cv->result_bitmap->cardinality(), 2);
}

TEST_F(SearchFunctionQueryCacheTest, overwrite_same_key) {
    auto bm1 = std::make_shared<roaring::Roaring>();
    bm1->add(1);
    auto bm2 = std::make_shared<roaring::Roaring>();
    bm2->add(99);
    auto null_bm = std::make_shared<roaring::Roaring>();

    SearchFunctionQueryCache::CacheKey key {"seg", "dsl"};

    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key, bm1, null_bm, &h);
    }

    // Insert again with different bitmap
    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key, bm2, null_bm, &h);
    }

    // Lookup should return the latest value
    InvertedIndexQueryCacheHandle h;
    EXPECT_TRUE(_cache->lookup(key, &h));
    auto* cv = h.get_value<SearchFunctionQueryCache::CacheValue>();
    ASSERT_NE(cv, nullptr);
    EXPECT_TRUE(cv->result_bitmap->contains(99));
}

} // namespace doris::segment_v2
