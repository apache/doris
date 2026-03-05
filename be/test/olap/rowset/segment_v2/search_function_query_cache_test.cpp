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

#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"

namespace doris::segment_v2 {

class SearchDslQueryCacheTest : public testing::Test {
public:
    static const int kCacheSize = 4096;

    void SetUp() override { _cache = new InvertedIndexQueryCache(kCacheSize, 1); }

    void TearDown() override { delete _cache; }

    InvertedIndexQueryCache::CacheKey make_key(const std::string& seg_prefix,
                                               const std::string& dsl_sig) {
        return InvertedIndexQueryCache::CacheKey {
                seg_prefix, "__search_dsl__", InvertedIndexQueryType::SEARCH_DSL_QUERY, dsl_sig};
    }

protected:
    InvertedIndexQueryCache* _cache = nullptr;
};

TEST_F(SearchDslQueryCacheTest, insert_and_lookup) {
    auto bm = std::make_shared<roaring::Roaring>();
    bm->add(1);
    bm->add(3);
    bm->add(5);

    auto key = make_key("segment_0001", "thrift_sig_abc");

    InvertedIndexQueryCacheHandle handle;
    _cache->insert(key, bm, &handle);

    // Lookup should succeed
    InvertedIndexQueryCacheHandle lookup_handle;
    EXPECT_TRUE(_cache->lookup(key, &lookup_handle));

    auto cached_bm = lookup_handle.get_bitmap();
    ASSERT_NE(cached_bm, nullptr);
    EXPECT_TRUE(cached_bm->contains(1));
    EXPECT_TRUE(cached_bm->contains(3));
    EXPECT_TRUE(cached_bm->contains(5));
    EXPECT_FALSE(cached_bm->contains(2));
    EXPECT_EQ(cached_bm->cardinality(), 3);
}

TEST_F(SearchDslQueryCacheTest, lookup_miss) {
    auto key = make_key("segment_0001", "thrift_sig_abc");

    InvertedIndexQueryCacheHandle handle;
    EXPECT_FALSE(_cache->lookup(key, &handle));
}

TEST_F(SearchDslQueryCacheTest, different_keys_independent) {
    auto bm1 = std::make_shared<roaring::Roaring>();
    bm1->add(10);
    auto bm2 = std::make_shared<roaring::Roaring>();
    bm2->add(20);

    auto key1 = make_key("seg_a", "dsl_1");
    auto key2 = make_key("seg_a", "dsl_2");

    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key1, bm1, &h);
    }
    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key2, bm2, &h);
    }

    // Lookup key1
    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(key1, &h));
        auto cached = h.get_bitmap();
        ASSERT_NE(cached, nullptr);
        EXPECT_TRUE(cached->contains(10));
        EXPECT_FALSE(cached->contains(20));
    }

    // Lookup key2
    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(key2, &h));
        auto cached = h.get_bitmap();
        ASSERT_NE(cached, nullptr);
        EXPECT_TRUE(cached->contains(20));
        EXPECT_FALSE(cached->contains(10));
    }
}

TEST_F(SearchDslQueryCacheTest, different_segments_independent) {
    auto bm1 = std::make_shared<roaring::Roaring>();
    bm1->add(1);
    auto bm2 = std::make_shared<roaring::Roaring>();
    bm2->add(2);

    auto key1 = make_key("seg_a", "same_dsl");
    auto key2 = make_key("seg_b", "same_dsl");

    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key1, bm1, &h);
    }
    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key2, bm2, &h);
    }

    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(key1, &h));
        EXPECT_TRUE(h.get_bitmap()->contains(1));
        EXPECT_FALSE(h.get_bitmap()->contains(2));
    }
    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(key2, &h));
        EXPECT_TRUE(h.get_bitmap()->contains(2));
        EXPECT_FALSE(h.get_bitmap()->contains(1));
    }
}

TEST_F(SearchDslQueryCacheTest, no_collision_with_regular_query_cache) {
    // SEARCH_DSL_QUERY key should not collide with a regular EQUAL_QUERY key
    // even with same index_path and value
    auto bm_dsl = std::make_shared<roaring::Roaring>();
    bm_dsl->add(100);
    auto bm_eq = std::make_shared<roaring::Roaring>();
    bm_eq->add(200);

    InvertedIndexQueryCache::CacheKey dsl_key {
            "seg_a", "__search_dsl__", InvertedIndexQueryType::SEARCH_DSL_QUERY, "some_value"};
    InvertedIndexQueryCache::CacheKey eq_key {"seg_a", "__search_dsl__",
                                              InvertedIndexQueryType::EQUAL_QUERY, "some_value"};

    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(dsl_key, bm_dsl, &h);
    }
    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(eq_key, bm_eq, &h);
    }

    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(dsl_key, &h));
        EXPECT_TRUE(h.get_bitmap()->contains(100));
    }
    {
        InvertedIndexQueryCacheHandle h;
        EXPECT_TRUE(_cache->lookup(eq_key, &h));
        EXPECT_TRUE(h.get_bitmap()->contains(200));
    }
}

TEST_F(SearchDslQueryCacheTest, overwrite_same_key) {
    auto bm1 = std::make_shared<roaring::Roaring>();
    bm1->add(1);
    auto bm2 = std::make_shared<roaring::Roaring>();
    bm2->add(99);

    auto key = make_key("seg", "dsl");

    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key, bm1, &h);
    }
    {
        InvertedIndexQueryCacheHandle h;
        _cache->insert(key, bm2, &h);
    }

    InvertedIndexQueryCacheHandle h;
    EXPECT_TRUE(_cache->lookup(key, &h));
    auto cached = h.get_bitmap();
    ASSERT_NE(cached, nullptr);
    EXPECT_TRUE(cached->contains(99));
}

} // namespace doris::segment_v2
