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

#include "storage/index/ann/ann_index_result_cache/ann_index_result_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "storage/index/ann/ann_index_result_cache/ann_index_result_cache_handle.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/ann/vector_search_utils.h"

namespace doris::segment_v2 {

// ============================================================================
// Helper to build AnnTopNParam for cache key tests
// ============================================================================
static AnnTopNParam make_topn_param(const std::vector<float>& vec, size_t limit,
                                    roaring::Roaring* bitmap, size_t rows_of_segment,
                                    VectorSearchUserParams user_params = {}) {
    AnnTopNParam p {
            .query_value = vec.data(),
            .query_value_size = vec.size(),
            .limit = limit,
            ._user_params = user_params,
            .roaring = bitmap,
            .rows_of_segment = rows_of_segment,
    };
    return p;
}

// ============================================================================
// TopN Cache Key Tests
// ============================================================================
class TopnCacheKeyTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache_ = std::make_unique<AnnIndexResultCache>(/*capacity=*/64 * 1024 * 1024,
                                                       /*shards=*/1);
        vec_ = {1.0f, 2.0f, 3.0f, 4.0f};
    }
    std::unique_ptr<AnnIndexResultCache> cache_;
    std::vector<float> vec_;
};

TEST_F(TopnCacheKeyTest, SameParamsSameKey) {
    auto p1 = make_topn_param(vec_, 10, nullptr, 1000);
    auto p2 = make_topn_param(vec_, 10, nullptr, 1000);

    AnnIndexResultCacheHandle h;
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);
    EXPECT_TRUE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, DifferentQueryVectorMisses) {
    auto p1 = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    std::vector<float> vec2 = {5.0f, 6.0f, 7.0f, 8.0f};
    auto p2 = make_topn_param(vec2, 10, nullptr, 1000);
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, DifferentRowsetMisses) {
    auto p = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, &h);
    } while (false);

    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs2", 0, 1, p, &h));
}

TEST_F(TopnCacheKeyTest, DifferentSegmentMisses) {
    auto p = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, &h);
    } while (false);

    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 1, 1, p, &h));
}

TEST_F(TopnCacheKeyTest, DifferentLimitMisses) {
    auto p1 = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    auto p2 = make_topn_param(vec_, 20, nullptr, 1000);
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, NullBitmapAndFullBitmapSameKey) {
    // null bitmap
    auto p1 = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    // full bitmap (cardinality == rows_of_segment)
    roaring::Roaring full;
    full.addRange(0, 1000);
    ASSERT_EQ(full.cardinality(), 1000u);
    auto p2 = make_topn_param(vec_, 10, &full, 1000);
    AnnIndexResultCacheHandle h;
    EXPECT_TRUE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, PartialBitmapDiffersFromNull) {
    auto p1 = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    roaring::Roaring partial;
    partial.addRange(0, 500);
    auto p2 = make_topn_param(vec_, 10, &partial, 1000);
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, DifferentPartialBitmapsMiss) {
    roaring::Roaring bm1;
    bm1.addRange(0, 500);
    auto p1 = make_topn_param(vec_, 10, &bm1, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    roaring::Roaring bm2;
    bm2.addRange(200, 700);
    auto p2 = make_topn_param(vec_, 10, &bm2, 1000);
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, SamePartialBitmapHits) {
    roaring::Roaring bm1;
    bm1.addRange(0, 500);
    auto p1 = make_topn_param(vec_, 10, &bm1, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    roaring::Roaring bm2;
    bm2.addRange(0, 500);
    auto p2 = make_topn_param(vec_, 10, &bm2, 1000);
    AnnIndexResultCacheHandle h;
    EXPECT_TRUE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, DifferentHnswEfSearchMisses) {
    VectorSearchUserParams up1;
    up1.hnsw_ef_search = 32;
    auto p1 = make_topn_param(vec_, 10, nullptr, 1000, up1);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, &h);
    } while (false);

    VectorSearchUserParams up2;
    up2.hnsw_ef_search = 64;
    auto p2 = make_topn_param(vec_, 10, nullptr, 1000, up2);
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, &h));
}

TEST_F(TopnCacheKeyTest, DifferentIndexIdMisses) {
    auto p = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, &h);
    } while (false);

    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 2, p, &h));
}

// ============================================================================
// Range Cache Key Tests
// ============================================================================
class RangeCacheKeyTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache_ = std::make_unique<AnnIndexResultCache>(64 * 1024 * 1024, 1);
        vec_ = {1.0f, 2.0f, 3.0f, 4.0f};
    }
    std::unique_ptr<AnnIndexResultCache> cache_;
    std::vector<float> vec_;
    VectorSearchUserParams user_params_;
};

TEST_F(RangeCacheKeyTest, SameParamsHit) {
    AnnRangeSearchParams p;
    p.query_value = vec_.data();
    p.radius = 5.0f;
    p.is_le_or_lt = true;
    p.roaring = nullptr;

    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, user_params_, 4, 1000, &h);
    } while (false);
    AnnIndexResultCacheHandle h;
    EXPECT_TRUE(cache_->lookup("rs1", 0, 1, p, user_params_, 4, 1000, &h));
}

TEST_F(RangeCacheKeyTest, DifferentRadiusMisses) {
    AnnRangeSearchParams p;
    p.query_value = vec_.data();
    p.radius = 5.0f;
    p.is_le_or_lt = true;
    p.roaring = nullptr;
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, user_params_, 4, 1000, &h);
    } while (false);

    AnnRangeSearchParams p2;
    p2.query_value = vec_.data();
    p2.radius = 10.0f;
    p2.is_le_or_lt = true;
    p2.roaring = nullptr;
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, user_params_, 4, 1000, &h));
}

TEST_F(RangeCacheKeyTest, DifferentIsLeOrLtMisses) {
    AnnRangeSearchParams p;
    p.query_value = vec_.data();
    p.radius = 5.0f;
    p.is_le_or_lt = true;
    p.roaring = nullptr;
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, user_params_, 4, 1000, &h);
    } while (false);

    AnnRangeSearchParams p2;
    p2.query_value = vec_.data();
    p2.radius = 5.0f;
    p2.is_le_or_lt = false;
    p2.roaring = nullptr;
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p2, user_params_, 4, 1000, &h));
}

TEST_F(RangeCacheKeyTest, NullAndFullBitmapSameKey) {
    AnnRangeSearchParams p1;
    p1.query_value = vec_.data();
    p1.radius = 5.0f;
    p1.is_le_or_lt = true;
    p1.roaring = nullptr;
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p1, user_params_, 4, 1000, &h);
    } while (false);

    roaring::Roaring full;
    full.addRange(0, 1000);
    AnnRangeSearchParams p2;
    p2.query_value = vec_.data();
    p2.radius = 5.0f;
    p2.is_le_or_lt = true;
    p2.roaring = &full;
    AnnIndexResultCacheHandle h;
    EXPECT_TRUE(cache_->lookup("rs1", 0, 1, p2, user_params_, 4, 1000, &h));
}

TEST_F(RangeCacheKeyTest, DifferentIndexIdMisses) {
    AnnRangeSearchParams p;
    p.query_value = vec_.data();
    p.radius = 5.0f;
    p.is_le_or_lt = true;
    p.roaring = nullptr;
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, p, user_params_, 4, 1000, &h);
    } while (false);

    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 2, p, user_params_, 4, 1000, &h));
}

TEST_F(RangeCacheKeyTest, TopnAndRangeKeysDontCollide) {
    // Insert a topn entry
    auto topn_p = make_topn_param(vec_, 10, nullptr, 1000);
    do {
        AnnIndexResultCacheHandle h;
        cache_->insert("rs1", 0, 1, topn_p, &h);
    } while (false);

    // Lookup range with same vec should miss (type=1 vs type=0)
    AnnRangeSearchParams rp;
    rp.query_value = vec_.data();
    rp.radius = 10.0f;
    rp.is_le_or_lt = true;
    rp.roaring = nullptr;
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, rp, user_params_, 4, 1000, &h));
}

// ============================================================================
// Cache Lifecycle Tests
// ============================================================================
class CacheLifecycleTest : public ::testing::Test {
protected:
    void SetUp() override { cache_ = std::make_unique<AnnIndexResultCache>(64 * 1024 * 1024, 1); }
    std::unique_ptr<AnnIndexResultCache> cache_;
};

TEST_F(CacheLifecycleTest, LookupWithoutInsertMisses) {
    std::vector<float> vec = {1.0f, 2.0f};
    auto p = make_topn_param(vec, 5, nullptr, 100);
    AnnIndexResultCacheHandle h;
    EXPECT_FALSE(cache_->lookup("rs1", 0, 1, p, &h));
}

TEST_F(CacheLifecycleTest, InsertThenLookupReturnsData) {
    std::vector<float> vec = {1.0f, 2.0f};
    auto p = make_topn_param(vec, 5, nullptr, 100);

    AnnIndexResultCacheHandle insert_h;
    insert_h.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {10, 20, 30});
    insert_h.distances = std::shared_ptr<float[]>(new float[3] {0.1f, 0.2f, 0.3f});
    auto bm = std::make_shared<roaring::Roaring>();
    bm->add(10);
    bm->add(20);
    bm->add(30);
    insert_h.roaring = bm;

    ASSERT_TRUE(cache_->insert("rs1", 0, 1, p, &insert_h));

    AnnIndexResultCacheHandle out;
    ASSERT_TRUE(cache_->lookup("rs1", 0, 1, p, &out));
    ASSERT_NE(out.row_ids, nullptr);
    EXPECT_EQ(out.row_ids->size(), 3u);
    EXPECT_EQ((*out.row_ids)[0], 10u);
    EXPECT_EQ((*out.row_ids)[1], 20u);
    EXPECT_EQ((*out.row_ids)[2], 30u);
    ASSERT_NE(out.distances, nullptr);
    EXPECT_FLOAT_EQ(out.distances[0], 0.1f);
    EXPECT_FLOAT_EQ(out.distances[1], 0.2f);
    EXPECT_FLOAT_EQ(out.distances[2], 0.3f);
    ASSERT_NE(out.roaring, nullptr);
    EXPECT_TRUE(out.roaring->contains(10));
    EXPECT_TRUE(out.roaring->contains(20));
    EXPECT_TRUE(out.roaring->contains(30));
}

// ============================================================================
// IndexSearchResult::to_cache_handle() Tests
// ============================================================================
class IndexSearchResultToCacheHandleTest : public ::testing::Test {};

TEST_F(IndexSearchResultToCacheHandleTest, AllFieldsSet) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1, 2, 3});
    r.distances = std::shared_ptr<float[]>(new float[3]);
    r.distances[0] = 0.1f;
    r.distances[1] = 0.2f;
    r.distances[2] = 0.3f;
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(1);
    r.roaring->add(2);
    r.roaring->add(3);

    auto h = r.to_cache_handle();
    ASSERT_NE(h.row_ids, nullptr);
    EXPECT_EQ(h.row_ids->size(), 3u);
    ASSERT_NE(h.distances, nullptr);
    EXPECT_FLOAT_EQ(h.distances[0], 0.1f);
    ASSERT_NE(h.roaring, nullptr);
    EXPECT_EQ(h.roaring->cardinality(), 3u);
}

TEST_F(IndexSearchResultToCacheHandleTest, OnlyRowIdsSet) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {5, 10});

    auto h = r.to_cache_handle();
    ASSERT_NE(h.row_ids, nullptr);
    EXPECT_EQ(h.row_ids->size(), 2u);
    EXPECT_EQ(h.distances, nullptr);
    EXPECT_EQ(h.roaring, nullptr);
}

TEST_F(IndexSearchResultToCacheHandleTest, OnlyRoaringSet) {
    IndexSearchResult r;
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(42);

    auto h = r.to_cache_handle();
    EXPECT_EQ(h.row_ids, nullptr);
    EXPECT_EQ(h.distances, nullptr);
    ASSERT_NE(h.roaring, nullptr);
    EXPECT_TRUE(h.roaring->contains(42));
}

TEST_F(IndexSearchResultToCacheHandleTest, EmptyResult) {
    IndexSearchResult r;
    auto h = r.to_cache_handle();
    EXPECT_EQ(h.row_ids, nullptr);
    EXPECT_EQ(h.distances, nullptr);
    EXPECT_EQ(h.roaring, nullptr);
}

TEST_F(IndexSearchResultToCacheHandleTest, DistancesWithZeroRows) {
    IndexSearchResult r;
    r.distances = std::shared_ptr<float[]>(new float[0]);

    auto h = r.to_cache_handle();
    // distances are shared even when row_ids is null
    ASSERT_NE(h.distances, nullptr);
}

// ============================================================================
// AnnIndexResultCacheHandle::to_index_search_result() Tests
// ============================================================================
class CacheHandleToIndexSearchResultTest : public ::testing::Test {};

TEST_F(CacheHandleToIndexSearchResultTest, AllFieldsSet) {
    AnnIndexResultCacheHandle h;
    h.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1, 2});
    h.distances = std::shared_ptr<float[]>(new float[2] {0.5f, 1.5f});
    h.roaring = std::make_shared<roaring::Roaring>();
    h.roaring->add(1);
    h.roaring->add(2);

    auto r = h.to_index_search_result();
    ASSERT_NE(r.row_ids, nullptr);
    EXPECT_EQ(r.row_ids->size(), 2u);
    ASSERT_NE(r.distances, nullptr);
    EXPECT_FLOAT_EQ(r.distances[0], 0.5f);
    EXPECT_FLOAT_EQ(r.distances[1], 1.5f);
    ASSERT_NE(r.roaring, nullptr);
    EXPECT_EQ(r.roaring->cardinality(), 2u);
}

TEST_F(CacheHandleToIndexSearchResultTest, DistancesNull) {
    AnnIndexResultCacheHandle h;
    h.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1});

    auto r = h.to_index_search_result();
    EXPECT_EQ(r.distances, nullptr);
    ASSERT_NE(r.row_ids, nullptr);
    EXPECT_EQ(r.row_ids->size(), 1u);
}

TEST_F(CacheHandleToIndexSearchResultTest, RowIdsNull) {
    AnnIndexResultCacheHandle h;
    h.distances = std::shared_ptr<float[]>(new float[1] {1.0f});

    auto r = h.to_index_search_result();
    EXPECT_EQ(r.row_ids, nullptr);
    ASSERT_NE(r.distances, nullptr);
}

TEST_F(CacheHandleToIndexSearchResultTest, AllNull) {
    AnnIndexResultCacheHandle h;
    auto r = h.to_index_search_result();
    EXPECT_EQ(r.distances, nullptr);
    EXPECT_EQ(r.row_ids, nullptr);
    EXPECT_EQ(r.roaring, nullptr);
}

TEST_F(CacheHandleToIndexSearchResultTest, RoaringShared) {
    AnnIndexResultCacheHandle h;
    h.roaring = std::make_shared<roaring::Roaring>();
    h.roaring->add(99);

    auto r = h.to_index_search_result();
    ASSERT_NE(r.roaring, nullptr);
    // roaring is shared, not deep-copied
    EXPECT_EQ(r.roaring.get(), h.roaring.get());
}

// ============================================================================
// AnnRangeSearchResult::to_cache_handle() Tests
// ============================================================================
class RangeResultToCacheHandleTest : public ::testing::Test {};

TEST_F(RangeResultToCacheHandleTest, AllFieldsSet) {
    AnnRangeSearchResult r;
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(1);
    r.roaring->add(2);
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1, 2});
    r.distance = std::shared_ptr<float[]>(new float[2]);
    r.distance[0] = 0.1f;
    r.distance[1] = 0.2f;

    auto h = r.to_cache_handle();
    ASSERT_NE(h.roaring, nullptr);
    EXPECT_EQ(h.roaring->cardinality(), 2u);
    // Shared pointer: no deep copy
    EXPECT_EQ(h.roaring.get(), r.roaring.get());
    ASSERT_NE(h.row_ids, nullptr);
    EXPECT_EQ(h.row_ids->size(), 2u);
    ASSERT_NE(h.distances, nullptr);
    EXPECT_FLOAT_EQ(h.distances[0], 0.1f);
}

TEST_F(RangeResultToCacheHandleTest, OnlyRoaringSet) {
    AnnRangeSearchResult r;
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(42);

    auto h = r.to_cache_handle();
    ASSERT_NE(h.roaring, nullptr);
    EXPECT_TRUE(h.roaring->contains(42));
    EXPECT_EQ(h.row_ids, nullptr);
    EXPECT_EQ(h.distances, nullptr);
}

TEST_F(RangeResultToCacheHandleTest, EmptyResult) {
    AnnRangeSearchResult r;
    auto h = r.to_cache_handle();
    EXPECT_EQ(h.roaring, nullptr);
    EXPECT_EQ(h.row_ids, nullptr);
    EXPECT_EQ(h.distances, nullptr);
}

// ============================================================================
// AnnRangeSearchResult::from_cache_handle() Tests
// ============================================================================
class RangeResultFromCacheHandleTest : public ::testing::Test {};

TEST_F(RangeResultFromCacheHandleTest, RoundtripFidelity) {
    AnnRangeSearchResult orig;
    orig.roaring = std::make_shared<roaring::Roaring>();
    orig.roaring->add(3);
    orig.roaring->add(7);
    orig.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {3, 7});
    orig.distance = std::shared_ptr<float[]>(new float[2]);
    orig.distance[0] = 1.1f;
    orig.distance[1] = 2.2f;

    auto h = orig.to_cache_handle();
    auto restored = AnnRangeSearchResult::from_cache_handle(h);

    ASSERT_NE(restored.roaring, nullptr);
    EXPECT_TRUE(restored.roaring->contains(3));
    EXPECT_TRUE(restored.roaring->contains(7));
    EXPECT_EQ(restored.roaring->cardinality(), 2u);
    ASSERT_NE(restored.row_ids, nullptr);
    EXPECT_EQ(restored.row_ids->size(), 2u);
    EXPECT_EQ((*restored.row_ids)[0], 3u);
    EXPECT_EQ((*restored.row_ids)[1], 7u);
    ASSERT_NE(restored.distance, nullptr);
    EXPECT_FLOAT_EQ(restored.distance[0], 1.1f);
    EXPECT_FLOAT_EQ(restored.distance[1], 2.2f);
    EXPECT_EQ(restored.roaring.get(), orig.roaring.get());
    EXPECT_EQ(restored.row_ids.get(), orig.row_ids.get());
    EXPECT_EQ(restored.distance.get(), orig.distance.get());
}

TEST_F(RangeResultFromCacheHandleTest, AllNullFields) {
    AnnIndexResultCacheHandle h;
    auto r = AnnRangeSearchResult::from_cache_handle(h);
    EXPECT_EQ(r.roaring, nullptr);
    EXPECT_EQ(r.row_ids, nullptr);
    EXPECT_EQ(r.distance, nullptr);
}

TEST_F(RangeResultFromCacheHandleTest, OnlyRowIdsInHandle) {
    AnnIndexResultCacheHandle h;
    h.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {5});

    auto r = AnnRangeSearchResult::from_cache_handle(h);
    ASSERT_NE(r.row_ids, nullptr);
    EXPECT_EQ(r.row_ids->size(), 1u);
    EXPECT_EQ(r.roaring, nullptr);
    EXPECT_EQ(r.distance, nullptr);
}

// ============================================================================
// normalize_topn_result() Tests
//
// normalize_topn_result is a static function in ann_index_reader.cpp.
// We replicate its logic here for unit testing since it's not publicly
// accessible. These tests verify all 6 if-branch conditions.
// ============================================================================

static void normalize_topn_result(IndexSearchResult& result) {
    if (result.roaring == nullptr && result.row_ids != nullptr) {
        auto rebuilt = std::make_shared<roaring::Roaring>();
        for (auto row_id : *result.row_ids) {
            rebuilt->add(static_cast<uint32_t>(row_id));
        }
        result.roaring = std::move(rebuilt);
    }

    if (result.roaring == nullptr) {
        result.roaring = std::make_shared<roaring::Roaring>();
    }

    if (result.row_ids == nullptr) {
        result.row_ids = std::make_shared<std::vector<uint64_t>>();
    }

    size_t rows = std::min<size_t>(result.row_ids->size(), result.roaring->cardinality());

    if (result.row_ids->size() != rows) {
        result.row_ids->resize(rows);
    }

    if (result.distances == nullptr) {
        result.distances = std::shared_ptr<float[]>(new float[rows]);
        for (size_t i = 0; i < rows; ++i) {
            result.distances[i] = 0.0f;
        }
    }

    if (result.roaring->cardinality() != rows) {
        auto rebuilt = std::make_shared<roaring::Roaring>();
        for (size_t i = 0; i < rows; ++i) {
            rebuilt->add(static_cast<uint32_t>((*result.row_ids)[i]));
        }
        result.roaring = std::move(rebuilt);
    }
}

class NormalizeTopnResultTest : public ::testing::Test {};

// Branch 1: roaring null + row_ids present → roaring rebuilt from row_ids
TEST_F(NormalizeTopnResultTest, RoaringNullRowIdsPresent) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {5, 10, 15});
    r.distances = std::shared_ptr<float[]>(new float[3]);
    r.distances[0] = 1.0f;
    r.distances[1] = 2.0f;
    r.distances[2] = 3.0f;

    normalize_topn_result(r);

    ASSERT_NE(r.roaring, nullptr);
    EXPECT_EQ(r.roaring->cardinality(), 3u);
    EXPECT_TRUE(r.roaring->contains(5));
    EXPECT_TRUE(r.roaring->contains(10));
    EXPECT_TRUE(r.roaring->contains(15));
}

// Branch 2: roaring null + row_ids null → both set to empty
TEST_F(NormalizeTopnResultTest, BothNull) {
    IndexSearchResult r;

    normalize_topn_result(r);

    ASSERT_NE(r.roaring, nullptr);
    EXPECT_EQ(r.roaring->cardinality(), 0u);
    ASSERT_NE(r.row_ids, nullptr);
    EXPECT_EQ(r.row_ids->size(), 0u);
    ASSERT_NE(r.distances, nullptr);
}

// Branch 3: distances null → filled with 0.0f
TEST_F(NormalizeTopnResultTest, DistancesNull) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1, 2});
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(1);
    r.roaring->add(2);

    normalize_topn_result(r);

    ASSERT_NE(r.distances, nullptr);
    EXPECT_FLOAT_EQ(r.distances[0], 0.0f);
    EXPECT_FLOAT_EQ(r.distances[1], 0.0f);
}

// Branch 4: row_ids larger than roaring cardinality → truncated to min
TEST_F(NormalizeTopnResultTest, RowIdsLargerThanRoaring) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1, 2, 3, 4, 5});
    r.distances = std::shared_ptr<float[]>(new float[5]);
    for (int i = 0; i < 5; ++i) r.distances[i] = static_cast<float>(i);
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(1);
    r.roaring->add(2);

    normalize_topn_result(r);

    // rows = min(5, 2) = 2
    EXPECT_EQ(r.row_ids->size(), 2u);
    // roaring rebuilt from truncated row_ids
    EXPECT_EQ(r.roaring->cardinality(), 2u);
    EXPECT_TRUE(r.roaring->contains(1));
    EXPECT_TRUE(r.roaring->contains(2));
}

// Branch 5: roaring cardinality larger than row_ids → roaring rebuilt
TEST_F(NormalizeTopnResultTest, RoaringLargerThanRowIds) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {1});
    r.distances = std::shared_ptr<float[]>(new float[1]);
    r.distances[0] = 0.5f;
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(1);
    r.roaring->add(2);
    r.roaring->add(3);

    normalize_topn_result(r);

    // rows = min(1, 3) = 1, row_ids stays size 1, roaring rebuilt with just {1}
    EXPECT_EQ(r.row_ids->size(), 1u);
    EXPECT_EQ(r.roaring->cardinality(), 1u);
    EXPECT_TRUE(r.roaring->contains(1));
}

// Branch 6: all fields consistent → no changes
TEST_F(NormalizeTopnResultTest, AllConsistent) {
    IndexSearchResult r;
    r.row_ids = std::make_shared<std::vector<uint64_t>>(std::vector<uint64_t> {10, 20});
    r.distances = std::shared_ptr<float[]>(new float[2]);
    r.distances[0] = 1.0f;
    r.distances[1] = 2.0f;
    r.roaring = std::make_shared<roaring::Roaring>();
    r.roaring->add(10);
    r.roaring->add(20);

    auto* orig_row_ids = r.row_ids.get();
    auto* orig_distances = r.distances.get();
    auto orig_roaring = r.roaring;

    normalize_topn_result(r);

    EXPECT_EQ(r.row_ids.get(), orig_row_ids);
    EXPECT_EQ(r.distances.get(), orig_distances);
    // roaring not rebuilt when cardinality matches
    EXPECT_EQ(r.roaring.get(), orig_roaring.get());
    EXPECT_EQ(r.row_ids->size(), 2u);
    EXPECT_EQ(r.roaring->cardinality(), 2u);
}

} // namespace doris::segment_v2
