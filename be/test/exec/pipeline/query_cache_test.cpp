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

#include "runtime/query_cache/query_cache.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "core/data_type/data_type_number.h"
#include "cpp/sync_point.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "storage/data_dir.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "testutil/column_helper.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {
class QueryCacheTest : public testing::Test {
public:
    void SetUp() override {}
};

TEST_F(QueryCacheTest, create_global_cache) {
    auto* cache = QueryCache::create_global_cache(1024 * 1024 * 1024, 16);
    delete cache;
}

TEST_F(QueryCacheTest, build_cache_key) {
    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.emplace_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.emplace_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_xyz"});
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        std::cout << st.msg() << std::endl;
        EXPECT_FALSE(st.ok());
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range;
        TPaloScanRange palp_scan_range;
        palp_scan_range.__set_tablet_id(42);
        palp_scan_range.__set_version("114514");
        scan_range.scan_range.__set_palo_scan_range(palp_scan_range);
        scan_ranges.push_back(scan_range);
        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        std::cout << st.msg() << std::endl;
        std::cout << version << std::endl;
        EXPECT_FALSE(st.ok());
    }
    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range;
        TPaloScanRange palp_scan_range;
        palp_scan_range.__set_tablet_id(42);
        palp_scan_range.__set_version("114514");
        scan_range.scan_range.__set_palo_scan_range(palp_scan_range);
        scan_ranges.push_back(scan_range);
        TQueryCacheParam cache_param;
        cache_param.__set_digest("be ut");
        cache_param.tablet_to_range.insert({42, "test"});
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        std::cout << st.msg() << std::endl;
        std::cout << version << std::endl;
        std::cout << cache_key << std::endl;
        EXPECT_TRUE(st.ok());
    }
}

TEST_F(QueryCacheTest, build_cache_key_multiple_tablets) {
    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(3);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(1);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TScanRangeParams scan_range3;
        TPaloScanRange palp_scan_range3;
        palp_scan_range3.__set_tablet_id(2);
        palp_scan_range3.__set_version("100");
        scan_range3.scan_range.__set_palo_scan_range(palp_scan_range3);
        scan_ranges.push_back(scan_range3);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_abc"});
        cache_param.tablet_to_range.insert({3, "range_abc"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_TRUE(st.ok());
        EXPECT_EQ(version, 100);

        int64_t expected_tablet1 = 1;
        int64_t expected_tablet2 = 2;
        int64_t expected_tablet3 = 3;
        std::string expected_key =
                "test_digest" +
                std::string(reinterpret_cast<char*>(&expected_tablet1), sizeof(expected_tablet1)) +
                std::string(reinterpret_cast<char*>(&expected_tablet2), sizeof(expected_tablet2)) +
                std::string(reinterpret_cast<char*>(&expected_tablet3), sizeof(expected_tablet3)) +
                "range_abc";

        EXPECT_EQ(cache_key, expected_key);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("200");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_abc"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("same version") != std::string::npos);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_xyz"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("same tablet_to_range") != std::string::npos);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({3, "range_abc"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("Not find tablet") != std::string::npos);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("empty") != std::string::npos);
    }
}

TEST_F(QueryCacheTest, insert_and_lookup) {
    std::unique_ptr<QueryCache> query_cache {QueryCache::create_global_cache(1024 * 1024 * 1024)};
    std::string cache_key = "be ut";
    {
        //insert
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, 42, result, {1, 2, 3}, 1);
    }

    {
        //lookup
        std::unique_ptr<QueryCacheHandle> handle = std::make_unique<QueryCacheHandle>();
        EXPECT_TRUE(query_cache->lookup(cache_key, 42, handle.get()));
        EXPECT_TRUE(ColumnHelper::block_equal(
                *handle->get_cache_result()->back(),
                ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5})));
        EXPECT_EQ(handle->get_cache_slot_orders()->size(), 3);
        EXPECT_EQ(handle->get_cache_version(), 42);

        QueryCacheHandle handle1 {std::move(*handle)};
        QueryCacheHandle handle2;
        handle2 = std::move(handle1);

        EXPECT_TRUE(ColumnHelper::block_equal(
                *handle2.get_cache_result()->back(),
                ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5})));
        EXPECT_EQ(handle2.get_cache_slot_orders()->size(), 3);
        EXPECT_EQ(handle2.get_cache_version(), 42);
    }
}

// ./run-be-ut.sh --run --filter=DataQueueTest.*

namespace {

std::vector<TScanRangeParams> make_scan_ranges(int64_t tablet_id, const std::string& version) {
    std::vector<TScanRangeParams> scan_ranges;
    TScanRangeParams scan_range;
    TPaloScanRange palo_scan_range;
    palo_scan_range.__set_tablet_id(tablet_id);
    palo_scan_range.__set_version(version);
    scan_range.scan_range.__set_palo_scan_range(palo_scan_range);
    scan_ranges.push_back(scan_range);
    return scan_ranges;
}

TQueryCacheParam make_cache_param(int64_t tablet_id) {
    TQueryCacheParam cache_param;
    cache_param.__set_digest("runtime_test_digest");
    cache_param.tablet_to_range.insert({tablet_id, "range"});
    // FE always sets the entry limits; keep them roomy so decision tests do
    // not trip the write-back feasibility check unintentionally.
    cache_param.__set_entry_max_bytes(1024 * 1024);
    cache_param.__set_entry_max_rows(100000);
    return cache_param;
}

void insert_entry(QueryCache* cache, const std::string& cache_key, int64_t version,
                  int64_t delta_count) {
    CacheResult result;
    result.push_back(std::make_unique<Block>());
    *result.back() = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
    cache->insert(cache_key, version, result, {0}, 1, delta_count);
}

} // namespace

TEST_F(QueryCacheTest, runtime_decision_miss_and_idempotent) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    QueryCacheRuntime runtime(make_cache_param(42), cache.get());

    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->current_version, 100);
    EXPECT_FALSE(decision->handle.valid());

    // Idempotent: the second caller (e.g. the other operator) observes the
    // same decision object.
    auto decision2 = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision.get(), decision2.get());
}

TEST_F(QueryCacheTest, runtime_decision_invalid_key) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    // Tablet 42 is missing from tablet_to_range, so build_cache_key fails and
    // the query degrades to an uncached scan instead of failing.
    TQueryCacheParam cache_param;
    cache_param.__set_digest("runtime_test_digest");
    cache_param.tablet_to_range.insert({43, "range"});
    QueryCacheRuntime runtime(cache_param, cache.get());

    int64_t stale_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_FALSE(decision->key_valid);
    // Every caller shares one immutable invalid decision (and one log line).
    EXPECT_EQ(decision.get(), runtime.get_or_make_decision(scan_ranges).get());
    // The degraded-scan path returns before the candidate build, so it must
    // settle no metrics at all.
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(), stale_before);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before);
}

TEST_F(QueryCacheTest, runtime_decision_hit) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 100, 0);

    int64_t stale_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::HIT);
    EXPECT_TRUE(decision->handle.valid());
    EXPECT_EQ(decision->handle.get_cache_version(), 100);
    // An exact hit is neither a stale hit nor a fallback.
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(), stale_before);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before);
}

TEST_F(QueryCacheTest, runtime_decision_force_refresh) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 100, 0);

    cache_param.__set_force_refresh_query_cache(true);
    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    // Recompute and write back even though a fresh entry exists.
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_FALSE(decision->handle.valid());
    // A forced refresh is a deliberate MISS with an empty reason, not a
    // fallback: the caller-side settlement must not count it.
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before);
}

TEST_F(QueryCacheTest, runtime_decision_binlog_scan) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 100, 0);

    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    QueryCacheRuntime runtime(cache_param, cache.get());
    runtime.disable_for_binlog_scan();
    auto decision = runtime.get_or_make_decision(scan_ranges);
    // A binlog scan must neither serve the cached entry nor write back, and
    // its deliberate MISS (empty reason) must not count as a fallback.
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_FALSE(decision->key_valid);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before);
}

TEST_F(QueryCacheTest, runtime_decision_stale_without_incremental) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 50, 0);

    // allow_incremental unset: a stale entry is a plain miss (full recompute).
    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_FALSE(decision->handle.valid());
    // Incremental merge never engaged (empty reason), so the caller-side
    // settlement must not count a fallback.
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before);
}

TEST_F(QueryCacheTest, runtime_decision_stale_incremental_fallbacks) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());

    {
        // The entry is newer than the version this replica is asked to read:
        // never usable, fall back to a full scan of the requested version.
        insert_entry(cache.get(), cache_key, 200, 0);
        QueryCacheRuntime runtime(cache_param, cache.get());
        auto decision = runtime.get_or_make_decision(scan_ranges);
        EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
        EXPECT_TRUE(decision->key_valid);
        EXPECT_EQ(decision->incremental_fallback_reason, "cached entry is newer");
    }
    {
        // Too many accumulated incremental merges: force a full recompute to
        // compact the entry. (Checked before any tablet access.)
        insert_entry(cache.get(), cache_key, 50, config::query_cache_max_incremental_merge_count);
        QueryCacheRuntime runtime(cache_param, cache.get());
        auto decision = runtime.get_or_make_decision(scan_ranges);
        EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
        EXPECT_TRUE(decision->key_valid);
        EXPECT_EQ(decision->incremental_fallback_reason,
                  "delta count reached compaction threshold");
    }
    {
        // Use a tablet id that exists nowhere, so fetching the tablet fails
        // regardless of whether some other suite left a storage engine behind
        // in this test process, and the decision safely falls back to MISS.
        auto missing_scan_ranges = make_scan_ranges(424242424242, "100");
        auto missing_cache_param = make_cache_param(424242424242);
        missing_cache_param.__set_allow_incremental(true);
        std::string missing_cache_key;
        int64_t missing_version = 0;
        EXPECT_TRUE(QueryCache::build_cache_key(missing_scan_ranges, missing_cache_param,
                                                &missing_cache_key, &missing_version)
                            .ok());
        insert_entry(cache.get(), missing_cache_key, 50, 0);
        QueryCacheRuntime runtime(missing_cache_param, cache.get());
        auto decision = runtime.get_or_make_decision(missing_scan_ranges);
        EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
        EXPECT_TRUE(decision->key_valid);
        EXPECT_FALSE(decision->handle.valid());
        EXPECT_EQ(decision->incremental_fallback_reason, "tablet not found");
    }
}

TEST_F(QueryCacheTest, lookup_any_version_and_delta_count) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());

    QueryCacheHandle handle;
    EXPECT_FALSE(cache->lookup_any_version(cache_key, &handle));

    int64_t write_backs_before = DorisMetrics::instance()->query_cache_write_back_total->value();
    insert_entry(cache.get(), cache_key, 50, 3);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_write_back_total->value(),
              write_backs_before + 1);
    QueryCacheHandle handle2;
    EXPECT_TRUE(cache->lookup_any_version(cache_key, &handle2));
    EXPECT_EQ(handle2.get_cache_version(), 50);
    EXPECT_EQ(handle2.get_cache_delta_count(), 3);
    // insert_entry stores one 3-row block with cache_size 1.
    EXPECT_EQ(handle2.get_cache_total_rows(), 3);
    EXPECT_EQ(handle2.get_cache_total_bytes(), 1);

    // Exact-version lookup still rejects the stale entry.
    QueryCacheHandle handle3;
    EXPECT_FALSE(cache->lookup(cache_key, 100, &handle3));
}

TEST_F(QueryCacheTest, runtime_default_global_cache) {
    // The single-argument constructor falls back to the global instance
    // (whatever it is in this test environment).
    QueryCacheRuntime runtime(make_cache_param(42));
    EXPECT_EQ(runtime.cache(), QueryCache::instance());
}

TEST_F(QueryCacheTest, take_delta_read_source) {
    auto decision = std::make_shared<QueryCacheInstanceDecision>();
    decision->_delta_read_sources[42] = std::make_unique<TabletReadSource>();

    EXPECT_EQ(decision->take_delta_read_source(41), nullptr);
    auto source = decision->take_delta_read_source(42);
    EXPECT_NE(source, nullptr);
    // Each read source can be consumed exactly once.
    EXPECT_EQ(decision->take_delta_read_source(42), nullptr);
}

// Exercises the per-tablet part of the incremental decision against a real
// (metadata-only) tablet registered in a real storage engine: capturing the
// delta read source never touches segment files, so no data is needed.
class QueryCacheIncrementalTest : public testing::Test {
protected:
    static constexpr int64_t kTabletId = 15673;
    static constexpr const char* kTestDir = "/ut_dir/query_cache_incremental_test";

    // Fatal assertions: every test in this fixture dereferences the storage
    // engine and the data dir, so a failed setup must not fall through to the
    // test body (TearDown still runs and tolerates the partial state).
    void SetUp() override {
        char buffer[1024];
        ASSERT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _absolute_dir = std::string(buffer) + kTestDir;
        Status st = io::global_local_filesystem()->delete_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;

        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _data_dir = std::make_unique<DataDir>(*_engine, _absolute_dir);
        st = _data_dir->init();
        ASSERT_TRUE(st.ok()) << st;
        _cache.reset(QueryCache::create_global_cache(1024 * 1024));
    }

    void TearDown() override {
        // Idempotent guard: a fatal assertion between enable_processing() and
        // the in-test cleanup (see the racer tests) must not leak an enabled
        // SyncPoint with callbacks over dead stack variables to later tests.
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
        _cache.reset();
        _data_dir.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _engine = nullptr;
        Status st = io::global_local_filesystem()->delete_directory(_absolute_dir);
        EXPECT_TRUE(st.ok()) << st;
    }

    void init_rs_meta(RowsetMetaSharedPtr& rs_meta, const TabletMetaSharedPtr& tablet_meta,
                      int64_t start, int64_t end, bool with_delete_predicate) {
        static const std::string json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670
        })";
        RowsetMetaPB rowset_meta_pb;
        ASSERT_TRUE(json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb));
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        // One distinct rowset id per rowset (the json template repeats one id):
        // the merge-on-write history-rewrite check tells baseline and delta
        // rowsets apart by rowset id, see rowset_id_for().
        rowset_meta_pb.set_rowset_id(540081 + start);
        rowset_meta_pb.set_creation_time(10000);
        if (with_delete_predicate) {
            DeletePredicatePB* delete_predicate = rowset_meta_pb.mutable_delete_predicate();
            delete_predicate->set_version(static_cast<int32_t>(start));
            delete_predicate->add_sub_predicates("k1='1'");
        }
        ASSERT_TRUE(rs_meta->init_from_pb(rowset_meta_pb));
        rs_meta->set_tablet_schema(tablet_meta->tablet_schema());
    }

    TabletSharedPtr create_tablet(TKeysType::type keys_type, bool enable_merge_on_write,
                                  const std::vector<std::pair<int64_t, int64_t>>& versions,
                                  int64_t delete_predicate_start_version = -1) {
        TTabletSchema schema;
        schema.keys_type = keys_type;
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1, 2, kTabletId, 15674, 4, 5, schema, 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, enable_merge_on_write));
        for (auto [start, end] : versions) {
            RowsetMetaSharedPtr rs_meta(new RowsetMeta());
            init_rs_meta(rs_meta, tablet_meta, start, end, start == delete_predicate_start_version);
            Status st = tablet_meta->add_rs_meta(rs_meta);
            EXPECT_TRUE(st.ok()) << st;
            if (!st.ok()) {
                return nullptr;
            }
        }
        auto tablet = std::make_shared<Tablet>(*_engine, std::move(tablet_meta), _data_dir.get());
        Status st = tablet->init();
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok()) {
            return nullptr;
        }
        auto& tablet_map = _engine->tablet_manager()->_get_tablet_map(kTabletId);
        tablet_map[kTabletId] = tablet;
        return tablet;
    }

    // The rowset id a fixture rowset starting at `start_version` ends up with,
    // matching init_rs_meta() above (numeric ids use the v1 format).
    static RowsetId rowset_id_for(int64_t start_version) {
        RowsetId id;
        id.init(540081 + start_version);
        return id;
    }

    // Common scenario: a stale entry of version 50 while the query reads
    // version 100 with allow_incremental set.
    std::shared_ptr<QueryCacheInstanceDecision> make_stale_decision() {
        auto scan_ranges = make_scan_ranges(kTabletId, "100");
        auto cache_param = make_cache_param(kTabletId);
        cache_param.__set_allow_incremental(true);
        std::string cache_key;
        int64_t version = 0;
        EXPECT_TRUE(
                QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
        insert_entry(_cache.get(), cache_key, 50, 0);
        QueryCacheRuntime runtime(cache_param, _cache.get());
        return runtime.get_or_make_decision(scan_ranges);
    }

    // Runs two get_or_make_decision callers concurrently, parked at the
    // pre-publish sync point until both built a live candidate, so the
    // publish genuinely races. Returns both results (same object expected).
    std::pair<std::shared_ptr<QueryCacheInstanceDecision>,
              std::shared_ptr<QueryCacheInstanceDecision>>
    race_two_callers(QueryCacheRuntime& runtime, const std::vector<TScanRangeParams>& scan_ranges) {
        auto* sp = SyncPoint::get_instance();
        std::atomic<int> arrived {0};
        sp->set_call_back("QueryCacheRuntime::get_or_make_decision.before_publish", [&](auto&&) {
            arrived.fetch_add(1);
            // Deadline instead of an unbounded spin: if publication ever
            // stops passing through this sync point, fail the test instead
            // of hanging the whole UT binary.
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (arrived.load() < 2 && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
            EXPECT_GE(arrived.load(), 2) << "racer barrier timed out";
        });
        sp->enable_processing();

        std::shared_ptr<QueryCacheInstanceDecision> d1;
        std::shared_ptr<QueryCacheInstanceDecision> d2;
        std::thread t1;
        std::thread t2;
        // If the second thread's constructor ever throws (resource
        // exhaustion), destroying the still-joinable first one would
        // std::terminate() the whole UT binary; join both on every exit.
        Defer join_guard {[&] {
            if (t1.joinable()) {
                t1.join();
            }
            if (t2.joinable()) {
                t2.join();
            }
        }};
        t1 = std::thread([&] { d1 = runtime.get_or_make_decision(scan_ranges); });
        t2 = std::thread([&] { d2 = runtime.get_or_make_decision(scan_ranges); });
        t1.join();
        t2.join();
        sp->disable_processing();
        sp->clear_all_call_backs();
        // Guard against vacuous passes: if the production sync point is ever
        // renamed or removed, the barrier never fires and the racer tests
        // would silently stop exercising the two-candidate race.
        EXPECT_EQ(arrived.load(), 2);
        return {d1, d2};
    }

    std::string _absolute_dir;
    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    std::unique_ptr<QueryCache> _cache;
};

TEST_F(QueryCacheIncrementalTest, incremental_success) {
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 80}, {81, 100}});
    int64_t stale_hits_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(),
              stale_hits_before + 1);

    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_TRUE(decision->handle.valid());
    EXPECT_TRUE(decision->write_back_feasible);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    EXPECT_EQ(decision->cached_version, 50);
    EXPECT_EQ(decision->cached_delta_count, 0);
    EXPECT_EQ(decision->current_version, 100);

    // The pre-captured read source covers exactly the delta (50, 100].
    auto source = decision->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 2);
    EXPECT_TRUE(source->delete_predicates.empty());
    // Consumable exactly once.
    EXPECT_EQ(decision->take_delta_read_source(kTabletId), nullptr);
}

TEST_F(QueryCacheIncrementalTest, incremental_write_back_infeasible) {
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 100}});
    // The cached entry alone (3 rows) already exceeds entry_max_rows, so the
    // merged entry could never be written back: still scan only the delta, but
    // announce upfront that cloning blocks for a write back is pointless.
    auto scan_ranges = make_scan_ranges(kTabletId, "100");
    auto cache_param = make_cache_param(kTabletId);
    cache_param.__set_allow_incremental(true);
    cache_param.__set_entry_max_rows(2);
    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(_cache.get(), cache_key, 50, 0);

    QueryCacheRuntime runtime(cache_param, _cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_FALSE(decision->write_back_feasible);
}

TEST_F(QueryCacheIncrementalTest, fallback_on_agg_keys) {
    create_tablet(TKeysType::AGG_KEYS, false, {{0, 50}, {51, 100}});
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    // Storage-layer aggregation breaks "cached + delta == new snapshot".
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_FALSE(decision->handle.valid());
    EXPECT_EQ(decision->incremental_fallback_reason, "keys type not append-only");
}

TEST_F(QueryCacheIncrementalTest, fallback_on_merge_on_read) {
    // Merge-on-read UNIQUE resolves duplicates by merging across rowsets at
    // read time, so a delta-only scan cannot stand alone: always fall back.
    create_tablet(TKeysType::UNIQUE_KEYS, false, {{0, 50}, {51, 100}});
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "keys type not append-only");
}

TEST_F(QueryCacheIncrementalTest, mow_pure_append_incremental) {
    // Merge-on-write UNIQUE with no delete-bitmap entry in the delta window:
    // the hourly-append pattern. Incremental merge is as safe as on DUP.
    create_tablet(TKeysType::UNIQUE_KEYS, true, {{0, 50}, {51, 80}, {81, 100}});
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    auto source = decision->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 2);
    // capture_read_source hands the delete bitmap to the delta scan, so rows
    // replaced within the delta window itself are filtered by the reader.
    EXPECT_NE(source->delete_bitmap, nullptr);
}

TEST_F(QueryCacheIncrementalTest, mow_history_rewrite_falls_back) {
    // A load inside the delta window marked a row of a baseline rowset as
    // deleted (an upsert / backfill hit a pre-existing key): rows already
    // folded into the cached entry cannot be subtracted, so fall back.
    auto tablet = create_tablet(TKeysType::UNIQUE_KEYS, true, {{0, 50}, {51, 100}});
    ASSERT_NE(tablet, nullptr);
    // Stamped exactly at the window's upper edge (= current_version), which
    // must be inclusive.
    tablet->tablet_meta()->delete_bitmap().add({rowset_id_for(0), 0, 100}, 7);
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_FALSE(decision->handle.valid());
    EXPECT_EQ(decision->incremental_fallback_reason, "delta rewrites history rows");
}

TEST_F(QueryCacheIncrementalTest, mow_cached_side_pinned_across_classification) {
    // The rewrite markers classification looks for live on the cached side,
    // which the delta capture does not pin. Unpinned, a compaction spanning
    // both sides can retire such a rowset without relocating the marker of a
    // row it dropped, and the unused-rowset GC can then wipe that marker
    // before classification reads it -- a stale entry would silently merge
    // with the rows that replaced it. So the pin must cover the whole cached
    // range and must still be held while classification runs, which is where
    // this sync point observes it.
    ASSERT_NE(create_tablet(TKeysType::UNIQUE_KEYS, true, {{0, 50}, {51, 100}}), nullptr);
    auto* sp = SyncPoint::get_instance();
    int64_t pinned_start = -1;
    int64_t pinned_end = -1;
    int observed = 0;
    sp->set_call_back("QueryCacheRuntime::_capture_tablet_delta.cached_side_pinned",
                      [&](auto&& args) {
                          auto* pinned = try_any_cast<std::vector<RowsetSharedPtr>*>(args[0]);
                          ++observed;
                          EXPECT_FALSE(pinned->empty());
                          if (!pinned->empty()) {
                              pinned_start = pinned->front()->start_version();
                              pinned_end = pinned->back()->end_version();
                          }
                      });
    sp->enable_processing();
    Defer clear_sp {[&] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    // Guard against a vacuous pass: a renamed or removed sync point would
    // leave the pin unobserved while the assertions below still held.
    EXPECT_EQ(observed, 1);
    // The cached snapshot is everything up to the cached version, and every
    // bit of it must be pinned: an unpinned tail is evidence we could lose.
    EXPECT_EQ(pinned_start, 0);
    EXPECT_EQ(pinned_end, 50);
}

TEST_F(QueryCacheIncrementalTest, mow_cached_side_not_pinnable_falls_back) {
    // Part of the cached snapshot is gone (compacted away and swept), so its
    // rewrite markers may already have been collected: classification could no
    // longer tell an append from an overwrite. The delta is unaffected and
    // still captures, so only the cached-side pin can catch this. The pin asks
    // quietly, which yields whatever prefix it walked -- here [0, 40], short
    // of the cached version 50 -- so it is the coverage check, not emptiness,
    // that must reject it.
    ASSERT_NE(create_tablet(TKeysType::UNIQUE_KEYS, true, {{0, 40}, {51, 80}, {81, 100}}), nullptr);
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cached versions not pinnable");
}

TEST_F(QueryCacheIncrementalTest, mow_irrelevant_bitmap_entries_ignored) {
    // Delete-bitmap entries that cannot affect the cached snapshot must not
    // spoil the incremental path: entries targeting the delta rowsets (a key
    // written twice within the window, a lost sequence-column race or a delete
    // sign on a window-local key), entries outside the version window (older
    // dedup history, a concurrent load newer than the read version, pending
    // entries at TEMP_VERSION_COMMON = 0) and empty bitmaps.
    auto tablet = create_tablet(TKeysType::UNIQUE_KEYS, true, {{0, 50}, {51, 100}});
    ASSERT_NE(tablet, nullptr);
    auto& delete_bitmap = tablet->tablet_meta()->delete_bitmap();
    delete_bitmap.add({rowset_id_for(51), 0, 90}, 3); // targets the delta itself
    delete_bitmap.add({rowset_id_for(0), 0, 40}, 1);  // version <= cached
    delete_bitmap.add({rowset_id_for(0), 0, 150}, 2); // version > current
    delete_bitmap.add({rowset_id_for(0), 0, 0}, 4);   // pending (TEMP_VERSION_COMMON)
    delete_bitmap.set({rowset_id_for(0), 0, 60}, roaring::Roaring()); // empty bitmap
    // An out-of-int64-range version, only producible by corrupt persisted
    // meta: must take the above-window hop and stay skipped. Placed as the
    // ONLY entry of its (rowset, segment) group on purpose -- the int64-cast
    // predecessor of the skip-scan classified such a key as below-window and
    // its lower_bound hop then landed back on the key itself, livelocking.
    // An above-window sibling in the same group would mask that: its own hop
    // overshoots this key so it is never classified (an in-window sibling
    // would merely relocate the landing into a two-entry livelock).
    delete_bitmap.add({rowset_id_for(0), 1, std::numeric_limits<uint64_t>::max()}, 5);
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
}

TEST_F(QueryCacheIncrementalTest, mow_rewrite_on_later_segment_still_detected) {
    // Pins two precision properties of the bitmap skip-scan: the above-window
    // hop must stay within its (rowset, segment) group -- an in-window
    // rewrite on a LATER segment of the same rowset must still be seen --
    // and the window's lower edge (cached_version + 1) is inclusive. A hop
    // that jumped the whole rowset would miss the segment-1 evidence and
    // wrongly keep the incremental path.
    auto tablet = create_tablet(TKeysType::UNIQUE_KEYS, true, {{0, 50}, {51, 100}});
    ASSERT_NE(tablet, nullptr);
    auto& delete_bitmap = tablet->tablet_meta()->delete_bitmap();
    delete_bitmap.add({rowset_id_for(0), 0, 150}, 1); // segment 0: above window, hops
    delete_bitmap.add({rowset_id_for(0), 1, 51}, 2);  // segment 1: at window begin, foreign
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta rewrites history rows");
}

TEST_F(QueryCacheIncrementalTest, fallback_on_version_gap) {
    // The whole history lives in one compacted rowset [0, 100]: no version
    // path can serve (50, 100] alone, so the capture comes back empty and we
    // fall back.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 100}});
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta versions not capturable");
}

TEST_F(QueryCacheIncrementalTest, fallback_on_capture_error) {
    // Non-pathfinding capture failures surface as real errors even under the
    // quiet option (a pathfinding miss instead returns an empty or partial
    // read source, see fallback_on_version_gap and fallback_on_partial_capture).
    // The storage debug point simulates such a failure, e.g. a rowset object
    // missing for a found path.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 100}});
    config::enable_debug_points = true;
    DebugPoints::instance()->add_with_params("Tablet::capture_consistent_versions.inject_failure",
                                             {{"tablet_id", "-2"}});
    auto decision = make_stale_decision();
    DebugPoints::instance()->remove("Tablet::capture_consistent_versions.inject_failure");
    config::enable_debug_points = false;
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta versions not capturable");
}

TEST_F(QueryCacheIncrementalTest, fallback_on_delete_predicate) {
    // A delete predicate inside the delta logically removes rows that are
    // already folded into the cached blocks; that cannot be merged away.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 60}, {61, 100}},
                  /*delete_predicate_start_version=*/61);
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta contains delete predicates");
}

TEST_F(QueryCacheIncrementalTest, fallback_on_partial_capture) {
    // The quiet capture returns whatever prefix of the version path it could
    // walk: with rowsets {0,50},{51,80} and a query at version 100 (a replica
    // whose local view ends short of the queried version), the (50,100]
    // window walks up to 80 and stops, yielding a non-empty partial prefix.
    // Treating it as INCREMENTAL would silently drop the (80,100] rows, so
    // the endpoint coverage check must reject it.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 80}});
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta versions not capturable");
}

TEST_F(QueryCacheIncrementalTest, mismatched_engine_falls_back) {
    // is_cloud_mode() can flip on a live local deployment (cloud_unique_id is
    // a mutable config): ExecEnv::get_tablet then still hands out a plain
    // local Tablet, and the decision must degrade the misconfiguration to a
    // full recompute instead of dereferencing the failed CloudTablet cast.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 100}});
    std::string saved_deploy_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    auto decision = make_stale_decision();
    config::deploy_mode = saved_deploy_mode;
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "tablet is not a cloud tablet");
}

TEST_F(QueryCacheIncrementalTest, concurrent_racers_share_one_decision) {
    // The candidate decision is built outside the runtime lock (a stale
    // decision may scan a large delete bitmap there, and on cloud tablets
    // first sync the view from the meta service), so two operators of the
    // same instance can race: the sync point parks both threads right before
    // publication, forcing two live candidates. Exactly one must win, both
    // callers must observe the winner, and the metrics must be settled once.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 100}});
    auto scan_ranges = make_scan_ranges(kTabletId, "100");
    auto cache_param = make_cache_param(kTabletId);
    cache_param.__set_allow_incremental(true);
    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(_cache.get(), cache_key, 50, 0);
    QueryCacheRuntime runtime(cache_param, _cache.get());

    int64_t stale_hits_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    auto [d1, d2] = race_two_callers(runtime, scan_ranges);

    ASSERT_NE(d1, nullptr);
    EXPECT_EQ(d1.get(), d2.get());
    EXPECT_EQ(d1->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(),
              stale_hits_before + 1);
    // The winner's captured read source is intact no matter which candidate
    // won: the loser's duplicate capture released with its own candidate.
    auto source = d1->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 1);
}

TEST_F(QueryCacheIncrementalTest, concurrent_racers_on_miss_settle_no_metrics) {
    // No cache entry exists at all: both racers build plain-MISS candidates
    // with an empty fallback reason (incremental merge never engaged), so
    // publication must settle neither metric while both callers still adopt
    // one shared object.
    auto scan_ranges = make_scan_ranges(kTabletId, "100");
    auto cache_param = make_cache_param(kTabletId);
    cache_param.__set_allow_incremental(true);
    QueryCacheRuntime runtime(cache_param, _cache.get());

    int64_t stale_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto [d1, d2] = race_two_callers(runtime, scan_ranges);

    ASSERT_NE(d1, nullptr);
    EXPECT_EQ(d1.get(), d2.get());
    EXPECT_EQ(d1->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(d1->incremental_fallback_reason.empty());
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(), stale_before);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before);
}

TEST_F(QueryCacheIncrementalTest, concurrent_racers_settle_fallback_once) {
    // Both racers build a fallback candidate (an AGG-keys tablet rejects the
    // incremental path with a non-empty reason): the fallback counter must
    // move exactly once, by the published winner.
    create_tablet(TKeysType::AGG_KEYS, false, {{0, 50}, {51, 100}});
    auto scan_ranges = make_scan_ranges(kTabletId, "100");
    auto cache_param = make_cache_param(kTabletId);
    cache_param.__set_allow_incremental(true);
    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(_cache.get(), cache_key, 50, 0);
    QueryCacheRuntime runtime(cache_param, _cache.get());

    int64_t fallback_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto [d1, d2] = race_two_callers(runtime, scan_ranges);

    ASSERT_NE(d1, nullptr);
    EXPECT_EQ(d1.get(), d2.get());
    EXPECT_EQ(d1->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d1->incremental_fallback_reason, "keys type not append-only");
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallback_before + 1);
}

// Exercises the incremental decision against a real CloudTablet materialized
// through the regular cache-miss load of ExecEnv::get_tablet, with the two
// meta-service RPCs intercepted through sync points (the same pattern as
// cloud_schema_change_job_test): get_tablet_meta serves a local meta and the
// loader's first sync_tablet_rowsets installs the requested rowsets. The
// decision's own sync is then either short-cut by the local max version
// covering query_version, or (as the second sync call) answered with an
// injected result without changing the view. Capturing the delta read source
// never touches rowset data, so none exists.
class QueryCacheCloudIncrementalTest : public testing::Test {
protected:
    static constexpr int64_t kTabletId = 15673;

    void SetUp() override {
        _saved_deploy_mode = config::deploy_mode;
        config::deploy_mode = "cloud";
        auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _cache.reset(QueryCache::create_global_cache(1024 * 1024));
        _sp = SyncPoint::get_instance();
        _sp->enable_processing();
    }

    void TearDown() override {
        _sp->disable_processing();
        _sp->clear_all_call_backs();
        _cache.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _engine = nullptr;
        config::deploy_mode = _saved_deploy_mode;
    }

    // Builds metadata-only rowsets for `versions` and installs them into the
    // tablet's view, exactly like a production sync would. Also records them
    // in _installed_rowsets for tests that stamp delete-bitmap entries onto
    // specific rowsets.
    void install_rowsets(CloudTablet* tablet,
                         const std::vector<std::pair<int64_t, int64_t>>& versions) {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.reserve(versions.size());
        for (auto [start, end] : versions) {
            auto rs_meta = std::make_shared<RowsetMeta>();
            rs_meta->set_rowset_type(BETA_ROWSET);
            rs_meta->set_version({start, end});
            rs_meta->set_rowset_id(_engine->next_rowset_id());
            rs_meta->set_num_segments(1);
            rs_meta->set_tablet_schema(tablet->tablet_meta()->tablet_schema());
            RowsetSharedPtr rowset;
            Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
            EXPECT_TRUE(st.ok()) << st;
            if (st.ok()) {
                rowsets.push_back(std::move(rowset));
            }
        }
        _installed_rowsets.insert(_installed_rowsets.end(), rowsets.begin(), rowsets.end());
        std::unique_lock lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), /*version_overlap=*/false, lock);
    }

    void intercept_tablet_load(
            std::vector<std::pair<int64_t, int64_t>> versions, Status decision_sync_result,
            TKeysType::type keys_type = TKeysType::DUP_KEYS, bool enable_merge_on_write = false,
            std::vector<std::pair<int64_t, int64_t>> decision_sync_installs = {},
            // Delete-bitmap marks {index into _installed_rowsets, version} the
            // decision's sync stamps AFTER installing its rowsets, but ONLY when
            // that sync carried sync_delete_bitmap (mirroring the real body).
            // Lets a test both exercise cloud sync-completeness and GUARD the
            // flag: a rewrite entry the loader's sync never carried, that only a
            // sync_delete_bitmap=true decision sync brings, then feeds the
            // history check.
            std::vector<std::pair<size_t, int64_t>> decision_sync_bitmap_marks = {}) {
        _sp->set_call_back("CloudMetaMgr::get_tablet_meta",
                           [keys_type, enable_merge_on_write](auto&& args) {
                               TTabletSchema schema;
                               schema.keys_type = keys_type;
                               auto meta = std::make_shared<TabletMeta>(
                                       1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                       std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                       UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                       TCompressionType::LZ4F, 0, enable_merge_on_write);
                               // Pin the RUNNING state (also the ctor default) explicitly: a
                               // tablet that is not RUNNING takes sync_if_not_running(), which
                               // clears the just-installed view and re-syncs through the same
                               // interception, breaking both the version view and _sync_calls.
                               meta->set_tablet_state(TABLET_RUNNING);
                               *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
                               try_any_cast_ret<Status>(args)->second = true;
                           });
        _sp->set_call_back(
                "CloudMetaMgr::sync_tablet_rowsets",
                [this, versions = std::move(versions), decision_sync_result,
                 decision_sync_installs = std::move(decision_sync_installs),
                 decision_sync_bitmap_marks =
                         std::move(decision_sync_bitmap_marks)](auto&& args) {
                    auto* tablet = try_any_cast<CloudTablet*>(args[0]);
                    const auto* options = try_any_cast<const SyncOptions*>(args[1]);
                    auto* ret = try_any_cast_ret<Status>(args);
                    if (_sync_calls.fetch_add(1) == 0) {
                        // The loader's sync: install the requested rowsets.
                        install_rowsets(tablet, versions);
                    } else {
                        // The incremental decision's sync: install the extra
                        // rowsets, if any, stamp any delete-bitmap marks it is
                        // responsible for bringing, and inject the result.
                        install_rowsets(tablet, decision_sync_installs);
                        // Gate the marks on sync_delete_bitmap exactly as the real
                        // sync body does (cloud_meta_mgr.cpp): a sync that did not
                        // carry the flag brings no delete bitmap. This makes the
                        // mock flag-faithful, so a test can prove the pre-sync set
                        // the flag -- flip it to false and the mark is dropped.
                        if (options->sync_delete_bitmap) {
                            for (auto [rs_idx, ver] : decision_sync_bitmap_marks) {
                                // .at(): a mistyped index in a future test should
                                // fail loudly, not be silent out-of-bounds UB.
                                tablet->tablet_meta()->delete_bitmap().add(
                                        {_installed_rowsets.at(rs_idx)->rowset_id(), 0, ver}, 7);
                            }
                        }
                        ret->first = decision_sync_result;
                    }
                    ret->second = true;
                });
    }

    // A stale entry of version 50 while the query reads version 100 with
    // allow_incremental set, mirroring the local-storage fixture.
    std::shared_ptr<QueryCacheInstanceDecision> make_stale_decision() {
        auto scan_ranges = make_scan_ranges(kTabletId, "100");
        auto cache_param = make_cache_param(kTabletId);
        cache_param.__set_allow_incremental(true);
        std::string cache_key;
        int64_t version = 0;
        EXPECT_TRUE(
                QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
        insert_entry(_cache.get(), cache_key, 50, 0);
        QueryCacheRuntime runtime(cache_param, _cache.get());
        return runtime.get_or_make_decision(scan_ranges);
    }

    std::string _saved_deploy_mode;
    CloudStorageEngine* _engine = nullptr;
    std::unique_ptr<QueryCache> _cache;
    SyncPoint* _sp = nullptr;
    std::atomic<int> _sync_calls {0};
    // The rowsets the loader's sync installed, for tests that stamp
    // delete-bitmap entries onto specific rowsets.
    std::vector<RowsetSharedPtr> _installed_rowsets;
};

TEST_F(QueryCacheCloudIncrementalTest, incremental_success_on_synced_view) {
    // The loader installs a view that already covers query_version 100, so
    // the decision's sync_rowsets() takes its no-op shortcut (the sync point
    // sees exactly one call) and the decision merges incrementally, exactly
    // like on local storage.
    intercept_tablet_load({{0, 50}, {51, 100}}, Status::OK());
    int64_t stale_hits_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(),
              stale_hits_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_EQ(decision->cached_version, 50);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    EXPECT_EQ(_sync_calls.load(), 1);
    auto source = decision->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 1);
    EXPECT_EQ(source->rs_splits.front().rs_reader->rowset()->start_version(), 51);
    EXPECT_EQ(source->rs_splits.front().rs_reader->rowset()->end_version(), 100);
}

TEST_F(QueryCacheCloudIncrementalTest, fallback_on_sync_failure) {
    // The local view (max 50) does not cover query_version 100, so the
    // decision must sync from the meta service first; the injected failure
    // falls the query back to a full recompute.
    intercept_tablet_load({{0, 50}}, Status::InternalError<false>("injected sync failure"));
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud rowset sync failed");
    EXPECT_EQ(_sync_calls.load(), 2);
}

TEST_F(QueryCacheCloudIncrementalTest, incremental_after_decision_sync_installs_delta) {
    // The flagship cloud composition: the local view (max 50) does not cover
    // query_version 100, so the decision's sync really goes to the meta
    // service; the injected response installs the missing delta rowset, and
    // the capture on the freshly synced view then merges incrementally.
    intercept_tablet_load({{0, 50}}, Status::OK(), TKeysType::DUP_KEYS,
                          /*enable_merge_on_write=*/false,
                          /*decision_sync_installs=*/ {{51, 100}});
    int64_t stale_hits_before = DorisMetrics::instance()->query_cache_stale_hit_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_stale_hit_total->value(),
              stale_hits_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    EXPECT_EQ(_sync_calls.load(), 2);
    auto source = decision->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 1);
    EXPECT_EQ(source->rs_splits.front().rs_reader->rowset()->start_version(), 51);
    EXPECT_EQ(source->rs_splits.front().rs_reader->rowset()->end_version(), 100);
}

TEST_F(QueryCacheCloudIncrementalTest, fallback_when_synced_view_misses_delta) {
    // The decision's sync succeeds (injected OK) but leaves the view at max
    // 50, so the delta versions (51, 100] are not on the local version
    // graph: the quiet capture returns an empty read source and the decision
    // falls back instead of silently dropping the delta rows.
    intercept_tablet_load({{0, 50}}, Status::OK());
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta versions not capturable");
    EXPECT_EQ(_sync_calls.load(), 2);
}

TEST_F(QueryCacheCloudIncrementalTest, mow_pure_append_incremental) {
    // A merge-on-write UNIQUE cloud tablet whose delta window carries no
    // delete-bitmap entry (the hourly-append pattern) merges incrementally,
    // exactly like on local storage: the history-rewrite check runs on the
    // freshly synced bitmap and finds nothing.
    intercept_tablet_load({{0, 50}, {51, 100}}, Status::OK(), TKeysType::UNIQUE_KEYS,
                          /*enable_merge_on_write=*/true);
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    EXPECT_EQ(_sync_calls.load(), 1);
    auto source = decision->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 1);
}

TEST_F(QueryCacheCloudIncrementalTest, mow_history_rewrite_falls_back) {
    // A delete-bitmap entry inside the delta window that targets the
    // baseline rowset (a backfill rewrote a key predating the cached
    // version): the same history-rewrite fallback as on local storage, on a
    // bitmap the cloud sync is responsible for keeping complete.
    intercept_tablet_load({{0, 50}, {51, 100}}, Status::OK(), TKeysType::UNIQUE_KEYS,
                          /*enable_merge_on_write=*/true);
    auto tablet_res = ExecEnv::get_tablet(kTabletId);
    ASSERT_TRUE(tablet_res.has_value()) << tablet_res.error();
    ASSERT_EQ(_installed_rowsets.size(), 2);
    tablet_res.value()->tablet_meta()->delete_bitmap().add(
            {_installed_rowsets[0]->rowset_id(), 0, 60}, 7);

    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta rewrites history rows");
    // The loader already brought the view up to 100, so the decision's sync
    // took the no-op shortcut: still exactly one meta-service round trip.
    EXPECT_EQ(_sync_calls.load(), 1);
}

TEST_F(QueryCacheCloudIncrementalTest, not_incremental_skips_decision_sync) {
    // allow_incremental unset: the stale entry is a plain miss and the
    // decision never reaches the per-tablet path, so it must not load the
    // tablet or talk to the meta service at all (no hidden RPC cost when the
    // feature is off).
    intercept_tablet_load({{0, 50}}, Status::OK());
    auto scan_ranges = make_scan_ranges(kTabletId, "100");
    auto cache_param = make_cache_param(kTabletId);
    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(_cache.get(), cache_key, 50, 0);
    QueryCacheRuntime runtime(cache_param, _cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    EXPECT_EQ(_sync_calls.load(), 0);
}

TEST_F(QueryCacheCloudIncrementalTest, agg_keys_rejected_before_decision_sync) {
    // The keys-type check precedes the cloud sync, so an AGG tablet is
    // rejected after the loader's round trip but without a second one: no
    // meta-service RPC is spent on a tablet that can never merge.
    intercept_tablet_load({{0, 50}}, Status::OK(), TKeysType::AGG_KEYS);
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "keys type not append-only");
    EXPECT_EQ(_sync_calls.load(), 1);
}

TEST_F(QueryCacheCloudIncrementalTest, newer_entry_rejected_before_tablet_access) {
    // The cached entry (version 200) is newer than the queried version 100:
    // rejected before the per-tablet loop, so the tablet is never loaded and
    // the meta service is never contacted.
    intercept_tablet_load({{0, 50}}, Status::OK());
    auto scan_ranges = make_scan_ranges(kTabletId, "100");
    auto cache_param = make_cache_param(kTabletId);
    cache_param.__set_allow_incremental(true);
    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(_cache.get(), cache_key, 200, 0);
    QueryCacheRuntime runtime(cache_param, _cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cached entry is newer");
    EXPECT_EQ(_sync_calls.load(), 0);
}

TEST_F(QueryCacheCloudIncrementalTest, presync_tablet_load_failure_falls_back) {
    // The pre-sync fan-out issues the first ExecEnv::get_tablet; a meta-service
    // failure injected there fails that load, so _presync_cloud_delta_tablets
    // takes its !tablet_res skip (records no reason, leaving the miss for the
    // capture loop to report), and the capture loop's own get_tablet then falls
    // the query back to a full recompute with "tablet not found". This exercises
    // the one pre-sync branch the other cloud tests never reach: they all load
    // the tablet successfully before syncing.
    std::atomic<int> meta_loads {0};
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&meta_loads](auto&& args) {
        meta_loads.fetch_add(1);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->first = Status::InternalError<false>("injected: tablet meta unavailable");
        ret->second = true;
    });
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "tablet not found");
    // The pre-sync get_tablet failed before any sync, so no meta-service rowset
    // sync RPC was ever issued.
    EXPECT_EQ(_sync_calls.load(), 0);
    // Discriminating witness that the PRE-SYNC skip branch actually ran (not
    // that the pre-sync was bypassed and only the capture loop reported "tablet
    // not found", which would give the same mode/reason/_sync_calls): the
    // pre-sync fan-out and the capture loop each attempt their own get_tablet,
    // so the load is tried twice. A bypassed pre-sync would attempt it once.
    EXPECT_EQ(meta_loads.load(), 2);
}

TEST_F(QueryCacheCloudIncrementalTest, unique_non_mow_rejected_before_decision_sync) {
    // A UNIQUE_KEYS tablet WITHOUT merge-on-write (the legacy merge-on-read
    // unique table) is not append-only either, so the keys-type check rejects
    // it before the cloud sync, exactly like an AGG tablet. Exercises the
    // enable_unique_key_merge_on_write()==false leg of the append-only test:
    // the merge-on-write cases take its true leg, AGG never evaluates it (short
    // -circuited by the keys-type compare), so this is the one input that drives
    // that leg false.
    intercept_tablet_load({{0, 50}}, Status::OK(), TKeysType::UNIQUE_KEYS,
                          /*enable_merge_on_write=*/false);
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "keys type not append-only");
    // Rejected at the keys-type check, after the loader's single round trip and
    // without a second (decision) sync.
    EXPECT_EQ(_sync_calls.load(), 1);
}

TEST_F(QueryCacheCloudIncrementalTest, mow_decision_sync_brings_bitmap_detects_rewrite) {
    // Cloud sync-completeness, GUARDING options.sync_delete_bitmap=true: the
    // loader view stops at 50 with no delta and no delete-bitmap entry, so the
    // decision's sync really runs (_sync_calls==2). The fixture mock is
    // flag-faithful -- it stamps the injected rewrite mark only when that sync
    // carried sync_delete_bitmap=true (the CloudMetaMgr::sync_tablet_rowsets sync
    // point forwards the SyncOptions, mirroring the real body's gate). Because the
    // pre-sync sets the flag, the mark on the baseline rowset (version 60, inside
    // the delta window) lands, the history-rewrite check reads that freshly-synced
    // bitmap, and the query falls back. If a refactor flipped the flag to false,
    // the mock would drop the mark just as a real meta-service would drop the
    // bitmap sync, the rewrite would go undetected, and this test would fail
    // (INCREMENTAL instead of MISS) -- so the load-bearing MoW invariant is
    // UT-guarded, not just documented. mow_history_rewrite_falls_back cannot show
    // this: there the loader carried the view to 100, so the decision sync was a
    // no-op and the mark was hand-placed.
    intercept_tablet_load({{0, 50}}, Status::OK(), TKeysType::UNIQUE_KEYS,
                          /*enable_merge_on_write=*/true,
                          /*decision_sync_installs=*/{{51, 100}},
                          /*decision_sync_bitmap_marks=*/{{0, 60}});
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta rewrites history rows");
    // The loader's view (max 50) did not cover the query, so the decision's sync
    // really ran -- two round trips -- and it is that second sync that brought
    // the rewrite evidence the classification tripped on.
    EXPECT_EQ(_sync_calls.load(), 2);
}

TEST_F(QueryCacheCloudIncrementalTest, mow_cloud_cached_side_not_pinnable_falls_back) {
    // The cloud analogue of mow_cached_side_not_pinnable_falls_back, exercising a
    // DEFENSIVE branch that is unreachable after a real cloud sync: the meta
    // service serves a gapless committed set, so a synced [0,max] view never has
    // a hole below the cached version and [0,cached] is always pinnable. The test
    // FORGES the hole ([41,50] missing) with install_rowsets; since the loader
    // already carries the view to 100 the decision sync short-cuts (_sync_calls
    // ==1), so the pin runs against that loader-installed forged view. It
    // confirms the pin-and-endpoint guard is wired on the cloud path (the block
    // is now shared, not cloud-early-returned) and would fire if the gapless
    // invariant were ever broken: the cached-side pin walks only [0,40] and the
    // endpoint check -- short of cached version 50 -- rejects it (not emptiness).
    intercept_tablet_load({{0, 40}, {51, 80}, {81, 100}}, Status::OK(),
                          TKeysType::UNIQUE_KEYS, /*enable_merge_on_write=*/true);
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cached versions not pinnable");
    // The loader carried the view to 100, so the decision's sync took its no-op
    // shortcut: one meta-service round trip.
    EXPECT_EQ(_sync_calls.load(), 1);
}

TEST_F(QueryCacheCloudIncrementalTest, mow_decision_sync_clean_bitmap_stays_incremental) {
    // Positive complement (and non-vacuousness proof) of
    // mow_decision_sync_brings_bitmap_detects_rewrite: identical shape -- loader
    // view 50, the decision's real sync (_sync_calls==2) installs the delta
    // (50,100] on a merge-on-write tablet -- but with NO rewrite mark. The
    // history-rewrite check runs on the freshly synced bitmap, finds nothing, and
    // the merge stays INCREMENTAL. This shows the sibling's fallback is driven by
    // its injected mark and not by the decision-sync-on-MoW path itself: same
    // path, no mark, no fallback.
    intercept_tablet_load({{0, 50}}, Status::OK(), TKeysType::UNIQUE_KEYS,
                          /*enable_merge_on_write=*/true,
                          /*decision_sync_installs=*/{{51, 100}});
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
    EXPECT_EQ(_sync_calls.load(), 2);
    auto source = decision->take_delta_read_source(kTabletId);
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->rs_splits.size(), 1);
    EXPECT_EQ(source->rs_splits.front().rs_reader->rowset()->start_version(), 51);
    EXPECT_EQ(source->rs_splits.front().rs_reader->rowset()->end_version(), 100);
}

TEST_F(QueryCacheCloudIncrementalTest, presync_fans_out_across_tablets_and_merges_reason) {
    // Two append-only tablets in one instance drive the parallel pre-sync
    // fan-out (_presync_cloud_delta_tablets) at N>1 -- every other cloud test
    // here exercises it only at N=1. T1's loader view already covers the queried
    // version, so its decision sync short-cuts; T2's stops short, so its decision
    // sync runs and is injected to fail. The fan-out must sync both tablets, the
    // post-join merge must surface T2's failure keyed by its tablet id, and the
    // capture loop must fall the whole decision back on it -- exercising the
    // multi-tablet fan-out, the per-index reason merge, and the capture-loop
    // consumption together, none of which the single-tablet cases reach.
    constexpr int64_t kTabletId2 = 15680;

    // get_tablet_meta serves a DUP_KEYS meta stamped with the REQUESTED tablet
    // id (args[0]), so both scan ranges materialize as distinct tablets. It is
    // stateless, so the concurrent fan-out loads need no synchronization here.
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        auto tid = try_any_cast<int64_t>(args[0]);
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(
                1, 2, tid, 15674, 4, 5, schema, 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });

    // sync_tablet_rowsets fires concurrently across the fan-out bthreads; a mutex
    // guards the per-tablet call count and the metadata-only install (which draws
    // from the shared engine's rowset-id generator). Per tablet the first call is
    // the loader: T1 gets a view covering 100 (its later decision sync short-cuts,
    // so it never calls back a second time), T2 stops at 50 so its decision sync
    // runs as the second call and is failed there. add_rowsets takes each
    // tablet's own header lock, so the two installs never contend.
    std::mutex mu;
    std::unordered_map<int64_t, int> calls;
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        auto* ret = try_any_cast_ret<Status>(args);
        std::lock_guard<std::mutex> lk(mu);
        if (calls[tablet->tablet_id()]++ == 0) {
            std::vector<std::pair<int64_t, int64_t>> versions =
                    tablet->tablet_id() == kTabletId
                            ? std::vector<std::pair<int64_t, int64_t>> {{0, 50}, {51, 100}}
                            : std::vector<std::pair<int64_t, int64_t>> {{0, 50}};
            std::vector<RowsetSharedPtr> rowsets;
            for (auto [start, end] : versions) {
                auto rs_meta = std::make_shared<RowsetMeta>();
                rs_meta->set_rowset_type(BETA_ROWSET);
                rs_meta->set_version({start, end});
                rs_meta->set_rowset_id(_engine->next_rowset_id());
                rs_meta->set_num_segments(1);
                rs_meta->set_tablet_schema(tablet->tablet_meta()->tablet_schema());
                RowsetSharedPtr rowset;
                if (RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok()) {
                    rowsets.push_back(std::move(rowset));
                }
            }
            std::unique_lock lock(tablet->get_header_lock());
            tablet->add_rowsets(std::move(rowsets), /*version_overlap=*/false, lock);
        } else {
            // T2's decision sync (T1's short-cuts and never reaches here).
            ret->first = Status::InternalError<false>("injected sync failure");
        }
        ret->second = true;
    });

    // A stale entry (version 50) keyed over both tablets, queried at 100.
    std::vector<TScanRangeParams> scan_ranges = make_scan_ranges(kTabletId, "100");
    scan_ranges.push_back(make_scan_ranges(kTabletId2, "100").front());
    auto cache_param = make_cache_param(kTabletId);
    cache_param.tablet_to_range.insert({kTabletId2, "range"});
    cache_param.__set_allow_incremental(true);
    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(_cache.get(), cache_key, 50, 0);
    QueryCacheRuntime runtime(cache_param, _cache.get());

    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud rowset sync failed");
    // Both tablets fanned out and were synced: T1 once (loader; decision
    // short-cut) and T2 twice (loader + the failed decision sync).
    std::lock_guard<std::mutex> lk(mu);
    EXPECT_EQ(calls[kTabletId], 1);
    EXPECT_EQ(calls[kTabletId2], 2);
}

TEST_F(QueryCacheCloudIncrementalTest, decision_sync_timeout_falls_back) {
    // A meta-service brownout: the decision's rowset sync stalls past the
    // fast-fail budget (query_cache_decision_sync_timeout_ms). The decision must
    // abandon the wait and fall the query back to a full recompute rather than
    // hold its (bounded) admission thread for the RPC retry budget. Made
    // deterministic with a gate that keeps the decision sync blocked until AFTER
    // the assertion, so the decision's wait_for is guaranteed to expire first;
    // the test then releases it and waits for the detached sync to fully exit
    // its callback before teardown, so the callback never runs after the fixture
    // (and the std::function it runs in) is torn down.
    int32_t saved_timeout = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 50;
    Defer restore_timeout {[&] { config::query_cache_decision_sync_timeout_ms = saved_timeout; }};

    std::atomic<bool> release_gate {false};
    std::atomic<bool> decision_sync_returned {false};
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(
                1, 2, kTabletId, 15674, 4, 5, schema, 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                       [this, &release_gate, &decision_sync_returned](auto&& args) {
                           auto* tablet = try_any_cast<CloudTablet*>(args[0]);
                           auto* ret = try_any_cast_ret<Status>(args);
                           ret->second = true;
                           if (_sync_calls.fetch_add(1) == 0) {
                               // Loader sync: install a view that does NOT cover
                               // query 100, so the decision sync really runs.
                               install_rowsets(tablet, {{0, 50}});
                               return;
                           }
                           // The decision's sync stalls until the test releases
                           // the gate (after it has asserted the fast-fail).
                           while (!release_gate.load()) {
                               std::this_thread::sleep_for(std::chrono::milliseconds(1));
                           }
                           // Last statement: signals the callback body is done,
                           // so the test can wait for it before teardown.
                           decision_sync_returned = true;
                       });

    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    int64_t decision_sync_ms_before =
            DorisMetrics::instance()->query_cache_decision_sync_time_ms->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud rowset sync timed out");
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    // The timeout path still records the wall time it blocked before giving up
    // (steady_clock taken before the fan-out, read again after wait_for expires),
    // so the sync-time counter advances by at least the budget. This is the one
    // test that proves the metric is wired on the fast-fail path; wait_for is
    // guaranteed not to return before its duration elapses, so >= budget is not
    // a timing bet.
    EXPECT_GE(DorisMetrics::instance()->query_cache_decision_sync_time_ms->value() -
                      decision_sync_ms_before,
              config::query_cache_decision_sync_timeout_ms);

    // Release the stalled sync and wait for its callback to fully exit before
    // teardown clears the sync points (destroying the callback while it runs is
    // UB). The detached fan-out task reaches its decision sync eventually even
    // on a slow host, so this converges.
    release_gate = true;
    for (int i = 0; i < 2000 && !decision_sync_returned.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(decision_sync_returned.load());
    // Both syncs ran once the fan-out joined: the loader and the (stalled)
    // decision sync. The decision just did not wait for the second to finish.
    EXPECT_EQ(_sync_calls.load(), 2);
    // The decision deliberately abandons its fan-out future, so there is no
    // handle to join the detached sync on. This margin covers the microsecond
    // stack unwind of the callback and its SyncPoint shell after the last store
    // above, plus the task's tail (releasing a non-last CloudTablet ref -- the
    // tablet cache still holds one, so nothing is destroyed here -- and the
    // fork-join bookkeeping). None of that tail touches fixture state, so a
    // 200ms margin over a microsecond window is a safe cushion, not a bet on
    // timing being fast enough.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

} // namespace doris
