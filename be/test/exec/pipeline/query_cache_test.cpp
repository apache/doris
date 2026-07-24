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
#include "common/exception.h"
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
#include "util/threadpool.h"
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

// (c) query_cache_decision_sync_timeout_ms <= 0 disables cloud incremental
// merge: the presync must spawn NO sync work and fall every scanned tablet back.
// No storage engine is installed, so the <= 0 guard returning cleanly (before
// to_cloud()) is itself proof nothing launched -- any launch would have
// dereferenced the absent engine. The sync-point counter double-confirms zero
// sync_rowsets calls.
TEST_F(QueryCacheTest, presync_skips_launch_when_timeout_non_positive) {
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 0;
    Defer cfg_defer {[&] { config::query_cache_decision_sync_timeout_ms = saved; }};

    auto scan_ranges = make_scan_ranges(101, "100");
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(101), 1);
    EXPECT_EQ(reasons[101], "cloud incremental sync disabled");
    EXPECT_EQ(sync_calls.load(), 0);
}

// (a) A stopped engine must bail before launching: positive budget, but
// stopped() is true, so no sync_rowsets runs and every tablet falls back. The
// stopped() check precedes the pool access, so an un-opened engine's null pool
// is never touched.
TEST_F(QueryCacheTest, presync_bails_when_engine_stopped) {
    auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    engine->stop(); // sets _stopped = true; safe on an un-opened engine.
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    Defer eng_defer {[] { ExecEnv::GetInstance()->set_storage_engine(nullptr); }};

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 2000; // positive: would launch if not stopped.
    Defer cfg_defer {[&] { config::query_cache_decision_sync_timeout_ms = saved; }};

    auto scan_ranges = make_scan_ranges(42, "100");
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(42), 1);
    EXPECT_EQ(reasons[42], "be is stopping, sync skipped");
    EXPECT_EQ(sync_calls.load(), 0);
}

// (finding 1) is_cloud_mode() can flip true on a live LOCAL deployment (the
// mutable cloud_unique_id), leaving a local StorageEngine installed. Reaching
// the fan-out then must degrade gracefully, not abort in the hard CHECK inside
// to_cloud(): the engine-type dynamic_cast fails, so every tablet falls back
// with "storage engine is not cloud" and no sync is issued.
TEST_F(QueryCacheTest, presync_falls_back_on_non_cloud_engine) {
    auto saved_mode = config::deploy_mode;
    config::deploy_mode = "cloud"; // is_cloud_mode() true while the engine stays local
    ExecEnv::GetInstance()->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    Defer eng_defer {[&] {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        config::deploy_mode = saved_mode;
    }};

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms =
            2000; // positive: would launch on a cloud engine.
    Defer cfg_defer {[&] { config::query_cache_decision_sync_timeout_ms = saved; }};

    auto scan_ranges = make_scan_ranges(42, "100");
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(42), 1);
    EXPECT_EQ(reasons[42], "storage engine is not cloud");
    EXPECT_EQ(sync_calls.load(), 0);
}

// The dedicated pre-sync pool refuses work (here: shut down, the same rejection a
// full bounded queue gives) while the engine itself is still live, so the fan-out
// is reached rather than short-cut by the stopped() guard. Every submit_func then
// fails, and the inline submit-failure path must fall each tablet back AND release
// its own latch slot via publish_slot so the bounded wait still settles instead of
// hanging on a slot no task will ever count down. Two tablets prove the latch
// reaches N purely through the inline claim. No sync RPC is issued (rejected before
// enqueue, so the closure never runs).
TEST_F(QueryCacheTest, presync_falls_back_when_pool_rejects_submit) {
    auto saved_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    auto* engine_ptr = engine.get();
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    Defer eng_defer {[&] {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        config::deploy_mode = saved_mode;
    }};
    // Shut the pool so do_submit rejects before enqueue; the engine stays live
    // (not stopped()), so the fan-out is entered and hits the inline fallback.
    engine_ptr->query_cache_delta_sync_pool().shutdown();

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 2000; // positive: a healthy pool would launch.
    Defer cfg_defer {[&] { config::query_cache_decision_sync_timeout_ms = saved; }};

    auto scan_ranges = make_scan_ranges(42, "100");
    scan_ranges.push_back(make_scan_ranges(43, "100").front());
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(42), 1);
    ASSERT_EQ(reasons.count(43), 1);
    EXPECT_EQ(reasons[42], "cloud rowset sync not scheduled");
    EXPECT_EQ(reasons[43], "cloud rowset sync not scheduled");
    EXPECT_EQ(sync_calls.load(), 0);
}

TEST_F(QueryCacheTest, presync_over_concurrency_cap_falls_back) {
    // The decision-sync wait runs on the query-admission light pool. When the
    // concurrent-waiter soft cap (config::query_cache_max_concurrent_decision_sync) is
    // already reached, a new decision must NOT enter that wait: doing so would park
    // yet another admission worker, and under a brownout of DISTINCT (non-coalescing)
    // keys that is exactly what would starve the whole pool. It falls every scanned
    // tablet back to a full recompute instead. Simulate the cap being reached by
    // presetting the global counter, then assert the fan-out is never launched and
    // every tablet carries the over-capacity reason. Pre-fix (no cap) this same call
    // proceeds into the fan-out and returns scheduling reasons, not this reason.
    auto saved_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    Defer eng_defer {[&] {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        config::deploy_mode = saved_mode;
    }};

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved_timeout = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 2000; // positive: a healthy pool would launch.
    auto saved_cap = config::query_cache_max_concurrent_decision_sync;
    config::query_cache_max_concurrent_decision_sync = 1;
    Defer cfg_defer {[&] {
        config::query_cache_decision_sync_timeout_ms = saved_timeout;
        config::query_cache_max_concurrent_decision_sync = saved_cap;
    }};

    // Simulate the single slot already held by one parked waiter.
    QueryCacheRuntime::presync_active_waiters_for_test().store(1, std::memory_order_release);
    Defer waiter_defer {[] {
        QueryCacheRuntime::presync_active_waiters_for_test().store(0, std::memory_order_release);
    }};

    auto scan_ranges = make_scan_ranges(42, "100");
    scan_ranges.push_back(make_scan_ranges(43, "100").front());
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(42), 1);
    ASSERT_EQ(reasons.count(43), 1);
    EXPECT_EQ(reasons[42], "cloud decision sync at capacity");
    EXPECT_EQ(reasons[43], "cloud decision sync at capacity");
    // The cap gate returns BEFORE the registry and fan-out, so no sync RPC is issued,
    // and the rejected path takes no slot (fetch_add then immediate fetch_sub), so the
    // counter is left exactly as preset.
    EXPECT_EQ(sync_calls.load(), 0);
    EXPECT_EQ(QueryCacheRuntime::presync_active_waiters_for_test().load(), 1);
}

TEST_F(QueryCacheTest, presync_cap_clamps_to_configured_light_pool_width) {
    // The waiter cap must derive from the ACTUAL light-pool width, not just the config.
    // brpc_light_work_pool_threads is operator-configurable; shrunk to 16, the effective
    // cap is width/2 = 8, so the config default of 32 does NOT bind and the 9th concurrent
    // waiter still falls back. Pre-fix (a fixed 32) the counter at 8 is under the cap, so
    // this same call would ADMIT and enter the fan-out instead of rejecting -- exactly the
    // starvation a small configured pool would suffer.
    auto saved_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    Defer eng_defer {[&] {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        config::deploy_mode = saved_mode;
    }};

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved_timeout = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 2000; // positive: a healthy pool would launch.
    auto saved_cap = config::query_cache_max_concurrent_decision_sync;
    auto saved_pool = config::brpc_light_work_pool_threads;
    // A 16-thread pool caps effective waiters at 8, below the untouched config of 32.
    config::brpc_light_work_pool_threads = 16;
    config::query_cache_max_concurrent_decision_sync = 32;
    Defer cfg_defer {[&] {
        config::query_cache_decision_sync_timeout_ms = saved_timeout;
        config::query_cache_max_concurrent_decision_sync = saved_cap;
        config::brpc_light_work_pool_threads = saved_pool;
    }};

    // Exactly the width-derived cap (16/2) already held: the next waiter is over it even
    // though the config cap (32) is not.
    QueryCacheRuntime::presync_active_waiters_for_test().store(8, std::memory_order_release);
    Defer waiter_defer {[] {
        QueryCacheRuntime::presync_active_waiters_for_test().store(0, std::memory_order_release);
    }};

    auto scan_ranges = make_scan_ranges(42, "100");
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(42), 1);
    EXPECT_EQ(reasons[42], "cloud decision sync at capacity");
    EXPECT_EQ(sync_calls.load(), 0);
    EXPECT_EQ(QueryCacheRuntime::presync_active_waiters_for_test().load(), 8);
}

TEST_F(QueryCacheTest, presync_single_thread_light_pool_never_parks_sole_worker) {
    // A 1-thread light pool floors the width-derived cap (1/2) to 0, so NO decision-sync
    // waiter may be admitted: parking the only worker for the timeout would starve all
    // unrelated fragment admission. Every stale query falls back immediately, even with
    // the counter at 0. Pre-fix (max(1, width/2)) the cap was 1, so the first waiter was
    // admitted and parked the sole worker.
    auto saved_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    Defer eng_defer {[&] {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        config::deploy_mode = saved_mode;
    }};

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::atomic<int> sync_calls {0};
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [&](auto&&) { sync_calls.fetch_add(1); });
    Defer sp_defer {[sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    auto saved_timeout = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 2000;
    auto saved_cap = config::query_cache_max_concurrent_decision_sync;
    auto saved_pool = config::brpc_light_work_pool_threads;
    config::brpc_light_work_pool_threads = 1; // width/2 floors to 0 -> admit nobody
    config::query_cache_max_concurrent_decision_sync = 32;
    Defer cfg_defer {[&] {
        config::query_cache_decision_sync_timeout_ms = saved_timeout;
        config::query_cache_max_concurrent_decision_sync = saved_cap;
        config::brpc_light_work_pool_threads = saved_pool;
    }};

    // Even the FIRST waiter (counter 0) is rejected on a 1-thread pool.
    QueryCacheRuntime::presync_active_waiters_for_test().store(0, std::memory_order_release);
    Defer waiter_defer {[] {
        QueryCacheRuntime::presync_active_waiters_for_test().store(0, std::memory_order_release);
    }};

    auto scan_ranges = make_scan_ranges(42, "100");
    auto reasons = QueryCacheRuntime::presync_cloud_delta_tablets_for_test(scan_ranges, 100);

    ASSERT_EQ(reasons.count(42), 1);
    EXPECT_EQ(reasons[42], "cloud decision sync at capacity");
    EXPECT_EQ(sync_calls.load(), 0);
    // The rejected fetch_add was immediately backed out, so the counter is left at 0.
    EXPECT_EQ(QueryCacheRuntime::presync_active_waiters_for_test().load(), 0);
}

// The BE-side mirror of the FE incremental knob gates, applied where the
// fragment context creates the shared runtime. A same-version FE already
// cleared allow_incremental in these cases; the mirror exists for the
// rolling-upgrade window where an older FE without the knob gates still
// requests incremental while a knob is active. The truth table is the FE
// gate's, verbatim: freshness suppresses for every table type, prefer-cached
// only for non-merge-on-write (a MOW read is version-exact regardless of the
// knob), no active knob suppresses nothing.
TEST_F(QueryCacheTest, cloud_knobs_suppress_incremental_truth_table) {
    // Freshness tolerance active: suppresses regardless of the other inputs.
    EXPECT_TRUE(QueryCacheRuntime::cloud_knobs_suppress_incremental(true, false, false));
    EXPECT_TRUE(QueryCacheRuntime::cloud_knobs_suppress_incremental(true, false, true));
    EXPECT_TRUE(QueryCacheRuntime::cloud_knobs_suppress_incremental(true, true, false));
    EXPECT_TRUE(QueryCacheRuntime::cloud_knobs_suppress_incremental(true, true, true));
    // Prefer-cached-rowset active: suppresses only non-MOW.
    EXPECT_TRUE(QueryCacheRuntime::cloud_knobs_suppress_incremental(false, true, false));
    EXPECT_FALSE(QueryCacheRuntime::cloud_knobs_suppress_incremental(false, true, true));
    // No knob active: never suppresses.
    EXPECT_FALSE(QueryCacheRuntime::cloud_knobs_suppress_incremental(false, false, false));
    EXPECT_FALSE(QueryCacheRuntime::cloud_knobs_suppress_incremental(false, false, true));
}

// The whole gate the fragment context applies at runtime creation, as a pure
// function. Covers the two wrappers the truth table above does not: the
// cloud-only guard, and that the gate only ever clears an FE request (never
// grants one). The fragment context's own call site is trivial wiring on top of
// this (it passes config::is_cloud_mode() and the session getters).
TEST_F(QueryCacheTest, gate_allow_incremental_for_cloud_knobs) {
    // Cloud + a suppressing knob (freshness) + FE requested it: cleared. This is
    // the rolling-upgrade case an older FE without the gates produces.
    EXPECT_FALSE(QueryCacheRuntime::gate_allow_incremental_for_cloud_knobs(
            /*requested=*/true, /*is_mow=*/false, /*cloud=*/true, /*freshness=*/true,
            /*prefer=*/false));
    // Same knob, but LOCAL storage: the knobs are inert, so the gate must not
    // fire (local incremental keeps working under an inert freshness setting).
    EXPECT_TRUE(QueryCacheRuntime::gate_allow_incremental_for_cloud_knobs(
            /*requested=*/true, /*is_mow=*/false, /*cloud=*/false, /*freshness=*/true,
            /*prefer=*/false));
    // Cloud + prefer-cached, but the index is MOW: prefer does not apply to MOW,
    // so the request survives (the MOW read is version-exact regardless).
    EXPECT_TRUE(QueryCacheRuntime::gate_allow_incremental_for_cloud_knobs(
            /*requested=*/true, /*is_mow=*/true, /*cloud=*/true, /*freshness=*/false,
            /*prefer=*/true));
    // The gate never grants: FE did not request incremental, so a knob (or its
    // absence) leaves it off.
    EXPECT_FALSE(QueryCacheRuntime::gate_allow_incremental_for_cloud_knobs(
            /*requested=*/false, /*is_mow=*/false, /*cloud=*/true, /*freshness=*/true,
            /*prefer=*/false));
    EXPECT_FALSE(QueryCacheRuntime::gate_allow_incremental_for_cloud_knobs(
            /*requested=*/false, /*is_mow=*/false, /*cloud=*/true, /*freshness=*/false,
            /*prefer=*/false));
    // Cloud, request set, but no knob active: passes through untouched.
    EXPECT_TRUE(QueryCacheRuntime::gate_allow_incremental_for_cloud_knobs(
            /*requested=*/true, /*is_mow=*/false, /*cloud=*/true, /*freshness=*/false,
            /*prefer=*/false));
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
    // is_cloud_mode() can flip on a live local deployment (cloud_unique_id is a
    // mutable config) while the installed engine stays a local StorageEngine. The
    // decision must degrade the misconfiguration to a full recompute instead of
    // aborting in the hard CHECK inside to_cloud(): the pre-sync's engine-type
    // dynamic_cast fails first, so every scanned tablet falls back with "storage
    // engine is not cloud" (earlier and more precise than the per-tablet cast).
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 50}, {51, 100}});
    std::string saved_deploy_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    auto decision = make_stale_decision();
    config::deploy_mode = saved_deploy_mode;
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "storage engine is not cloud");
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
        // Drain the dedicated delta-sync pool BEFORE tearing anything down: a
        // task's publish_slot reap now touches _cache->_presync_flights_lock (the
        // round-2 tombstone reap), so a worker that resumes after _cache is freed
        // would use-after-free the registry lock (the 200ms cushions the tests add
        // do not guarantee a descheduled worker finished). wait() blocks while the
        // cache, engine, and SyncPoint callbacks are all still alive; a task's
        // remaining publish_slot work triggers no SyncPoint callback, so waiting
        // with the callbacks still installed is safe. Only then clear callbacks and
        // destroy the cache/engine -- mirroring production, where
        // CloudStorageEngine::stop() drains this pool before the cache is torn down.
        if (_engine != nullptr) {
            _engine->query_cache_delta_sync_pool().wait();
        }
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
                 decision_sync_bitmap_marks = std::move(decision_sync_bitmap_marks)](auto&& args) {
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
    // failure injected there fails that load on the WORKER thread, and the task
    // publishes "cloud tablet load failed" into its slot. The capture loop must
    // consume that reason and fall back WITHOUT retrying the cache-miss load on
    // the admission thread: during a brownout (the one situation where this
    // load fails slowly) a capture-side retry would repeat the slow RPC on the
    // bounded admission pool, outside the fast-fail deadline. This exercises
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
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud tablet load failed");
    // The pre-sync get_tablet failed before any sync, so no meta-service rowset
    // sync RPC was ever issued.
    EXPECT_EQ(_sync_calls.load(), 0);
    // Exactly ONE load: the worker's. Two would mean the capture loop retried
    // the failed load on the admission thread (the pre-fix behavior); the
    // published reason above is the second witness of the same ordering (a
    // capture-side retry would report its own "tablet not found" instead).
    EXPECT_EQ(meta_loads.load(), 1);
}

TEST_F(QueryCacheCloudIncrementalTest, presync_decision_sync_raise_falls_back) {
    // The worker-side task body calls get_tablet and sync_rowsets. get_tablet fans out
    // through CloudTabletMgr's single-flight loader, which THROWS (std::system_error)
    // when a duplicate concurrent load's CountdownEvent wait fails, and sync_rowsets
    // can surface a bad_alloc under memory pressure. FunctionRunnable::run invokes the
    // task with no catch, so pre-fix an escaped exception std::terminates the BE and
    // defeats the fallback contract (a sync failure must cost only the incremental
    // merge, never the process). The task-body guard must convert ANY throw into a
    // per-tablet fallback. Inject the throw at the decision's sync: it propagates
    // uncaught through CloudTablet::sync_rowsets (no try/catch there, its RAII locks
    // release on unwind) into the guard, exactly as the single-flight throw would.
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [this](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        if (_sync_calls.fetch_add(1) == 0) {
            // The loader's sync brings a stale view (< the queried version 100), so the
            // decision's own sync below actually runs and can raise.
            install_rowsets(tablet, {{0, 50}});
            try_any_cast_ret<Status>(args)->second = true;
            return;
        }
        // The decision's sync, inside the guarded task body: raise.
        throw std::runtime_error("injected: decision sync raised");
    });
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud tablet sync raised");
    // Two syncs entered: the loader's (installed the stale view) and the decision's
    // (raised, then converted to a fallback by the guard rather than escaping to
    // terminate the BE).
    EXPECT_EQ(_sync_calls.load(), 2);
}

TEST_F(QueryCacheCloudIncrementalTest, presync_decision_sync_raises_doris_exception_falls_back) {
    // Same guard, but the decision sync raises a doris::Exception (what a nested
    // THROW_IF_ERROR surfaces). The task body catches doris::Exception BEFORE the
    // generic std::exception (the standard catch order per be/src/common/AGENTS.md), so
    // the Doris error code reaches the log, and converts it to the same per-tablet
    // fallback rather than letting it escape and terminate the BE.
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [this](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        if (_sync_calls.fetch_add(1) == 0) {
            install_rowsets(tablet, {{0, 50}});
            try_any_cast_ret<Status>(args)->second = true;
            return;
        }
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "injected: decision sync doris exception");
    });
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud tablet sync raised");
    EXPECT_EQ(_sync_calls.load(), 2);
}

TEST_F(QueryCacheCloudIncrementalTest, capture_site_get_tablet_is_cache_only_on_eviction) {
    // The presync fan-out synced this tablet, but capacity pressure evicted it
    // from the cloud tablet cache before the serial capture consumed the result.
    // With no recorded presync reason, _capture_tablet_delta's capture-site
    // get_tablet must be cache-only (force_use_only_cached=true): a miss becomes
    // an immediate fallback instead of a synchronous meta-service reload on this
    // light admission thread (the very blocking window the presync fast-fail
    // budget caps, which a plain reload would reopen after the fact). Both
    // callbacks below serve a valid tablet, so a regression to a plain get_tablet
    // would SUCCEED the reload and fail this test on the load count rather than
    // crash, keeping the revert a clean red.
    std::atomic<int> meta_loads {0};
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&meta_loads](auto&& args) {
        ++meta_loads;
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [this](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        install_rowsets(tablet, {{0, 50}, {51, 100}});
        try_any_cast_ret<Status>(args)->second = true;
    });

    QueryCacheRuntime runtime(make_cache_param(kTabletId), _cache.get());
    QueryCacheInstanceDecision decision;
    decision.current_version = 100;
    decision.cached_version = 50;
    // Empty presync reasons + a tablet absent from the cloud tablet cache (never
    // loaded here) reproduce "synced OK by presync, then evicted".
    bool incremental = runtime.capture_tablet_delta_for_test(kTabletId, 50, {}, &decision);
    EXPECT_FALSE(incremental);
    // Shared with a genuinely absent tablet: both are "not resident locally".
    EXPECT_EQ(decision.incremental_fallback_reason, "tablet not found");
    // The load-bearing assertion: the cache-only lookup issued NO meta-service
    // RPC. A plain get_tablet would reload the evicted tablet here (meta_loads
    // == 1), which is exactly the admission-thread blocking this fix removes.
    EXPECT_EQ(meta_loads.load(), 0);
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
                          /*decision_sync_installs=*/ {{51, 100}},
                          /*decision_sync_bitmap_marks=*/ {{0, 60}});
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
    intercept_tablet_load({{0, 40}, {51, 80}, {81, 100}}, Status::OK(), TKeysType::UNIQUE_KEYS,
                          /*enable_merge_on_write=*/true);
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
                          /*decision_sync_installs=*/ {{51, 100}});
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
                1, 2, tid, 15674, 4, 5, schema, 6, std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false);
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

TEST_F(QueryCacheCloudIncrementalTest, pool_rejection_skips_admission_tablet_load) {
    // Composition of two degradations: the dedicated pre-sync pool refuses the
    // sync (here: shut down, the same inline rejection a full bounded queue
    // gives) AND the tablet is absent from the local tablet cache (nothing --
    // loader or pre-sync, whose closure never ran -- ever loaded it). The
    // decision must consume the recorded "not scheduled" reason BEFORE touching
    // the tablet: the capture loop's get_tablet would otherwise take the
    // synchronous cache-miss meta-service load on the admission thread -- the
    // blocking window the fast-fail budget exists to cap -- to fetch a tablet
    // whose decision is already a known fallback. The meta-load counter proves
    // the admission path stayed load-free; a regression in the consumption
    // order shows up twice (a nonzero count, and the weaker "tablet not found"
    // reason from the failed load displacing the pool's).
    std::atomic<int> meta_loads {0};
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        meta_loads.fetch_add(1);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->first = Status::InternalError<false>("meta service unreachable");
        ret->second = true;
    });
    _engine->query_cache_delta_sync_pool().shutdown();

    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud rowset sync not scheduled");
    EXPECT_EQ(meta_loads.load(), 0);
    EXPECT_EQ(_sync_calls.load(), 0);
}

TEST_F(QueryCacheCloudIncrementalTest, presync_single_flight_coalesces_and_unrelated_key_proceeds) {
    // Two fragment runtimes over the SAME stale key must coalesce onto ONE
    // presync fan-out through the cache's single-flight registry (runtimes are
    // private per fragment context, so without the registry each would submit
    // the full fan-out), while a runtime over an UNRELATED key builds its own
    // flight and completes even though the first flight is parked on a slow
    // meta service. Deterministic: the shared flight's decision sync parks on a
    // gate, the second runtime's join is witnessed by the follower_joined sync
    // point, and only then does the unrelated key run and the gate open.
    constexpr int64_t kOtherTablet = 15690;
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        auto tid = try_any_cast<int64_t>(args[0]);
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(
                1, 2, tid, 15674, 4, 5, schema, 6, std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    std::mutex mu;
    std::unordered_map<int64_t, int> calls;
    std::atomic<bool> gate_open {false};
    std::atomic<bool> gated_sync_parked {false};
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->second = true;
        int n;
        {
            std::lock_guard<std::mutex> lk(mu);
            n = calls[tablet->tablet_id()]++;
        }
        if (n == 0) {
            // Loader sync: a view that stops short of query version 100, so
            // the decision sync really runs. Serialized under the test mutex
            // (install draws from the shared engine rowset-id generator).
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{0, 50}});
            return;
        }
        if (tablet->tablet_id() == kTabletId) {
            gated_sync_parked = true;
            while (!gate_open.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        std::lock_guard<std::mutex> lk(mu);
        install_rowsets(tablet, {{51, 100}});
    });

    auto ranges_a = make_scan_ranges(kTabletId, "100");
    auto param_a = make_cache_param(kTabletId);
    param_a.__set_allow_incremental(true);
    std::string key_a;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges_a, param_a, &key_a, &version).ok());
    insert_entry(_cache.get(), key_a, 50, 0);
    auto ranges_c = make_scan_ranges(kOtherTablet, "100");
    auto param_c = make_cache_param(kOtherTablet);
    param_c.__set_allow_incremental(true);
    std::string key_c;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges_c, param_c, &key_c, &version).ok());
    insert_entry(_cache.get(), key_c, 50, 0);

    QueryCacheRuntime runtime_a1(param_a, _cache.get());
    QueryCacheRuntime runtime_a2(param_a, _cache.get());
    QueryCacheRuntime runtime_c(param_c, _cache.get());

    std::atomic<bool> follower_joined {false};
    _sp->set_call_back("QueryCacheRuntime::_presync_cloud_delta_tablets.follower_joined",
                       [&](auto&&) { follower_joined = true; });

    std::shared_ptr<QueryCacheInstanceDecision> d1;
    std::shared_ptr<QueryCacheInstanceDecision> d2;
    std::thread t1;
    std::thread t2;
    // A fatal ASSERT below returns while a spawned thread is still joinable; its
    // std::thread destructor would std::terminate the whole UT binary and mask
    // the real failure. Open the gate (so a parked worker can finish) and join
    // both on every exit, mirroring race_two_callers.
    Defer thread_guard {[&] {
        gate_open = true;
        if (t1.joinable()) {
            t1.join();
        }
        if (t2.joinable()) {
            t2.join();
        }
    }};

    t1 = std::thread([&] { d1 = runtime_a1.get_or_make_decision(ranges_a); });
    for (int i = 0; i < 10000 && !gated_sync_parked.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_parked.load());

    t2 = std::thread([&] { d2 = runtime_a2.get_or_make_decision(ranges_a); });
    for (int i = 0; i < 10000 && !follower_joined.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(follower_joined.load());

    // With the shared flight parked, an unrelated key runs end to end on its
    // own flight: registry entries do not interfere across keys.
    auto d3 = runtime_c.get_or_make_decision(ranges_c);
    EXPECT_EQ(d3->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);

    gate_open = true;
    t1.join();
    t2.join();
    EXPECT_EQ(d1->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_EQ(d2->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    {
        std::lock_guard<std::mutex> lk(mu);
        // ONE shared fan-out for the coalesced key: the loader plus the single
        // gated decision sync. The joined runtime added no sync of its own; the
        // unrelated tablet ran its own loader + decision pair.
        EXPECT_EQ(calls[kTabletId], 2);
        EXPECT_EQ(calls[kOtherTablet], 2);
    }
    // All waiters left, so the registry drained (BE UT compiles with
    // -fno-access-control, so peek at the internals directly).
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        EXPECT_TRUE(_cache->_presync_flights.empty());
    }
}

TEST_F(QueryCacheCloudIncrementalTest,
       presync_single_flight_last_leaver_keeps_tombstone_reaped_on_drain) {
    // Two coalesced waiters expire their OWN deadlines while the shared decision
    // sync is parked on a slow meta service. Three properties:
    //  (1) The FIRST leaver must NOT abandon/erase -- a timed-out waiter cannot
    //      cancel work the still-waiting sibling is entitled to.
    //  (2) The LAST leaver, seeing the fan-out still un-drained (task parked),
    //      marks the flight abandoned but KEEPS the registry entry as a
    //      tombstone. Erasing on timeout would let the next identical query
    //      become a fresh owner and submit a DUPLICATE fan-out during the very
    //      brownout single-flight exists to smooth.
    //  (3) The tombstone is reaped only once the fan-out DRAINS: after the parked
    //      task returns (latch hits 0), a later identical arrival joins the
    //      drained flight, reuses its settled slots, and erases it on leave.
    // To observe the intermediate "first left, second still waiting" state the two
    // waiters are given DETERMINISTIC per-runtime deadlines via the
    // ..._presync_cloud_delta_tablets.deadline_ms test seam (below): the owner gets
    // 2000ms, the follower 4000ms. Because the follower cannot even start until the
    // owner's task is already parked (its sync_fanout_start is strictly later), the
    // owner's deadline (t_owner + 2000) is ALWAYS earlier than the follower's
    // (t_follower + 4000 >= t_owner + 4000), so the owner times out first by
    // construction -- no wall-clock staggering, and no CI deschedule can invert the
    // order. The 2000ms owner deadline also gives the follower the suite's standard
    // 2000ms of scheduling headroom to REGISTER while the owner is still waiting
    // (the two-waiter overlap the intermediate assertions observe) -- the same
    // headroom every other parked-coalescing test gets from the 2000ms default
    // timeout, so this test is not the flakiest link. (The prior versions staggered
    // starts by a fixed 500ms sleep, then used 500/1000ms deadlines whose 500ms
    // registration window was tighter than the suite convention.)
    int32_t saved_timeout = config::query_cache_decision_sync_timeout_ms;
    // Kept > 0 so the early enable check passes; the per-runtime seam overrides the
    // actual deadline for both waiters, so this nominal value is otherwise unused here.
    config::query_cache_decision_sync_timeout_ms = 2000;
    Defer restore_timeout {[&] { config::query_cache_decision_sync_timeout_ms = saved_timeout; }};

    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    std::mutex mu;
    std::atomic<int> syncs {0};
    std::atomic<bool> gate_open {false};
    std::atomic<bool> gated_sync_parked {false};
    std::atomic<bool> gated_sync_returned {false};
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->second = true;
        if (syncs.fetch_add(1) == 0) {
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{0, 50}});
            return;
        }
        gated_sync_parked = true;
        while (!gate_open.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // On release the parked decision sync completes the view to the queried
        // version, so the later reaping arrival can go INCREMENTAL off the
        // settled slot -- proving the reuse path, not just the erase.
        {
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{51, 100}});
        }
        gated_sync_returned = true;
    });

    auto ranges = make_scan_ranges(kTabletId, "100");
    auto param = make_cache_param(kTabletId);
    param.__set_allow_incremental(true);
    std::string key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges, param, &key, &version).ok());
    insert_entry(_cache.get(), key, 50, 0);
    QueryCacheRuntime runtime_a(param, _cache.get());
    QueryCacheRuntime runtime_b(param, _cache.get());

    // Deterministic deadlines by coalescing role: the flight owner gets 2000ms and a
    // joining follower 4000ms, so the owner times out first by construction (see the
    // header comment), independent of scheduling jitter. runtime_a starts first and
    // owns the flight; runtime_b joins it.
    _sp->set_call_back("QueryCacheRuntime::_presync_cloud_delta_tablets.deadline_ms",
                       [](auto&& args) {
                           bool is_owner = try_any_cast<bool>(args[0]);
                           *try_any_cast<int64_t*>(args[1]) = is_owner ? 2000 : 4000;
                       });

    std::atomic<bool> follower_joined {false};
    _sp->set_call_back("QueryCacheRuntime::_presync_cloud_delta_tablets.follower_joined",
                       [&](auto&&) { follower_joined = true; });

    std::shared_ptr<QueryCacheInstanceDecision> d1;
    std::shared_ptr<QueryCacheInstanceDecision> d2;
    std::thread t1;
    std::thread t2;
    // A fatal ASSERT below returns while a spawned thread is still joinable; its
    // std::thread destructor would std::terminate the whole UT binary and mask
    // the real failure. Open the gate (so the parked worker can finish) and join
    // both on every exit, mirroring race_two_callers.
    Defer thread_guard {[&] {
        gate_open = true;
        if (t1.joinable()) {
            t1.join();
        }
        if (t2.joinable()) {
            t2.join();
        }
    }};

    t1 = std::thread([&] { d1 = runtime_a.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !gated_sync_parked.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_parked.load());

    // The follower can start as soon as the owner's task has parked: its longer
    // per-runtime deadline (4000ms vs the owner's 2000ms) guarantees the owner times
    // out first regardless of when exactly the follower joins, so no start stagger.
    t2 = std::thread([&] { d2 = runtime_b.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !follower_joined.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(follower_joined.load());

    // Capture the shared flight while both wait: present, two waiters, not abandoned.
    std::shared_ptr<QueryCache::PresyncFlight> flight;
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        ASSERT_EQ(_cache->_presync_flights.size(), 1);
        flight = _cache->_presync_flights.begin()->second;
        EXPECT_EQ(flight->waiters, 2);
    }
    EXPECT_FALSE(flight->abandoned->load());

    // The owner times out first (it started ~500ms earlier). As the FIRST leaver
    // it must NOT abandon or erase: exactly one waiter left, entry still
    // registered, flag still clear.
    t1.join();
    EXPECT_EQ(d1->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d1->incremental_fallback_reason, "cloud rowset sync timed out");
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        EXPECT_EQ(_cache->_presync_flights.size(), 1);
        EXPECT_EQ(flight->waiters, 1);
    }
    EXPECT_FALSE(flight->abandoned->load());

    // The follower times out next. As the LAST leaver, with the fan-out still
    // parked (un-drained), it marks the flight abandoned but KEEPS the tombstone
    // -- it is NOT erased on timeout (that is the codex-flagged premature-erase
    // regression this design fixes).
    t2.join();
    EXPECT_EQ(d2->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d2->incremental_fallback_reason, "cloud rowset sync timed out");
    EXPECT_TRUE(flight->abandoned->load());
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        ASSERT_EQ(_cache->_presync_flights.size(), 1);
        EXPECT_EQ(_cache->_presync_flights.begin()->second, flight);
        EXPECT_EQ(flight->waiters, 0);
    }

    // Release the parked task and let it fully return. As it drives the latch to 0
    // with ZERO waiters remaining (both timed out into the tombstone above), the
    // FINAL task reaps the tombstone itself in publish_slot -- no later arrival is
    // needed (the round-2 reap-by-final-task fix; the round-1 code left this
    // waiterless tombstone stranded). NOTE: an identical arrival here would NOT
    // join and reuse the slot -- it would find the registry already reaped by the
    // final task and re-own, so the two-waiter tombstone's final-task reap is what
    // this phase proves (join-not-reown is proved by
    // presync_single_flight_post_timeout_arrival_joins_not_reowns).
    gate_open = true;
    for (int i = 0; i < 10000 && !gated_sync_returned.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_returned.load());
    bool reaped = false;
    for (int i = 0; i < 10000; ++i) {
        {
            std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
            if (_cache->_presync_flights.empty()) {
                reaped = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(reaped) << "the final task must reap the waiterless tombstone on drain";
    EXPECT_EQ(flight->fanout_done->count(), 0u);
    // The parked task published exactly once (loader + the single decision sync);
    // no leaver re-submitted a duplicate fan-out.
    EXPECT_EQ(syncs.load(), 2);
    // Deterministically drain the callback shell's unwind after its last store, mirroring
    // decision_sync_timeout_falls_back, before TearDown clears the sync points: pool.wait()
    // hard-synchronizes the task's exit instead of betting a fixed sleep outlasts it.
    _engine->query_cache_delta_sync_pool().wait();
}

TEST_F(QueryCacheCloudIncrementalTest,
       presync_single_flight_post_timeout_arrival_joins_not_reowns) {
    // The codex-flagged brownout regression, directly: after every original
    // waiter times out on a parked (still-running) fan-out, a NEW identical query
    // must JOIN that draining flight, NOT become a fresh owner and submit a
    // DUPLICATE fan-out (which would double the bounded-pool pressure wave after
    // wave during a sustained meta-service brownout). The owner times out while
    // its single decision sync is parked, leaving an abandoned-but-kept tombstone;
    // a second runtime then arrives, joins, and adds NO sync of its own -- proven
    // by the sync-call count staying at loader + the one parked decision sync.
    int32_t saved_timeout = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 500;
    Defer restore_timeout {[&] { config::query_cache_decision_sync_timeout_ms = saved_timeout; }};

    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    std::mutex mu;
    std::atomic<int> syncs {0};
    std::atomic<bool> gate_open {false};
    std::atomic<bool> gated_sync_parked {false};
    std::atomic<bool> gated_sync_returned {false};
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->second = true;
        if (syncs.fetch_add(1) == 0) {
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{0, 50}});
            return;
        }
        gated_sync_parked = true;
        while (!gate_open.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{51, 100}});
        }
        gated_sync_returned = true;
    });

    auto ranges = make_scan_ranges(kTabletId, "100");
    auto param = make_cache_param(kTabletId);
    param.__set_allow_incremental(true);
    std::string key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges, param, &key, &version).ok());
    insert_entry(_cache.get(), key, 50, 0);
    QueryCacheRuntime runtime_owner(param, _cache.get());
    QueryCacheRuntime runtime_late(param, _cache.get());

    std::atomic<bool> late_joined {false};
    _sp->set_call_back("QueryCacheRuntime::_presync_cloud_delta_tablets.follower_joined",
                       [&](auto&&) { late_joined = true; });

    std::shared_ptr<QueryCacheInstanceDecision> d_owner;
    std::shared_ptr<QueryCacheInstanceDecision> d_late;
    std::thread t_owner;
    std::thread t_late;
    Defer thread_guard {[&] {
        gate_open = true;
        if (t_owner.joinable()) {
            t_owner.join();
        }
        if (t_late.joinable()) {
            t_late.join();
        }
    }};

    t_owner = std::thread([&] { d_owner = runtime_owner.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !gated_sync_parked.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_parked.load());

    // The owner times out while the decision sync is still parked, leaving an
    // abandoned tombstone (the fan-out is un-drained, so it is NOT erased).
    t_owner.join();
    EXPECT_EQ(d_owner->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d_owner->incremental_fallback_reason, "cloud rowset sync timed out");
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        ASSERT_EQ(_cache->_presync_flights.size(), 1);
        EXPECT_TRUE(_cache->_presync_flights.begin()->second->abandoned->load());
    }

    // A NEW identical query arrives while the fan-out is still parked. It must
    // JOIN the tombstone (witnessed by the follower_joined sync point), not
    // re-own and submit a second fan-out.
    t_late = std::thread([&] { d_late = runtime_late.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !late_joined.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(late_joined.load());

    // Release the parked task; the late arrival (waiting on the shared latch)
    // wakes, reuses the completed view, goes INCREMENTAL, and reaps the flight.
    gate_open = true;
    t_late.join();
    EXPECT_EQ(d_late->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    // The crux: exactly ONE fan-out ran (loader + the single parked decision
    // sync). The late arrival added no sync -- it joined instead of re-owning.
    EXPECT_EQ(syncs.load(), 2);
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        EXPECT_TRUE(_cache->_presync_flights.empty());
    }
    for (int i = 0; i < 10000 && !gated_sync_returned.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_returned.load());
    // Drain the task's tail after its last store deterministically (pool.wait() hard-
    // synchronizes the callback shell's exit) before TearDown clears the sync points.
    _engine->query_cache_delta_sync_pool().wait();
}

TEST_F(QueryCacheCloudIncrementalTest,
       presync_single_flight_tombstone_reaped_on_drain_without_arrival) {
    // The round-2 tombstone-leak fix: a flight whose sole/last waiter times out
    // while a task is still parked is KEPT as a tombstone, but once the fan-out
    // DRAINS it must be reaped by the FINAL task itself, even if NO later
    // identical query ever arrives to join and reap it. Without this the entry
    // leaks for the BE lifetime on exactly the brownout path the feature targets.
    // A single owner times out; the parked task is then released and drains the
    // latch with ZERO waiters; the registry must end empty with no reaping
    // arrival. (Pre-fix -- reap only in leave_flight -- this asserts a permanent
    // leak: the final _presync_flights.empty() never holds.)
    int32_t saved_timeout = config::query_cache_decision_sync_timeout_ms;
    config::query_cache_decision_sync_timeout_ms = 500;
    Defer restore_timeout {[&] { config::query_cache_decision_sync_timeout_ms = saved_timeout; }};

    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    std::mutex mu;
    std::atomic<int> syncs {0};
    std::atomic<bool> gate_open {false};
    std::atomic<bool> gated_sync_parked {false};
    std::atomic<bool> gated_sync_returned {false};
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->second = true;
        if (syncs.fetch_add(1) == 0) {
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{0, 50}});
            return;
        }
        gated_sync_parked = true;
        while (!gate_open.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        gated_sync_returned = true;
    });

    auto ranges = make_scan_ranges(kTabletId, "100");
    auto param = make_cache_param(kTabletId);
    param.__set_allow_incremental(true);
    std::string key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges, param, &key, &version).ok());
    insert_entry(_cache.get(), key, 50, 0);
    // Baseline the query-cache MemTracker AFTER the cache entry is in (that charge is
    // a constant offset for the rest of the test). make_flight charges the whole
    // flight's estimated retained bytes (object + the per_range_reason / tablet_ids /
    // slot_counted O(tablets) buffers + control blocks + key) to this SAME stable
    // limiter as ONE lump, and ~PresyncFlight releases exactly that lump, so
    // consumption must rise while the flight is live and return to EXACTLY this baseline
    // once the flight is reaped and destroyed -- the balance that proves the charge is
    // stable and fully refunded. The byte-exact EQ below also
    // depends on this fixture keeping the query-cache tracker otherwise quiescent
    // between baseline and assertion (one tiny cache entry, no eviction, no second
    // flight); do not add cache traffic in between without switching to a tolerance
    // band.
    const int64_t qc_mem_baseline =
            ExecEnv::GetInstance()->query_cache_mem_tracker()->consumption();
    QueryCacheRuntime runtime_owner(param, _cache.get());

    std::shared_ptr<QueryCacheInstanceDecision> d;
    std::thread t;
    Defer thread_guard {[&] {
        gate_open = true;
        if (t.joinable()) {
            t.join();
        }
    }};

    t = std::thread([&] { d = runtime_owner.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !gated_sync_parked.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_parked.load());

    // The owner times out while its task is parked, leaving an abandoned tombstone
    // with zero waiters and the fan-out not yet drained.
    t.join();
    EXPECT_EQ(d->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d->incremental_fallback_reason, "cloud rowset sync timed out");
    std::shared_ptr<QueryCache::PresyncFlight> flight;
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        ASSERT_EQ(_cache->_presync_flights.size(), 1);
        flight = _cache->_presync_flights.begin()->second;
        EXPECT_EQ(flight->waiters, 0);
    }
    EXPECT_TRUE(flight->abandoned->load());
    // The live flight's buffers are charged to the stable query-cache MemTracker.
    EXPECT_GT(ExecEnv::GetInstance()->query_cache_mem_tracker()->consumption(), qc_mem_baseline)
            << "the live flight's buffers must be charged to the query-cache MemTracker";

    // Release the parked task; with zero waiters, the FINAL task reaps the
    // tombstone itself -- no later identical query is ever issued.
    gate_open = true;
    for (int i = 0; i < 10000 && !gated_sync_returned.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_returned.load());
    bool reaped = false;
    for (int i = 0; i < 10000; ++i) {
        {
            std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
            if (_cache->_presync_flights.empty()) {
                reaped = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(reaped) << "the final task must reap the drained tombstone with no arrival";
    // Drain the pool so the parked task's closure (a holder of the flight's shared
    // pieces) is destroyed, then drop the test's own captured reference. With the
    // registry entry reaped and no waiters, the flight and all its buffers are now
    // freed, so the query-cache MemTracker must return to EXACTLY the pre-flight
    // baseline -- proving the O(tablets) buffers were charged to the stable limiter
    // and fully released on reap (no leak, no over-release). This also removes the
    // former unwind cushion: pool.wait() hard-synchronizes the callback shell's exit.
    _engine->query_cache_delta_sync_pool().wait();
    flight.reset();
    EXPECT_EQ(ExecEnv::GetInstance()->query_cache_mem_tracker()->consumption(), qc_mem_baseline)
            << "reaping the flight must release its charge back to the query-cache MemTracker";
}

TEST_F(QueryCacheCloudIncrementalTest,
       presync_single_flight_reversed_order_maps_reasons_by_tablet) {
    // The single-flight coalescing hazard the shared slots must survive:
    // build_cache_key sorts tablet ids, so two runtimes whose scan ranges hold
    // the same two tablets in a DIFFERENT order share one flight. The owner's
    // fan-out writes the index-aligned slots in the OWNER's order; a follower
    // that keyed the merged reasons off its OWN (reversed) order would misassign
    // the failed tablet's reason to its sibling, leave the truly failed tablet
    // unreasoned, and so drive a synchronous admission-thread get_tablet for it
    // outside the fast-fail budget -- the exact brownout stall the presync
    // exists to avoid. With the reasons keyed off the owner's tablet ids, the
    // follower must reach the SAME decision as the owner: MISS, keyed to the
    // failed tablet. (Pre-fix this test fails: the follower's fallback reason is
    // a capture-side one for the sibling, not the owner's sync-failure reason.)
    constexpr int64_t kTabletId2 = 15680;

    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        auto tid = try_any_cast<int64_t>(args[0]);
        TTabletSchema schema;
        schema.keys_type = TKeysType::DUP_KEYS;
        auto meta = std::make_shared<TabletMeta>(
                1, 2, tid, 15674, 4, 5, schema, 6, std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    std::mutex mu;
    std::unordered_map<int64_t, int> calls;
    std::atomic<bool> gate_open {false};
    std::atomic<bool> gated_sync_parked {false};
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->second = true;
        int n;
        {
            std::lock_guard<std::mutex> lk(mu);
            n = calls[tablet->tablet_id()]++;
        }
        if (n == 0) {
            // Loader: a view stopping short of 100 so the decision sync runs.
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{0, 50}});
            return;
        }
        // Decision sync. The failing tablet parks first, so the follower joins
        // the still-live flight before the fan-out settles; then it fails. The
        // other tablet's decision sync succeeds.
        if (tablet->tablet_id() == kTabletId2) {
            gated_sync_parked = true;
            while (!gate_open.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            ret->first = Status::InternalError<false>("injected sync failure");
            return;
        }
        std::lock_guard<std::mutex> lk(mu);
        install_rowsets(tablet, {{51, 100}});
    });

    // One stale entry keyed over both tablets, queried at 100. The owner scans
    // [kTabletId, kTabletId2]; the follower scans the SAME tablets REVERSED.
    auto ranges_owner = make_scan_ranges(kTabletId, "100");
    ranges_owner.push_back(make_scan_ranges(kTabletId2, "100").front());
    auto ranges_follower = make_scan_ranges(kTabletId2, "100");
    ranges_follower.push_back(make_scan_ranges(kTabletId, "100").front());
    auto param = make_cache_param(kTabletId);
    param.tablet_to_range.insert({kTabletId2, "range"});
    param.__set_allow_incremental(true);
    std::string key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges_owner, param, &key, &version).ok());
    // Sanity: the reversed order yields the identical key (so they coalesce).
    std::string key_reversed;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges_follower, param, &key_reversed, &version).ok());
    ASSERT_EQ(key, key_reversed);
    insert_entry(_cache.get(), key, 50, 0);

    QueryCacheRuntime runtime_owner(param, _cache.get());
    QueryCacheRuntime runtime_follower(param, _cache.get());

    std::atomic<bool> follower_joined {false};
    _sp->set_call_back("QueryCacheRuntime::_presync_cloud_delta_tablets.follower_joined",
                       [&](auto&&) { follower_joined = true; });

    std::shared_ptr<QueryCacheInstanceDecision> d_owner;
    std::shared_ptr<QueryCacheInstanceDecision> d_follower;
    std::thread t_owner;
    std::thread t_follower;
    Defer thread_guard {[&] {
        gate_open = true;
        if (t_owner.joinable()) {
            t_owner.join();
        }
        if (t_follower.joinable()) {
            t_follower.join();
        }
    }};

    t_owner = std::thread([&] { d_owner = runtime_owner.get_or_make_decision(ranges_owner); });
    for (int i = 0; i < 10000 && !gated_sync_parked.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_parked.load());

    t_follower = std::thread(
            [&] { d_follower = runtime_follower.get_or_make_decision(ranges_follower); });
    for (int i = 0; i < 10000 && !follower_joined.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(follower_joined.load());

    gate_open = true;
    t_owner.join();
    t_follower.join();

    // Both decisions fall the whole instance back, keyed to the SAME failed
    // tablet: the owner surfaces kTabletId2's sync failure, and the reversed
    // follower -- reading the owner-ordered slots by the owner's tablet ids --
    // surfaces the identical reason rather than misattributing it.
    EXPECT_EQ(d_owner->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d_owner->incremental_fallback_reason, "cloud rowset sync failed");
    EXPECT_EQ(d_follower->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_EQ(d_follower->incremental_fallback_reason, "cloud rowset sync failed");
    {
        std::lock_guard<std::mutex> lk(mu);
        // ONE shared fan-out: each tablet synced its loader plus its single
        // decision sync. The coalesced follower added no sync of its own.
        EXPECT_EQ(calls[kTabletId], 2);
        EXPECT_EQ(calls[kTabletId2], 2);
    }
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        EXPECT_TRUE(_cache->_presync_flights.empty());
    }
}

TEST_F(QueryCacheCloudIncrementalTest, presync_single_flight_coalesces_mow_tablet) {
    // Coalescing over a MERGE-ON-WRITE tablet: the owner's presync carries
    // options.sync_delete_bitmap=true (a MoW read is delete-bitmap-sensitive),
    // and a coalesced follower REUSES that one sync rather than issuing its own.
    // Both must reach the SAME decision off the shared, bitmap-complete view.
    // Covers the MoW x single-flight combination the DUP_KEYS coalesce tests do
    // not: the DUP tests prove the registry mechanics, the single-runtime MoW
    // tests prove the delete-bitmap capture, and this proves a follower riding
    // the owner's bitmap sync classifies identically (clean bitmap + pure-append
    // delta -> both INCREMENTAL).
    _sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        auto tid = try_any_cast<int64_t>(args[0]);
        TTabletSchema schema;
        schema.keys_type = TKeysType::UNIQUE_KEYS;
        auto meta = std::make_shared<TabletMeta>(
                1, 2, tid, 15674, 4, 5, schema, 6, std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0,
                /*enable_merge_on_write=*/true);
        meta->set_tablet_state(TABLET_RUNNING);
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::move(meta);
        try_any_cast_ret<Status>(args)->second = true;
    });
    std::mutex mu;
    std::unordered_map<int64_t, int> calls;
    std::atomic<bool> gate_open {false};
    std::atomic<bool> gated_sync_parked {false};
    std::atomic<bool> saw_sync_delete_bitmap {false};
    _sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        const auto* options = try_any_cast<const SyncOptions*>(args[1]);
        auto* ret = try_any_cast_ret<Status>(args);
        ret->second = true;
        int n;
        {
            std::lock_guard<std::mutex> lk(mu);
            n = calls[tablet->tablet_id()]++;
        }
        if (n == 0) {
            std::lock_guard<std::mutex> lk(mu);
            install_rowsets(tablet, {{0, 50}});
            return;
        }
        // Decision sync on a MoW tablet must carry the delete-bitmap sync.
        if (options != nullptr && options->sync_delete_bitmap) {
            saw_sync_delete_bitmap = true;
        }
        gated_sync_parked = true;
        while (!gate_open.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::lock_guard<std::mutex> lk(mu);
        install_rowsets(tablet, {{51, 100}});
    });

    auto ranges = make_scan_ranges(kTabletId, "100");
    auto param = make_cache_param(kTabletId);
    param.__set_allow_incremental(true);
    std::string key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(ranges, param, &key, &version).ok());
    insert_entry(_cache.get(), key, 50, 0);
    QueryCacheRuntime runtime_a(param, _cache.get());
    QueryCacheRuntime runtime_b(param, _cache.get());

    std::atomic<bool> follower_joined {false};
    _sp->set_call_back("QueryCacheRuntime::_presync_cloud_delta_tablets.follower_joined",
                       [&](auto&&) { follower_joined = true; });

    std::shared_ptr<QueryCacheInstanceDecision> d1;
    std::shared_ptr<QueryCacheInstanceDecision> d2;
    std::thread t1;
    std::thread t2;
    Defer thread_guard {[&] {
        gate_open = true;
        if (t1.joinable()) {
            t1.join();
        }
        if (t2.joinable()) {
            t2.join();
        }
    }};

    t1 = std::thread([&] { d1 = runtime_a.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !gated_sync_parked.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(gated_sync_parked.load());
    t2 = std::thread([&] { d2 = runtime_b.get_or_make_decision(ranges); });
    for (int i = 0; i < 10000 && !follower_joined.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(follower_joined.load());

    gate_open = true;
    t1.join();
    t2.join();
    // Clean bitmap + pure-append delta: both coalesced MoW waiters go INCREMENTAL
    // and, riding the same sync, reach the identical decision.
    EXPECT_EQ(d1->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_EQ(d2->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_EQ(d1->mode, d2->mode);
    EXPECT_EQ(d1->incremental_fallback_reason, d2->incremental_fallback_reason);
    // The owner's shared decision sync carried the MoW delete-bitmap sync.
    EXPECT_TRUE(saw_sync_delete_bitmap.load());
    {
        std::lock_guard<std::mutex> lk(mu);
        // ONE shared fan-out: loader + the single gated decision sync. The
        // follower rode the owner's MoW sync, adding none of its own.
        EXPECT_EQ(calls[kTabletId], 2);
    }
    {
        std::lock_guard<std::mutex> lk(_cache->_presync_flights_lock);
        EXPECT_TRUE(_cache->_presync_flights.empty());
    }
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
        auto meta = std::make_shared<TabletMeta>(1, 2, kTabletId, 15674, 4, 5, schema, 6,
                                                 std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                 UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
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
    // fork-join bookkeeping). Drain it deterministically with pool.wait(), which hard-
    // synchronizes the callback shell's exit, rather than betting a fixed sleep outlasts
    // that tail before TearDown clears the sync points.
    _engine->query_cache_delta_sync_pool().wait();
}

} // namespace doris
