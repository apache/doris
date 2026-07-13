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

#include <memory>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "core/data_type/data_type_number.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "storage/data_dir.h"
#include "storage/options.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "testutil/column_helper.h"
#include "util/debug_points.h"
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

    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_FALSE(decision->key_valid);
    // Every caller shares one immutable invalid decision (and one log line).
    EXPECT_EQ(decision.get(), runtime.get_or_make_decision(scan_ranges).get());
}

TEST_F(QueryCacheTest, runtime_decision_hit) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 100, 0);

    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::HIT);
    EXPECT_TRUE(decision->handle.valid());
    EXPECT_EQ(decision->handle.get_cache_version(), 100);
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
    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    // Recompute and write back even though a fresh entry exists.
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_FALSE(decision->handle.valid());
}

TEST_F(QueryCacheTest, runtime_decision_binlog_scan) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 100, 0);

    QueryCacheRuntime runtime(cache_param, cache.get());
    runtime.disable_for_binlog_scan();
    auto decision = runtime.get_or_make_decision(scan_ranges);
    // A binlog scan must neither serve the cached entry nor write back.
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_FALSE(decision->key_valid);
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
    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_FALSE(decision->handle.valid());
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

TEST_F(QueryCacheTest, runtime_decision_stale_incremental_cloud_mode) {
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024));
    auto scan_ranges = make_scan_ranges(42, "100");
    auto cache_param = make_cache_param(42);
    cache_param.__set_allow_incremental(true);

    std::string cache_key;
    int64_t version = 0;
    EXPECT_TRUE(QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version).ok());
    insert_entry(cache.get(), cache_key, 50, 0);

    // Incremental merge only supports local storage for now.
    std::string saved_deploy_mode = config::deploy_mode;
    config::deploy_mode = "cloud";
    QueryCacheRuntime runtime(cache_param, cache.get());
    auto decision = runtime.get_or_make_decision(scan_ranges);
    config::deploy_mode = saved_deploy_mode;

    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "cloud mode");
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
    tablet->tablet_meta()->delete_bitmap().add({rowset_id_for(0), 0, 60}, 7);
    int64_t fallbacks_before =
            DorisMetrics::instance()->query_cache_incremental_fallback_total->value();
    auto decision = make_stale_decision();
    EXPECT_EQ(DorisMetrics::instance()->query_cache_incremental_fallback_total->value(),
              fallbacks_before + 1);
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_FALSE(decision->handle.valid());
    EXPECT_EQ(decision->incremental_fallback_reason, "delta rewrites history rows");
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
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::INCREMENTAL);
    EXPECT_TRUE(decision->incremental_fallback_reason.empty());
}

TEST_F(QueryCacheIncrementalTest, fallback_on_version_gap) {
    // The whole history lives in one compacted rowset [0, 100]: no version
    // path can serve (50, 100] alone, so capture fails and we fall back.
    create_tablet(TKeysType::DUP_KEYS, false, {{0, 100}});
    auto decision = make_stale_decision();
    EXPECT_EQ(decision->mode, QueryCacheInstanceDecision::Mode::MISS);
    EXPECT_TRUE(decision->key_valid);
    EXPECT_EQ(decision->incremental_fallback_reason, "delta versions not capturable");
}

TEST_F(QueryCacheIncrementalTest, fallback_on_capture_error) {
    // Non-pathfinding capture failures surface as real errors even under the
    // quiet option (a pathfinding miss instead returns an empty read source,
    // see fallback_on_version_gap). The storage debug point simulates such a
    // failure, e.g. a rowset object missing for a found path.
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

} // namespace doris
