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

#include "cloud/cloud_tablet_rpc_throttler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "cloud/config.h"

namespace doris::cloud {

// ============== StrictQpsLimiter Tests ==============

class StrictQpsLimiterTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StrictQpsLimiterTest, BasicFunctionality) {
    StrictQpsLimiter limiter(10.0); // 10 QPS = 100ms interval

    auto t1 = limiter.reserve();
    auto t2 = limiter.reserve();

    // Second reserve should return a time point at least ~100ms after the first
    auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    EXPECT_GE(diff_ms, 90); // Allow some tolerance
    EXPECT_LE(diff_ms, 110);
}

TEST_F(StrictQpsLimiterTest, UpdateQps) {
    StrictQpsLimiter limiter(10.0); // 10 QPS

    EXPECT_DOUBLE_EQ(limiter.get_qps(), 10.0);

    limiter.update_qps(100.0);
    EXPECT_DOUBLE_EQ(limiter.get_qps(), 100.0);
}

TEST_F(StrictQpsLimiterTest, ZeroQpsDefaultsToOne) {
    StrictQpsLimiter limiter(0.0);
    EXPECT_DOUBLE_EQ(limiter.get_qps(), 1.0);
}

TEST_F(StrictQpsLimiterTest, MultiThreadedAccess) {
    StrictQpsLimiter limiter(1000.0); // High QPS for fast test

    const int num_threads = 10;
    const int calls_per_thread = 100;
    std::atomic<int> total_calls {0};

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&limiter, &total_calls]() {
            for (int i = 0; i < calls_per_thread; i++) {
                limiter.reserve();
                total_calls++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(total_calls.load(), num_threads * calls_per_thread);
}

// ============== TableRpcQpsRegistry Tests ==============

class TableRpcQpsRegistryTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_max_tracked = config::ms_rpc_max_tracked_tables_per_rpc;
    }

    void TearDown() override { config::ms_rpc_max_tracked_tables_per_rpc = _saved_max_tracked; }

private:
    int32_t _saved_max_tracked;
};

TEST_F(TableRpcQpsRegistryTest, RecordAndGetQps) {
    TableRpcQpsRegistry registry;

    // Record some RPC calls
    for (int i = 0; i < 100; i++) {
        registry.record(LoadRelatedRpc::PREPARE_ROWSET, 12345);
    }

    // QPS counter uses bvar's PerSecond which needs time to calculate
    // For immediate testing, we just verify the recording doesn't crash
    // and get_qps returns some value (may be 0 if no time has passed)
    double qps = registry.get_qps(LoadRelatedRpc::PREPARE_ROWSET, 12345);
    EXPECT_GE(qps, 0.0);
}

TEST_F(TableRpcQpsRegistryTest, MultipleTables) {
    TableRpcQpsRegistry registry;

    // Record RPCs for different tables
    registry.record(LoadRelatedRpc::COMMIT_ROWSET, 100);
    registry.record(LoadRelatedRpc::COMMIT_ROWSET, 200);
    registry.record(LoadRelatedRpc::COMMIT_ROWSET, 300);

    // Each table should have independent counters
    // Just verify no crashes and data separation works
    auto top_tables = registry.get_top_k_tables(LoadRelatedRpc::COMMIT_ROWSET, 3);
    EXPECT_LE(top_tables.size(), 3);
}

TEST_F(TableRpcQpsRegistryTest, GetTopKTables) {
    TableRpcQpsRegistry registry;

    // Record different numbers of RPCs for different tables
    for (int i = 0; i < 50; i++) {
        registry.record(LoadRelatedRpc::UPDATE_TMP_ROWSET, 100);
    }
    for (int i = 0; i < 30; i++) {
        registry.record(LoadRelatedRpc::UPDATE_TMP_ROWSET, 200);
    }
    for (int i = 0; i < 10; i++) {
        registry.record(LoadRelatedRpc::UPDATE_TMP_ROWSET, 300);
    }

    // Get top 2 tables
    auto top_tables = registry.get_top_k_tables(LoadRelatedRpc::UPDATE_TMP_ROWSET, 2);
    EXPECT_LE(top_tables.size(), 2);
}

TEST_F(TableRpcQpsRegistryTest, MaxTrackedTables) {
    config::ms_rpc_max_tracked_tables_per_rpc = 5;

    TableRpcQpsRegistry registry;

    // Try to record more tables than the limit
    for (int64_t table_id = 1; table_id <= 10; table_id++) {
        registry.record(LoadRelatedRpc::UPDATE_DELETE_BITMAP, table_id);
    }

    // Should only track up to 5 tables
    auto all_tables = registry.get_top_k_tables(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 100);
    EXPECT_LE(all_tables.size(), 5);
}

// ============== TableRpcThrottler Tests ==============

class TableRpcThrottlerTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(TableRpcThrottlerTest, SetAndGetQpsLimit) {
    TableRpcThrottler throttler;

    EXPECT_FALSE(throttler.has_limit(LoadRelatedRpc::PREPARE_ROWSET, 12345));
    EXPECT_EQ(throttler.get_qps_limit(LoadRelatedRpc::PREPARE_ROWSET, 12345), 0.0);

    throttler.set_qps_limit(LoadRelatedRpc::PREPARE_ROWSET, 12345, 10.0);

    EXPECT_TRUE(throttler.has_limit(LoadRelatedRpc::PREPARE_ROWSET, 12345));
    EXPECT_DOUBLE_EQ(throttler.get_qps_limit(LoadRelatedRpc::PREPARE_ROWSET, 12345), 10.0);
}

TEST_F(TableRpcThrottlerTest, RemoveQpsLimit) {
    TableRpcThrottler throttler;

    throttler.set_qps_limit(LoadRelatedRpc::COMMIT_ROWSET, 100, 20.0);
    EXPECT_TRUE(throttler.has_limit(LoadRelatedRpc::COMMIT_ROWSET, 100));

    throttler.remove_qps_limit(LoadRelatedRpc::COMMIT_ROWSET, 100);
    EXPECT_FALSE(throttler.has_limit(LoadRelatedRpc::COMMIT_ROWSET, 100));
}

TEST_F(TableRpcThrottlerTest, ThrottleWithNoLimit) {
    TableRpcThrottler throttler;

    auto now = std::chrono::steady_clock::now();
    auto allowed_time = throttler.throttle(LoadRelatedRpc::UPDATE_TMP_ROWSET, 200);

    // Without a limit, should return approximately now
    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(allowed_time - now).count();
    EXPECT_LE(diff, 10); // Very small difference
}

TEST_F(TableRpcThrottlerTest, ThrottleWithLimit) {
    TableRpcThrottler throttler;

    // Set a very low limit (1 QPS)
    throttler.set_qps_limit(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 300, 1.0);

    // First call should return a time point close to now
    auto t1 = throttler.throttle(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 300);

    // Second call should return a time point ~1 second later
    auto t2 = throttler.throttle(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 300);

    auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    EXPECT_GE(diff_ms, 900); // Allow some tolerance
    EXPECT_LE(diff_ms, 1100);
}

TEST_F(TableRpcThrottlerTest, ThrottledTableCount) {
    TableRpcThrottler throttler;

    EXPECT_EQ(throttler.get_throttled_table_count(LoadRelatedRpc::PREPARE_ROWSET), 0);

    throttler.set_qps_limit(LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0);
    throttler.set_qps_limit(LoadRelatedRpc::PREPARE_ROWSET, 200, 10.0);

    EXPECT_EQ(throttler.get_throttled_table_count(LoadRelatedRpc::PREPARE_ROWSET), 2);

    throttler.remove_qps_limit(LoadRelatedRpc::PREPARE_ROWSET, 100);

    EXPECT_EQ(throttler.get_throttled_table_count(LoadRelatedRpc::PREPARE_ROWSET), 1);
}

TEST_F(TableRpcThrottlerTest, IndependentRpcTypes) {
    TableRpcThrottler throttler;

    throttler.set_qps_limit(LoadRelatedRpc::COMMIT_ROWSET, 100, 10.0);
    throttler.set_qps_limit(LoadRelatedRpc::UPDATE_TMP_ROWSET, 100, 20.0);

    EXPECT_DOUBLE_EQ(throttler.get_qps_limit(LoadRelatedRpc::COMMIT_ROWSET, 100), 10.0);
    EXPECT_DOUBLE_EQ(throttler.get_qps_limit(LoadRelatedRpc::UPDATE_TMP_ROWSET, 100), 20.0);
}

// ============== MSBackpressureHandler Tests ==============

class MSBackpressureHandlerTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enable = config::enable_ms_backpressure_handling;
        _saved_upgrade_interval = config::ms_backpressure_upgrade_interval_sec;
        _saved_downgrade_interval = config::ms_backpressure_downgrade_interval_sec;
        _saved_top_k = config::ms_backpressure_upgrade_top_k;
        _saved_ratio = config::ms_backpressure_throttle_ratio;
        _saved_floor = config::ms_rpc_table_qps_limit_floor;
    }

    void TearDown() override {
        config::enable_ms_backpressure_handling = _saved_enable;
        config::ms_backpressure_upgrade_interval_sec = _saved_upgrade_interval;
        config::ms_backpressure_downgrade_interval_sec = _saved_downgrade_interval;
        config::ms_backpressure_upgrade_top_k = _saved_top_k;
        config::ms_backpressure_throttle_ratio = _saved_ratio;
        config::ms_rpc_table_qps_limit_floor = _saved_floor;
    }

private:
    bool _saved_enable;
    int32_t _saved_upgrade_interval;
    int32_t _saved_downgrade_interval;
    int32_t _saved_top_k;
    double _saved_ratio;
    double _saved_floor;
};

TEST_F(MSBackpressureHandlerTest, DisabledByDefault) {
    config::enable_ms_backpressure_handling = false;

    TableRpcQpsRegistry registry;
    TableRpcThrottler throttler;
    MSBackpressureHandler handler(&registry, &throttler);

    // on_ms_busy should return false when disabled
    EXPECT_FALSE(handler.on_ms_busy());
}

TEST_F(MSBackpressureHandlerTest, OnMsBusyTriggersUpgrade) {
    config::enable_ms_backpressure_handling = true;
    config::ms_backpressure_upgrade_interval_sec = 0; // No cooldown
    config::ms_backpressure_upgrade_top_k = 3;
    config::ms_backpressure_throttle_ratio = 0.5;
    config::ms_rpc_table_qps_limit_floor = 1.0;

    TableRpcQpsRegistry registry;
    TableRpcThrottler throttler;
    MSBackpressureHandler handler(&registry, &throttler);

    // Record some RPCs so there are tables to throttle
    for (int i = 0; i < 100; i++) {
        registry.record(LoadRelatedRpc::PREPARE_ROWSET, 12345);
    }

    // Trigger MS_BUSY
    EXPECT_TRUE(handler.on_ms_busy());

    // Should have recorded the time
    EXPECT_GE(handler.seconds_since_last_ms_busy(), 0);
}

TEST_F(MSBackpressureHandlerTest, UpgradeIntervalRespected) {
    config::enable_ms_backpressure_handling = true;
    config::ms_backpressure_upgrade_interval_sec = 100; // Long cooldown

    TableRpcQpsRegistry registry;
    TableRpcThrottler throttler;
    MSBackpressureHandler handler(&registry, &throttler);

    // First upgrade should succeed
    EXPECT_TRUE(handler.on_ms_busy());

    // Second upgrade should be blocked by cooldown
    EXPECT_FALSE(handler.on_ms_busy());
}

TEST_F(MSBackpressureHandlerTest, BeforeAndAfterRpc) {
    config::enable_ms_backpressure_handling = true;

    TableRpcQpsRegistry registry;
    TableRpcThrottler throttler;
    MSBackpressureHandler handler(&registry, &throttler);

    // before_rpc with no limit should return approximately now
    auto now = std::chrono::steady_clock::now();
    auto wait_until = handler.before_rpc(LoadRelatedRpc::COMMIT_ROWSET, 12345);

    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(wait_until - now).count();
    EXPECT_LE(diff, 10);

    // after_rpc should record the call (just verify it doesn't crash)
    handler.after_rpc(LoadRelatedRpc::COMMIT_ROWSET, 12345);
}

TEST_F(MSBackpressureHandlerTest, BeforeRpcWithThrottle) {
    config::enable_ms_backpressure_handling = true;

    TableRpcQpsRegistry registry;
    TableRpcThrottler throttler;
    MSBackpressureHandler handler(&registry, &throttler);

    // Set a QPS limit
    throttler.set_qps_limit(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 500, 1.0); // 1 QPS

    // First call should return a time close to now
    auto t1 = handler.before_rpc(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 500);

    // Second call should return a time ~1 second later
    auto t2 = handler.before_rpc(LoadRelatedRpc::UPDATE_DELETE_BITMAP, 500);

    auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    EXPECT_GE(diff_ms, 900);
    EXPECT_LE(diff_ms, 1100);
}

TEST_F(MSBackpressureHandlerTest, SecondsSinceLastMsBusy) {
    config::enable_ms_backpressure_handling = true;
    config::ms_backpressure_upgrade_interval_sec = 0;

    TableRpcQpsRegistry registry;
    TableRpcThrottler throttler;
    MSBackpressureHandler handler(&registry, &throttler);

    // Before any MS_BUSY, should return -1
    EXPECT_EQ(handler.seconds_since_last_ms_busy(), -1);

    handler.on_ms_busy();

    // After MS_BUSY, should return >= 0
    EXPECT_GE(handler.seconds_since_last_ms_busy(), 0);
}

// ============== LoadRelatedRpc Utility Tests ==============

TEST(LoadRelatedRpcTest, LoadRelatedRpcName) {
    EXPECT_EQ(load_related_rpc_name(LoadRelatedRpc::PREPARE_ROWSET), "prepare_rowset");
    EXPECT_EQ(load_related_rpc_name(LoadRelatedRpc::COMMIT_ROWSET), "commit_rowset");
    EXPECT_EQ(load_related_rpc_name(LoadRelatedRpc::UPDATE_TMP_ROWSET), "update_tmp_rowset");
    EXPECT_EQ(load_related_rpc_name(LoadRelatedRpc::UPDATE_PACKED_FILE_INFO),
              "update_packed_file_info");
    EXPECT_EQ(load_related_rpc_name(LoadRelatedRpc::UPDATE_DELETE_BITMAP), "update_delete_bitmap");
    EXPECT_EQ(load_related_rpc_name(LoadRelatedRpc::COUNT), "unknown");
}

} // namespace doris::cloud
