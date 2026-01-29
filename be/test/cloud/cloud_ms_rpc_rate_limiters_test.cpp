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

#include "cloud/cloud_ms_rpc_rate_limiters.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "cloud/config.h"
#include "util/cpu_info.h"

namespace doris::cloud {

// Basic tests using uniform QPS constructor (completely independent of config and CPU cores)
class HostLevelMSRpcRateLimitersTest : public testing::Test {
protected:
    void SetUp() override { _saved_enable = config::enable_ms_rpc_host_level_rate_limit; }

    void TearDown() override { config::enable_ms_rpc_host_level_rate_limit = _saved_enable; }

private:
    bool _saved_enable;
};

// Test that limit returns 0 when rate limiting is disabled
TEST_F(HostLevelMSRpcRateLimitersTest, DisabledRateLimiting) {
    config::enable_ms_rpc_host_level_rate_limit = false;

    HostLevelMSRpcRateLimiters limiters(100);

    // Should return 0 immediately when disabled
    for (int i = 0; i < 200; i++) {
        EXPECT_EQ(limiters.limit(MetaServiceRPC::GET_TABLET_META), 0);
    }
}

// Test that all RPC types exist and can be rate limited
TEST_F(HostLevelMSRpcRateLimitersTest, AllRpcTypesExist) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(10000);  // High QPS to avoid waiting

    // All RPC types should return 0 (no wait) for first call with high QPS
    for (size_t i = 0; i < static_cast<size_t>(MetaServiceRPC::COUNT); ++i) {
        MetaServiceRPC rpc = static_cast<MetaServiceRPC>(i);
        EXPECT_EQ(limiters.limit(rpc), 0) << "RPC: " << meta_service_rpc_display_name(rpc);
    }
}

// Test that rate limiting actually throttles requests with low QPS
TEST_F(HostLevelMSRpcRateLimitersTest, RateLimitingThrottles) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    // Use low QPS (50) - burst is also 50, so after 50 calls we should start throttling
    HostLevelMSRpcRateLimiters limiters(50);

    const int num_calls = 100;
    int64_t total_sleep_ns = 0;
    for (int i = 0; i < num_calls; i++) {
        total_sleep_ns += limiters.limit(MetaServiceRPC::GET_TABLET_META);
    }

    // With rate limiting, requests beyond burst should be throttled
    EXPECT_GT(total_sleep_ns, 0) << "Rate limiting should have caused some sleep";
}

// Test multiple RPC types have independent rate limiters
TEST_F(HostLevelMSRpcRateLimitersTest, IndependentRateLimiters) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(50);

    const int num_calls = 100;

    // Exhaust one RPC type's tokens
    int64_t sleep1 = 0;
    for (int i = 0; i < num_calls; i++) {
        sleep1 += limiters.limit(MetaServiceRPC::GET_TABLET_META);
    }

    // Another RPC type should still have its own tokens
    // First call should not sleep (has its own burst)
    int64_t sleep2 = limiters.limit(MetaServiceRPC::GET_ROWSET);

    EXPECT_GT(sleep1, 0);
    EXPECT_EQ(sleep2, 0);
}

// Test concurrent access from multiple threads
TEST_F(HostLevelMSRpcRateLimitersTest, MultiThreadedAccess) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(50);

    const int num_threads = 10;
    const int calls_per_thread = 20;

    std::vector<std::thread> threads;
    std::atomic<int64_t> total_sleep_ns {0};
    std::atomic<int> total_calls {0};

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&limiters, &total_sleep_ns, &total_calls]() {
            for (int i = 0; i < calls_per_thread; i++) {
                int64_t sleep = limiters.limit(MetaServiceRPC::GET_TABLET_META);
                total_sleep_ns += sleep;
                total_calls++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(total_calls.load(), num_threads * calls_per_thread);
    // Total calls (200) > burst (50), so there should be throttling
    EXPECT_GT(total_sleep_ns.load(), 0);
}

// Test reset functionality
TEST_F(HostLevelMSRpcRateLimitersTest, ResetSingleRpc) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(20);

    // Exhaust tokens with low QPS
    int64_t sleep1 = 0;
    for (int i = 0; i < 50; i++) {
        sleep1 += limiters.limit(MetaServiceRPC::GET_TABLET_META);
    }
    EXPECT_GT(sleep1, 0);

    // Reset to high QPS
    limiters.reset(MetaServiceRPC::GET_TABLET_META, 10000);

    // Should not throttle with high QPS
    int64_t sleep2 = 0;
    for (int i = 0; i < 50; i++) {
        sleep2 += limiters.limit(MetaServiceRPC::GET_TABLET_META);
    }
    EXPECT_EQ(sleep2, 0);
}

// Test display name function
TEST_F(HostLevelMSRpcRateLimitersTest, DisplayNames) {
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::GET_TABLET_META), "get tablet meta");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::GET_ROWSET), "get rowset");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::PREPARE_ROWSET), "prepare rowset");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::COMMIT_ROWSET), "commit rowset");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::UPDATE_TMP_ROWSET),
              "update tmp rowset");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::COMMIT_TXN), "commit txn");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::ABORT_TXN), "abort txn");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::PRECOMMIT_TXN), "precommit txn");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::GET_OBJ_STORE_INFO),
              "get obj store info");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::START_TABLET_JOB), "start tablet job");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::FINISH_TABLET_JOB), "finish tablet job");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::GET_DELETE_BITMAP),
              "get delete bitmap");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::UPDATE_DELETE_BITMAP),
              "update delete bitmap");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::GET_DELETE_BITMAP_UPDATE_LOCK),
              "get delete bitmap update lock");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::REMOVE_DELETE_BITMAP_UPDATE_LOCK),
              "remove delete bitmap update lock");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::GET_INSTANCE), "get instance");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::PREPARE_RESTORE_JOB),
              "prepare restore job");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::COMMIT_RESTORE_JOB),
              "commit restore job");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::FINISH_RESTORE_JOB),
              "finish restore job");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::LIST_SNAPSHOTS), "list snapshots");
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::UPDATE_PACKED_FILE_INFO),
              "update packed file info");
}

// Test that invalid MetaServiceRPC returns "unknown"
TEST_F(HostLevelMSRpcRateLimitersTest, InvalidRpcDisplayName) {
    EXPECT_EQ(meta_service_rpc_display_name(MetaServiceRPC::COUNT), "unknown");
    EXPECT_EQ(meta_service_rpc_display_name(static_cast<MetaServiceRPC>(999)), "unknown");
}

// Config-dependent tests: verify each config controls the corresponding rate limiter
class HostLevelMSRpcRateLimitersConfigTest : public testing::Test {
protected:
    void SetUp() override {
        doris::CpuInfo::init();

        _saved_enable = config::enable_ms_rpc_host_level_rate_limit;
        _saved_default = config::ms_rpc_qps_default;

// Save all per-RPC configs
#define SAVE_CONFIG(enum_name, config_suffix, display_name) \
    _saved_##config_suffix = config::ms_rpc_qps_##config_suffix;
        META_SERVICE_RPC_TYPES(SAVE_CONFIG)
#undef SAVE_CONFIG
    }

    void TearDown() override {
        config::enable_ms_rpc_host_level_rate_limit = _saved_enable;
        config::ms_rpc_qps_default = _saved_default;

// Restore all per-RPC configs
#define RESTORE_CONFIG(enum_name, config_suffix, display_name) \
    config::ms_rpc_qps_##config_suffix = _saved_##config_suffix;
        META_SERVICE_RPC_TYPES(RESTORE_CONFIG)
#undef RESTORE_CONFIG
    }

private:
    bool _saved_enable;
    int32_t _saved_default;

// Declare saved config variables for each RPC
#define DECLARE_SAVED_CONFIG(enum_name, config_suffix, display_name) int32_t _saved_##config_suffix;
    META_SERVICE_RPC_TYPES(DECLARE_SAVED_CONFIG)
#undef DECLARE_SAVED_CONFIG
};

// Test that each per-RPC config controls its corresponding rate limiter
// Directly check limiter QPS values instead of relying on throttling behavior
TEST_F(HostLevelMSRpcRateLimitersConfigTest, EachConfigControlsCorrespondingLimiter) {
    config::enable_ms_rpc_host_level_rate_limit = true;
    config::ms_rpc_qps_default = 10;  // Default QPS

    int num_cores = doris::CpuInfo::num_cores();

// Test each RPC type with different config values
#define TEST_RPC_CONFIG(enum_name, config_suffix, display_name)                                   \
    {                                                                                             \
        /* Test with config = 0 (disabled) */                                                     \
        config::ms_rpc_qps_##config_suffix = 0;                                                   \
        HostLevelMSRpcRateLimiters limiters1;                                                     \
        size_t idx = static_cast<size_t>(MetaServiceRPC::enum_name);                              \
        EXPECT_EQ(limiters1._limiters[idx].load(), nullptr)                                       \
                << "RPC " << #enum_name << " should be disabled when config=0";                   \
                                                                                                  \
        /* Test with config = -1 (use default) */                                                 \
        config::ms_rpc_qps_##config_suffix = -1;                                                  \
        HostLevelMSRpcRateLimiters limiters2;                                                     \
        ASSERT_NE(limiters2._limiters[idx].load(), nullptr)                                       \
                << "RPC " << #enum_name << " should use default when config=-1";                  \
        size_t expected_qps = 10 * num_cores;                                                     \
        EXPECT_EQ(limiters2._limiters[idx].load()->limiter->get_max_speed(), expected_qps)        \
                << "RPC " << #enum_name << " should use default QPS when config=-1";              \
                                                                                                  \
        /* Test with config = specific value */                                                   \
        config::ms_rpc_qps_##config_suffix = 5;                                                   \
        HostLevelMSRpcRateLimiters limiters3;                                                     \
        ASSERT_NE(limiters3._limiters[idx].load(), nullptr)                                       \
                << "RPC " << #enum_name << " should be enabled with config=5";                    \
        expected_qps = 5 * num_cores;                                                             \
        EXPECT_EQ(limiters3._limiters[idx].load()->limiter->get_max_speed(), expected_qps)        \
                << "RPC " << #enum_name << " should have QPS=5*num_cores when config=5";          \
    }

    META_SERVICE_RPC_TYPES(TEST_RPC_CONFIG)
#undef TEST_RPC_CONFIG
}

// Test that reset_all re-reads config
TEST_F(HostLevelMSRpcRateLimitersConfigTest, ResetAllReadsConfig) {
    config::enable_ms_rpc_host_level_rate_limit = true;
    config::ms_rpc_qps_default = -1;
    config::ms_rpc_qps_get_tablet_meta = 0;  // Disabled initially

    HostLevelMSRpcRateLimiters limiters;

    // Should be nullptr (disabled)
    size_t idx = static_cast<size_t>(MetaServiceRPC::GET_TABLET_META);
    EXPECT_EQ(limiters._limiters[idx].load(), nullptr) << "Should be disabled when config=0";

    // Change config to enable with specific QPS
    config::ms_rpc_qps_get_tablet_meta = 20;

    // Reset all limiters
    limiters.reset_all();

    // Should now be enabled with the new QPS
    int num_cores = doris::CpuInfo::num_cores();
    size_t expected_qps = 20 * num_cores;
    ASSERT_NE(limiters._limiters[idx].load(), nullptr) << "Should be enabled after reset_all";
    EXPECT_EQ(limiters._limiters[idx].load()->limiter->get_max_speed(), expected_qps)
            << "Should have new QPS after reset_all";
}

} // namespace doris::cloud
