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
#include <chrono>
#include <thread>
#include <vector>

#include "cloud/config.h"
#include "cpp/sync_point.h"

namespace doris::cloud {

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

    HostLevelMSRpcRateLimiters limiters(10);

    // Should return 0 immediately when disabled
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(limiters.limit("get tablet meta"), 0);
    }
}

// Test that known RPC methods are rate limited
TEST_F(HostLevelMSRpcRateLimitersTest, KnownRpcMethodsExist) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(10000); // High QPS to avoid waiting

    // All known RPC methods should return 0 (no wait) for first call with high QPS
    std::vector<std::string> known_methods = {
            "get tablet meta",
            "get rowset",
            "prepare rowset",
            "commit rowset",
            "update tmp rowset",
            "commit txn",
            "abort txn",
            "precommit txn",
            "get obj store info",
            "start tablet job",
            "finish tablet job",
            "get delete bitmap",
            "update delete bitmap",
            "get delete bitmap update lock",
            "remove delete bitmap update lock",
            "get instance",
    };

    for (const auto& method : known_methods) {
        EXPECT_EQ(limiters.limit(method), 0) << "Method: " << method;
    }
}

// Test that unknown RPC methods return 0 (no rate limiting)
TEST_F(HostLevelMSRpcRateLimitersTest, UnknownRpcMethodNoLimit) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(1);

    // Unknown method should return 0 (not found in map)
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(limiters.limit("unknown_rpc_method"), 0);
    }
}

// Test that rate limiting actually throttles requests
TEST_F(HostLevelMSRpcRateLimitersTest, RateLimitingThrottles) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(50); // Low QPS to trigger throttling

    // Use a fixed large number of calls that will definitely exceed burst
    const int num_calls = 100;

    int64_t total_sleep_ns = 0;
    for (int i = 0; i < num_calls; i++) {
        total_sleep_ns += limiters.limit("get tablet meta");
    }

    // With rate limiting, some requests should have been throttled
    EXPECT_GT(total_sleep_ns, 0) << "Rate limiting should have caused some sleep";
}

// Test multiple RPC methods have independent rate limiters
TEST_F(HostLevelMSRpcRateLimitersTest, IndependentRateLimiters) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(50);

    // Use a fixed large number of calls to exhaust tokens
    const int num_calls = 100;

    // Exhaust one method's tokens
    int64_t sleep1 = 0;
    for (int i = 0; i < num_calls; i++) {
        sleep1 += limiters.limit("get tablet meta");
    }

    // Another method should still have its own tokens
    // First call should not sleep (has its own burst)
    int64_t sleep2 = limiters.limit("get rowset");

    // get_tablet_meta should have had significant sleep
    EXPECT_GT(sleep1, 0);
    // First call to get_rowset should not sleep (it has its own bucket)
    EXPECT_EQ(sleep2, 0);
}

// Test concurrent access from multiple threads
TEST_F(HostLevelMSRpcRateLimitersTest, MultiThreadedAccess) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(50);

    const int num_threads = 10;
    // Each thread makes enough calls to exceed the burst
    // With 10 threads making 50 calls each = 500 total calls, should definitely throttle
    const int calls_per_thread = 10;

    std::vector<std::thread> threads;
    std::atomic<int64_t> total_sleep_ns {0};
    std::atomic<int> total_calls {0};

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&limiters, &total_sleep_ns, &total_calls]() {
            for (int i = 0; i < calls_per_thread; i++) {
                int64_t sleep = limiters.limit("get tablet meta");
                total_sleep_ns += sleep;
                total_calls++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(total_calls.load(), num_threads * calls_per_thread);
    // With many concurrent calls exceeding burst, there should be throttling
    EXPECT_GT(total_sleep_ns.load(), 0);
}

// Test concurrent access to different RPC methods with throttling verification
TEST_F(HostLevelMSRpcRateLimitersTest, MultiThreadedDifferentMethods) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(50);

    std::vector<std::string> methods = {"get tablet meta", "get rowset", "commit txn",
                                        "prepare rowset"};
    const int num_threads = 8;
    // Each thread makes enough calls to exceed the burst for its method
    // With 2 threads per method making 50 calls each, should definitely throttle
    const int calls_per_thread = 50;

    std::vector<std::thread> threads;
    std::atomic<int> total_calls {0};
    std::atomic<int64_t> total_sleep_ns {0};

    // Track total sleep time for each RPC method
    std::map<std::string, std::atomic<int64_t>> method_sleep_ns;
    for (const auto& method : methods) {
        method_sleep_ns[method] = 0;
    }

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&limiters, &methods, &total_calls, &total_sleep_ns, &method_sleep_ns, t]() {
            std::string method = methods[t % methods.size()];
            for (int i = 0; i < calls_per_thread; i++) {
                int64_t sleep = limiters.limit(method);
                total_sleep_ns += sleep;
                method_sleep_ns[method] += sleep;
                total_calls++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(total_calls.load(), num_threads * calls_per_thread);
    // Each method has 2 threads, both exceeding burst, so there should be throttling
    EXPECT_GT(total_sleep_ns.load(), 0);

    // Verify each RPC method experienced throttling
    for (const auto& method : methods) {
        EXPECT_GT(method_sleep_ns[method].load(), 0) << "Method: " << method;
    }
}

// Test reset functionality
TEST_F(HostLevelMSRpcRateLimitersTest, ResetQps) {
    config::enable_ms_rpc_host_level_rate_limit = true;

    HostLevelMSRpcRateLimiters limiters(20);

    // Exhaust tokens with low QPS
    int64_t sleep1 = 0;
    for (int i = 0; i < 50; i++) {
        sleep1 += limiters.limit("get tablet meta");
    }
    EXPECT_GT(sleep1, 0);

    // Reset to high QPS
    limiters.reset(10000);

    // Should not throttle with high QPS
    int64_t sleep2 = 0;
    for (int i = 0; i < 50; i++) {
        sleep2 += limiters.limit("get tablet meta");
    }
    EXPECT_EQ(sleep2, 0);
}

} // namespace doris::cloud
