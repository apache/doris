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

#include "util/s3_rate_limiter_manager.h"

#include <gtest/gtest.h>

#include <limits>

#include "common/config.h"

namespace doris {

namespace {

// Saves every rate limiter related config on construction, restores it and re-applies
// the limiters on destruction so tests do not leak state into each other.
struct RateLimiterConfigGuard {
    bool enable = config::enable_s3_rate_limiter;
    int64_t get_tps = config::s3_get_token_per_second;
    int64_t get_bucket = config::s3_get_bucket_tokens;
    int64_t get_limit = config::s3_get_token_limit;
    int64_t put_tps = config::s3_put_token_per_second;
    int64_t put_bucket = config::s3_put_bucket_tokens;
    int64_t put_limit = config::s3_put_token_limit;
    int64_t get_per_core = config::s3_get_qps_per_core;
    int64_t put_per_core = config::s3_put_qps_per_core;
    int64_t get_qps_max = config::s3_get_qps_max;
    int64_t put_qps_max = config::s3_put_qps_max;
    int64_t get_bytes_per_core = config::s3_get_bytes_per_second_per_core;
    int64_t put_bytes_per_core = config::s3_put_bytes_per_second_per_core;
    int64_t get_bytes_max = config::s3_get_bytes_per_second_max;
    int64_t put_bytes_max = config::s3_put_bytes_per_second_max;
    int64_t cpu_cores = config::s3_rate_limiter_cpu_cores;

    ~RateLimiterConfigGuard() {
        config::enable_s3_rate_limiter = enable;
        config::s3_get_token_per_second = get_tps;
        config::s3_get_bucket_tokens = get_bucket;
        config::s3_get_token_limit = get_limit;
        config::s3_put_token_per_second = put_tps;
        config::s3_put_bucket_tokens = put_bucket;
        config::s3_put_token_limit = put_limit;
        config::s3_get_qps_per_core = get_per_core;
        config::s3_put_qps_per_core = put_per_core;
        config::s3_get_qps_max = get_qps_max;
        config::s3_put_qps_max = put_qps_max;
        config::s3_get_bytes_per_second_per_core = get_bytes_per_core;
        config::s3_put_bytes_per_second_per_core = put_bytes_per_core;
        config::s3_get_bytes_per_second_max = get_bytes_max;
        config::s3_put_bytes_per_second_max = put_bytes_max;
        config::s3_rate_limiter_cpu_cores = cpu_cores;
        S3RateLimiterManager::instance().refresh();
    }
};

constexpr int64_t kCores = 4;
constexpr size_t kNoThrottle = 1ULL << 40;

} // namespace

TEST(S3RateLimiterResolveTest, legacy_config_wins_when_per_core_unset) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = -1;
    config::s3_get_token_per_second = 123;
    config::s3_get_bucket_tokens = 456;
    config::s3_get_token_limit = 789;
    config::s3_get_qps_max = 9; // must be ignored on the legacy path
    config::s3_get_bytes_per_second_per_core = -1;

    auto limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(123, limit.qps);
    EXPECT_EQ(456, limit.burst);
    EXPECT_EQ(789, limit.count_limit);
    EXPECT_EQ(0, limit.bytes_per_second);
}

TEST(S3RateLimiterResolveTest, per_core_config_overrides_legacy) {
    RateLimiterConfigGuard guard;
    config::s3_put_qps_per_core = 7;
    config::s3_put_qps_max = 0;
    config::s3_put_token_per_second = 123;
    config::s3_put_bucket_tokens = 456;
    config::s3_put_token_limit = 789;

    auto limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(7 * kCores, limit.qps);
    EXPECT_EQ(7 * kCores, limit.burst);
    EXPECT_EQ(0, limit.count_limit); // legacy count cap is dropped on the per-core path
}

TEST(S3RateLimiterResolveTest, per_core_zero_disables_qps_limiting) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = 0;
    config::s3_get_token_per_second = 123;
    config::s3_get_token_limit = 789;

    auto limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(0, limit.qps);
    EXPECT_EQ(0, limit.burst);
    EXPECT_EQ(0, limit.count_limit);
}

TEST(S3RateLimiterResolveTest, qps_max_caps_per_core_result) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = 1000000;
    config::s3_get_qps_max = 256;

    auto limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(256, limit.qps);
    EXPECT_EQ(256, limit.burst);
}

TEST(S3RateLimiterResolveTest, overflowing_multiplication_is_capped) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = std::numeric_limits<int64_t>::max();
    config::s3_get_qps_max = 1024;

    auto limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(1024, limit.qps);

    // Without a cap the overflowing product saturates instead of wrapping around.
    config::s3_get_qps_max = 0;
    limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), limit.qps);
}

TEST(S3RateLimiterResolveTest, bytes_per_core_derives_bandwidth) {
    RateLimiterConfigGuard guard;
    config::s3_get_bytes_per_second_per_core = 1024;
    config::s3_get_bytes_per_second_max = 0;

    auto limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(1024 * kCores, limit.bytes_per_second);

    config::s3_get_bytes_per_second_max = 2048;
    limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(2048, limit.bytes_per_second);

    // -1 and 0 both disable bandwidth limiting.
    config::s3_get_bytes_per_second_per_core = 0;
    limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(0, limit.bytes_per_second);
}

TEST(S3RateLimiterResolveTest, cpu_cores_override_config_wins) {
    RateLimiterConfigGuard guard;
    config::s3_rate_limiter_cpu_cores = 16;
    EXPECT_EQ(16, s3_rate_limiter_cpu_cores());

    config::s3_rate_limiter_cpu_cores = 0;
    EXPECT_GE(s3_rate_limiter_cpu_cores(), 1); // auto-detect always yields >= 1
}

TEST(S3RateLimiterManagerTest, refresh_with_same_parameters_keeps_consumed_state) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);

    config::s3_get_qps_per_core = -1;
    config::s3_get_token_per_second = kNoThrottle;
    config::s3_get_bucket_tokens = kNoThrottle;
    config::s3_get_token_limit = 2;
    manager.refresh();

    EXPECT_EQ(0, get_qps->add(1));
    EXPECT_EQ(0, get_qps->add(1));

    // Identical parameters must preserve the published bucket and its cumulative count.
    // If refresh() reset the bucket, this third request would be admitted.
    manager.refresh();
    EXPECT_EQ(-1, get_qps->add(1));
}

TEST(S3RateLimiterManagerTest, refresh_applies_core_changes) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);

    config::s3_rate_limiter_cpu_cores = kCores;
    config::s3_get_qps_per_core = 100;
    config::s3_get_qps_max = 0;
    manager.refresh();
    EXPECT_EQ(100 * kCores, get_qps->get_max_speed());
    EXPECT_EQ(100 * kCores, get_qps->get_max_burst());

    // Simulate a serverless resize: the core count is an input of refresh().
    config::s3_rate_limiter_cpu_cores = 2 * kCores;
    manager.refresh();
    EXPECT_EQ(100 * 2 * kCores, get_qps->get_max_speed());
}

TEST(S3RateLimiterManagerTest, refresh_applies_bytes_limit_and_enables_bucket) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* put_bytes = manager.bytes_limiter(S3RateLimitType::PUT);

    config::s3_rate_limiter_cpu_cores = kCores;
    config::s3_put_bytes_per_second_per_core = -1;
    manager.refresh();
    EXPECT_FALSE(put_bytes->is_enabled());

    config::s3_put_bytes_per_second_per_core = 1000;
    manager.refresh();
    EXPECT_TRUE(put_bytes->is_enabled());
    EXPECT_EQ(1000 * kCores, put_bytes->get_max_speed());

    config::s3_put_bytes_per_second_per_core = -1;
    manager.refresh();
    EXPECT_FALSE(put_bytes->is_enabled());
}

TEST(S3RateLimitGuardTest, disabled_limiter_admits_everything) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = false;
    S3RateLimiterManager::instance().qps_limiter(S3RateLimitType::GET)->reset(0, 0, 1);

    for (int i = 0; i < 3; ++i) {
        S3RateLimitGuard g(S3RateLimitType::GET, 100);
        EXPECT_TRUE(g.ok());
    }
}

TEST(S3RateLimitGuardTest, legacy_count_limit_rejects) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    // No throttling, hard count limit of 2.
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 2);

    S3RateLimitGuard g1(S3RateLimitType::GET, 0);
    EXPECT_TRUE(g1.ok());
    S3RateLimitGuard g2(S3RateLimitType::GET, 0);
    EXPECT_TRUE(g2.ok());
    S3RateLimitGuard g3(S3RateLimitType::GET, 0);
    EXPECT_FALSE(g3.ok());
}

TEST(S3RateLimitGuardTest, settle_refunds_short_read) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    // A large speed/burst removes wall-clock refill from the assertion. The count limit
    // makes the refund observable exactly.
    bytes->reset(kNoThrottle, kNoThrottle, 1000);

    {
        S3RateLimitGuard g(S3RateLimitType::GET, 600);
        ASSERT_TRUE(g.ok());
        g.settle(100); // short read: 500 tokens must come back
    }
    // The guard leaves 100 cumulatively charged bytes, so exactly 900 more are admitted.
    EXPECT_EQ(0, bytes->add(900));
    EXPECT_EQ(-1, bytes->add(1));
}

TEST(S3RateLimitGuardTest, put_payload_reservation_remains_charged_without_settle) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::PUT);
    manager.qps_limiter(S3RateLimitType::PUT)->reset(0, 0, 0);
    bytes->reset(kNoThrottle, kNoThrottle, 1000);

    {
        S3RateLimitGuard g(S3RateLimitType::PUT, 600);
        ASSERT_TRUE(g.ok());
        // No settle: e.g. the request failed. The reservation stays charged.
    }
    EXPECT_EQ(0, bytes->add(400)); // exactly the cumulative count remainder
    EXPECT_EQ(-1, bytes->add(1));
}

TEST(S3RateLimitGuardTest, settle_across_reset_does_not_pollute_new_bucket) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    bytes->reset(kNoThrottle, kNoThrottle, 1000);

    {
        S3RateLimitGuard g(S3RateLimitType::GET, 600);
        ASSERT_TRUE(g.ok());
        // The bucket is swapped while the request is in flight.
        bytes->reset(kNoThrottle, kNoThrottle, 1000);
        EXPECT_EQ(0, bytes->add(600));
        g.settle(100); // refund lands on the OLD generation, not the fresh bucket
    }
    // The fresh generation remains charged for 600 bytes. A refund to the wrong
    // generation would reduce its cumulative count to 100 and admit the last request.
    EXPECT_EQ(0, bytes->add(400));
    EXPECT_EQ(-1, bytes->add(1));
}

TEST(S3RateLimitGuardTest, reservation_is_clamped_to_one_second_of_bandwidth) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    // Keep max_speed at 1000 to define the one-second reservation, but use a huge
    // burst to make the test independent from elapsed time while observing the exact
    // charged amount through the cumulative limit.
    bytes->reset(1000, kNoThrottle, 1500);

    {
        // A huge IO only reserves max_speed (=1000). Since actual_bytes exceeds that
        // reservation, settle() does not refund it.
        S3RateLimitGuard g(S3RateLimitType::GET, 1000000);
        ASSERT_TRUE(g.ok());
        g.settle(2000);
    }
    EXPECT_EQ(0, bytes->add(500));
    EXPECT_EQ(-1, bytes->add(1));
}

} // namespace doris
