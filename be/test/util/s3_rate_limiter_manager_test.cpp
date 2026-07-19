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

TEST(S3RateLimiterManagerTest, refresh_is_idempotent_and_applies_core_changes) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);

    config::s3_rate_limiter_cpu_cores = kCores;
    config::s3_get_qps_per_core = 100;
    config::s3_get_qps_max = 0;
    manager.refresh();
    EXPECT_EQ(100 * kCores, get_qps->get_max_speed());
    EXPECT_EQ(100 * kCores, get_qps->get_max_burst());

    // No config change -> no reset (params stay identical).
    manager.refresh();
    EXPECT_EQ(100 * kCores, get_qps->get_max_speed());

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
    // Big enough bucket that nothing throttles; we only observe token accounting.
    bytes->reset(1000, 1000, 0);

    {
        S3RateLimitGuard g(S3RateLimitType::GET, 600);
        ASSERT_TRUE(g.ok());
        g.settle(100); // short read: 500 tokens must come back
    }
    // 1000 - 600 + 500 = 900 tokens remain; a 900-byte reservation passes without
    // sleeping, which we observe as add() returning 0.
    EXPECT_EQ(0, bytes->add(900));
    // Now the bucket is empty; the next add must throttle (sleep > 0).
    EXPECT_GT(bytes->add(100), 0);
}

TEST(S3RateLimitGuardTest, unsettled_guard_keeps_reservation) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::PUT);
    manager.qps_limiter(S3RateLimitType::PUT)->reset(0, 0, 0);
    bytes->reset(1000, 1000, 0);

    {
        S3RateLimitGuard g(S3RateLimitType::PUT, 600);
        ASSERT_TRUE(g.ok());
        // No settle: e.g. the request failed. The reservation stays charged.
    }
    EXPECT_EQ(0, bytes->add(400)); // exactly the remainder
    EXPECT_GT(bytes->add(100), 0); // anything more throttles
}

TEST(S3RateLimitGuardTest, settle_across_reset_does_not_pollute_new_bucket) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    bytes->reset(1000, 1000, 0);

    {
        S3RateLimitGuard g(S3RateLimitType::GET, 600);
        ASSERT_TRUE(g.ok());
        // The bucket is swapped while the request is in flight.
        bytes->reset(1000, 1000, 0);
        g.settle(100); // refund lands on the OLD generation, not the fresh bucket
    }
    // The fresh bucket must still be exactly full (1000): a 1000-byte reservation
    // passes without sleeping, anything more throttles. If the 500-byte refund had
    // leaked into it, the bucket would be over-filled... which the burst cap masks,
    // so verify the other direction: no tokens were taken from it either.
    EXPECT_EQ(0, bytes->add(1000));
    EXPECT_GT(bytes->add(100), 0);
}

TEST(S3RateLimitGuardTest, reservation_is_clamped_to_one_second_of_bandwidth) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    bytes->reset(1000, 1000, 0);

    {
        // A huge IO only reserves max_speed (=1000) instead of going into deep debt.
        S3RateLimitGuard g(S3RateLimitType::GET, 1000000);
        ASSERT_TRUE(g.ok());
    }
    // The bucket was drained exactly to zero, not to -999000: a following small add
    // throttles briefly instead of sleeping for ~1000 seconds.
    auto sleep_ns = bytes->add(100);
    EXPECT_GT(sleep_ns, 0);
    EXPECT_LT(sleep_ns, 1000000000L); // well under 1 second of debt
}

} // namespace doris
