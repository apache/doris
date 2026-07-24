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
#include "common/status.h"

namespace doris {

extern bvar::Adder<int64_t> s3_get_rate_limit_sleep_ns;
extern bvar::Adder<int64_t> s3_get_rate_limit_sleep_count;
extern bvar::Adder<int64_t> s3_get_rate_limit_rejected_count;
extern bvar::Adder<int64_t> s3_get_bytes_rate_limit_sleep_count;
extern bvar::Adder<int64_t> s3_get_bytes_rate_limit_rejected_count;
extern bvar::Adder<int64_t> s3_get_bytes_rate_limit_sleep_ns;
extern bvar::Adder<int64_t> s3_put_rate_limit_sleep_ns;
extern bvar::Adder<int64_t> s3_put_rate_limit_sleep_count;
extern bvar::Adder<int64_t> s3_put_rate_limit_rejected_count;
extern bvar::Adder<int64_t> s3_put_bytes_rate_limit_sleep_ns;
extern bvar::Adder<int64_t> s3_put_bytes_rate_limit_sleep_count;
extern bvar::Adder<int64_t> s3_put_bytes_rate_limit_rejected_count;

namespace {

struct RateLimiterMetricSnapshot {
    int64_t qps_sleep_ns;
    int64_t qps_sleep_count;
    int64_t qps_rejected_count;
    int64_t bytes_sleep_ns;
    int64_t bytes_sleep_count;
    int64_t bytes_rejected_count;
};

RateLimiterMetricSnapshot metric_snapshot(S3RateLimitType type) {
    CHECK(type == S3RateLimitType::GET || type == S3RateLimitType::PUT);
    if (type == S3RateLimitType::GET) {
        return {.qps_sleep_ns = s3_get_rate_limit_sleep_ns.get_value(),
                .qps_sleep_count = s3_get_rate_limit_sleep_count.get_value(),
                .qps_rejected_count = s3_get_rate_limit_rejected_count.get_value(),
                .bytes_sleep_ns = s3_get_bytes_rate_limit_sleep_ns.get_value(),
                .bytes_sleep_count = s3_get_bytes_rate_limit_sleep_count.get_value(),
                .bytes_rejected_count = s3_get_bytes_rate_limit_rejected_count.get_value()};
    }
    return {.qps_sleep_ns = s3_put_rate_limit_sleep_ns.get_value(),
            .qps_sleep_count = s3_put_rate_limit_sleep_count.get_value(),
            .qps_rejected_count = s3_put_rate_limit_rejected_count.get_value(),
            .bytes_sleep_ns = s3_put_bytes_rate_limit_sleep_ns.get_value(),
            .bytes_sleep_count = s3_put_bytes_rate_limit_sleep_count.get_value(),
            .bytes_rejected_count = s3_put_bytes_rate_limit_rejected_count.get_value()};
}

void expect_metrics_unchanged(const RateLimiterMetricSnapshot& before,
                              const RateLimiterMetricSnapshot& after) {
    EXPECT_EQ(before.qps_sleep_ns, after.qps_sleep_ns);
    EXPECT_EQ(before.qps_sleep_count, after.qps_sleep_count);
    EXPECT_EQ(before.qps_rejected_count, after.qps_rejected_count);
    EXPECT_EQ(before.bytes_sleep_ns, after.bytes_sleep_ns);
    EXPECT_EQ(before.bytes_sleep_count, after.bytes_sleep_count);
    EXPECT_EQ(before.bytes_rejected_count, after.bytes_rejected_count);
}

void expect_only_bytes_sleep_grows(const RateLimiterMetricSnapshot& before,
                                   const RateLimiterMetricSnapshot& after) {
    EXPECT_EQ(before.qps_sleep_ns, after.qps_sleep_ns);
    EXPECT_EQ(before.qps_sleep_count, after.qps_sleep_count);
    EXPECT_EQ(before.qps_rejected_count, after.qps_rejected_count);
    EXPECT_LT(before.bytes_sleep_ns, after.bytes_sleep_ns);
    EXPECT_EQ(before.bytes_sleep_count + 1, after.bytes_sleep_count);
    EXPECT_EQ(before.bytes_rejected_count, after.bytes_rejected_count);
}

void expect_only_qps_sleep_grows(const RateLimiterMetricSnapshot& before,
                                 const RateLimiterMetricSnapshot& after) {
    EXPECT_LT(before.qps_sleep_ns, after.qps_sleep_ns);
    EXPECT_EQ(before.qps_sleep_count + 1, after.qps_sleep_count);
    EXPECT_EQ(before.qps_rejected_count, after.qps_rejected_count);
    EXPECT_EQ(before.bytes_sleep_ns, after.bytes_sleep_ns);
    EXPECT_EQ(before.bytes_sleep_count, after.bytes_sleep_count);
    EXPECT_EQ(before.bytes_rejected_count, after.bytes_rejected_count);
}

void verify_cpu_aware_bytes_and_legacy_qps_metrics(S3RateLimitType type, S3RateLimitType other_type,
                                                   int64_t& qps_per_core, int64_t& bytes_per_core) {
    config::enable_s3_rate_limiter = true;
    config::s3_rate_limiter_cpu_cores = 1;
    config::s3_get_qps_max = 0;
    config::s3_put_qps_max = 0;
    config::s3_get_bytes_per_second_max = 0;
    config::s3_put_bytes_per_second_max = 0;
    config::s3_get_token_per_second = 1000;
    config::s3_put_token_per_second = 1000;
    config::s3_get_bucket_tokens = 1;
    config::s3_put_bucket_tokens = 1;
    config::s3_get_token_limit = 0;
    config::s3_put_token_limit = 0;

    auto& manager = S3RateLimiterManager::instance();

    // Enable only the target direction's CPU-aware bytes limiter. Keep a low
    // legacy QPS configuration in place to prove qps_per_core=0 bypasses it.
    config::s3_get_qps_per_core = 0;
    config::s3_put_qps_per_core = 0;
    config::s3_get_bytes_per_second_per_core = 0;
    config::s3_put_bytes_per_second_per_core = 0;
    bytes_per_core = 1000;
    manager.refresh();

    auto* qps = manager.qps_limiter(type);
    auto* bytes = manager.bytes_limiter(type);
    EXPECT_FALSE(qps->is_enabled());
    ASSERT_TRUE(bytes->is_enabled());
    EXPECT_EQ(1000, bytes->get_max_speed());
    EXPECT_EQ(1000, bytes->get_max_burst());

    auto target_before = metric_snapshot(type);
    auto other_before = metric_snapshot(other_type);
    EXPECT_GT(bytes->add(bytes->get_max_burst() + 1), 0);
    auto target_after = metric_snapshot(type);
    auto other_after = metric_snapshot(other_type);
    expect_only_bytes_sleep_grows(target_before, target_after);
    expect_metrics_unchanged(other_before, other_after);

    // Disable CPU-aware bytes and select qps_per_core=-1. The same low legacy
    // configuration now becomes effective, so only the original QPS metrics grow.
    qps_per_core = -1;
    bytes_per_core = 0;
    manager.refresh();

    EXPECT_TRUE(qps->is_enabled());
    EXPECT_EQ(1000, qps->get_max_speed());
    EXPECT_EQ(1, qps->get_max_burst());
    EXPECT_FALSE(bytes->is_enabled());

    target_before = metric_snapshot(type);
    other_before = metric_snapshot(other_type);
    EXPECT_GT(qps->add(qps->get_max_burst() + 1), 0);
    target_after = metric_snapshot(type);
    other_after = metric_snapshot(other_type);
    expect_only_qps_sleep_grows(target_before, target_after);
    expect_metrics_unchanged(other_before, other_after);
}

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

TEST(S3RateLimiterResolveTest, registered_defaults_preserve_legacy_compatibility) {
    const auto& fields = *config::Register::_s_field_map;
    EXPECT_STREQ("-1", fields.at("s3_get_qps_per_core").defval);
    EXPECT_STREQ("-1", fields.at("s3_put_qps_per_core").defval);
    EXPECT_STREQ("-1", fields.at("s3_get_bytes_per_second_per_core").defval);
    EXPECT_STREQ("-1", fields.at("s3_put_bytes_per_second_per_core").defval);
    EXPECT_STREQ("0", fields.at("s3_rate_limiter_cpu_cores").defval);
}

TEST(S3RateLimiterConfigTest, invalid_dynamic_values_are_rejected_without_changing_config) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = false;
    config::s3_get_qps_per_core = -1;
    config::s3_get_bytes_per_second_per_core = -1;
    config::s3_put_bytes_per_second_max = 0;

    auto status = config::set_config("s3_get_qps_per_core", "-2");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("validate s3_get_qps_per_core=-2 failed"));
    EXPECT_EQ(-1, config::s3_get_qps_per_core);

    status = config::set_config("s3_get_bytes_per_second_per_core", "Ab");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("convert 'Ab' as int64_t failed"));
    EXPECT_EQ(-1, config::s3_get_bytes_per_second_per_core);

    status = config::set_config("enable_s3_rate_limiter", "A");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("convert 'A' as bool failed"));
    EXPECT_FALSE(config::enable_s3_rate_limiter);

    status = config::set_config("s3_put_bytes_per_second_max", "9223372036854775808");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos,
              status.to_string().find("convert '9223372036854775808' as int64_t failed"));
    EXPECT_EQ(0, config::s3_put_bytes_per_second_max);
}

TEST(S3RateLimiterResolveTest, legacy_config_wins_when_per_core_unset) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = -1;
    config::s3_get_token_per_second = 123;
    config::s3_get_bucket_tokens = 456;
    config::s3_get_token_limit = 789;
    config::s3_get_qps_max = 9; // must be ignored on the legacy path
    config::s3_put_qps_per_core = -1;
    config::s3_put_token_per_second = 321;
    config::s3_put_bucket_tokens = 654;
    config::s3_put_token_limit = 987;
    config::s3_put_qps_max = 8; // must be ignored on the legacy path
    config::s3_get_bytes_per_second_per_core = -1;
    config::s3_put_bytes_per_second_per_core = -1;

    auto get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(123, get_limit.qps);
    EXPECT_EQ(456, get_limit.burst);
    EXPECT_EQ(789, get_limit.count_limit);
    EXPECT_EQ(0, get_limit.bytes_per_second);

    auto put_limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(321, put_limit.qps);
    EXPECT_EQ(654, put_limit.burst);
    EXPECT_EQ(987, put_limit.count_limit);
    EXPECT_EQ(0, put_limit.bytes_per_second);
}

TEST(S3RateLimiterResolveTest, get_and_put_per_core_configs_override_legacy_independently) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = 3;
    config::s3_get_qps_max = 0;
    config::s3_get_token_per_second = 321;
    config::s3_get_bucket_tokens = 654;
    config::s3_get_token_limit = 987;
    config::s3_put_qps_per_core = 7;
    config::s3_put_qps_max = 0;
    config::s3_put_token_per_second = 123;
    config::s3_put_bucket_tokens = 456;
    config::s3_put_token_limit = 789;

    auto get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(3 * kCores, get_limit.qps);
    EXPECT_EQ(3 * kCores, get_limit.burst);
    EXPECT_EQ(0, get_limit.count_limit);

    auto put_limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(7 * kCores, put_limit.qps);
    EXPECT_EQ(7 * kCores, put_limit.burst);
    EXPECT_EQ(0, put_limit.count_limit);
}

TEST(S3RateLimiterResolveTest, per_core_zero_disables_qps_limiting) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = 0;
    config::s3_get_token_per_second = 123;
    config::s3_get_token_limit = 789;
    config::s3_put_qps_per_core = 0;
    config::s3_put_token_per_second = 321;
    config::s3_put_token_limit = 987;

    auto get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(0, get_limit.qps);
    EXPECT_EQ(0, get_limit.burst);
    EXPECT_EQ(0, get_limit.count_limit);

    auto put_limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(0, put_limit.qps);
    EXPECT_EQ(0, put_limit.burst);
    EXPECT_EQ(0, put_limit.count_limit);
}

TEST(S3RateLimiterResolveTest, get_and_put_qps_max_cap_per_core_results_independently) {
    RateLimiterConfigGuard guard;
    config::s3_get_qps_per_core = 1000000;
    config::s3_get_qps_max = 256;
    config::s3_put_qps_per_core = 2000000;
    config::s3_put_qps_max = 512;

    auto get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(256, get_limit.qps);
    EXPECT_EQ(256, get_limit.burst);

    auto put_limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(512, put_limit.qps);
    EXPECT_EQ(512, put_limit.burst);
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

TEST(S3RateLimiterResolveTest, get_and_put_bytes_caps_are_resolved_independently) {
    RateLimiterConfigGuard guard;
    config::s3_get_bytes_per_second_per_core = 1024;
    config::s3_get_bytes_per_second_max = 0;
    config::s3_put_bytes_per_second_per_core = 2048;
    config::s3_put_bytes_per_second_max = 6144;

    auto get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(1024 * kCores, get_limit.bytes_per_second);

    auto put_limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(6144, put_limit.bytes_per_second);

    config::s3_get_bytes_per_second_max = 2048;
    get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    EXPECT_EQ(2048, get_limit.bytes_per_second);

    // -1 and 0 both disable bandwidth limiting.
    config::s3_get_bytes_per_second_per_core = 0;
    config::s3_put_bytes_per_second_per_core = -1;
    get_limit = resolve_s3_rate_limit(S3RateLimitType::GET, kCores);
    put_limit = resolve_s3_rate_limit(S3RateLimitType::PUT, kCores);
    EXPECT_EQ(0, get_limit.bytes_per_second);
    EXPECT_EQ(0, put_limit.bytes_per_second);
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

TEST(S3RateLimiterManagerTest, legacy_get_config_throttles_when_per_core_is_unset) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);

    config::s3_get_qps_per_core = -1;
    config::s3_get_token_per_second = 100;
    config::s3_get_bucket_tokens = 1;
    config::s3_get_token_limit = 0;
    manager.refresh();

    EXPECT_EQ(100, get_qps->get_max_speed());
    EXPECT_EQ(1, get_qps->get_max_burst());
    EXPECT_EQ(0, get_qps->get_limit());
    // Charging two requests against a one-token burst must wait for the legacy
    // token_per_second refill, proving that fallback affects runtime behavior.
    EXPECT_GT(get_qps->add(2), 0);
}

TEST(S3RateLimiterManagerTest, refresh_applies_qps_speed_burst_and_limit_changes) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);

    config::s3_get_qps_per_core = -1;
    config::s3_get_token_per_second = 100;
    config::s3_get_bucket_tokens = 200;
    config::s3_get_token_limit = 300;
    manager.refresh();
    EXPECT_EQ(100, get_qps->get_max_speed());
    EXPECT_EQ(200, get_qps->get_max_burst());
    EXPECT_EQ(300, get_qps->get_limit());

    config::s3_rate_limiter_cpu_cores = kCores;
    config::s3_get_qps_per_core = 100;
    config::s3_get_qps_max = 0;
    manager.refresh();
    EXPECT_EQ(100 * kCores, get_qps->get_max_speed());
    EXPECT_EQ(100 * kCores, get_qps->get_max_burst());
    EXPECT_EQ(0, get_qps->get_limit());

    // Simulate a serverless resize: the core count is an input of refresh().
    config::s3_rate_limiter_cpu_cores = 2 * kCores;
    manager.refresh();
    EXPECT_EQ(100 * 2 * kCores, get_qps->get_max_speed());
    EXPECT_EQ(100 * 2 * kCores, get_qps->get_max_burst());
    EXPECT_EQ(0, get_qps->get_limit());
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
    EXPECT_EQ(1000 * kCores, put_bytes->get_max_burst());
    EXPECT_EQ(0, put_bytes->get_limit());

    config::s3_put_bytes_per_second_per_core = -1;
    manager.refresh();
    EXPECT_FALSE(put_bytes->is_enabled());
}

TEST(TokenBucketRateLimiterTest, rejected_add_rolls_back_count_and_tokens) {
    TokenBucketRateLimiter limiter(1000, 1000, 1000);

    EXPECT_EQ(0, limiter.add(1000));
    EXPECT_EQ(-1, limiter.add(1000));

    // Releasing the admitted request makes room for another request. The rejected
    // request must not leave either count or token debt behind.
    limiter.refund(1000);
    EXPECT_EQ(0, limiter.add(1000));
}

TEST(S3RateLimiterManagerTest, qps_and_bytes_buckets_are_independent) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* qps = manager.qps_limiter(S3RateLimitType::GET);
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    qps->reset(kNoThrottle, kNoThrottle, 1);
    bytes->reset(kNoThrottle, kNoThrottle, 100);

    EXPECT_EQ(0, qps->add(1));
    EXPECT_EQ(-1, qps->add(1));
    EXPECT_EQ(0, bytes->add(100));
    EXPECT_EQ(-1, bytes->add(1));
}

TEST(S3RateLimiterManagerTest, get_and_put_qps_buckets_are_independent) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);
    auto* put_qps = manager.qps_limiter(S3RateLimitType::PUT);
    get_qps->reset(kNoThrottle, kNoThrottle, 1);
    put_qps->reset(kNoThrottle, kNoThrottle, 2);

    EXPECT_EQ(0, get_qps->add(1));
    EXPECT_EQ(-1, get_qps->add(1));
    EXPECT_EQ(0, put_qps->add(2));
    EXPECT_EQ(-1, put_qps->add(1));
}

TEST(S3RateLimiterManagerTest, get_and_put_bytes_buckets_are_independent) {
    RateLimiterConfigGuard guard;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_bytes = manager.bytes_limiter(S3RateLimitType::GET);
    auto* put_bytes = manager.bytes_limiter(S3RateLimitType::PUT);
    get_bytes->reset(kNoThrottle, kNoThrottle, 1);
    put_bytes->reset(kNoThrottle, kNoThrottle, 2);

    EXPECT_EQ(0, get_bytes->add(1));
    EXPECT_EQ(-1, get_bytes->add(1));
    EXPECT_EQ(0, put_bytes->add(2));
    EXPECT_EQ(-1, put_bytes->add(1));
}

TEST(S3RateLimitGuardTest, disabled_limiter_does_not_consume_qps_or_bytes) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = false;
    auto& manager = S3RateLimiterManager::instance();
    auto* get_qps = manager.qps_limiter(S3RateLimitType::GET);
    auto* put_qps = manager.qps_limiter(S3RateLimitType::PUT);
    auto* get_bytes = manager.bytes_limiter(S3RateLimitType::GET);
    auto* put_bytes = manager.bytes_limiter(S3RateLimitType::PUT);
    get_qps->reset(0, 0, 1);
    put_qps->reset(0, 0, 1);
    get_bytes->reset(0, 0, 100);
    put_bytes->reset(0, 0, 100);

    for (int i = 0; i < 3; ++i) {
        S3RateLimitGuard get_guard(S3RateLimitType::GET, 100);
        S3RateLimitGuard put_guard(S3RateLimitType::PUT, 100);
        EXPECT_TRUE(get_guard.ok());
        EXPECT_TRUE(put_guard.ok());
    }

    // The disabled guards must not consume either cumulative limit. Each bucket still
    // admits exactly its configured limit after the guards have completed.
    for (auto type : {S3RateLimitType::GET, S3RateLimitType::PUT}) {
        auto* qps = manager.qps_limiter(type);
        auto* bytes = manager.bytes_limiter(type);
        EXPECT_EQ(0, qps->add(1));
        EXPECT_EQ(-1, qps->add(1));
        EXPECT_EQ(0, bytes->add(100));
        EXPECT_EQ(-1, bytes->add(1));
    }
}

TEST(S3RateLimiterMetricsTest, bytes_wait_and_rejection_have_distinct_counters) {
    RateLimiterConfigGuard guard;
    auto* bytes = S3RateLimiterManager::instance().bytes_limiter(S3RateLimitType::GET);
    const int64_t sleep_count_before = s3_get_bytes_rate_limit_sleep_count.get_value();
    const int64_t rejected_count_before = s3_get_bytes_rate_limit_rejected_count.get_value();

    // The first add exceeds the one-token burst. It waits for the debt to refill and
    // succeeds instead of being rejected.
    bytes->reset(1000, 1, 0);
    EXPECT_GT(bytes->add(2), 0);
    EXPECT_EQ(sleep_count_before + 1, s3_get_bytes_rate_limit_sleep_count.get_value());
    EXPECT_EQ(rejected_count_before, s3_get_bytes_rate_limit_rejected_count.get_value());

    // A cumulative-limit rejection is immediate and must not be reported as a wait.
    bytes->reset(kNoThrottle, kNoThrottle, 1);
    EXPECT_EQ(-1, bytes->add(2));
    EXPECT_EQ(sleep_count_before + 1, s3_get_bytes_rate_limit_sleep_count.get_value());
    EXPECT_EQ(rejected_count_before + 1, s3_get_bytes_rate_limit_rejected_count.get_value());
}

TEST(S3RateLimiterMetricsTest, get_cpu_aware_bytes_and_legacy_qps_update_separate_metrics) {
    RateLimiterConfigGuard guard;
    verify_cpu_aware_bytes_and_legacy_qps_metrics(S3RateLimitType::GET, S3RateLimitType::PUT,
                                                  config::s3_get_qps_per_core,
                                                  config::s3_get_bytes_per_second_per_core);
}

TEST(S3RateLimiterMetricsTest, put_cpu_aware_bytes_and_legacy_qps_update_separate_metrics) {
    RateLimiterConfigGuard guard;
    verify_cpu_aware_bytes_and_legacy_qps_metrics(S3RateLimitType::PUT, S3RateLimitType::GET,
                                                  config::s3_put_qps_per_core,
                                                  config::s3_put_bytes_per_second_per_core);
}

TEST(S3RateLimitGuardTest, legacy_count_limit_rejects) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    // No throttling, hard count limit of 2.
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 2);

    {
        S3RateLimitGuard g1(S3RateLimitType::GET, 0);
        EXPECT_TRUE(g1.ok());
    }
    {
        S3RateLimitGuard g2(S3RateLimitType::GET, 0);
        EXPECT_TRUE(g2.ok());
    }
    // QPS guards do not release the legacy cumulative count on destruction.
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

TEST(S3RateLimitGuardTest, first_oversized_reservation_is_admitted_without_waiting) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    auto* bytes = manager.bytes_limiter(S3RateLimitType::GET);
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 0);
    bytes->reset(1000, 1000, 0);
    const int64_t sleep_count_before = s3_get_bytes_rate_limit_sleep_count.get_value();

    // The guard clamps this request to max_speed (= one full bucket). Charging exactly
    // the initial bucket does not create debt, so the request is admitted without wait.
    S3RateLimitGuard g(S3RateLimitType::GET, 1000000);
    EXPECT_TRUE(g.ok());
    EXPECT_EQ(sleep_count_before, s3_get_bytes_rate_limit_sleep_count.get_value());

    // A later oversized request is clamped in the same way. With the initial burst
    // already consumed, it waits for refill and is still admitted rather than rejected.
    S3RateLimitGuard next(S3RateLimitType::GET, 1000000);
    EXPECT_TRUE(next.ok());
    EXPECT_GT(s3_get_bytes_rate_limit_sleep_count.get_value(), sleep_count_before);
}

} // namespace doris
