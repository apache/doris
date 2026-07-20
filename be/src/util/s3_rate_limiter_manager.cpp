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

#include <algorithm>
#include <limits>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "util/cgroup_util.h"

namespace doris {

bvar::Adder<int64_t> s3_get_bytes_rate_limit_sleep_ns("s3_get_bytes_rate_limit_sleep_ns");
bvar::Adder<int64_t> s3_get_bytes_rate_limit_sleep_count("s3_get_bytes_rate_limit_sleep_count");
bvar::Adder<int64_t> s3_put_bytes_rate_limit_sleep_ns("s3_put_bytes_rate_limit_sleep_ns");
bvar::Adder<int64_t> s3_put_bytes_rate_limit_sleep_count("s3_put_bytes_rate_limit_sleep_count");

namespace {

std::function<void(int64_t)> bytes_rate_limiter_metric_func(S3RateLimitType type) {
    switch (type) {
    case S3RateLimitType::GET:
        return metric_func_factory(s3_get_bytes_rate_limit_sleep_ns,
                                   s3_get_bytes_rate_limit_sleep_count);
    case S3RateLimitType::PUT:
        return metric_func_factory(s3_put_bytes_rate_limit_sleep_ns,
                                   s3_put_bytes_rate_limit_sleep_count);
    default:
        return [](int64_t) {};
    }
}

// min(per_core * cores, cap) with overflow protection; cap == 0 means no cap.
int64_t cap_multiply(int64_t per_core, int64_t cores, int64_t cap) {
    cap = cap > 0 ? cap : std::numeric_limits<int64_t>::max();
    if (per_core > cap / cores) {
        return cap;
    }
    return per_core * cores;
}

size_t index_of(S3RateLimitType type) {
    CHECK(type == S3RateLimitType::GET || type == S3RateLimitType::PUT) << to_string(type);
    return static_cast<size_t>(type);
}

} // namespace

S3EffectiveRateLimit resolve_s3_rate_limit(S3RateLimitType type, int64_t cores) {
    const bool is_get = type == S3RateLimitType::GET;
    const int64_t qps_per_core = is_get ? config::s3_get_qps_per_core : config::s3_put_qps_per_core;
    const int64_t qps_max = is_get ? config::s3_get_qps_max : config::s3_put_qps_max;
    const int64_t bytes_per_core = is_get ? config::s3_get_bytes_per_second_per_core
                                          : config::s3_put_bytes_per_second_per_core;
    const int64_t bytes_max =
            is_get ? config::s3_get_bytes_per_second_max : config::s3_put_bytes_per_second_max;
    cores = std::max<int64_t>(1, cores);

    S3EffectiveRateLimit limit;
    if (qps_per_core < 0) {
        // Unset: the legacy absolute configs stay in charge, bit-for-bit compatible.
        limit.qps = is_get ? config::s3_get_token_per_second : config::s3_put_token_per_second;
        limit.burst = is_get ? config::s3_get_bucket_tokens : config::s3_put_bucket_tokens;
        limit.count_limit = is_get ? config::s3_get_token_limit : config::s3_put_token_limit;
    } else if (qps_per_core > 0) {
        limit.qps = cap_multiply(qps_per_core, cores, qps_max);
        limit.burst = limit.qps; // burst = 1 second worth of quota
    }                            // qps_per_core == 0: QPS limiting disabled, all fields stay 0.

    if (bytes_per_core > 0) {
        limit.bytes_per_second = cap_multiply(bytes_per_core, cores, bytes_max);
    }
    return limit;
}

int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst, size_t limit) {
    if (type == S3RateLimitType::UNKNOWN) {
        return -1;
    }
    return S3RateLimiterManager::instance().qps_limiter(type)->reset(max_speed, max_burst, limit);
}

int64_t s3_rate_limiter_cpu_cores() {
    if (int64_t overridden = config::s3_rate_limiter_cpu_cores; overridden > 0) {
        return overridden;
    }
    int physical = static_cast<int>(std::thread::hardware_concurrency());
    // Re-read the cgroup quota on every call: serverless BEs can be resized in place,
    // and the daemon refresh thread picks the change up through here.
    int limited = CGroupUtil::get_cgroup_limited_cpu_number(physical);
    return std::max(1, limited);
}

S3RateLimiterManager::S3RateLimiterManager() {
    const int64_t cores = s3_rate_limiter_cpu_cores();
    for (auto type : {S3RateLimitType::GET, S3RateLimitType::PUT}) {
        auto limit = resolve_s3_rate_limit(type, cores);
        _qps_limiters[index_of(type)] = std::make_unique<S3RateLimiterHolder>(
                limit.qps, limit.burst, limit.count_limit, s3_rate_limiter_metric_func(type));
        _bytes_limiters[index_of(type)] = std::make_unique<S3RateLimiterHolder>(
                limit.bytes_per_second, limit.bytes_per_second, limit.bytes_per_second,
                bytes_rate_limiter_metric_func(type));
    }
}

S3RateLimiterManager& S3RateLimiterManager::instance() {
    static S3RateLimiterManager ret;
    return ret;
}

S3RateLimiterHolder* S3RateLimiterManager::qps_limiter(S3RateLimitType type) {
    return _qps_limiters[index_of(type)].get();
}

S3RateLimiterHolder* S3RateLimiterManager::bytes_limiter(S3RateLimitType type) {
    return _bytes_limiters[index_of(type)].get();
}

void S3RateLimiterManager::refresh() {
    std::lock_guard guard(_refresh_lock);
    const int64_t cores = s3_rate_limiter_cpu_cores();
    for (auto type : {S3RateLimitType::GET, S3RateLimitType::PUT}) {
        const auto limit = resolve_s3_rate_limit(type, cores);

        auto* qps = qps_limiter(type);
        if (qps->get_max_speed() != static_cast<size_t>(limit.qps) ||
            qps->get_max_burst() != static_cast<size_t>(limit.burst) ||
            qps->get_limit() != static_cast<size_t>(limit.count_limit)) {
            qps->reset(limit.qps, limit.burst, limit.count_limit);
            LOG(INFO) << "reset S3 " << to_string(type) << " QPS rate limiter, qps=" << limit.qps
                      << ", burst=" << limit.burst << ", count_limit=" << limit.count_limit
                      << ", cores=" << cores;
        }

        auto* bytes = bytes_limiter(type);
        const auto bytes_per_second = static_cast<size_t>(limit.bytes_per_second);
        if (bytes->get_max_speed() != bytes_per_second ||
            bytes->get_max_burst() != bytes_per_second || bytes->get_limit() != bytes_per_second) {
            bytes->reset(bytes_per_second, bytes_per_second, bytes_per_second);
            LOG(INFO) << "reset S3 " << to_string(type)
                      << " bytes rate limiter, bytes_per_second=" << limit.bytes_per_second
                      << ", cores=" << cores;
        }
    }
}

S3RateLimitGuard::S3RateLimitGuard(S3RateLimitType type, size_t estimated_bytes) {
    if (!config::enable_s3_rate_limiter) {
        return;
    }
    auto& mgr = S3RateLimiterManager::instance();

    auto* qps = mgr.qps_limiter(type);
    if (qps->is_enabled() &&
        apply_s3_rate_limit(type, qps, config::s3_rate_limiter_log_interval) < 0) {
        _ok = false;
        return;
    }

    if (estimated_bytes == 0) {
        return;
    }
    auto* bytes = mgr.bytes_limiter(type);
    if (!bytes->is_enabled()) {
        return;
    }
    // Clamp the reservation to 1 second worth of bandwidth so a single oversized IO
    // (e.g. a whole-file read_at) cannot create unbounded upfront debt. The clamped
    // remainder is intentionally not accounted; effective quotas below the single-IO
    // upper bound are excluded by the config contract (see config.cpp).
    _reserved = std::min(estimated_bytes, bytes->get_max_speed());
    if (_reserved > 0) {
        // Pin the admitted bucket generation for settlement and count release.
        _charged_bucket = bytes->charge(_reserved);
        if (_charged_bucket == nullptr) {
            _ok = false;
        }
    }
}

S3RateLimitGuard::~S3RateLimitGuard() {
    if (_charged_bucket != nullptr) {
        _charged_bucket->refund_count(_reserved);
    }
}

void S3RateLimitGuard::settle(size_t actual_bytes) {
    if (_settled) {
        return;
    }
    _settled = true;
    if (_charged_bucket != nullptr && _reserved > actual_bytes) {
        _charged_bucket->refund_tokens(_reserved - actual_bytes);
    }
}

} // namespace doris
