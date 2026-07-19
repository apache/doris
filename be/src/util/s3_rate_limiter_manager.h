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

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>

#include "cpp/token_bucket_rate_limiter.h"

namespace doris {

// The final rate limit values applied to the token buckets, resolved from all the
// s3_{get,put}_* configs plus the current CPU core count. All precedence rules
// (per-core vs legacy, caps, overflow guard) live in resolve_s3_rate_limit() only.
struct S3EffectiveRateLimit {
    int64_t qps = 0;              // QPS bucket rate; 0 = unlimited
    int64_t burst = 0;            // QPS bucket capacity
    int64_t count_limit = 0;      // cumulative request cap (legacy path only); 0 = unlimited
    int64_t bytes_per_second = 0; // bytes bucket rate; 0 = unlimited

    bool operator==(const S3EffectiveRateLimit&) const = default;
};

// Pure function of configs and `cores`; no side effects.
// s3_{get,put}_qps_per_core == -1 falls back to the legacy absolute token configs and
// `cores` does not participate; otherwise qps = min(per_core * cores, qps_max).
S3EffectiveRateLimit resolve_s3_rate_limit(S3RateLimitType type, int64_t cores);

// Cores used to derive effective limits: config::s3_rate_limiter_cpu_cores override
// (> 0) wins; otherwise re-read the cgroup cpu quota (serverless BEs can be resized
// in place), falling back to physical cores. Always >= 1.
int64_t s3_rate_limiter_cpu_cores();

// Directly reset the GET/PUT QPS bucket. Note that the daemon refresh thread will
// override a manual reset as soon as the config-resolved parameters differ.
int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst, size_t limit);

// Owns the 4 process-wide token buckets (GET/PUT x QPS/bytes). Independent of
// S3ClientFactory so that instantiating it never initializes the AWS SDK.
class S3RateLimiterManager {
public:
    static S3RateLimiterManager& instance();

    // Idempotent: re-resolve effective limits from the current configs and core count,
    // and reset only the buckets whose parameters actually changed. The comparison
    // baseline is each bucket's own parameters -- there is no shadow state to drift.
    // Called from the daemon refresh thread; safe to call from anywhere.
    void refresh();

    S3RateLimiterHolder* qps_limiter(S3RateLimitType type);
    S3RateLimiterHolder* bytes_limiter(S3RateLimitType type);

    S3RateLimiterManager(const S3RateLimiterManager&) = delete;
    S3RateLimiterManager& operator=(const S3RateLimiterManager&) = delete;

private:
    S3RateLimiterManager();

    std::mutex _refresh_lock;
    std::array<std::unique_ptr<S3RateLimiterHolder>, 2> _qps_limiters;
    std::array<std::unique_ptr<S3RateLimiterHolder>, 2> _bytes_limiters;
};

// RAII admission for one logical object storage request.
//
// The constructor charges the QPS bucket (may sleep when throttled; rejected only by
// the legacy token_limit cumulative cap) and then reserves `estimated_bytes` from the
// bytes bucket, clamped to at most 1 second worth of bandwidth so a single huge IO
// cannot create unbounded upfront debt (may sleep; never rejects).
//
// settle(actual) refunds the difference when the actual transferred bytes are smaller
// than the reservation (e.g. a short read at EOF). An unsettled guard keeps the full
// reservation charged, which is the conservative choice for failed requests.
//
// The guard pins the bucket generation it charged: if refresh() resets the bytes
// bucket while the request is in flight, settle() refunds on the old generation
// (kept alive by the guard's shared_ptr) instead of polluting the fresh bucket.
class S3RateLimitGuard {
public:
    S3RateLimitGuard(S3RateLimitType type, size_t estimated_bytes);
    ~S3RateLimitGuard() = default;

    S3RateLimitGuard(const S3RateLimitGuard&) = delete;
    S3RateLimitGuard& operator=(const S3RateLimitGuard&) = delete;

    bool ok() const { return _ok; }
    void settle(size_t actual_bytes);

private:
    size_t _reserved = 0;
    std::shared_ptr<TokenBucketRateLimiter> _charged_bucket;
    bool _ok = true;
    bool _settled = false;
};

} // namespace doris
