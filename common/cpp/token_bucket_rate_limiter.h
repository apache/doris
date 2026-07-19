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
#include <bvar/bvar.h>

#include <atomic>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace doris {
enum class S3RateLimitType : int {
    GET = 0,
    PUT,
    UNKNOWN,
};

extern std::string to_string(S3RateLimitType type);
extern S3RateLimitType string_to_s3_rate_limit_type(std::string_view value);

inline auto metric_func_factory(bvar::Adder<int64_t>& sleep_ns_bvar,
                                bvar::Adder<int64_t>& sleep_count_bvar,
                                bvar::Adder<int64_t>* rejected_count_bvar = nullptr) {
    return [&, rejected_count_bvar](int64_t ns) {
        if (ns > 0) {
            sleep_ns_bvar << ns;
            sleep_count_bvar << 1;
        } else if (ns < 0 && rejected_count_bvar != nullptr) {
            *rejected_count_bvar << 1;
        }
    };
}

class TokenBucketRateLimiter {
public:
    static constexpr size_t default_burst_seconds = 1;

    TokenBucketRateLimiter(size_t max_speed, size_t max_burst, size_t limit);
    ~TokenBucketRateLimiter();

    // Use `amount` remain_tokens, sleeps if required or throws exception on limit overflow.
    // Returns duration of sleep in nanoseconds (to distinguish sleeping on different kinds of S3RateLimiters for metrics)
    int64_t add(size_t amount);

    // Return `amount` tokens to the bucket (capped at max_burst) and roll back the
    // cumulative counter. Used to reconcile a reservation with the actually consumed
    // amount, e.g. a short read at EOF.
    void refund(size_t amount);

    size_t get_max_speed() const { return _max_speed; }

    size_t get_max_burst() const { return _max_burst; }

    size_t get_limit() const { return _limit; }

private:
    std::pair<size_t, double> _update_remain_token(long now, size_t amount);
    size_t _count {0};
    const size_t _max_speed {0}; // in tokens per second. which indicates the QPS
    const size_t _max_burst {0}; // in tokens. which indicates the token bucket size
    const uint64_t _limit {0};   // 0 - not limited.
    class SimpleSpinLock;
    std::unique_ptr<SimpleSpinLock> _mutex;
    // Amount of remain_tokens available in token bucket. Updated in `add` method.
    double _remain_tokens {0};
    long _prev_ns_count {0}; // Previous `add` call time (in nanoseconds).
};

struct TokenBucketRateLimiterResult {
    int64_t sleep_duration;
    size_t max_speed;
    size_t max_burst;
    size_t limit;
};

class TokenBucketRateLimiterHolder {
public:
    TokenBucketRateLimiterHolder(size_t max_speed, size_t max_burst, size_t limit,
                                 std::function<void(int64_t)> metric_func);
    ~TokenBucketRateLimiterHolder();

    int64_t add(size_t amount);
    TokenBucketRateLimiterResult add_with_config(size_t amount);

    // Charge `amount` like add(), but return the limiter generation the tokens were
    // taken from. Callers that later refund a reservation must refund on the returned
    // object, so that a concurrent reset() cannot make the refund pollute a fresh
    // bucket that never saw the original charge.
    std::shared_ptr<TokenBucketRateLimiter> charge(size_t amount);

    int reset(size_t max_speed, size_t max_burst, size_t limit);

    // Whether the currently published limiter can throttle or reject at all
    // (max_speed > 0 or limit > 0). Lock-free fast path for callers that want to
    // skip disabled limiters.
    bool is_enabled() const { return _enabled.load(std::memory_order_acquire); }

    size_t get_max_speed() const;
    size_t get_max_burst() const;
    size_t get_limit() const;

private:
    mutable std::shared_mutex rate_limiter_rw_lock;
    std::shared_ptr<TokenBucketRateLimiter> rate_limiter;
    std::atomic<bool> _enabled;
    // Record the correspoding sleeping time(unit is ms)
    std::function<void(int64_t)> metric_func;
};

using S3RateLimiter = TokenBucketRateLimiter;
using S3RateLimiterHolder = TokenBucketRateLimiterHolder;

std::function<void(int64_t)> s3_rate_limiter_metric_func(S3RateLimitType type);
int64_t apply_s3_rate_limit(S3RateLimitType type, S3RateLimiterHolder* rate_limiter,
                            int64_t log_interval);
} // namespace doris
