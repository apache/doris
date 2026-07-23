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

#include <functional>
#include <memory>
#include <shared_mutex>

namespace doris {
class TokenBucketRateLimiter {
public:
    static constexpr size_t default_burst_seconds = 1;

    TokenBucketRateLimiter(size_t max_speed, size_t max_burst, size_t limit);
    ~TokenBucketRateLimiter();

    // Use `amount` remain_tokens, sleeps if required or throws exception on limit overflow.
    // Returns duration of sleep in nanoseconds for metrics.
    int64_t add(size_t amount);

    size_t get_max_speed() const { return _max_speed; }

    size_t get_max_burst() const { return _max_burst; }

    size_t get_limit() const { return _limit; }

private:
    std::pair<size_t, double> _update_remain_token(long now, size_t amount);
    size_t _count {0};
    const size_t _max_speed {0};
    const size_t _max_burst {0};
    const uint64_t _limit {0};
    class SimpleSpinLock;
    std::unique_ptr<SimpleSpinLock> _mutex;
    double _remain_tokens {0};
    long _prev_ns_count {0};
};

class TokenBucketRateLimiterHolder {
public:
    TokenBucketRateLimiterHolder(size_t max_speed, size_t max_burst, size_t limit,
                                 std::function<void(int64_t)> metric_func);
    ~TokenBucketRateLimiterHolder();

    int64_t add(size_t amount);

    int reset(size_t max_speed, size_t max_burst, size_t limit);

    size_t get_max_speed() const { return rate_limiter->get_max_speed(); }

    size_t get_max_burst() const { return rate_limiter->get_max_burst(); }

    size_t get_limit() const { return rate_limiter->get_limit(); }

private:
    std::shared_mutex rate_limiter_rw_lock;
    std::unique_ptr<TokenBucketRateLimiter> rate_limiter;
    std::function<void(int64_t)> metric_func;
};
} // namespace doris
