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
#include <chrono>
#include <memory>
#include <shared_mutex>

namespace doris::cloud {

enum class S3RateLimitType : int {
    GET = 0,
    PUT,
    UNKNOWN,
};

extern std::string to_string(S3RateLimitType type);
extern S3RateLimitType string_to_s3_rate_limit_type(std::string_view value);

class SpinLock {
public:
    SpinLock() = default;

    void lock() {
        while (_flag.test_and_set(std::memory_order_acq_rel)) {
            // Spin until we acquire the lock
        }
    }

    void unlock() { _flag.clear(std::memory_order_acq_rel); }

private:
    std::atomic_flag _flag = ATOMIC_FLAG_INIT;
};

class S3RateLimiter {
public:
    static constexpr size_t default_burst_seconds = 1;

    S3RateLimiter(size_t max_speed, size_t max_burst, size_t limit)
            : _max_speed(max_speed),
              _max_burst(max_burst),
              _limit(limit),
              _remain_tokens(max_burst) {}

    // Use `amount` remain_tokens, sleeps if required or throws exception on limit overflow.
    // Returns duration of sleep in nanoseconds (to distinguish sleeping on different kinds of S3RateLimiters for metrics)
    int64_t add(size_t amount);

private:
    std::pair<size_t, double> _update_remain_token(std::chrono::system_clock::time_point now,
                                                   size_t amount);
    size_t _count {0};
    const size_t _max_speed {0}; // in tokens per second. which indicates the QPS
    const size_t _max_burst {0}; // in tokens. which indicates the token bucket size
    const uint64_t _limit {0};   // 0 - not limited.
    SpinLock _mutex;
    // Amount of remain_tokens available in token bucket. Updated in `add` method.
    double _remain_tokens {0};
    std::chrono::system_clock::time_point _prev_ms; // Previous `add` call time (in nanoseconds).
};

class S3RateLimiterHolder {
public:
    S3RateLimiterHolder(S3RateLimitType type, size_t max_speed, size_t max_burst, size_t limit);

    int64_t add(size_t amount);

    int reset(size_t max_speed, size_t max_burst, size_t limit);

private:
    std::shared_mutex rate_limiter_rw_lock;
    std::unique_ptr<S3RateLimiter> rate_limiter;
    bvar::Adder<uint64_t> rate_limit_bvar;
};
} // namespace doris::cloud