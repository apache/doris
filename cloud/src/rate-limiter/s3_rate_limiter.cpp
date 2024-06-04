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

#include "s3_rate_limiter.h"

#include <fmt/format.h>

#include <chrono>
#include <thread>

namespace doris::cloud {
// Just 10^6.
static constexpr auto MS = 1000000UL;

std::pair<size_t, double> S3RateLimiter::_update_remain_token(
        std::chrono::system_clock::time_point now, size_t amount) {
    // Values obtained under lock to be checked after release
    size_t count_value;
    double tokens_value;
    {
        std::lock_guard<SpinLock> lock(_mutex);
        if (_max_speed) {
            double delta_seconds = static_cast<double>((now - _prev_ms).count()) / MS;
            _remain_tokens = std::min<double>(_remain_tokens + _max_speed * delta_seconds - amount,
                                              _max_burst);
        }
        _count += amount;
        count_value = _count;
        tokens_value = _remain_tokens;
        _prev_ms = now;
    }
    return {count_value, tokens_value};
}

int64_t S3RateLimiter::add(size_t amount) {
    // Values obtained under lock to be checked after release
    auto [count_value, tokens_value] =
            _update_remain_token(std::chrono::high_resolution_clock::now(), amount);

    if (_limit && count_value > _limit) {
        // CK would throw exception
        return -1;
    }

    // Wait unless there is positive amount of remain_tokens - throttling
    int64_t sleep_time_ms = 0;
    if (_max_speed && tokens_value < 0) {
        sleep_time_ms = static_cast<int64_t>(-tokens_value / _max_speed * MS);
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_time_ms));
    }

    return sleep_time_ms;
}

S3RateLimiterHolder::S3RateLimiterHolder(S3RateLimitType type, size_t max_speed, size_t max_burst,
                                         size_t limit)
        : rate_limiter(std::make_unique<S3RateLimiter>(max_speed, max_burst, limit)),
          rate_limit_bvar(bvar::Adder<uint64_t>(fmt::format("{}_rate_limit_ms", to_string(type)))) {
}

int64_t S3RateLimiterHolder::add(size_t amount) {
    int64_t sleep;
    {
        std::shared_lock read {rate_limiter_rw_lock};
        sleep = rate_limiter->add(amount);
    }
    if (sleep > 0) {
        rate_limit_bvar << sleep;
    }
    return sleep;
}

int S3RateLimiterHolder::reset(size_t max_speed, size_t max_burst, size_t limit) {
    {
        std::unique_lock write {rate_limiter_rw_lock};
        rate_limiter = std::make_unique<S3RateLimiter>(max_speed, max_burst, limit);
    }
    return 0;
}

std::string to_string(S3RateLimitType type) {
    switch (type) {
    case S3RateLimitType::GET:
        return "get";
    case S3RateLimitType::PUT:
        return "put";
    default:
        return std::to_string(static_cast<size_t>(type));
    }
}

S3RateLimitType string_to_s3_rate_limit_type(std::string_view value) {
    if (value == "get") {
        return S3RateLimitType::GET;
    } else if (value == "put") {
        return S3RateLimitType::PUT;
    }
    return S3RateLimitType::UNKNOWN;
}
} // namespace doris::cloud