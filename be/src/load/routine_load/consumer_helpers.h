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

#include <chrono>
#include <string>
#include <unordered_map>

namespace doris {

// Helper class for tracking metrics in data consumers
class ConsumerMetrics {
public:
    ConsumerMetrics() = default;

    // Track a single message fetch operation
    void track_get_msg(int64_t latency_us);

    // Track consumed bytes
    void track_consume_bytes(int64_t bytes);

    // Track consumed rows
    void track_consume_rows(int64_t rows);

    // Get accumulated metrics
    int64_t get_msg_count() const { return _get_msg_count; }
    int64_t consume_bytes() const { return _consume_bytes; }
    int64_t consume_rows() const { return _consume_rows; }

private:
    int64_t _get_msg_count = 0;
    int64_t _consume_bytes = 0;
    int64_t _consume_rows = 0;
};

// Helper class for retry logic with configurable backoff
class RetryPolicy {
public:
    explicit RetryPolicy(int max_retries = 3, int backoff_ms = 200)
            : _max_retries(max_retries), _backoff_ms(backoff_ms), _retry_count(0) {}

    // Check if should retry
    bool should_retry() const { return _retry_count < _max_retries; }

    // Increment retry count and sleep with backoff
    void retry_with_backoff();

    // Reset retry counter (call on success)
    void reset() { _retry_count = 0; }

    // Get current retry count
    int retry_count() const { return _retry_count; }

private:
    int _max_retries;
    int _backoff_ms;
    int _retry_count;
};

// Helper class for exponential backoff (used for throttling)
class ThrottleBackoff {
public:
    explicit ThrottleBackoff(int initial_backoff_ms = 1000, int max_backoff_ms = 10000)
            : _initial_backoff_ms(initial_backoff_ms),
              _max_backoff_ms(max_backoff_ms),
              _throttle_count(0) {}

    // Increment throttle count and sleep with exponential backoff
    void backoff_and_sleep();

    // Reset throttle counter (call on success)
    void reset() { _throttle_count = 0; }

    // Get current throttle count
    int throttle_count() const { return _throttle_count; }

private:
    int _initial_backoff_ms;
    int _max_backoff_ms;
    int _throttle_count;
};

// Helper class for comparing custom properties between consumers
class PropertyMatcher {
public:
    // Check if two property maps match exactly
    template <typename MapType1, typename MapType2>
    static bool properties_match(const MapType1& props1, const MapType2& props2) {
        if (props1.size() != props2.size()) {
            return false;
        }

        for (const auto& [key, value] : props1) {
            auto it = props2.find(key);
            if (it == props2.end() || it->second != value) {
                return false;
            }
        }

        return true;
    }
};

} // namespace doris
