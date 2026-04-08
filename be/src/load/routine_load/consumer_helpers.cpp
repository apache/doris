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

#include "load/routine_load/consumer_helpers.h"

#include <algorithm>
#include <thread>

#include "common/metrics/doris_metrics.h"

namespace doris {

// ConsumerMetrics implementation
void ConsumerMetrics::track_get_msg(int64_t latency_us) {
    _get_msg_count++;
    DorisMetrics::instance()->routine_load_get_msg_count->increment(1);
    DorisMetrics::instance()->routine_load_get_msg_latency->increment(latency_us / 1000);
}

void ConsumerMetrics::track_consume_bytes(int64_t bytes) {
    _consume_bytes += bytes;
    DorisMetrics::instance()->routine_load_consume_bytes->increment(bytes);
}

void ConsumerMetrics::track_consume_rows(int64_t rows) {
    _consume_rows += rows;
    DorisMetrics::instance()->routine_load_consume_rows->increment(rows);
}

// RetryPolicy implementation
void RetryPolicy::retry_with_backoff() {
    _retry_count++;
    if (_retry_count <= _max_retries) {
        std::this_thread::sleep_for(std::chrono::milliseconds(_backoff_ms));
    }
}

// ThrottleBackoff implementation
void ThrottleBackoff::backoff_and_sleep() {
    _throttle_count++;
    // Exponential backoff: initial_ms * 2^(count-1), capped at max_ms
    int backoff_ms = std::min(_initial_backoff_ms * (1 << std::min(_throttle_count - 1, 3)),
                              _max_backoff_ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
}

} // namespace doris
