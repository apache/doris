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

#include "io/cache/file_cache_io_adaptor.h"

#include <algorithm>

#include "cpp/sync_point.h"
#include "util/time.h"

namespace doris::io {

size_t FileCacheIOAdaptor::bucket_index(size_t bytes) {
    return static_cast<size_t>(std::ranges::lower_bound(SIZE_LIMITS, bytes) - SIZE_LIMITS.begin());
}

void FileCacheIOAdaptor::Window::record(int64_t latency_us) {
    // Failed/invalid timings must not train the controller; a sub-microsecond read may be zero.
    if (latency_us < 0) {
        return;
    }
    if (count == WINDOW_SIZE) {
        sum -= samples[next];
    } else {
        ++count;
    }
    samples[next] = latency_us;
    sum += latency_us;
    next = (next + 1) % WINDOW_SIZE;
}

int64_t FileCacheIOAdaptor::Window::average() const {
    return count == 0 ? 0 : static_cast<int64_t>(sum / count);
}

FileCacheIOAdaptor::Decision FileCacheIOAdaptor::select(size_t bytes, double skip_read_factor) {
    auto now_us = MonotonicMicros();
    TEST_SYNC_POINT_CALLBACK("FileCacheIOAdaptor::select:now_us", &now_us);
    return _select(bytes, skip_read_factor, now_us);
}

FileCacheIOAdaptor::Decision FileCacheIOAdaptor::_select(size_t bytes, double skip_read_factor,
                                                         int64_t now_us) {
    std::lock_guard lock(_mutex);
    const auto& bucket = _buckets[bucket_index(bytes)];
    Decision decision;
    decision.local_latency_us = bucket.local.average();
    decision.remote_latency_us = bucket.remote.average();
    // Compare actual service times only within a size bucket: a 4 KiB / 100 us local read
    // must not become a 12.8 ms full-block estimate and lose to an unrelated 1 MiB GET.
    if (bucket.local.count == WINDOW_SIZE && bucket.remote.count == WINDOW_SIZE &&
        decision.local_latency_us >= MIN_LOCAL_LATENCY_US) {
        decision.bypass = static_cast<double>(decision.local_latency_us) * skip_read_factor >
                          static_cast<double>(decision.remote_latency_us);
    }
    _requests_since_probe = std::min(_requests_since_probe + 1, PROBE_REQUEST_INTERVAL);
    // History cannot reveal an unobserved recovery (e.g. local 20 ms -> 100 us). Route a real
    // request through the other path, at most once per second and once per 64 eligible hits.
    // Keep one probe in flight across the entire cache path, including slow local disk reads.
    if (bucket.local.count == WINDOW_SIZE && !_probe_inflight && now_us >= _next_probe_us &&
        _requests_since_probe == PROBE_REQUEST_INTERVAL) {
        decision.local_probe = decision.bypass;
        decision.remote_probe = !decision.bypass;
        decision.bypass = !decision.bypass;
        _probe_inflight = true;
        _requests_since_probe = 0;
        _next_probe_us = now_us + PROBE_INTERVAL_US;
    }
    return decision;
}

void FileCacheIOAdaptor::finish_probe() {
    std::lock_guard lock(_mutex);
    _probe_inflight = false;
}

void FileCacheIOAdaptor::record_local(size_t bytes, int64_t latency_us) {
    std::lock_guard lock(_mutex);
    _buckets[bucket_index(bytes)].local.record(latency_us);
}

void FileCacheIOAdaptor::record_remote(size_t bytes, int64_t latency_us) {
    std::lock_guard lock(_mutex);
    _buckets[bucket_index(bytes)].remote.record(latency_us);
}

} // namespace doris::io
