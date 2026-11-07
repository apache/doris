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
#include <limits>

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
    const auto index = bucket_index(bytes);
    for (auto& candidate : _buckets) {
        // Only seen buckets age; saturation prevents an idle bucket from wrapping into activity.
        if (candidate.probe_wait_requests != 0 &&
            candidate.idle_requests != std::numeric_limits<size_t>::max()) {
            ++candidate.idle_requests;
        }
    }
    auto& bucket = _buckets[index];
    // With 64 small hits then one large hit, a fixed 64-hit expiry steals the large bucket's
    // turn just before it returns. Retain its observed gap until it gets a probe; a short
    // burst must not erase the longer wait either.
    bucket.probe_wait_requests =
            std::max({bucket.probe_wait_requests, bucket.idle_requests, PROBE_REQUEST_INTERVAL});
    bucket.idle_requests = 0;
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
    // Below the 5 ms floor, remote observations cannot enable bypass. Probing a 100 us local
    // hit through a 20 ms remote path would only increase healthy-cache tail latency.
    if (bucket.local.count == WINDOW_SIZE && decision.local_latency_us >= MIN_LOCAL_LATENCY_US &&
        !_probe_inflight && now_us >= _next_probe_us &&
        _requests_since_probe == PROBE_REQUEST_INTERVAL) {
        // With 63 small hits followed by one large hit, granting every 64th request would
        // probe only the large bucket forever. Rotate among buckets within their observed
        // request gaps, retaining quota for the selected bucket's next request. A bucket that
        // stays absent beyond its gap expires without blocking the remaining traffic forever.
        // Apply the same latency floor here: a fast 4 KiB bucket must not reserve a slow
        // 1 MiB bucket's probe turn when it cannot use that turn itself.
        auto probe_bucket = _next_probe_bucket;
        for (size_t visited = 0; visited < BUCKET_COUNT; ++visited) {
            const auto& candidate = _buckets[probe_bucket];
            if (candidate.local.count == WINDOW_SIZE &&
                candidate.local.average() >= MIN_LOCAL_LATENCY_US &&
                candidate.idle_requests < candidate.probe_wait_requests) {
                break;
            }
            probe_bucket = (probe_bucket + 1) % BUCKET_COUNT;
        }
        if (probe_bucket == index) {
            decision.local_probe = decision.bypass;
            decision.remote_probe = !decision.bypass;
            decision.bypass = !decision.bypass;
            _probe_inflight = true;
            _requests_since_probe = 0;
            _next_probe_us = now_us + PROBE_INTERVAL_US;
            _next_probe_bucket = (index + 1) % BUCKET_COUNT;
            // Reset after admission so an old sparse burst cannot pin later probe turns.
            bucket.probe_wait_requests = PROBE_REQUEST_INTERVAL;
        }
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
