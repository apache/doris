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
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace doris::io {

// Queries using the same cache path share measured service times, grouped by request size.
class FileCacheIOAdaptor {
public:
    struct Decision {
        bool bypass = false;
        bool local_probe = false;
        bool remote_probe = false;
        int64_t local_latency_us = 0;
        int64_t remote_latency_us = 0;
        bool read_remote() const { return bypass; }
        bool is_probe() const { return local_probe || remote_probe; }
    };

    Decision select(size_t bytes, double skip_read_factor = 1.0);
    void finish_probe();
    void record_local(size_t bytes, int64_t latency_us);
    void record_remote(size_t bytes, int64_t latency_us);

private:
    static constexpr size_t WINDOW_SIZE = 50;
    static constexpr int64_t MIN_LOCAL_LATENCY_US = 5000;
    static constexpr int64_t PROBE_INTERVAL_US = 1'000'000;
    static constexpr size_t PROBE_REQUEST_INTERVAL = 64;
    static constexpr std::array<size_t, 7> SIZE_LIMITS = {
            4096, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 16 * 1024 * 1024};
    static constexpr size_t BUCKET_COUNT = SIZE_LIMITS.size() + 1;

    struct Window {
        std::array<int64_t, WINDOW_SIZE> samples {};
        size_t count = 0;
        size_t next = 0;
        // Wide accumulation preserves the average even for unusually long, valid IO durations.
        __int128 sum = 0;
        void record(int64_t latency_us);
        int64_t average() const;
    };
    struct Bucket {
        Window local;
        Window remote;
    };
    static size_t bucket_index(size_t bytes);
    Decision _select(size_t bytes, double skip_read_factor, int64_t now_us);

    std::mutex _mutex;
    std::array<Bucket, BUCKET_COUNT> _buckets;
    // Probe limits span all size buckets: adding request sizes must not multiply disk pressure.
    size_t _requests_since_probe = 0;
    int64_t _next_probe_us = 0;
    bool _probe_inflight = false;
};

} // namespace doris::io
