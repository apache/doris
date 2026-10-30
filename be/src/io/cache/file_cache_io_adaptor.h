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
#include <limits>
#include <mutex>

namespace doris::io {

// Share recent external-read latencies across queries so short scans can reuse observations.
class FileCacheIOAdaptor {
public:
    struct Decision {
        bool bypass = false;
        bool local_feedback = false;
        bool remote_feedback = false;
        int64_t local_latency_us = 0;
        int64_t remote_latency_us = 0;
        bool read_remote() const { return bypass; }
    };

    static FileCacheIOAdaptor& instance();
    Decision select(double skip_read_factor = 1.0);
    void record_local(int64_t latency_us);
    void record_remote(int64_t latency_us);

    // Estimate service time per native cache block and damp size extrapolation with a ratio of two.
    // Normalize both sides because Doris can read a partial local block without a block buffer.
    static int64_t latency_per_block_us(size_t bytes, int64_t elapsed_ns, size_t block_size);

private:
    static constexpr size_t WINDOW_SIZE = 50;
    static constexpr int64_t MIN_LOCAL_LATENCY_US = 5000;
    static constexpr int64_t UPPER_MIN_LATENCY_US = 100'000'000;
    static constexpr uint64_t MINIMUM_LIFE_LIMIT = 500'000;
    struct Window {
        std::array<int64_t, WINDOW_SIZE> samples {};
        size_t count = 0;
        size_t next = 0;
        // Wide accumulation preserves the average even for unusually long, valid IO durations.
        __int128 sum = 0;
        int64_t minimum = std::numeric_limits<int64_t>::max();
        uint64_t minimum_life = 0;
        void record(int64_t latency_us);
        void append(int64_t latency_us);
        bool feedback();
        int64_t average() const;
    };
    std::mutex _mutex;
    Window _local;
    Window _remote;
};

} // namespace doris::io
