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

namespace doris::io {

FileCacheIOAdaptor& FileCacheIOAdaptor::instance() {
    static FileCacheIOAdaptor adaptor;
    return adaptor;
}

void FileCacheIOAdaptor::Window::append(int64_t latency_us) {
    if (count == WINDOW_SIZE) {
        sum -= samples[next];
    } else {
        ++count;
    }
    samples[next] = latency_us;
    sum += latency_us;
    next = (next + 1) % WINDOW_SIZE;
}

void FileCacheIOAdaptor::Window::record(int64_t latency_us) {
    if (latency_us < 0) {
        return;
    }
    if (latency_us < minimum || minimum_life > MINIMUM_LIFE_LIMIT) {
        minimum = latency_us;
        minimum_life = 0;
    } else {
        ++minimum_life;
    }
    append(latency_us);
}

bool FileCacheIOAdaptor::Window::feedback() {
    if (minimum < 0 || minimum > UPPER_MIN_LATENCY_US) {
        return false;
    }
    // Feed the remembered minimum into the unused path to reconsider its estimated cost.
    // Only actual observations may advance the minimum's observation age.
    append(minimum);
    return true;
}

int64_t FileCacheIOAdaptor::Window::average() const {
    return count == 0 ? 0 : static_cast<int64_t>(sum / count);
}

FileCacheIOAdaptor::Decision FileCacheIOAdaptor::select(double skip_read_factor) {
    std::lock_guard lock(_mutex);
    Decision decision;
    decision.local_latency_us = _local.average();
    decision.remote_latency_us = _remote.average();
    if (_local.count < WINDOW_SIZE || _remote.count < WINDOW_SIZE ||
        decision.local_latency_us < MIN_LOCAL_LATENCY_US) {
        return decision;
    }
    // Preserve fractional factors when comparing the two average service times.
    decision.bypass = static_cast<double>(decision.local_latency_us) * skip_read_factor >
                      static_cast<double>(decision.remote_latency_us);
    if (decision.bypass) {
        decision.local_feedback = _local.feedback();
    } else {
        decision.remote_feedback = _remote.feedback();
    }
    return decision;
}

void FileCacheIOAdaptor::record_local(int64_t latency_us) {
    std::lock_guard lock(_mutex);
    _local.record(latency_us);
}

void FileCacheIOAdaptor::record_remote(int64_t latency_us) {
    std::lock_guard lock(_mutex);
    _remote.record(latency_us);
}

int64_t FileCacheIOAdaptor::latency_per_block_us(size_t bytes, int64_t elapsed_ns,
                                                 size_t block_size) {
    if (bytes == 0 || block_size == 0 || elapsed_ns < 0) {
        return -1;
    }
    const auto latency_us = static_cast<uint64_t>(elapsed_ns / 1000);
    const auto scaled = static_cast<unsigned __int128>(latency_us) * block_size / bytes;
    unsigned __int128 estimate = latency_us;
    if (bytes > block_size && bytes - block_size > block_size) {
        estimate = std::min(estimate, scaled * 2);
    } else if (bytes < block_size && bytes < block_size - bytes) {
        estimate = std::max(estimate, scaled / 2);
    }
    return static_cast<int64_t>(std::min(
            estimate, static_cast<unsigned __int128>(std::numeric_limits<int64_t>::max())));
}

} // namespace doris::io
