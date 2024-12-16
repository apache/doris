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

#include "runtime/workload_management/io_throttle.h"

#include "util/defer_op.h"
#include "util/time.h"

namespace doris {

bool IOThrottle::acquire(int64_t block_timeout_ms) {
    if (_io_bytes_per_second_limit < 0) {
        return true;
    }

    std::unique_lock<std::mutex> w_lock(_mutex);
    int64_t current_time = GetCurrentTimeMicros();
    int64_t block_finish_time = block_timeout_ms <= 0 ? 0 : current_time + block_timeout_ms * 1000;

    while (current_time <= _next_io_time_micros) {
        if (block_finish_time > 0 && current_time >= block_finish_time) {
            return false;
        }
        wait_condition.wait_for(w_lock,
                                std::chrono::microseconds(_next_io_time_micros - current_time));
        current_time = GetCurrentTimeMicros();
    }
    return true;
}

bool IOThrottle::try_acquire() {
    if (_io_bytes_per_second_limit < 0) {
        return true;
    }
    std::unique_lock<std::mutex> w_lock(_mutex);
    return GetCurrentTimeMicros() > _next_io_time_micros;
}

void IOThrottle::update_next_io_time(int64_t io_bytes) {
    if (_io_bytes_per_second_limit <= 0 || io_bytes <= 0) {
        return;
    }
    int64_t read_bytes_per_second = _io_bytes_per_second_limit;
    {
        std::unique_lock<std::mutex> w_lock(_mutex);
        double io_bytes_float = static_cast<double>(io_bytes);
        double ret = (io_bytes_float / static_cast<double>(read_bytes_per_second)) *
                     static_cast<double>(MICROS_PER_SEC);
        int64_t current_time = GetCurrentTimeMicros();

        if (current_time > _next_io_time_micros) {
            _next_io_time_micros = current_time;
        }
        _next_io_time_micros += ret < 1 ? static_cast<int64_t>(1) : static_cast<int64_t>(ret);
    }
}

void IOThrottle::set_io_bytes_per_second(int64_t io_bytes_per_second) {
    _io_bytes_per_second_limit = io_bytes_per_second;
}

}; // namespace doris