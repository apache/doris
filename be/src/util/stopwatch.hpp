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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/stopwatch.hpp
// and modified by Doris

#pragma once

#include <time.h>

#include <ctime>
#include <sstream>
#include <thread>

#include "util/time.h"

namespace doris {

// Stop watch for reporting elapsed time in nanosec based on CLOCK_MONOTONIC.
// It is as fast as Rdtsc.
// It is also accurate because it not affected by cpu frequency changes and
// it is not affected by user setting the system clock.
// CLOCK_MONOTONIC represents monotonic time since some unspecified starting point.
// It is good for computing elapsed time.
template <clockid_t Clock>
class CustomStopWatch {
public:
    CustomStopWatch(bool auto_start = false) {
        _total_time = 0;
        _running = false;
        if (auto_start) {
            start();
        }
    }

    timespec start_time() const { return _start; }

    void start() {
        if (!_running) {
            _record_start_thread_id();
            clock_gettime(Clock, &_start);
            _running = true;
        }
    }

    void stop() {
        if (_running) {
            _total_time += elapsed_time();
            _running = false;
        }
    }

    // Restarts the timer. Returns the elapsed time until this point.
    int64_t reset() {
        int64_t ret = elapsed_time();

        if (_running) {
            _record_start_thread_id();
            clock_gettime(Clock, &_start);
        }

        return ret;
    }

    // Returns time in nanosecond.
    // Clamped to 0 to guard against rare CLOCK_MONOTONIC rollbacks.
    int64_t elapsed_time() const {
        if (!_running) {
            return _total_time;
        }

        _check_thread_id();
        timespec end;
        clock_gettime(Clock, &end);
        auto start_nanos = _start.tv_sec * NANOS_PER_SEC + _start.tv_nsec;
        auto end_nanos = end.tv_sec * NANOS_PER_SEC + end.tv_nsec;
        if (end_nanos < start_nanos) {
            LOG(INFO) << "WARNING: time went backwards from " << start_nanos << " to " << end_nanos;
            return 0;
        }
        return end_nanos - start_nanos;
    }

    // Return time in microseconds
    int64_t elapsed_time_microseconds() const { return elapsed_time() / 1000; }

    // Return time in milliseconds
    int64_t elapsed_time_milliseconds() const { return elapsed_time() / 1000 / 1000; }

    // Returns time in seconds.
    // Clamped to 0 to guard against rare CLOCK_MONOTONIC rollbacks.
    int64_t elapsed_time_seconds(timespec end) const {
        if (!_running) {
            return _total_time / 1000L / 1000L / 1000L;
        }
        if (end.tv_sec < _start.tv_sec) {
            auto start_nanos = _start.tv_sec * NANOS_PER_SEC + _start.tv_nsec;
            auto end_nanos = end.tv_sec * NANOS_PER_SEC + end.tv_nsec;
            LOG(INFO) << "WARNING: time went backwards from " << start_nanos << " to " << end_nanos;
            return 0;
        }
        return end.tv_sec - _start.tv_sec;
    }

private:
    static std::string _get_thread_id() {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        return ss.str();
    }
    void _record_start_thread_id() {
#ifndef NDEBUG
        if constexpr (Clock == CLOCK_THREAD_CPUTIME_ID) {
            // CLOCK_THREAD_CPUTIME_ID is not supported on some platforms, e.g. macOS.
            // So that we need to check it at runtime and fallback to CLOCK_MONOTONIC if it is not supported.
            _start_thread_id = _get_thread_id();
        }
#endif
    }

    void _check_thread_id() const {
#ifndef NDEBUG
        if constexpr (Clock == CLOCK_THREAD_CPUTIME_ID) {
            auto current_thread_id = _get_thread_id();
            if (current_thread_id != _start_thread_id) {
                LOG(WARNING) << "StopWatch started in thread " << _start_thread_id
                             << " but stopped in thread " << current_thread_id;
            }
        }
#endif
    }

    timespec _start;
    int64_t _total_time; // in nanosec
    bool _running;
#ifndef NDEBUG
    std::string _start_thread_id;
#endif
};

// Stop watch for reporting elapsed time in nanosec based on CLOCK_MONOTONIC.
// It is as fast as Rdtsc.
// It is also accurate because it not affected by cpu frequency changes and
// it is not affected by user setting the system clock.
// CLOCK_MONOTONIC represents monotonic time since some unspecified starting point.
// It is good for computing elapsed time.
using MonotonicStopWatch = CustomStopWatch<CLOCK_MONOTONIC>;

// Stop watch for reporting elapsed nanosec based on CLOCK_THREAD_CPUTIME_ID.
using ThreadCpuStopWatch = CustomStopWatch<CLOCK_THREAD_CPUTIME_ID>;

} // namespace doris
