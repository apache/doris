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
    CustomStopWatch() {
        _total_time = 0;
        _running = false;
    }

    timespec start_time() const { return _start; }

    void start() {
        if (!_running) {
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
    uint64_t reset() {
        uint64_t ret = elapsed_time();

        if (_running) {
            clock_gettime(Clock, &_start);
        }

        return ret;
    }

    // Returns time in nanosecond.
    uint64_t elapsed_time() const {
        if (!_running) {
            return _total_time;
        }

        timespec end;
        clock_gettime(Clock, &end);
        return (end.tv_sec - _start.tv_sec) * 1000L * 1000L * 1000L +
               (end.tv_nsec - _start.tv_nsec);
    }

    // Returns time in nanosecond.
    int64_t elapsed_time_seconds(timespec end) const {
        if (!_running) {
            return _total_time / 1000L / 1000L / 1000L;
        }
        return end.tv_sec - _start.tv_sec;
    }

private:
    timespec _start;
    uint64_t _total_time; // in nanosec
    bool _running;
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
