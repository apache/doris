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

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstdint>

namespace doris {

class Status;

// Supervises one serial producer of disk health checks. The producer must finish
// before stop()/destruction; in particular, keep this alive while joining it.
class DiskHealthCheckWatchdog {
public:
    // EX_IOERR: distinguish this fail-fast path without writing to a possibly
    // blocked log/stderr. The process supervisor can report this exit status.
    static constexpr int kTimeoutExitCode = 74;

    DiskHealthCheckWatchdog() = default;
    ~DiskHealthCheckWatchdog();
    DiskHealthCheckWatchdog(const DiskHealthCheckWatchdog&) = delete;
    DiskHealthCheckWatchdog& operator=(const DiskHealthCheckWatchdog&) = delete;

    // Called once, before any checks. Zero disables supervision. Not dynamic.
    Status start(std::chrono::milliseconds timeout);
    void stop();

    class ScopedCheck {
    public:
        explicit ScopedCheck(DiskHealthCheckWatchdog& watchdog);
        ~ScopedCheck();
        ScopedCheck(const ScopedCheck&) = delete;
        ScopedCheck& operator=(const ScopedCheck&) = delete;

    private:
        DiskHealthCheckWatchdog& _watchdog;
        const int64_t _deadline;
    };

private:
    int64_t _begin_check();
    void _end_check(int64_t deadline);
    bool _claim_timeout(int64_t deadline, int64_t now);
    void _run();

    static constexpr int64_t kTimedOut = -1;
    // 0 = idle, positive = monotonic deadline in milliseconds, -1 = terminal.
    // Completion and timeout race via CAS; whichever claims the check first wins.
    std::atomic<int64_t> _deadline {0};
    std::atomic<bool> _stopped {false};
    int64_t _timeout_ms = 0;
    pthread_t _thread {};
    bool _started = false;
};

} // namespace doris
