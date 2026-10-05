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

#include "storage/disk_health_check_watchdog.h"

#include <unistd.h>

#include <ctime>
#include <limits>

#include "common/check.h"
#include "common/status.h"
#include "util/time.h"

namespace doris {

static_assert(std::atomic<int64_t>::is_always_lock_free);
static_assert(std::atomic<bool>::is_always_lock_free);

DiskHealthCheckWatchdog::~DiskHealthCheckWatchdog() {
    stop();
}

Status DiskHealthCheckWatchdog::start(std::chrono::milliseconds timeout) {
    // NOLINTNEXTLINE(readability-inconsistent-ifelse-braces): DORIS_CHECK expands to if/else.
    DORIS_CHECK(!_started && !_stopped.load(std::memory_order_acquire));
    if (timeout.count() < 0 || timeout.count() > std::numeric_limits<int32_t>::max() * 1000LL) {
        return Status::InvalidArgument("Invalid disk health check timeout: {} ms", timeout.count());
    }
    _timeout_ms = timeout.count();
    if (_timeout_ms == 0) {
        return Status::OK();
    }
    // Unlike ordinary Doris workers, this thread must work before ExecEnv is
    // initialized and cannot depend on ThreadMgr locks or memory-tracker setup.
    // Its callback neither allocates nor logs and needs no Doris thread context.
    int err = pthread_create(
            &_thread, nullptr,
            [](void* arg) -> void* {
                static_cast<DiskHealthCheckWatchdog*>(arg)->_run();
                return nullptr;
            },
            this);
    if (err != 0) {
        _timeout_ms = 0;
        return Status::RuntimeError("Cannot start disk health check watchdog, pthread error {}",
                                    err);
    }
    _started = true;
    return Status::OK();
}

void DiskHealthCheckWatchdog::stop() {
    if (_started) {
        _stopped.store(true, std::memory_order_release);
        int err = pthread_join(_thread, nullptr);
        DORIS_CHECK_EQ(err, 0);
        _started = false;
    }
}

DiskHealthCheckWatchdog::ScopedCheck::ScopedCheck(DiskHealthCheckWatchdog& watchdog)
        : _watchdog(watchdog), _deadline(watchdog._begin_check()) {}

DiskHealthCheckWatchdog::ScopedCheck::~ScopedCheck() {
    _watchdog._end_check(_deadline);
}

int64_t DiskHealthCheckWatchdog::_begin_check() {
    if (_timeout_ms == 0) {
        return 0;
    }
    const int64_t deadline = MonotonicMillis() + _timeout_ms;
    int64_t idle = 0;
    const bool armed = _deadline.compare_exchange_strong(idle, deadline, std::memory_order_acq_rel);
    if (!armed && idle == kTimedOut) {
        // The poller may have claimed the previous check and then been
        // descheduled before _exit(). Do not enter logging/exception handling
        // when this producer completes and advances to its next check.
        _exit(kTimeoutExitCode);
    }
    // Checks from this producer must not overlap.
    // NOLINTNEXTLINE(readability-inconsistent-ifelse-braces): DORIS_CHECK expands to if/else.
    DORIS_CHECK(armed);
    return deadline;
}

void DiskHealthCheckWatchdog::_end_check(int64_t deadline) {
    if (deadline != 0) {
        // Never clear the terminal state after the watchdog has claimed a timeout.
        _deadline.compare_exchange_strong(deadline, 0, std::memory_order_acq_rel);
    }
}

bool DiskHealthCheckWatchdog::_claim_timeout(int64_t deadline, int64_t now) {
    return deadline > 0 && now >= deadline &&
           _deadline.compare_exchange_strong(deadline, kTimedOut, std::memory_order_acq_rel);
}

void DiskHealthCheckWatchdog::_run() {
    while (!_stopped.load(std::memory_order_acquire)) {
        const int64_t deadline = _deadline.load(std::memory_order_acquire);
        if (_claim_timeout(deadline, MonotonicMillis())) {
            // Do not log, flush, invoke fatal-signal handlers, destroy objects or
            // join threads here: any of those can wait on the failed disk.
            _exit(kTimeoutExitCode);
        }
        const timespec interval {.tv_sec = 0, .tv_nsec = 100000000};
        // An interrupted sleep only makes the next check earlier.
        nanosleep(&interval, nullptr);
    }
}

} // namespace doris
