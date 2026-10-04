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

#include "util/jni_scan_heap_gate.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "util/jni-util.h"
#include "util/time.h"

namespace doris {

namespace {

// How often a waiting reader looks again although nobody told it to: to see its query cancelled and
// its wait run out.
constexpr auto POLL_INTERVAL = std::chrono::milliseconds(100);
constexpr int64_t MB = 1024 * 1024;

// A share of the -Xmx the JVM was started with, read from the same options the JVM was created from
// - not measured, so nothing here calls into the JVM.
int64_t jvm_heap_budget() {
    const double budget = static_cast<double>(Jni::Util::get_max_jni_heap_memory_size()) *
                          config::jni_scanner_heap_budget_ratio;
    // A BE_TEST build reports an unlimited heap (SIZE_MAX), which stays unlimited here.
    constexpr auto UNLIMITED = std::numeric_limits<int64_t>::max();
    return budget >= static_cast<double>(UNLIMITED) ? UNLIMITED : static_cast<int64_t>(budget);
}

} // namespace

void JniScanHeapGate::Permit::release() {
    if (_gate != nullptr) {
        _gate->_release(_bytes);
        _gate = nullptr;
        _bytes = 0;
    }
}

JniScanHeapGate::JniScanHeapGate(std::function<int64_t()> budget) : _budget(std::move(budget)) {}

JniScanHeapGate* JniScanHeapGate::instance() {
    // Never destroyed: BE's exit runs static destructors while scanner threads are still alive, and
    // a reader closing then releases its permit into this gate.
    static auto* gate = new JniScanHeapGate(jvm_heap_budget);
    return gate;
}

void JniScanHeapGate::acquire(int64_t bytes, const std::function<bool()>& stop_waiting,
                              Permit* permit, int64_t* wait_ns) {
    DORIS_CHECK(bytes > 0);
    DORIS_CHECK(permit != nullptr);
    DORIS_CHECK(!permit->held());
    DORIS_CHECK(wait_ns != nullptr);
    const int64_t start = MonotonicNanos();
    std::unique_lock lock(_lock);
    const uint64_t ticket = _next_ticket++;
    _waiting.push_back(ticket);
    while (true) {
        // Asked without the lock: it is the caller's code.
        lock.unlock();
        const bool stop = stop_waiting();
        lock.lock();
        const int64_t waited = MonotonicNanos() - start;
        bool admit = stop || _fits(ticket, bytes);
        if (!admit && waited >= config::jni_scanner_heap_max_wait_ms * 1000 * 1000) {
            LOG_EVERY_T(WARNING, 10) << "A JNI scanner opens after waiting " << waited / 1000 / 1000
                                     << " ms for its share of the JVM heap, longer than "
                                        "jni_scanner_heap_max_wait_ms: it declared "
                                     << bytes / MB << " MB, while " << _holders << " scanners hold "
                                     << _admitted_bytes / MB << " MB of a " << _budget() / MB
                                     << " MB budget and " << _waiting.size() - 1 << " others wait";
            admit = true;
        }
        if (admit) {
            _waiting.erase(std::find(_waiting.begin(), _waiting.end(), ticket));
            _admitted_bytes += bytes;
            ++_holders;
            permit->_gate = this;
            permit->_bytes = bytes;
            *wait_ns = waited;
            // Whoever is first in line now may fit.
            _cv.notify_all();
            return;
        }
        _cv.wait_for(lock, POLL_INTERVAL);
    }
}

bool JniScanHeapGate::_fits(uint64_t ticket, int64_t bytes) const {
    if (_waiting.front() != ticket) {
        return false;
    }
    if (_holders == 0) {
        // Nobody would give a share back for this reader, so waiting cannot help it.
        return true;
    }
    return _admitted_bytes + bytes <= _budget();
}

void JniScanHeapGate::_release(int64_t bytes) {
    std::lock_guard lock(_lock);
    DORIS_CHECK(_holders > 0);
    DORIS_CHECK(_admitted_bytes >= bytes);
    --_holders;
    _admitted_bytes -= bytes;
    _cv.notify_all();
}

int64_t JniScanHeapGate::admitted_bytes() const {
    std::lock_guard lock(_lock);
    return _admitted_bytes;
}

int64_t JniScanHeapGate::holders() const {
    std::lock_guard lock(_lock);
    return _holders;
}

int64_t JniScanHeapGate::waiters() const {
    std::lock_guard lock(_lock);
    return static_cast<int64_t>(_waiting.size());
}

} // namespace doris
