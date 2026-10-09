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
#include "util/thread.h"
#include "util/time.h"

namespace doris {

namespace {

// How often the gate asks the waiting readers whether their scans stopped, and looks for those that
// waited too long, although nobody gave a share back.
constexpr int64_t POLL_INTERVAL_NS = 100L * 1000 * 1000;
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

JniScanHeapGate::Admission::Admission(JniScanHeapGate* gate, int64_t bytes,
                                      std::function<bool()> stop_waiting)
        : _gate(gate), _bytes(bytes), _stop_waiting(std::move(stop_waiting)) {}

JniScanHeapGate::Admission::~Admission() {
    _gate->_leave(this);
}

JniScanHeapGate::JniScanHeapGate(std::function<int64_t()> budget) : _budget(std::move(budget)) {
    CHECK(Thread::create(
                  "JniScanHeapGate", "jni_heap_gate", [this]() { _run(); }, &_thread)
                  .ok());
}

JniScanHeapGate::~JniScanHeapGate() {
    {
        std::lock_guard lock(_lock);
        // Every admission names its gate, so none may outlive it.
        DORIS_CHECK(_waiting.empty());
        DORIS_CHECK(_holders == 0);
        _stopping = true;
    }
    _cv.notify_all();
    _thread->join();
}

JniScanHeapGate* JniScanHeapGate::instance() {
    // Never destroyed: BE's exit runs static destructors while scanner threads are still alive, and
    // a reader closing then gives its share back to this gate.
    static auto* gate = new JniScanHeapGate(jvm_heap_budget);
    return gate;
}

std::unique_ptr<JniScanHeapGate::Admission> JniScanHeapGate::request(
        int64_t bytes, std::function<bool()> stop_waiting) {
    DORIS_CHECK(bytes > 0);
    std::unique_ptr<Admission> admission(new Admission(this, bytes, std::move(stop_waiting)));
    if (admission->_stop_waiting()) {
        // The reader's scan has stopped already: it will open nothing, so it takes no share.
        admission->_state.store(State::STOPPED, std::memory_order_release);
        admission->_future.set_value(Void {});
        return admission;
    }
    {
        std::lock_guard lock(_lock);
        admission->_ticket = _next_ticket++;
        admission->_start_ns = MonotonicNanos();
        _waiting.push_back(admission.get());
        _admit_locked(admission->_start_ns);
    }
    // The gate's thread completes what _admit_locked() settled, and polls a reader still in line.
    _cv.notify_all();
    return admission;
}

void JniScanHeapGate::_admit_locked(int64_t now) {
    while (!_waiting.empty()) {
        Admission* first = _waiting.front();
        // Nobody holding a share would give one back for this reader, so waiting cannot help it.
        if (_holders > 0 && _admitted_bytes + first->_bytes > _budget()) {
            return;
        }
        _waiting.pop_front();
        _settle_locked(first, State::ADMITTED, now);
    }
}

void JniScanHeapGate::_settle_locked(Admission* admission, State state, int64_t now) {
    DORIS_CHECK(admission->waiting());
    DORIS_CHECK(state != State::WAITING);
    admission->_wait_ns = now - admission->_start_ns;
    if (state == State::ADMITTED) {
        _admitted_bytes += admission->_bytes;
        ++_holders;
    }
    admission->_state.store(state, std::memory_order_release);
    _settled.push_back(admission->_future);
}

void JniScanHeapGate::_leave(Admission* admission) {
    {
        std::lock_guard lock(_lock);
        switch (admission->_state.load(std::memory_order_acquire)) {
        case State::WAITING: {
            // Its reader closed before its turn. Nobody will run the reader again, so its future
            // is left alone.
            auto it = std::find(_waiting.begin(), _waiting.end(), admission);
            DORIS_CHECK(it != _waiting.end());
            _waiting.erase(it);
            break;
        }
        case State::ADMITTED:
            DORIS_CHECK(_holders > 0);
            DORIS_CHECK(_admitted_bytes >= admission->_bytes);
            --_holders;
            _admitted_bytes -= admission->_bytes;
            break;
        case State::STOPPED:
            return;
        }
        // Whoever is first in line now may fit.
        _admit_locked(MonotonicNanos());
        if (_settled.empty()) {
            return;
        }
    }
    _cv.notify_all();
}

void JniScanHeapGate::_run() {
    std::unique_lock lock(_lock);
    int64_t last_poll = MonotonicNanos();
    while (true) {
        if (!_settled.empty()) {
            auto settled = std::exchange(_settled, {});
            // The futures' callbacks resume parked scans, which takes their contexts' locks.
            lock.unlock();
            for (auto& future : settled) {
                future.set_value(Void {});
            }
            lock.lock();
            continue;
        }
        if (_stopping) {
            return;
        }
        if (_waiting.empty()) {
            _cv.wait(lock);
            continue;
        }
        const int64_t since_poll = MonotonicNanos() - last_poll;
        if (since_poll >= POLL_INTERVAL_NS) {
            _poll(lock);
            last_poll = MonotonicNanos();
            continue;
        }
        _cv.wait_for(lock, std::chrono::nanoseconds(POLL_INTERVAL_NS - since_poll));
    }
}

void JniScanHeapGate::_poll(std::unique_lock<std::mutex>& lock) {
    // Asked without the lock: it is the readers' code. A reader can leave the line meanwhile, so
    // its answer is matched back by ticket.
    std::vector<std::pair<uint64_t, std::function<bool()>>> checks;
    checks.reserve(_waiting.size());
    for (const auto* admission : _waiting) {
        checks.emplace_back(admission->_ticket, admission->_stop_waiting);
    }
    lock.unlock();
    std::vector<uint64_t> stopped;
    for (const auto& [ticket, stop_waiting] : checks) {
        if (stop_waiting()) {
            stopped.push_back(ticket);
        }
    }
    lock.lock();

    const int64_t now = MonotonicNanos();
    const int64_t wait_limit_ns = config::jni_scanner_heap_max_wait_ms * 1000 * 1000;
    std::deque<Admission*> still_waiting;
    for (auto* admission : _waiting) {
        if (std::ranges::find(stopped, admission->_ticket) != stopped.end()) {
            // The reader's scan has stopped: it will open nothing, so it takes no share.
            _settle_locked(admission, State::STOPPED, now);
        } else if (now - admission->_start_ns >= wait_limit_ns) {
            LOG_EVERY_T(WARNING, 10)
                    << "A JNI scanner opens after waiting "
                    << (now - admission->_start_ns) / 1000 / 1000
                    << " ms for its share of the JVM heap, longer than "
                       "jni_scanner_heap_max_wait_ms: it declared "
                    << admission->_bytes / MB << " MB, while " << _holders << " scanners hold "
                    << _admitted_bytes / MB << " MB of a " << _budget() / MB << " MB budget and "
                    << _waiting.size() - 1 << " others wait";
            ++_admitted_after_wait_limit;
            _settle_locked(admission, State::ADMITTED, now);
        } else {
            still_waiting.push_back(admission);
        }
    }
    _waiting.swap(still_waiting);
    // The budget may have grown, or the first in line may have left it.
    _admit_locked(now);
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

int64_t JniScanHeapGate::admitted_after_wait_limit() const {
    std::lock_guard lock(_lock);
    return _admitted_after_wait_limit;
}

} // namespace doris
