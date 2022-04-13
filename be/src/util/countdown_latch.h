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

#ifndef DORIS_BE_SRC_UTIL_COUNTDOWN_LATCH_H
#define DORIS_BE_SRC_UTIL_COUNTDOWN_LATCH_H

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "common/logging.h"
#include "olap/olap_define.h"

namespace doris {

// This is a C++ implementation of the Java CountDownLatch
// class.
// See http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/CountDownLatch.html
class CountDownLatch {
public:
    // Initialize the latch with the given initial count.
    explicit CountDownLatch(int count) : _count(count) {}

    // Decrement the count of this latch by 'amount'
    // If the new count is less than or equal to zero, then all waiting threads are woken up.
    // If the count is already zero, this has no effect.
    void count_down(int amount) {
        DCHECK_GE(amount, 0);
        std::lock_guard<std::mutex> lock(_lock);
        if (_count == 0) {
            return;
        }

        if (amount >= _count) {
            _count = 0;
        } else {
            _count -= amount;
        }

        if (_count == 0) {
            // Latch has triggered.
            _cond.notify_all();
        }
    }

    // Decrement the count of this latch.
    // If the new count is zero, then all waiting threads are woken up.
    // If the count is already zero, this has no effect.
    void count_down() { count_down(1); }

    // Wait until the count on the latch reaches zero.
    // If the count is already zero, this returns immediately.
    void wait() {
        std::unique_lock<std::mutex> lock(_lock);
        while (_count > 0) {
            _cond.wait(lock);
        }
    }

    // Waits for the count on the latch to reach zero, or until 'delta' time elapses.
    // Returns true if the count became zero, false otherwise.
    template <class Rep, class Period>
    bool wait_for(const std::chrono::duration<Rep, Period>& delta) {
        std::unique_lock lock(_lock);
        return _cond.wait_for(lock, delta, [&]() { return _count <= 0; });
    }

    // Reset the latch with the given count. This is equivalent to reconstructing
    // the latch. If 'count' is 0, and there are currently waiters, those waiters
    // will be triggered as if you counted down to 0.
    void reset(uint64_t count) {
        std::lock_guard<std::mutex> lock(_lock);
        _count = count;
        if (_count == 0) {
            // Awake any waiters if we reset to 0.
            _cond.notify_all();
        }
    }

    uint64_t count() const {
        std::lock_guard<std::mutex> lock(_lock);
        return _count;
    }

private:
    mutable std::mutex _lock;
    mutable std::condition_variable _cond;

    uint64_t _count;

    CountDownLatch(const CountDownLatch&) = delete;
    void operator=(const CountDownLatch&) = delete;
};

// Utility class which calls latch->CountDown() in its destructor.
class CountDownOnScopeExit {
public:
    explicit CountDownOnScopeExit(CountDownLatch* latch) : _latch(latch) {}
    ~CountDownOnScopeExit() { _latch->count_down(); }

private:
    CountDownLatch* _latch;

    CountDownOnScopeExit(const CountDownOnScopeExit&) = delete;
    void operator=(const CountDownOnScopeExit&) = delete;
};

} // namespace doris

#endif //DORIS_BE_SRC_UTIL_COUNTDOWN_LATCH_H
