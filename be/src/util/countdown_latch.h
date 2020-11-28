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

#include "common/logging.h"
#include "olap/olap_define.h"
#include "util/condition_variable.h"
#include "util/monotime.h"
#include "util/mutex.h"

namespace doris {

// This is a C++ implementation of the Java CountDownLatch
// class.
// See http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/CountDownLatch.html
class CountDownLatch {
public:
    // Initialize the latch with the given initial count.
    explicit CountDownLatch(int count) : cond_(&lock_), count_(count) {}

    // Decrement the count of this latch by 'amount'
    // If the new count is less than or equal to zero, then all waiting threads are woken up.
    // If the count is already zero, this has no effect.
    void count_down(int amount) {
        DCHECK_GE(amount, 0);
        MutexLock lock(&lock_);
        if (count_ == 0) {
            return;
        }

        if (amount >= count_) {
            count_ = 0;
        } else {
            count_ -= amount;
        }

        if (count_ == 0) {
            // Latch has triggered.
            cond_.notify_all();
        }
    }

    // Decrement the count of this latch.
    // If the new count is zero, then all waiting threads are woken up.
    // If the count is already zero, this has no effect.
    void count_down() { count_down(1); }

    // Wait until the count on the latch reaches zero.
    // If the count is already zero, this returns immediately.
    void wait() const {
        MutexLock lock(&lock_);
        while (count_ > 0) {
            cond_.wait();
        }
    }

    // Waits for the count on the latch to reach zero, or until 'until' time is reached.
    // Returns true if the count became zero, false otherwise.
    bool wait_until(const MonoTime& when) const {
        MutexLock lock(&lock_);
        while (count_ > 0) {
            if (!cond_.wait_until(when)) {
                return false;
            }
        }
        return true;
    }

    // Waits for the count on the latch to reach zero, or until 'delta' time elapses.
    // Returns true if the count became zero, false otherwise.
    bool wait_for(const MonoDelta& delta) const { return wait_until(MonoTime::Now() + delta); }

    // Reset the latch with the given count. This is equivalent to reconstructing
    // the latch. If 'count' is 0, and there are currently waiters, those waiters
    // will be triggered as if you counted down to 0.
    void reset(uint64_t count) {
        MutexLock lock(&lock_);
        count_ = count;
        if (count_ == 0) {
            // Awake any waiters if we reset to 0.
            cond_.notify_all();
        }
    }

    uint64_t count() const {
        MutexLock lock(&lock_);
        return count_;
    }

private:
    mutable Mutex lock_;
    ConditionVariable cond_;

    uint64_t count_;
    DISALLOW_COPY_AND_ASSIGN(CountDownLatch);
};

// Utility class which calls latch->CountDown() in its destructor.
class CountDownOnScopeExit {
public:
    explicit CountDownOnScopeExit(CountDownLatch* latch) : latch_(latch) {}
    ~CountDownOnScopeExit() { latch_->count_down(); }

private:
    DISALLOW_COPY_AND_ASSIGN(CountDownOnScopeExit);

    CountDownLatch* latch_;
};

} // namespace doris

#endif //DORIS_BE_SRC_UTIL_COUNTDOWN_LATCH_H
