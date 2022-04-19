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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/spinlock.h
// and modified by Doris

#ifndef DORIS_BE_SRC_UTIL_SPINLOCK_H
#define DORIS_BE_SRC_UTIL_SPINLOCK_H

#include <atomic>

#include "common/logging.h"

namespace doris {

// Lightweight spinlock.
class SpinLock {
public:
    SpinLock() : _locked(false) {
        // do nothing
    }

    // Acquires the lock, spins until the lock becomes available
    void lock() {
        for (int spin_count = 0; !try_lock(); ++spin_count) {
            if (spin_count < NUM_SPIN_CYCLES) {
#if (defined(__i386) || defined(__x86_64__))
                asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
                asm volatile("yield\n" ::: "memory");
#endif
            } else {
                sched_yield();
                spin_count = 0;
            }
        }
    }

    void unlock() { _locked.clear(std::memory_order_release); }

    // Tries to acquire the lock
    bool try_lock() { return !_locked.test_and_set(std::memory_order_acquire); }

private:
    static const int NUM_SPIN_CYCLES = 70;
    std::atomic_flag _locked;
};

} // end namespace doris

#endif // DORIS_BE_SRC_UTIL_SPINLOCK_H
