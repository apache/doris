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

#ifndef DORIS_BE_SRC_UTIL_SPINLOCK_H
#define DORIS_BE_SRC_UTIL_SPINLOCK_H

#include "common/atomic.h"
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
        if (!try_lock()) {
            slow_acquire();
        }
    }

    void unlock() {
        __sync_bool_compare_and_swap(&_locked, true, false);
    }

    // Tries to acquire the lock
    inline bool try_lock() { return __sync_bool_compare_and_swap(&_locked, false, true); }

    void dcheck_locked() { DCHECK(_locked); }

private:
    // Out-of-line definition of the actual spin loop. The primary goal is to have the
    // actual lock method as short as possible to avoid polluting the i-cache with
    // unnecessary instructions in the non-contested case.
    void slow_acquire();

    // In typical spin lock implements, we want to spin (and keep the core fully busy),
    // for some number of cycles before yielding. Consider these three cases:
    //  1) lock is un-contended - spinning doesn't kick in and has no effect.
    //  2) lock is taken by another thread and that thread finishes quickly.
    //  3) lock is taken by another thread and that thread is slow (e.g. scheduled away).
    //
    // In case 3), we'd want to yield so another thread can do work. This thread
    // won't be able to do anything useful until the thread with the lock runs again.
    // In case 2), we don't want to yield (and give up our scheduling time slice)
    // since we will get to run soon after.
    // To try to get the best of everything, we will busy spin for a while before
    // yielding to another thread.
    // TODO: how do we set this.
    static const int NUM_SPIN_CYCLES = 70;
    // TODO: pad this to be a cache line?
    bool _locked;
};

} // end namespace doris

#endif // DORIS_BE_SRC_UTIL_SPINLOCK_H
