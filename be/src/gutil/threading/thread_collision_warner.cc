// Copyright (c) 2010 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gutil/threading/thread_collision_warner.h"

#ifdef __linux__
#include <syscall.h>
#else
#include <sys/syscall.h>
#endif

#include <glog/logging.h>
#include <unistd.h>

#include <cstdint>
#include <ostream>

namespace base {

void DCheckAsserter::warn(int64_t previous_thread_id, int64_t current_thread_id) {
    LOG(FATAL) << "Thread Collision! Previous thread id: " << previous_thread_id
               << ", current thread id: " << current_thread_id;
}

static subtle::Atomic64 CurrentThread() {
#if defined(__APPLE__)
    uint64_t tid;
    CHECK_EQ(0, pthread_threadid_np(NULL, &tid));
    return tid;
#elif defined(__linux__)
    return syscall(__NR_gettid);
#endif
}

void ThreadCollisionWarner::EnterSelf() {
    // If the active thread is 0 then I'll write the current thread ID
    // if two or more threads arrive here only one will succeed to
    // write on valid_thread_id_ the current thread ID.
    subtle::Atomic64 current_thread_id = CurrentThread();

    int64_t previous_thread_id =
            subtle::NoBarrier_CompareAndSwap(&valid_thread_id_, 0, current_thread_id);
    if (previous_thread_id != 0 && previous_thread_id != current_thread_id) {
        // gotcha! a thread is trying to use the same class and that is
        // not current thread.
        asserter_->warn(previous_thread_id, current_thread_id);
    }

    subtle::NoBarrier_AtomicIncrement(&counter_, 1);
}

void ThreadCollisionWarner::Enter() {
    subtle::Atomic64 current_thread_id = CurrentThread();

    int64_t previous_thread_id =
            subtle::NoBarrier_CompareAndSwap(&valid_thread_id_, 0, current_thread_id);
    if (previous_thread_id != 0) {
        // gotcha! another thread is trying to use the same class.
        asserter_->warn(previous_thread_id, current_thread_id);
    }

    subtle::NoBarrier_AtomicIncrement(&counter_, 1);
}

void ThreadCollisionWarner::Leave() {
    if (subtle::Barrier_AtomicIncrement(&counter_, -1) == 0) {
        subtle::NoBarrier_Store(&valid_thread_id_, 0);
    }
}

} // namespace base
