// Copyright (c) 2010 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gutil/threading/thread_collision_warner.h"

#include <common/logging.h>

#include "gutil/linux_syscall_support.h"

namespace base {

void DCheckAsserter::warn() {
  LOG(FATAL) << "Thread Collision";
}

#if 0
// Original source from Chromium -- we didn't import their threading library
// into Cloudera source as of yet

static subtle::Atomic32 CurrentThread() {
  const PlatformThreadId current_thread_id = PlatformThread::CurrentId();
  // We need to get the thread id into an atomic data type. This might be a
  // truncating conversion, but any loss-of-information just increases the
  // chance of a fault negative, not a false positive.
  const subtle::Atomic32 atomic_thread_id =
      static_cast<subtle::Atomic32>(current_thread_id);

  return atomic_thread_id;
}
#else

static subtle::Atomic64 CurrentThread() {
#if defined(__APPLE__)
  uint64_t tid;
  CHECK_EQ(0, pthread_threadid_np(NULL, &tid));
  return tid;
#elif defined(__linux__)
  return syscall(__NR_gettid);
#endif
}

#endif

void ThreadCollisionWarner::EnterSelf() {
  // If the active thread is 0 then I'll write the current thread ID
  // if two or more threads arrive here only one will succeed to
  // write on valid_thread_id_ the current thread ID.
  subtle::Atomic64 current_thread_id = CurrentThread();

  int64_t previous_value = subtle::NoBarrier_CompareAndSwap(&valid_thread_id_,
                                                            0,
                                                            current_thread_id);
  if (previous_value != 0 && previous_value != current_thread_id) {
    // gotcha! a thread is trying to use the same class and that is
    // not current thread.
    asserter_->warn();
  }

  subtle::NoBarrier_AtomicIncrement(&counter_, 1);
}

void ThreadCollisionWarner::Enter() {
  subtle::Atomic64 current_thread_id = CurrentThread();

  if (subtle::NoBarrier_CompareAndSwap(&valid_thread_id_,
                                       0,
                                       current_thread_id) != 0) {
    // gotcha! another thread is trying to use the same class.
    asserter_->warn();
  }

  subtle::NoBarrier_AtomicIncrement(&counter_, 1);
}

void ThreadCollisionWarner::Leave() {
  if (subtle::Barrier_AtomicIncrement(&counter_, -1) == 0) {
    subtle::NoBarrier_Store(&valid_thread_id_, 0);
  }
}

}  // namespace base
