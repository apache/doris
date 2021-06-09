// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// ConditionVariable wraps pthreads condition variable synchronization or, on
// Windows, simulates it.  This functionality is very helpful for having
// several threads wait for an event, as is common with a thread pool managed
// by a master.  The meaning of such an event in the (worker) thread pool
// scenario is that additional tasks are now available for processing.  It is
// used in Chrome in the DNS prefetching system to notify worker threads that
// a queue now has items (tasks) which need to be tended to.  A related use
// would have a pool manager waiting on a ConditionVariable, waiting for a
// thread in the pool to announce (signal) that there is now more room in a
// (bounded size) communications queue for the manager to deposit tasks, or,
// as a second example, that the queue of tasks is completely empty and all
// workers are waiting.
//
// USAGE NOTE 1: spurious signal events are possible with this and
// most implementations of condition variables.  As a result, be
// *sure* to retest your condition before proceeding.  The following
// is a good example of doing this correctly:
//
// while (!work_to_be_done()) Wait(...);
//
// In contrast do NOT do the following:
//
// if (!work_to_be_done()) Wait(...);  // Don't do this.
//
// Especially avoid the above if you are relying on some other thread only
// issuing a signal up *if* there is work-to-do.  There can/will
// be spurious signals.  Recheck state on waiting thread before
// assuming the signal was intentional. Caveat caller ;-).
//
// USAGE NOTE 2: notify_all() frees up all waiting threads at once,
// which leads to contention for the locks they all held when they
// called wait().  This results in POOR performance.  A much better
// approach to getting a lot of threads out of wait() is to have each
// thread (upon exiting wait()) call notify_one() to free up another
// Waiting thread.  Look at condition_variable_unittest.cc for
// both examples.
//
// notify_all() can be used nicely during teardown, as it gets the job
// done, and leaves no sleeping threads... and performance is less
// critical at that point.
//
// The semantics of notify_all() are carefully crafted so that *all*
// threads that were waiting when the request was made will indeed
// get signaled.  Some implementations mess up, and don't signal them
// all, while others allow the wait to be effectively turned off (for
// a while while waiting threads come around).  This implementation
// appears correct, as it will not "lose" any signals, and will guarantee
// that all threads get signaled by notify_all().
//
// This implementation offers support for "performance" in its selection of
// which thread to revive.  Performance, in direct contrast with "fairness,"
// assures that the thread that most recently began to wait() is selected by
// Signal to revive.  Fairness would (if publicly supported) assure that the
// thread that has wait()ed the longest is selected. The default policy
// may improve performance, as the selected thread may have a greater chance of
// having some of its stack data in various CPU caches.
//
// For a discussion of the many very subtle implementation details, see the FAQ
// at the end of condition_variable_win.cc.

#ifndef DORIS_BE_SRC_UTIL_CONDITION_VARIABLE_H
#define DORIS_BE_SRC_UTIL_CONDITION_VARIABLE_H

#include <pthread.h>

#include "olap/olap_define.h"

namespace doris {

class MonoDelta;
class MonoTime;
class Mutex;

class ConditionVariable {
public:
    // Construct a cv for use with ONLY one user lock.
    explicit ConditionVariable(Mutex* user_lock);

    ~ConditionVariable();

    // wait() releases the caller's critical section atomically as it starts to
    // sleep, and the reacquires it when it is signaled.
    void wait() const;

    // Like wait(), but only waits up to a certain point in time.
    //
    // Returns true if we were notify_one()'ed, or false if we reached 'until'.
    bool wait_until(const MonoTime& until) const;

    // Like wait(), but only waits up to a limited amount of time.
    //
    // Returns true if we were notify_one()'ed, or false if 'delta' elapsed.
    bool wait_for(const MonoDelta& delta) const;

    // notify_all() revives all waiting threads.
    void notify_all();

    // notify_one() revives one waiting thread.
    void notify_one();

private:
    mutable pthread_cond_t _condition;
    pthread_mutex_t* _user_mutex;

    DISALLOW_COPY_AND_ASSIGN(ConditionVariable);
};

} // namespace doris

#endif // DORIS_BE_SRC_UTIL_CONDITION_VARIABLE_H
