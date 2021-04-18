// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/condition_variable.h"

#include <sys/time.h>

#include <cerrno>
#include <cstdint>
#include <ctime>

#include "common/logging.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/monotime.h"
#include "util/mutex.h"

namespace doris {

ConditionVariable::ConditionVariable(Mutex* user_lock) : _user_mutex(&user_lock->_lock) {
    int rv = 0;
    pthread_condattr_t attrs;
    rv = pthread_condattr_init(&attrs);
    DCHECK_EQ(0, rv);
    pthread_condattr_setclock(&attrs, CLOCK_MONOTONIC);
    rv = pthread_cond_init(&_condition, &attrs);
    pthread_condattr_destroy(&attrs);
    DCHECK_EQ(0, rv);
}

ConditionVariable::~ConditionVariable() {
    int rv = pthread_cond_destroy(&_condition);
    DCHECK_EQ(0, rv);
}

void ConditionVariable::wait() const {
    debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
    int rv = pthread_cond_wait(&_condition, _user_mutex);
    DCHECK_EQ(0, rv);
}

bool ConditionVariable::wait_until(const MonoTime& until) const {
    debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
    // Have we already timed out?
    MonoTime now = MonoTime::Now();
    if (now > until) {
        return false;
    }

    struct timespec absolute_time;
    until.ToTimeSpec(&absolute_time);
    int rv = pthread_cond_timedwait(&_condition, _user_mutex, &absolute_time);
    DCHECK(rv == 0 || rv == ETIMEDOUT) << "unexpected pthread_cond_timedwait return value: " << rv;

    return rv == 0;
}

bool ConditionVariable::wait_for(const MonoDelta& delta) const {
    debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
    // Negative delta means we've already timed out.
    int64_t nsecs = delta.ToNanoseconds();
    if (nsecs < 0) {
        return false;
    }

    // The timeout argument to pthread_cond_timedwait is in absolute time.
    struct timespec absolute_time;
    MonoTime deadline = MonoTime::Now() + delta;
    deadline.ToTimeSpec(&absolute_time);
    int rv = pthread_cond_timedwait(&_condition, _user_mutex, &absolute_time);

    DCHECK(rv == 0 || rv == ETIMEDOUT) << "unexpected pthread_cond_timedwait return value: " << rv;
    return rv == 0;
}

void ConditionVariable::notify_all() {
    int rv = pthread_cond_broadcast(&_condition);
    DCHECK_EQ(0, rv);
}

void ConditionVariable::notify_one() {
    int rv = pthread_cond_signal(&_condition);
    DCHECK_EQ(0, rv);
}

} // namespace doris
