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

#include "util/mutex.h"

#include <cerrno>

#include "common/logging.h"

namespace doris {

#define PTHREAD_MUTEX_INIT_WITH_LOG(lockptr, param)                         \
    do {                                                                    \
        int lock_ret = 0;                                                   \
        if (0 != (lock_ret = pthread_mutex_init(lockptr, param))) {         \
            LOG(FATAL) << "fail to init mutex. err=" << strerror(lock_ret); \
        }                                                                   \
    } while (0)

#define PTHREAD_MUTEX_DESTROY_WITH_LOG(lockptr)                                \
    do {                                                                       \
        int lock_ret = 0;                                                      \
        if (0 != (lock_ret = pthread_mutex_destroy(lockptr))) {                \
            LOG(FATAL) << "fail to destroy mutex. err=" << strerror(lock_ret); \
        }                                                                      \
    } while (0)

#define PTHREAD_MUTEX_LOCK_WITH_LOG(lockptr)                                \
    do {                                                                    \
        int lock_ret = 0;                                                   \
        if (0 != (lock_ret = pthread_mutex_lock(lockptr))) {                \
            LOG(FATAL) << "fail to lock mutex. err=" << strerror(lock_ret); \
        }                                                                   \
    } while (0)

#define PTHREAD_MUTEX_UNLOCK_WITH_LOG(lockptr)                                \
    do {                                                                      \
        int lock_ret = 0;                                                     \
        if (0 != (lock_ret = pthread_mutex_unlock(lockptr))) {                \
            LOG(FATAL) << "fail to unlock mutex. err=" << strerror(lock_ret); \
        }                                                                     \
    } while (0)

#define PTHREAD_RWLOCK_INIT_WITH_LOG(lockptr, param)                         \
    do {                                                                     \
        int lock_ret = 0;                                                    \
        if (0 != (lock_ret = pthread_rwlock_init(lockptr, param))) {         \
            LOG(FATAL) << "fail to init rwlock. err=" << strerror(lock_ret); \
        }                                                                    \
    } while (0)

#define PTHREAD_RWLOCK_DESTROY_WITH_LOG(lockptr)                                \
    do {                                                                        \
        int lock_ret = 0;                                                       \
        if (0 != (lock_ret = pthread_rwlock_destroy(lockptr))) {                \
            LOG(FATAL) << "fail to destroy rwlock. err=" << strerror(lock_ret); \
        }                                                                       \
    } while (0)

#define PTHREAD_RWLOCK_RDLOCK_WITH_LOG(lockptr)                                   \
    do {                                                                          \
        int lock_ret = 0;                                                         \
        if (0 != (lock_ret = pthread_rwlock_rdlock(lockptr))) {                   \
            LOG(FATAL) << "fail to lock reader lock. err=" << strerror(lock_ret); \
        }                                                                         \
    } while (0)

#define PTHREAD_RWLOCK_WRLOCK_WITH_LOG(lockptr)                                   \
    do {                                                                          \
        int lock_ret = 0;                                                         \
        if (0 != (lock_ret = pthread_rwlock_wrlock(lockptr))) {                   \
            LOG(FATAL) << "fail to lock writer lock. err=" << strerror(lock_ret); \
        }                                                                         \
    } while (0)

#define PTHREAD_RWLOCK_UNLOCK_WITH_LOG(lockptr)                                \
    do {                                                                       \
        int lock_ret = 0;                                                      \
        if (0 != (lock_ret = pthread_rwlock_unlock(lockptr))) {                \
            LOG(FATAL) << "fail to unlock rwlock. err=" << strerror(lock_ret); \
        }                                                                      \
    } while (0)

Mutex::Mutex() {
    PTHREAD_MUTEX_INIT_WITH_LOG(&_lock, NULL);
}

Mutex::~Mutex() {
    PTHREAD_MUTEX_DESTROY_WITH_LOG(&_lock);
}

OLAPStatus Mutex::lock() {
    PTHREAD_MUTEX_LOCK_WITH_LOG(&_lock);
    return OLAP_SUCCESS;
}

OLAPStatus Mutex::trylock() {
    int rv = pthread_mutex_trylock(&_lock);
    if (rv != 0) {
        VLOG(3) << "failed to got the mutex lock. error=" << strerror(rv);
        return OLAP_ERR_RWLOCK_ERROR;
    }
    return OLAP_SUCCESS;
}

OLAPStatus Mutex::unlock() {
    PTHREAD_MUTEX_UNLOCK_WITH_LOG(&_lock);
    return OLAP_SUCCESS;
}

RWMutex::RWMutex(Priority prio) {
    int kind = PTHREAD_RWLOCK_PREFER_READER_NP;
    switch (prio) {
    case Priority::PREFER_READING:
        kind = PTHREAD_RWLOCK_PREFER_READER_NP;
        break;
    case Priority::PREFER_WRITING:
        kind = PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP;
        break;
    }

    // Initialize the new rwlock with the user's preference.
    pthread_rwlockattr_t attr;
    int rv = pthread_rwlockattr_init(&attr);
    DCHECK_EQ(0, rv) << strerror(rv);

    rv = pthread_rwlockattr_setkind_np(&attr, kind);
    DCHECK_EQ(0, rv) << strerror(rv);

    rv = pthread_rwlock_init(&_lock, &attr);
    DCHECK_EQ(0, rv) << strerror(rv);

    rv = pthread_rwlockattr_destroy(&attr);
    DCHECK_EQ(0, rv) << strerror(rv);
}

RWMutex::~RWMutex() {
    PTHREAD_RWLOCK_DESTROY_WITH_LOG(&_lock);
}

OLAPStatus RWMutex::rdlock() {
    PTHREAD_RWLOCK_RDLOCK_WITH_LOG(&_lock);
    return OLAP_SUCCESS;
}

OLAPStatus RWMutex::tryrdlock() {
    if (0 != pthread_rwlock_tryrdlock(&_lock)) {
        VLOG(3) << "failed to got the rwlock rdlock. err=" << strerror(errno);
        return OLAP_ERR_RWLOCK_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus RWMutex::trywrlock() {
    if (0 != pthread_rwlock_trywrlock(&_lock)) {
        VLOG(3) << "failed to got the rwlock rdlock. err=" << strerror(errno);
        return OLAP_ERR_RWLOCK_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus RWMutex::wrlock() {
    PTHREAD_RWLOCK_WRLOCK_WITH_LOG(&_lock);
    return OLAP_SUCCESS;
}

OLAPStatus RWMutex::unlock() {
    PTHREAD_RWLOCK_UNLOCK_WITH_LOG(&_lock);
    return OLAP_SUCCESS;
}

} // namespace doris
