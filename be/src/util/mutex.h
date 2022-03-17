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

#ifndef DORIS_BE_SRC_UTIL_MUTEX_H
#define DORIS_BE_SRC_UTIL_MUTEX_H
#include <mutex>
#include <shared_mutex>
#include "olap/olap_define.h"

namespace doris {

using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::lock_guard<std::shared_mutex>;
using UniqueWriteLock = std::unique_lock<std::shared_mutex>;

#define TRY_LOCK true

// encapsulation of pthread_mutex to lock the critical sources.
class Mutex {
public:
    Mutex();
    ~Mutex();

    // wait until obtain the lock
    OLAPStatus lock();

    // try obtaining the lock
    OLAPStatus trylock();

    // unlock is called after lock()
    OLAPStatus unlock();

    pthread_mutex_t* getlock() { return &_lock; }

private:
    friend class ConditionVariable;

    pthread_mutex_t _lock;
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

// Helper class than locks a mutex on construction
// and unlocks the mutex on deconstruction.
class MutexLock {
public:
    // wait until obtain the lock
    explicit MutexLock(Mutex* mutex, bool try_lock = false) : _mutex(mutex), _locked(false) {
        if (try_lock) {
            _locked = (_mutex->trylock() == OLAP_SUCCESS);
        } else {
            _mutex->lock();
            _locked = true;
        }
    }

    // unlock is called after
    ~MutexLock() {
        if (_locked) {
            _mutex->unlock();
        }
    }

    void lock() {
        _mutex->lock();
        _locked = true;
    }

    void unlock() {
        _mutex->unlock();
        _locked = false;
    }

    inline bool own_lock() const { return _locked; }

private:
    Mutex* _mutex;
    bool _locked;
    DISALLOW_COPY_AND_ASSIGN(MutexLock);
};
} //namespace doris

#endif // DORIS_BE_SRC_UTIL_MUTEX_H
