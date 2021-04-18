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

#include "olap/olap_define.h"

namespace doris {

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

// pthread_read/write_lock
class RWMutex {
public:
    // Possible fairness policies for the RWMutex.
    enum class Priority {
        // The lock will prioritize readers at the expense of writers.
        PREFER_READING,

        // The lock will prioritize writers at the expense of readers.
        //
        // Care should be taken when using this fairness policy, as it can lead to
        // unexpected deadlocks (e.g. a writer waiting on the lock will prevent
        // additional readers from acquiring it).
        PREFER_WRITING,
    };

    // Create an RWMutex that prioritized readers by default.
    RWMutex(Priority prio = Priority::PREFER_READING);
    ~RWMutex();
    // wait until obtaining the read lock
    OLAPStatus rdlock();
    // try obtaining the read lock
    OLAPStatus tryrdlock();
    // wait until obtaining the write lock
    OLAPStatus wrlock();
    // try obtaining the write lock
    OLAPStatus trywrlock();
    // unlock
    OLAPStatus unlock();

private:
    pthread_rwlock_t _lock;
};

// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
class ReadLock {
public:
    explicit ReadLock(RWMutex* mutex, bool try_lock = false) : _mutex(mutex), _locked(false) {
        if (try_lock) {
            _locked = this->_mutex->tryrdlock() == OLAP_SUCCESS;
        } else {
            this->_mutex->rdlock();
            _locked = true;
        }
    }
    ~ReadLock() {
        if (_locked) {
            this->_mutex->unlock();
        }
    }

    bool own_lock() { return _locked; }

private:
    RWMutex* _mutex;
    bool _locked;
    DISALLOW_COPY_AND_ASSIGN(ReadLock);
};

// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
class WriteLock {
public:
    explicit WriteLock(RWMutex* mutex, bool try_lock = false) : _mutex(mutex), _locked(false) {
        if (try_lock) {
            _locked = this->_mutex->trywrlock() == OLAP_SUCCESS;
        } else {
            this->_mutex->wrlock();
            _locked = true;
        }
    }
    ~WriteLock() {
        if (_locked) {
            this->_mutex->unlock();
        }
    }

    bool own_lock() { return _locked; }

private:
    RWMutex* _mutex;
    bool _locked;
    DISALLOW_COPY_AND_ASSIGN(WriteLock);
};

} //namespace doris

#endif // DORIS_BE_SRC_UTIL_MUTEX_H
