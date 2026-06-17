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

#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <climits>
#include <mutex>

namespace doris {

// A reader-writer lock for bthread contexts. It is a port of libc++'s
// std::shared_mutex (the two-gate condition-variable algorithm) onto
// bthread::Mutex/bthread::ConditionVariable. Unlike std::shared_mutex
// (pthread_rwlock_t), ownership carries no OS-thread identity, so it is safe to
// lock on one bthread worker and unlock on another after a bthread migrates.
// Satisfies the C++ SharedMutex requirements (usable with std::unique_lock /
// std::shared_lock). Writer-preferring.
class BthreadSharedMutex {
public:
    BthreadSharedMutex() = default;
    ~BthreadSharedMutex() = default;

    BthreadSharedMutex(const BthreadSharedMutex&) = delete;
    BthreadSharedMutex& operator=(const BthreadSharedMutex&) = delete;

    void lock() {
        std::unique_lock<bthread::Mutex> lk(_mutex);
        while (_state & _write_entered) {
            _gate1.wait(lk);
        }
        _state |= _write_entered;
        while (_state & _n_readers) {
            _gate2.wait(lk);
        }
    }

    bool try_lock() {
        std::unique_lock<bthread::Mutex> lk(_mutex);
        if (_state == 0) {
            _state = _write_entered;
            return true;
        }
        return false;
    }

    void unlock() {
        std::lock_guard<bthread::Mutex> lk(_mutex);
        _state = 0;
        // Notify while still holding `_mutex`. Releasing the mutex before
        // notifying can lose a wakeup with bthread::ConditionVariable when the
        // waiter is a pthread and the notifier is a bthread (or vice versa).
        _gate1.notify_all();
    }

    void lock_shared() {
        std::unique_lock<bthread::Mutex> lk(_mutex);
        while ((_state & _write_entered) || (_state & _n_readers) == _n_readers) {
            _gate1.wait(lk);
        }
        unsigned num_readers = (_state & _n_readers) + 1;
        _state &= ~_n_readers;
        _state |= num_readers;
    }

    bool try_lock_shared() {
        std::unique_lock<bthread::Mutex> lk(_mutex);
        unsigned num_readers = _state & _n_readers;
        if (!(_state & _write_entered) && num_readers != _n_readers) {
            ++num_readers;
            _state &= ~_n_readers;
            _state |= num_readers;
            return true;
        }
        return false;
    }

    void unlock_shared() {
        std::unique_lock<bthread::Mutex> lk(_mutex);
        unsigned num_readers = (_state & _n_readers) - 1;
        _state &= ~_n_readers;
        _state |= num_readers;
        // Notify while still holding `_mutex` (do not unlock first): with
        // bthread::ConditionVariable a notify issued after releasing the mutex
        // can be lost across pthread/bthread contexts, leaving a writer parked
        // forever on a reader count that is already zero.
        if (_state & _write_entered) {
            if (num_readers == 0) {
                _gate2.notify_one();
            }
        } else {
            if (num_readers == _n_readers - 1) {
                _gate1.notify_one();
            }
        }
    }

private:
    bthread::Mutex _mutex;
    bthread::ConditionVariable _gate1;
    bthread::ConditionVariable _gate2;
    unsigned _state {0};

    // High bit: a writer has entered. Remaining bits: active reader count.
    static constexpr unsigned _write_entered = 1U << (sizeof(unsigned) * CHAR_BIT - 1);
    static constexpr unsigned _n_readers = ~_write_entered;
};

} // namespace doris
