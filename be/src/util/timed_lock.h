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

#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

namespace doris {

// A RAII-style lock wrapper that can optionally record the time spent waiting for the lock.
// Similar to std::unique_lock but with timing capabilities.
//
// Usage example:
//   std::shared_mutex mutex;
//   int64_t wait_time_ns = 0;
//   {
//       TimedLock<std::shared_mutex> lock(mutex, &wait_time_ns);
//       // Critical section
//   }
//   // wait_time_ns now contains the time spent waiting for the lock in nanoseconds
template <typename Mutex>
class TimedLock {
public:
    using mutex_type = Mutex;

    // Constructor that acquires the lock and optionally records wait time
    // @param mutex: The mutex to lock
    // @param wait_time_ns: Optional pointer to store the wait time in nanoseconds
    explicit TimedLock(mutex_type& mutex, int64_t* wait_time_ns = nullptr)
            : _mutex(&mutex), _owns_lock(false), _wait_time_ns(wait_time_ns) {
        lock();
    }

    // Constructor with defer_lock - does not acquire the lock
    TimedLock(mutex_type& mutex, std::defer_lock_t, int64_t* wait_time_ns = nullptr) noexcept
            : _mutex(&mutex), _owns_lock(false), _wait_time_ns(wait_time_ns) {}

    // Constructor with adopt_lock - assumes the calling thread already owns the lock
    TimedLock(mutex_type& mutex, std::adopt_lock_t, int64_t* wait_time_ns = nullptr) noexcept
            : _mutex(&mutex), _owns_lock(true), _wait_time_ns(wait_time_ns) {}

    // Destructor - releases the lock if owned
    ~TimedLock() {
        if (_owns_lock) {
            unlock();
        }
    }

    // Delete copy constructor and assignment operator
    TimedLock(const TimedLock&) = delete;
    TimedLock& operator=(const TimedLock&) = delete;

    // Move constructor
    TimedLock(TimedLock&& other) noexcept
            : _mutex(other._mutex),
              _owns_lock(other._owns_lock),
              _wait_time_ns(other._wait_time_ns) {
        other._mutex = nullptr;
        other._owns_lock = false;
        other._wait_time_ns = nullptr;
    }

    // Move assignment operator
    TimedLock& operator=(TimedLock&& other) noexcept {
        if (this != &other) {
            if (_owns_lock) {
                unlock();
            }
            _mutex = other._mutex;
            _owns_lock = other._owns_lock;
            _wait_time_ns = other._wait_time_ns;
            other._mutex = nullptr;
            other._owns_lock = false;
            other._wait_time_ns = nullptr;
        }
        return *this;
    }

    // Acquires the lock and records wait time if pointer is provided
    void lock() {
        if (!_mutex) {
            throw std::system_error(std::make_error_code(std::errc::operation_not_permitted),
                                    "TimedLock: mutex is null");
        }
        if (_owns_lock) {
            throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur),
                                    "TimedLock: already owns lock");
        }

        auto start = std::chrono::steady_clock::now();
        _mutex->lock();
        auto end = std::chrono::steady_clock::now();

        _owns_lock = true;

        if (_wait_time_ns) {
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            *_wait_time_ns = duration.count();
        }
    }

    // Releases the lock
    void unlock() {
        if (!_owns_lock) {
            throw std::system_error(std::make_error_code(std::errc::operation_not_permitted),
                                    "TimedLock: does not own lock");
        }
        if (_mutex) {
            _mutex->unlock();
            _owns_lock = false;
        }
    }

    // Checks whether this object owns the lock
    bool owns_lock() const noexcept { return _owns_lock; }

    // Checks whether this object owns the lock (for use in boolean contexts)
    explicit operator bool() const noexcept { return owns_lock(); }

    // Returns a pointer to the associated mutex
    mutex_type* mutex() const noexcept { return _mutex; }

    // Disassociates the mutex without unlocking it
    mutex_type* release() noexcept {
        mutex_type* ret = _mutex;
        _mutex = nullptr;
        _owns_lock = false;
        return ret;
    }

    // Transfers ownership to a std::unique_lock
    // This is useful when you need to pass the lock to a function that expects std::unique_lock
    // After calling this method, this TimedLock object no longer owns the lock
    std::unique_lock<mutex_type> transfer_to_unique_lock() noexcept {
        if (!_owns_lock || !_mutex) {
            return std::unique_lock<mutex_type>();
        }
        mutex_type* m = release();
        return std::unique_lock<mutex_type>(*m, std::adopt_lock);
    }

private:
    mutex_type* _mutex;
    bool _owns_lock;
    int64_t* _wait_time_ns;
};

// Specialization for shared_mutex with shared (read) lock
template <typename Mutex>
class TimedSharedLock {
public:
    using mutex_type = Mutex;

    // Constructor that acquires the shared lock and optionally records wait time
    // @param mutex: The mutex to lock
    // @param wait_time_ns: Optional pointer to store the wait time in nanoseconds
    explicit TimedSharedLock(mutex_type& mutex, int64_t* wait_time_ns = nullptr)
            : _mutex(&mutex), _owns_lock(false), _wait_time_ns(wait_time_ns) {
        lock();
    }

    // Constructor with defer_lock - does not acquire the lock
    TimedSharedLock(mutex_type& mutex, std::defer_lock_t,
                    int64_t* wait_time_ns = nullptr) noexcept
            : _mutex(&mutex), _owns_lock(false), _wait_time_ns(wait_time_ns) {}

    // Constructor with adopt_lock - assumes the calling thread already owns the lock
    TimedSharedLock(mutex_type& mutex, std::adopt_lock_t,
                    int64_t* wait_time_ns = nullptr) noexcept
            : _mutex(&mutex), _owns_lock(true), _wait_time_ns(wait_time_ns) {}

    // Destructor - releases the lock if owned
    ~TimedSharedLock() {
        if (_owns_lock) {
            unlock();
        }
    }

    // Delete copy constructor and assignment operator
    TimedSharedLock(const TimedSharedLock&) = delete;
    TimedSharedLock& operator=(const TimedSharedLock&) = delete;

    // Move constructor
    TimedSharedLock(TimedSharedLock&& other) noexcept
            : _mutex(other._mutex),
              _owns_lock(other._owns_lock),
              _wait_time_ns(other._wait_time_ns) {
        other._mutex = nullptr;
        other._owns_lock = false;
        other._wait_time_ns = nullptr;
    }

    // Move assignment operator
    TimedSharedLock& operator=(TimedSharedLock&& other) noexcept {
        if (this != &other) {
            if (_owns_lock) {
                unlock();
            }
            _mutex = other._mutex;
            _owns_lock = other._owns_lock;
            _wait_time_ns = other._wait_time_ns;
            other._mutex = nullptr;
            other._owns_lock = false;
            other._wait_time_ns = nullptr;
        }
        return *this;
    }

    // Acquires the shared lock and records wait time if pointer is provided
    void lock() {
        if (!_mutex) {
            throw std::system_error(std::make_error_code(std::errc::operation_not_permitted),
                                    "TimedSharedLock: mutex is null");
        }
        if (_owns_lock) {
            throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur),
                                    "TimedSharedLock: already owns lock");
        }

        auto start = std::chrono::steady_clock::now();
        _mutex->lock_shared();
        auto end = std::chrono::steady_clock::now();

        _owns_lock = true;

        if (_wait_time_ns) {
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            *_wait_time_ns = duration.count();
        }
    }

    // Releases the shared lock
    void unlock() {
        if (!_owns_lock) {
            throw std::system_error(std::make_error_code(std::errc::operation_not_permitted),
                                    "TimedSharedLock: does not own lock");
        }
        if (_mutex) {
            _mutex->unlock_shared();
            _owns_lock = false;
        }
    }

    // Checks whether this object owns the lock
    bool owns_lock() const noexcept { return _owns_lock; }

    // Checks whether this object owns the lock (for use in boolean contexts)
    explicit operator bool() const noexcept { return owns_lock(); }

    // Returns a pointer to the associated mutex
    mutex_type* mutex() const noexcept { return _mutex; }

    // Disassociates the mutex without unlocking it
    mutex_type* release() noexcept {
        mutex_type* ret = _mutex;
        _mutex = nullptr;
        _owns_lock = false;
        return ret;
    }

    // Transfers ownership to a std::shared_lock
    // This is useful when you need to pass the lock to a function that expects std::shared_lock
    // After calling this method, this TimedSharedLock object no longer owns the lock
    std::shared_lock<mutex_type> transfer_to_shared_lock() noexcept {
        if (!_owns_lock || !_mutex) {
            return std::shared_lock<mutex_type>();
        }
        mutex_type* m = release();
        return std::shared_lock<mutex_type>(*m, std::adopt_lock);
    }

private:
    mutex_type* _mutex;
    bool _owns_lock;
    int64_t* _wait_time_ns;
};

} // namespace doris


