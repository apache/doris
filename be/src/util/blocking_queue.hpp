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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/blocking-queue.hpp
// and modified by Doris

#ifndef DORIS_BE_SRC_COMMON_UTIL_BLOCKING_QUEUE_HPP
#define DORIS_BE_SRC_COMMON_UTIL_BLOCKING_QUEUE_HPP

#include <list>
#include <unistd.h>
#include <condition_variable>
#include <mutex>

#include "common/logging.h"
#include "util/stopwatch.hpp"

namespace doris {

// Fixed capacity FIFO queue, where both BlockingGet and BlockingPut operations block
// if the queue is empty or full, respectively.
template <typename T>
class BlockingQueue {
public:
    BlockingQueue(size_t max_elements)
            : _shutdown(false),
              _max_elements(max_elements),
              _total_get_wait_time(0),
              _total_put_wait_time(0) {}

    // Get an element from the queue, waiting indefinitely for one to become available.
    // Returns false if we were shut down prior to getting the element, and there
    // are no more elements available.
    bool blocking_get(T* out) {
        MonotonicStopWatch timer;
        std::unique_lock<std::mutex> unique_lock(_lock);

        while (true) {
            if (!_list.empty()) {
                *out = _list.front();
                _list.pop_front();
                _total_get_wait_time += timer.elapsed_time();
                unique_lock.unlock();
                _put_cv.notify_one();
                return true;
            }

            if (_shutdown) {
                return false;
            }

            timer.start();
            _get_cv.wait(unique_lock);
            timer.stop();
        }
    }

    /// Puts an element into the queue, waiting until 'timeout_micros' elapses, if there is
    /// no space. If the queue is shut down, or if the timeout elapsed without being able to
    /// put the element, returns false.
    /*
    bool blocking_put_with_timeout(const T& val, int64_t timeout_micros) {
        MonotonicStopWatch timer;
        std::unique_lock<std::mutex> write_lock(_lock);
        std::system_time wtime = std::get_system_time() +
            std::posix_time::microseconds(timeout_micros);
        const struct timespec timeout = std::detail::to_timespec(wtime);
        bool notified = true;
        while (SizeLocked(write_lock) >= _max_elements && !_shutdown && notified) {
            timer.Start();
            // Wait until we're notified or until the timeout expires.
            notified = _put_cv.TimedWait(write_lock, &timeout);
            timer.Stop();
        }
        _total_put_wait_time += timer.ElapsedTime();
        // If the list is still full or if the the queue has been shut down, return false.
        // NOTE: We don't check 'notified' here as it appears that pthread condition variables
        // have a weird behavior in which they can return ETIMEDOUT from timed_wait even if
        // another thread did in fact signal
        if (SizeLocked(write_lock) >= _max_elements || _shutdown) return false;
        DCHECK_LT(put_list_.size(), _max_elements);
        _list.push_back(val);
        write_lock.unlock();
        _get_cv.NotifyOne();
        return true;
    }
    */

    // Puts an element into the queue, waiting indefinitely until there is space.
    // If the queue is shut down, returns false.
    bool blocking_put(const T& val) {
        MonotonicStopWatch timer;
        std::unique_lock<std::mutex> unique_lock(_lock);

        while (_list.size() >= _max_elements && !_shutdown) {
            timer.start();
            _put_cv.wait(unique_lock);
            timer.stop();
        }

        _total_put_wait_time += timer.elapsed_time();

        if (_shutdown) {
            return false;
        }

        DCHECK_LT(_list.size(), _max_elements);
        _list.push_back(val);
        unique_lock.unlock();
        _get_cv.notify_one();
        return true;
    }

    // Shut down the queue. Wakes up all threads waiting on BlockingGet or BlockingPut.
    void shutdown() {
        {
            std::lock_guard<std::mutex> guard(_lock);
            _shutdown = true;
        }

        _get_cv.notify_all();
        _put_cv.notify_all();
    }

    uint32_t get_size() const {
        std::unique_lock<std::mutex> l(_lock);
        return _list.size();
    }

    // Returns the total amount of time threads have blocked in BlockingGet.
    uint64_t total_get_wait_time() const {
        std::lock_guard<std::mutex> guard(_lock);
        return _total_get_wait_time;
    }

    // Returns the total amount of time threads have blocked in BlockingPut.
    uint64_t total_put_wait_time() const {
        std::lock_guard<std::mutex> guard(_lock);
        return _total_put_wait_time;
    }

private:
    uint32_t SizeLocked(const std::unique_lock<std::mutex>& lock) const {
        // The size of 'get_list_' is read racily to avoid getting 'get_lock_' in write path.
        DCHECK(lock.owns_lock());
        return _list.size();
    }

    bool _shutdown;
    const int _max_elements;
    std::condition_variable _get_cv; // 'get' callers wait on this
    std::condition_variable _put_cv; // 'put' callers wait on this
    // _lock guards access to _list, total_get_wait_time, and total_put_wait_time
    mutable std::mutex _lock;
    std::list<T> _list;
    uint64_t _total_get_wait_time;
    uint64_t _total_put_wait_time;
};

} // namespace doris

#endif
