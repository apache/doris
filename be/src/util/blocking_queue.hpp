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

#pragma once

#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <list>
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
              _total_put_wait_time(0),
              _get_waiting(0),
              _put_waiting(0) {}

    // Get an element from the queue, waiting indefinitely for one to become available.
    // Returns false if we were shut down prior to getting the element, and there
    // are no more elements available.
    bool blocking_get(T* out) {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock<std::mutex> unique_lock(_lock);
        while (!(_shutdown || !_list.empty())) {
            ++_get_waiting;
            _get_cv.wait(unique_lock);
        }
        _total_get_wait_time += timer.elapsed_time();

        if (!_list.empty()) {
            *out = _list.front();
            _list.pop_front();
            if (_put_waiting > 0) {
                --_put_waiting;
                unique_lock.unlock();
                _put_cv.notify_one();
            }
            return true;
        } else {
            assert(_shutdown);
            return false;
        }
    }

    // Puts an element into the queue, waiting indefinitely until there is space.
    // If the queue is shut down, returns false.
    bool blocking_put(const T& val) {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock<std::mutex> unique_lock(_lock);
        while (!(_shutdown || _list.size() < _max_elements)) {
            ++_put_waiting;
            _put_cv.wait(unique_lock);
        }
        _total_put_wait_time += timer.elapsed_time();

        if (_shutdown) {
            return false;
        }

        _list.push_back(val);
        if (_get_waiting > 0) {
            --_get_waiting;
            unique_lock.unlock();
            _get_cv.notify_one();
        }
        return true;
    }

    // Return false if queue full or has been shutdown.
    bool try_put(const T& val) {
        if (_shutdown || _list.size() >= _max_elements) {
            return false;
        }

        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock<std::mutex> unique_lock(_lock);
        _total_put_wait_time += timer.elapsed_time();

        if (_shutdown || _list.size() >= _max_elements) {
            return false;
        }

        _list.push_back(val);
        if (_get_waiting > 0) {
            --_get_waiting;
            unique_lock.unlock();
            _get_cv.notify_one();
        }
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
        std::lock_guard<std::mutex> l(_lock);
        return _list.size();
    }

    uint32_t get_capacity() const { return _max_elements; }

    // Returns the total amount of time threads have blocked in BlockingGet.
    uint64_t total_get_wait_time() const { return _total_get_wait_time; }

    // Returns the total amount of time threads have blocked in BlockingPut.
    uint64_t total_put_wait_time() const { return _total_put_wait_time; }

private:
    bool _shutdown;
    const int _max_elements;
    std::condition_variable _get_cv; // 'get' callers wait on this
    std::condition_variable _put_cv; // 'put' callers wait on this
    // _lock guards access to _list, total_get_wait_time, and total_put_wait_time
    mutable std::mutex _lock;
    std::list<T> _list;
    std::atomic<uint64_t> _total_get_wait_time;
    std::atomic<uint64_t> _total_put_wait_time;
    size_t _get_waiting;
    size_t _put_waiting;
};

} // namespace doris
