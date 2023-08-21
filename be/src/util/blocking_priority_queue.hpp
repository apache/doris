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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/blocking-priority-queue.hpp
// and modified by Doris

#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>

#include "common/config.h"
#include "util/blocking_queue.hpp"

namespace doris {

// Fixed capacity FIFO queue, where both blocking_get and blocking_put operations block
// if the queue is empty or full, respectively.
template <typename T>
class BlockingPriorityQueue : public BlockingQueue<T> {
public:
    BlockingPriorityQueue(size_t max_elements)
            : BlockingQueue<T>(max_elements), _upgrade_counter(0) {}

    using BlockingQueue<T>::_lock;
    using BlockingQueue<T>::_get_cv;
    using BlockingQueue<T>::_put_cv;
    using BlockingQueue<T>::_shutdown;
    using BlockingQueue<T>::_max_elements;
    using BlockingQueue<T>::_total_get_wait_time;
    using BlockingQueue<T>::_total_put_wait_time;

    // Get an element from the queue, waiting indefinitely (or until timeout) for one to become available.
    // Returns false if we were shut down prior to getting the element, and there
    // are no more elements available.
    // -- timeout_ms: 0 means wait indefinitely
    bool blocking_get(T* out, uint32_t timeout_ms = 0) {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock unique_lock(_lock);
        bool wait_successful = false;
#if !defined(USE_BTHREAD_SCANNER)
        if (timeout_ms > 0) {
            wait_successful = _get_cv.wait_for(unique_lock, std::chrono::milliseconds(timeout_ms),
                                               [this] { return _shutdown || !_queue.empty(); });
        } else {
            _get_cv.wait(unique_lock, [this] { return _shutdown || !_queue.empty(); });
            wait_successful = true;
        }
#else
        if (timeout_ms > 0) {
            wait_successful = true;
            while (!(_shutdown || !_queue.empty())) {
                if (_get_cv.wait_for(unique_lock, timeout_ms * 1000) != 0) {
                    // timeout
                    wait_successful = _shutdown || !_queue.empty();
                    break;
                }
            }
        } else {
            while (!(_shutdown || !_queue.empty())) {
                _get_cv.wait(unique_lock);
            }
            wait_successful = true;
        }
#endif
        _total_get_wait_time += timer.elapsed_time();
        if (wait_successful) {
            if (_upgrade_counter > config::priority_queue_remaining_tasks_increased_frequency) {
                std::priority_queue<T> tmp_queue;
                while (!_queue.empty()) {
                    T v = _queue.top();
                    _queue.pop();
                    ++v;
                    tmp_queue.push(v);
                }
                swap(_queue, tmp_queue);
                _upgrade_counter = 0;
            }
            if (!_queue.empty()) {
                *out = _queue.top();
                _queue.pop();
                ++_upgrade_counter;
                unique_lock.unlock();
                _put_cv.notify_one();
                return true;
            } else {
                assert(_shutdown);
                return false;
            }
        } else {
            //time out
            assert(!_shutdown);
            return false;
        }
    }

    bool non_blocking_get(T* out) {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock unique_lock(_lock);

        if (!_queue.empty()) {
            // 定期提高队列中残留的任务优先级
            // 保证优先级较低的大查询不至于完全饿死
            if (_upgrade_counter > config::priority_queue_remaining_tasks_increased_frequency) {
                std::priority_queue<T> tmp_queue;
                while (!_queue.empty()) {
                    T v = _queue.top();
                    _queue.pop();
                    ++v;
                    tmp_queue.push(v);
                }
                swap(_queue, tmp_queue);
                _upgrade_counter = 0;
            }
            *out = _queue.top();
            _queue.pop();
            ++_upgrade_counter;
            _total_get_wait_time += timer.elapsed_time();
            unique_lock.unlock();
            _put_cv.notify_one();
            return true;
        }

        return false;
    }

    // Puts an element into the queue, waiting indefinitely until there is space.
    // If the queue is shut down, returns false.
    bool blocking_put(const T& val) {
        MonotonicStopWatch timer;
        timer.start();
        std::unique_lock unique_lock(_lock);
        while (!(_shutdown || _queue.size() < _max_elements)) {
            _put_cv.wait(unique_lock);
        }
        _total_put_wait_time += timer.elapsed_time();

        if (_shutdown) {
            return false;
        }

        _queue.push(val);
        unique_lock.unlock();
        _get_cv.notify_one();
        return true;
    }

    // Return false if queue full or has been shutdown.
    bool try_put(const T& val) {
        std::unique_lock unique_lock(_lock);
        if (_queue.size() < BlockingQueue<T>::_max_elements && !_shutdown) {
            _queue.push(val);
            unique_lock.unlock();
            _get_cv.notify_one();
            return true;
        }
        return false;
    }

    uint32_t get_size() const {
        std::lock_guard l(_lock);
        return _queue.size();
    }

private:
    std::priority_queue<T> _queue;
    int _upgrade_counter;
};

} // namespace doris
