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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "common/simple_thread_pool.h"
#include "common/thread_name.h"
#include "common/thread_pool.h"

namespace doris::cloud {

class AdaptiveThreadPool : public ThreadPool {
public:
    using JobType = ThreadPool::JobType;

private:
    template <typename T, template <typename ELEM,
                                    typename ALLOC = std::allocator<ELEM>> class CONT = std::deque>
    class AdaptiveSyncQueue {
    public:
        AdaptiveSyncQueue() = default;
        explicit AdaptiveSyncQueue(size_t max_size) : _max_size(max_size) {}
        ~AdaptiveSyncQueue() { _queue.clear(); }

        template <typename... Args>
        void emplace(Args&&... args) {
            std::unique_lock<std::mutex> locker(_mutex);
            _not_full.wait(locker, [this]() { return _queue.size() < _max_size; });
            _queue.emplace_back(std::forward<Args>(args)...);
            _not_empty.notify_one();
        }

        template <typename... Args>
        bool emplace_with_timeout(int timeout /*in milliseconds*/, Args&&... args) {
            std::unique_lock<std::mutex> locker(_mutex);
            if (_not_full.wait_for(locker, std::chrono::milliseconds(timeout),
                                   [this]() { return _queue.size() < _max_size; })) {
                _queue.emplace_back(std::forward<Args>(args)...);
                _not_empty.notify_one();
                return true;
            }
            return false;
        }

        bool get_with_timeout(T* t, int timeout /*in milliseconds*/) {
            std::unique_lock<std::mutex> locker(_mutex);
            if (_not_empty.wait_for(locker, std::chrono::milliseconds(timeout),
                                    [this]() { return !_queue.empty(); })) {
                (*t) = std::move(_queue.front());
                _queue.pop_front();
                _not_full.notify_one();
                return true;
            }
            return false;
        }

        void set_max_size(size_t max_size) {
            std::lock_guard<std::mutex> locker(_mutex);
            bool expanded = max_size > _max_size;
            _max_size = max_size;
            if (expanded) {
                _not_full.notify_all();
            }
        }

        size_t max_size() {
            std::lock_guard<std::mutex> locker(_mutex);
            return _max_size;
        }

        void clear() {
            std::lock_guard<std::mutex> locker(_mutex);
            _queue.clear();
            _not_full.notify_all();
        }

        size_t size() {
            std::lock_guard<std::mutex> locker(_mutex);
            return _queue.size();
        }

        bool empty() {
            std::lock_guard<std::mutex> locker(_mutex);
            return _queue.empty();
        }

    private:
        CONT<T> _queue;
        std::mutex _mutex;
        std::condition_variable _not_empty;
        std::condition_variable _not_full;
        size_t _max_size = std::numeric_limits<size_t>::max();
    };

    struct Worker {
        std::thread thread;
        std::atomic<bool> should_stop {false};
        std::atomic<bool> exited {false};
    };

    std::shared_ptr<AdaptiveSyncQueue<JobType>> _job_queue;
    mutable std::mutex _mutex;
    std::vector<std::shared_ptr<Worker>> _workers;
    std::vector<std::shared_ptr<Worker>> _retired_workers;
    std::atomic<bool> _is_running {false};
    std::atomic<size_t> _pool_size {0};
    std::atomic<size_t> _active_threads {0};
    size_t _min_size;
    size_t _max_size;
    size_t _next_worker_id {0};
    std::string _pool_name;

public:
    AdaptiveThreadPool(size_t size, const std::string& name, size_t min_size, size_t max_size)
            : _is_running(false), _min_size(min_size), _max_size(max_size), _pool_name(name) {
        size_t init_size = std::clamp(size, _min_size, _max_size);
        _pool_size.store(init_size, std::memory_order_relaxed);
        _job_queue = std::make_shared<AdaptiveSyncQueue<JobType>>(_max_size * 2);
    }

    AdaptiveThreadPool() = delete;

    /**
     * Submits jobs, if job queue is full it will block
     *
     * @return 0 if succeeded
     */
    int submit(JobType f) override {
        if (!_is_running.load(std::memory_order_acquire)) {
            // not started
            return -1;
        }
        _job_queue->emplace(std::move(f));
        return 0;
    }

    /**
     * Submits jobs with a limit blocking time
     *
     * @param timeout max blocking time in milliseconds
     * @return 0 if success
     *         -1 if thread pool not start yet
     *         -2 if time out
     */
    int submit_with_timeout(JobType f, int timeout) override {
        if (!_is_running.load(std::memory_order_acquire)) {
            // not started
            return -1;
        }
        if (!_job_queue->emplace_with_timeout(timeout, std::move(f))) {
            return -2;
        }
        return 0;
    }

    /**
     *
     * @return always 0
     */
    int start() override {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_is_running.load(std::memory_order_acquire)) {
            return 0;
        }
        _is_running.store(true, std::memory_order_release);
        _workers.clear();
        _retired_workers.clear();
        _next_worker_id = 0;
        _pool_name = _pool_name.empty() ? "adaptive_thread_pool" : _pool_name;
        size_t target_size = _pool_size.load(std::memory_order_relaxed);
        for (size_t i = 0; i < target_size; ++i) {
            _spawn_worker_locked();
        }
        return 0;
    }

    /**
     * Stops to get jobs from job queue, the job being done will finish normally
     *
     * @return 0 if succeed, otherwise non-zero value returned
     */
    int stop() override {
        std::vector<std::shared_ptr<Worker>> workers_to_join;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (!_is_running.load(std::memory_order_acquire)) {
                // already stopped
                return -1;
            }
            _is_running.store(false, std::memory_order_release);
            workers_to_join.reserve(_workers.size() + _retired_workers.size());
            for (auto& w : _workers) {
                workers_to_join.push_back(std::move(w));
            }
            for (auto& w : _retired_workers) {
                workers_to_join.push_back(std::move(w));
            }
            _workers.clear();
            _retired_workers.clear();
        }
        for (auto& w : workers_to_join) {
            if (w->thread.joinable()) {
                w->thread.join();
            }
        }
        return 0;
    }

    /**
     * Gets size of the pool
     *
     * @return the thread number(pool size) in the thread pool
     */
    size_t size() const override { return _pool_size.load(std::memory_order_acquire); }

    /**
     * Gets number of worker threads currently executing jobs
     */
    size_t active_threads() const override {
        return _active_threads.load(std::memory_order_relaxed);
    }

    /**
     * Gets number of pending jobs in queue
     */
    size_t queue_size() const override { return _job_queue ? _job_queue->size() : 0; }

    /**
     * Gets queue capacity
     */
    size_t queue_capacity() const { return _job_queue ? _job_queue->max_size() : 0; }

    /**
     * Gets name of the pool
     */
    const std::string& name() const override { return _pool_name; }

    size_t min_size() const { return _min_size; }

    size_t max_size() const { return _max_size; }

    /**
     * Expands thread pool size by adding a positive count of worker threads.
     * The resulting size is clamped to max_size.
     *
     * @param count number of threads to add
     * @return 0 if succeeded, -1 if thread pool is not running
     */
    int expand_by_count(size_t count) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_is_running.load(std::memory_order_acquire)) {
            return -1;
        }
        size_t cur_size = _pool_size.load(std::memory_order_relaxed);
        size_t target_size = cur_size + count;
        return _resize_locked(target_size);
    }

    /**
     * Shrinks thread pool size by removing a positive count of worker threads.
     * The resulting size is clamped to min_size.
     *
     * @param count number of threads to remove
     * @return 0 if succeeded, -1 if thread pool is not running
     */
    int shrink_by_count(size_t count) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_is_running.load(std::memory_order_acquire)) {
            return -1;
        }
        size_t cur_size = _pool_size.load(std::memory_order_relaxed);
        size_t target_size = (cur_size > count) ? (cur_size - count) : 0;
        return _resize_locked(target_size);
    }

    /**
     * Expands thread pool size by a scale ratio (> 1.0).
     * For example, ratio = 1.5 scales current pool size up by 50%.
     * The resulting size is clamped to max_size.
     *
     * @param ratio expansion ratio, must be > 1.0
     * @return 0 if succeeded, -1 if thread pool is not running or ratio <= 1.0
     */
    int expand_by_ratio(double ratio) {
        if (ratio <= 1.0 || !std::isfinite(ratio)) {
            return -1;
        }
        size_t cur_size = size();
        size_t target_size =
                static_cast<size_t>(std::llround(static_cast<double>(cur_size) * ratio));
        size_t to_add = target_size > cur_size ? (target_size - cur_size) : 0;
        return expand_by_count(to_add);
    }

    /**
     * Shrinks thread pool size by a scale ratio (0.0 < ratio < 1.0).
     * For example, ratio = 0.5 shrinks current pool size down by half.
     * The resulting size is clamped to min_size.
     *
     * @param ratio shrink ratio, must be in range (0.0, 1.0)
     * @return 0 if succeeded, -1 if thread pool is not running or ratio not in (0.0, 1.0)
     */
    int shrink_by_ratio(double ratio) {
        if (ratio <= 0.0 || ratio >= 1.0 || !std::isfinite(ratio)) {
            return -1;
        }
        size_t cur_size = size();
        double scaled = static_cast<double>(cur_size) * ratio;
        size_t target_size = static_cast<size_t>(std::llround(scaled));
        size_t to_remove = cur_size > target_size ? (cur_size - target_size) : 0;
        return shrink_by_count(to_remove);
    }

    /**
     * Adjusts thread pool size by a signed count delta.
     * Positive delta expands the pool; negative delta shrinks the pool.
     * The resulting size is clamped to [min_size, max_size].
     *
     * @param delta number of threads to add (> 0) or remove (< 0)
     * @return 0 if succeeded, -1 if thread pool is not running
     */
    int adjust_by_count(int64_t delta) {
        if (delta > 0) {
            return expand_by_count(static_cast<size_t>(delta));
        } else if (delta < 0) {
            auto sub = static_cast<size_t>(-(delta + 1)) + 1;
            return shrink_by_count(sub);
        }
        return 0;
    }

    /**
     * Adjusts thread pool size by a multiplier ratio.
     * ratio > 1.0 expands the pool; 0.0 < ratio < 1.0 shrinks the pool.
     * The resulting size is clamped to [min_size, max_size].
     *
     * @param ratio multiplier factor (must be > 0)
     * @return 0 if succeeded, -1 if thread pool is not running or ratio is invalid
     */
    int adjust_by_ratio(double ratio) {
        if (ratio > 1.0) {
            return expand_by_ratio(ratio);
        } else if (ratio > 0.0 && ratio < 1.0) {
            return shrink_by_ratio(ratio);
        } else if (ratio == 1.0) {
            return 0;
        }
        return -1;
    }

    /**
     * Resizes thread pool to target_size, clamped to [min_size, max_size].
     *
     * @param target_size desired number of threads
     * @return 0 if succeeded, -1 if thread pool is not running
     */
    int resize(size_t target_size) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_is_running.load(std::memory_order_acquire)) {
            return -1;
        }
        return _resize_locked(target_size);
    }

    /**
     * Terminates the thread pool immediately, jobs in the queue will not be done
     * and the running threads will be detached
     *
     * @return 0 if succeed, otherwise non-zero value returned
     */
    int terminate() override {
        std::vector<std::shared_ptr<Worker>> workers_to_detach;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (!_is_running.load(std::memory_order_acquire)) {
                // not running
                return -1;
            }
            _is_running.store(false, std::memory_order_release);
            _job_queue->clear(); // abandon all jobs
            workers_to_detach.reserve(_workers.size() + _retired_workers.size());
            for (auto& w : _workers) {
                w->should_stop.store(true, std::memory_order_release);
                workers_to_detach.push_back(std::move(w));
            }
            for (auto& w : _retired_workers) {
                w->should_stop.store(true, std::memory_order_release);
                workers_to_detach.push_back(std::move(w));
            }
            _workers.clear();
            _retired_workers.clear();
        }
        for (auto& w : workers_to_detach) {
            if (w->thread.joinable()) {
                w->thread.detach();
            }
        }
        return 0;
    }

    ~AdaptiveThreadPool() override {
        if (_is_running.load(std::memory_order_acquire)) {
            stop();
        }
    }

private:
    void _spawn_worker_locked() {
        auto worker = std::make_shared<Worker>();
        std::string name = _pool_name + "_" + std::to_string(_next_worker_id++);
        auto queue = _job_queue;
        worker->thread = std::thread([this, worker, queue, name = std::move(name)] {
            set_current_thread_name(name.c_str());
            work(worker, queue);
        });
        _workers.push_back(std::move(worker));
    }

    void _reap_retired_workers_locked() {
        auto it = _retired_workers.begin();
        while (it != _retired_workers.end()) {
            if ((*it)->exited.load(std::memory_order_acquire) &&
                (*it)->thread.get_id() != std::this_thread::get_id()) {
                if ((*it)->thread.joinable()) {
                    (*it)->thread.join();
                }
                it = _retired_workers.erase(it);
            } else {
                ++it;
            }
        }
    }

    int _resize_locked(size_t target_size) {
        _reap_retired_workers_locked();
        target_size = std::clamp(target_size, _min_size, _max_size);
        size_t cur_size = _workers.size();
        if (target_size > cur_size) {
            size_t to_add = target_size - cur_size;
            for (size_t i = 0; i < to_add; ++i) {
                _spawn_worker_locked();
            }
        } else if (target_size < cur_size) {
            size_t to_remove = cur_size - target_size;
            for (size_t i = 0; i < to_remove; ++i) {
                auto worker = std::move(_workers.back());
                _workers.pop_back();
                worker->should_stop.store(true, std::memory_order_release);
                _retired_workers.push_back(std::move(worker));
            }
        }
        _pool_size.store(target_size, std::memory_order_release);
        return 0;
    }

    /**
     * Working thread
     */
    void work(const std::shared_ptr<Worker>& self,
              const std::shared_ptr<AdaptiveSyncQueue<JobType>>& job_queue) {
        while (!self->should_stop.load(std::memory_order_acquire) &&
               (_is_running.load(std::memory_order_acquire) || !job_queue->empty())) {
            JobType job;
            if (!job_queue->get_with_timeout(&job, 10)) {
                continue;
            }
            _active_threads.fetch_add(1, std::memory_order_relaxed);
            try {
                job();
            } catch (const std::exception& e) {
                std::cerr << "exception happened when execute job. err: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "exception happened when execute job." << std::endl;
            }
            _active_threads.fetch_sub(1, std::memory_order_relaxed);
        }
        self->exited.store(true, std::memory_order_release);
    }
};

/**
 * Factory functions to create ThreadPool instance based on configuration flag
 */
inline std::shared_ptr<ThreadPool> create_shared_thread_pool(
        bool use_adaptive, size_t initial_size, const std::string& name = "", size_t min_size = 1,
        size_t max_size = std::numeric_limits<size_t>::max()) {
    if (use_adaptive) {
        return std::make_shared<AdaptiveThreadPool>(initial_size, name, min_size, max_size);
    }
    return std::make_shared<SimpleThreadPool>(initial_size, name);
}

inline std::unique_ptr<ThreadPool> create_unique_thread_pool(
        bool use_adaptive, size_t initial_size, const std::string& name = "", size_t min_size = 1,
        size_t max_size = std::numeric_limits<size_t>::max()) {
    if (use_adaptive) {
        return std::make_unique<AdaptiveThreadPool>(initial_size, name, min_size, max_size);
    }
    return std::make_unique<SimpleThreadPool>(initial_size, name);
}

} // namespace doris::cloud
