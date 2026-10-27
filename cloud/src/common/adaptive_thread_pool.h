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

#include <glog/logging.h>
#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/bvars.h"
#include "common/config.h"

namespace doris::cloud {

// A thread pool whose concurrency follows a shrink policy (AIMD):
//   - the policy asks to shrink: halve the concurrency, at most once per
//     `adaptive_thread_pool_shrink_interval_ms`;
//   - the policy does not ask to shrink for `adaptive_thread_pool_expand_interval_ms`: add
//     `adaptive_thread_pool_expand_step` workers, then wait another interval before the next one.
// The concurrency is kept in [min(adaptive_thread_pool_min_size, max_size), max_size].
//
// The interface and return codes are the same as SimpleThreadPool, so a caller switches
// by changing the type only. `max_size` threads are created by start(), the ones beyond the
// current concurrency just wait. The queue capacity is 2 * concurrency, so submitters are
// slowed down together with the workers.
class AdaptiveThreadPool {
public:
    using JobType = std::function<void()>;
    // Returns true if the pool should shrink, e.g. the downstream is under pressure. It is called
    // at most once per second from a worker thread, so it must be cheap and must not block.
    using ShrinkPolicy = std::function<bool()>;

    AdaptiveThreadPool(size_t max_size, std::string name, ShrinkPolicy should_shrink)
            : _max_size(max_size),
              _name(std::move(name)),
              _should_shrink(std::move(should_shrink)),
              _concurrency(max_size) {
        CHECK_GT(_max_size, 0);
    }

    AdaptiveThreadPool(const AdaptiveThreadPool&) = delete;
    AdaptiveThreadPool& operator=(const AdaptiveThreadPool&) = delete;

    ~AdaptiveThreadPool() { stop(); }

    /**
     * @return always 0
     */
    int start() {
        auto now = Clock::now();
        {
            std::lock_guard lock(_mtx);
            CHECK(!_running) << "thread pool " << _name << " already started";
            _running = true;
            _next_adjust = now;
            _next_shrink = now;
            _next_expand = now + expand_interval();
        }
        g_bvar_adaptive_thread_pool_concurrency.put({_name}, static_cast<int64_t>(_concurrency));
        for (size_t i = 0; i < _max_size; ++i) {
            _workers.emplace_back(&AdaptiveThreadPool::work, this);
            std::string thread_name = _name + "_" + std::to_string(i);
            pthread_setname_np(_workers.back().native_handle(), thread_name.c_str());
        }
        return 0;
    }

    /**
     * Submits a job, blocks while the job queue is full
     *
     * @return 0 if succeeded, -1 if the thread pool is not running
     */
    int submit(JobType f) {
        std::unique_lock lock(_mtx);
        _not_full.wait(lock, [this] { return !_running || !queue_full(); });
        if (!_running) {
            return -1;
        }
        enqueue(std::move(f));
        return 0;
    }

    /**
     * Submits a job with a limited blocking time
     *
     * @param timeout max blocking time in milliseconds
     * @return 0 if succeeded
     *         -1 if the thread pool is not running
     *         -2 if timed out
     */
    int submit_with_timeout(JobType f, int timeout) {
        std::unique_lock lock(_mtx);
        if (!_not_full.wait_for(lock, std::chrono::milliseconds(timeout),
                                [this] { return !_running || !queue_full(); })) {
            return -2;
        }
        if (!_running) {
            return -1;
        }
        enqueue(std::move(f));
        return 0;
    }

    /**
     * Stops accepting jobs, waits until all queued jobs are done
     *
     * @return 0 if succeeded, -1 if the thread pool is not running
     */
    int stop() {
        {
            std::lock_guard lock(_mtx);
            if (!_running) {
                return -1;
            }
            _running = false;
        }
        _not_full.notify_all();
        _runnable.notify_all();
        for (auto& t : _workers) {
            t.join();
        }
        _workers.clear();
        return 0;
    }

    /**
     * @return the max thread number of the pool
     */
    size_t size() const { return _max_size; }

    /**
     * @return the number of jobs allowed to run at the same time
     */
    size_t concurrency() const {
        std::lock_guard lock(_mtx);
        return _concurrency;
    }

private:
    using Clock = std::chrono::steady_clock;
    static constexpr auto kAdjustInterval = std::chrono::seconds(1);

    static std::chrono::milliseconds shrink_interval() {
        return std::chrono::milliseconds(config::adaptive_thread_pool_shrink_interval_ms);
    }
    static std::chrono::milliseconds expand_interval() {
        return std::chrono::milliseconds(config::adaptive_thread_pool_expand_interval_ms);
    }
    size_t min_size() const {
        return std::clamp<size_t>(config::adaptive_thread_pool_min_size, 1, _max_size);
    }
    static size_t expand_step() {
        return static_cast<size_t>(std::max<int32_t>(1, config::adaptive_thread_pool_expand_step));
    }

    // The following functions must be called with `_mtx` held.
    bool queue_full() const { return _jobs.size() >= 2 * _concurrency; }

    void enqueue(JobType f) {
        _jobs.push_back(std::move(f));
        _runnable.notify_one();
    }

    void work() {
        while (true) {
            JobType job;
            {
                std::unique_lock lock(_mtx);
                _runnable.wait(lock, [this] {
                    return (!_jobs.empty() && _num_active < _concurrency) ||
                           (!_running && _jobs.empty());
                });
                if (_jobs.empty()) {
                    // Stopped and all jobs are done. Wake up the workers still waiting for a free
                    // slot, the notify_one() after each job is not enough to wake all of them
                    // after a shrink.
                    _runnable.notify_all();
                    return;
                }
                job = std::move(_jobs.front());
                _jobs.pop_front();
                ++_num_active;
            }
            _not_full.notify_one();

            try {
                job();
            } catch (const std::exception& e) {
                LOG(WARNING) << "exception happened when execute job, pool=" << _name
                             << " err=" << e.what();
            } catch (...) {
                LOG(WARNING) << "exception happened when execute job, pool=" << _name;
            }

            {
                std::lock_guard lock(_mtx);
                --_num_active;
            }
            _runnable.notify_one();
            maybe_adjust();
        }
    }

    // Called by a worker after each job. Consults the shrink policy at most once per
    // kAdjustInterval and moves the concurrency by at most one step (a halving or an
    // expand step).
    void maybe_adjust() {
        auto now = Clock::now();
        if (!should_adjust(now)) {
            return;
        }
        // The policy is user code, call it without holding the lock.
        bool shrink = _should_shrink();

        std::unique_lock lock(_mtx);
        size_t old_concurrency = _concurrency;
        _concurrency = shrink ? shrunk_concurrency(now) : expanded_concurrency(now);
        size_t new_concurrency = _concurrency;
        lock.unlock();

        if (new_concurrency == old_concurrency) {
            return;
        }
        if (new_concurrency > old_concurrency) {
            // Waiting workers may run more jobs, waiting submitters may enqueue more jobs.
            _runnable.notify_all();
            _not_full.notify_all();
        }
        g_bvar_adaptive_thread_pool_concurrency.put({_name}, static_cast<int64_t>(new_concurrency));
        LOG(INFO) << "adaptive thread pool " << (shrink ? "shrinks" : "expands")
                  << ", pool=" << _name << " concurrency=" << old_concurrency << "->"
                  << new_concurrency << " max_size=" << _max_size;
    }

    // Returns true if this worker should consult the shrink policy now. Only one worker does it
    // per kAdjustInterval, the others return false.
    bool should_adjust(Clock::time_point now) {
        std::lock_guard lock(_mtx);
        if (now < _next_adjust) {
            return false;
        }
        _next_adjust = now + kAdjustInterval;
        return true;
    }

    // The following two functions must be called with `_mtx` held, they return the new
    // concurrency and update the deadlines.

    // Multiplicative decrease: halve the concurrency, but not below the min size. After a shrink,
    // wait a shrink interval before the next one, so the effect of the last one can be seen.
    size_t shrunk_concurrency(Clock::time_point now) {
        // A shrink is asked, the shrink-free period required for expanding starts over.
        _next_expand = now + expand_interval();

        bool shrunk_recently = now < _next_shrink;
        if (shrunk_recently || _concurrency <= min_size()) {
            return _concurrency;
        }
        _next_shrink = now + shrink_interval();
        return std::max(min_size(), _concurrency / 2);
    }

    // Additive increase: add a step of workers (not above the max size) after each whole
    // expand interval without a shrink asked.
    size_t expanded_concurrency(Clock::time_point now) {
        bool shrink_free_long_enough = now >= _next_expand;
        if (!shrink_free_long_enough || _concurrency >= _max_size) {
            return _concurrency;
        }
        _next_expand = now + expand_interval();
        return std::min(_max_size, _concurrency + expand_step());
    }

    const size_t _max_size;
    const std::string _name;
    const ShrinkPolicy _should_shrink;

    mutable std::mutex _mtx;
    std::condition_variable _runnable; // a queued job may be taken by a worker
    std::condition_variable _not_full; // a submitter may enqueue
    std::deque<JobType> _jobs;
    std::vector<std::thread> _workers;
    bool _running = false;
    size_t _num_active = 0;
    size_t _concurrency;
    // Deadlines driving the AIMD policy, see maybe_adjust().
    Clock::time_point _next_adjust; // earliest time to consult the shrink policy again
    Clock::time_point _next_shrink; // earliest time to halve the concurrency again
    Clock::time_point _next_expand; // earliest time to add workers, pushed back by a shrink ask
};

} // namespace doris::cloud
