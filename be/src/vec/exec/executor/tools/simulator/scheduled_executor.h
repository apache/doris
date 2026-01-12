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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

#include "vec/exec/executor/tools/simulator/simulation_split.h"

namespace doris {
namespace vectorized {

struct ScheduledTask {
    std::chrono::steady_clock::time_point execute_at;
    std::function<void()> task;

    bool operator>(const ScheduledTask& other) const { return execute_at > other.execute_at; }
};

class ScheduledExecutor {
public:
    virtual ~ScheduledExecutor() = default;
    virtual void schedule(std::function<void()> task, std::chrono::nanoseconds delay) = 0;
    virtual void shutdown() = 0;
    virtual bool is_shutdown() const = 0;
};

class ScheduledThreadPoolExecutor : public ScheduledExecutor {
public:
    ScheduledThreadPoolExecutor(size_t numThreads) {
        for (size_t i = 0; i < numThreads; ++i) {
            _workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> _lock(_mutex);
                        _condition.wait(_lock, [this] { return _stop.load() || !_tasks.empty(); });

                        if (_stop.load() && _tasks.empty()) return;

                        auto now = std::chrono::steady_clock::now();
                        if (_tasks.top().execute_at <= now) {
                            task = std::move(_tasks.top().task);
                            _tasks.pop();
                        } else {
                            _condition.wait_until(_lock, _tasks.top().execute_at);
                            continue;
                        }
                    }
                    task();
                }
            });
        }
    }

    void schedule(std::function<void()> f, std::chrono::nanoseconds delay) override {
        auto execute_at = std::chrono::steady_clock::now() + delay;
        {
            std::unique_lock<std::mutex> _lock(_mutex);
            if (_stop.load())
                throw std::runtime_error("Submit on stopped ScheduledThreadPoolExecutor");
            _tasks.push(ScheduledTask {execute_at, std::move(f)});
        }
        _condition.notify_one();
    }

    bool is_shutdown() const override { return _stop.load(); }

    void shutdown() override {
        {
            std::unique_lock<std::mutex> _lock(_mutex);
            _stop.store(true);
        }
        _condition.notify_all();
    }

    ~ScheduledThreadPoolExecutor() {
        shutdown();
        for (std::thread& worker : _workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, std::greater<ScheduledTask>>
            _tasks;
    std::vector<std::thread> _workers;
    std::mutex _mutex;
    std::condition_variable _condition;
    std::atomic<bool> _stop {false};
};

} // namespace vectorized
} // namespace doris
