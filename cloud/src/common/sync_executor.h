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

#include <bthread/countdown_event.h>
#include <cpp/sync_point.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <chrono>
#include <future>
#include <string>

#include "common/defer.h"
#include "common/simple_thread_pool.h"

namespace doris::cloud {

template <typename T>
class SyncExecutor {
public:
    SyncExecutor(
            std::shared_ptr<SimpleThreadPool> pool, std::string name_tag,
            std::function<bool(const T&)> cancel = [](const T& /**/) { return false; })
            : _pool(std::move(pool)), _cancel(std::move(cancel)), _name_tag(std::move(name_tag)) {}
    auto add(std::function<T()> callback) -> SyncExecutor<T>& {
        auto task = std::make_unique<Task>(std::move(callback), _cancel, _count);
        _count.add_count();
        // The actual task logic would be wrapped by one promise and passed to the threadpool.
        // The result would be returned by the future once the task is finished.
        // Or the task would be invalid if the whole task is cancelled.
        int r = _pool->submit([this, t = task.get()]() { (*t)(_stop_token); });
        CHECK(r == 0);
        _res.emplace_back(std::move(task));
        return *this;
    }
    std::vector<T> when_all(bool* finished) {
        DORIS_CLOUD_DEFER {
            _reset();
        };
        timespec current_time;
        auto current_time_second = time(nullptr);
        current_time.tv_sec = current_time_second + 300;
        current_time.tv_nsec = 0;
        // Wait for all tasks to complete
        while (0 != _count.timed_wait(current_time)) {
            current_time.tv_sec += 300;
            LOG(WARNING) << _name_tag << " has already taken 5 min, cost: "
                         << time(nullptr) - current_time_second << " seconds";
        }
        *finished = !_stop_token;
        std::vector<T> res;
        res.reserve(_res.size());
        for (auto& task : _res) {
            if (!task->valid()) {
                *finished = false;
                return res;
            }
            size_t max_wait_ms = 10000;
            TEST_SYNC_POINT_CALLBACK("SyncExecutor::when_all.set_wait_time", &max_wait_ms);
            // _count.timed_wait has already ensured that all tasks are completed.
            // The 10 seconds here is just waiting for the task results to be returned,
            // so 10 seconds is more than enough.
            auto status = task->wait_for(max_wait_ms);
            if (status == std::future_status::ready) {
                res.emplace_back(task->get());
            } else {
                *finished = false;
                LOG(WARNING) << _name_tag << " task timed out after 10 seconds";
                return res;
            }
        }
        return res;
    }

private:
    void _reset() {
        _count.reset(0);
        _res.clear();
        _stop_token = false;
    }

private:
    class Task {
    public:
        Task(std::function<T()> callback, std::function<bool(const T&)> cancel,
             bthread::CountdownEvent& count)
                : _callback(std::move(callback)),
                  _cancel(std::move(cancel)),
                  _count(count),
                  _fut(_pro.get_future()) {}
        void operator()(std::atomic_bool& stop_token) {
            DORIS_CLOUD_DEFER {
                _count.signal();
            };
            if (stop_token) {
                _valid = false;
                return;
            }
            T t = _callback();
            // We'll return this task result to user even if this task return error
            // So we don't set _valid to false here
            if (_cancel(t)) {
                stop_token = true;
            }
            _pro.set_value(std::move(t));
        }
        std::future_status wait_for(size_t milliseconds) {
            return _fut.wait_for(std::chrono::milliseconds(milliseconds));
        }
        bool valid() { return _valid; }
        T get() { return _fut.get(); }

    private:
        // It's guarantted that the valid function can only be called inside SyncExecutor's `when_all()` function
        // and only be called when the _count.timed_wait function returned. So there would be no data race for
        // _valid then it doesn't need to be one atomic bool.
        bool _valid = true;
        std::function<T()> _callback;
        std::function<bool(const T&)> _cancel;
        std::promise<T> _pro;
        bthread::CountdownEvent& _count;
        std::future<T> _fut;
    };
    std::vector<std::unique_ptr<Task>> _res;
    // use CountdownEvent to do periodically log using CountdownEvent::time_wait()
    bthread::CountdownEvent _count {0};
    std::atomic_bool _stop_token {false};
    std::shared_ptr<SimpleThreadPool> _pool;
    std::function<bool(const T&)> _cancel;
    std::string _name_tag;
};
} // namespace doris::cloud