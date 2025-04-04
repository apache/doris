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

#include "vec/exec/executor/time_sharing/prioritized_split_runner.h"

#include <fmt/format.h>

#include <chrono>
#include <functional>
#include <thread>

#include "vec/exec/executor/time_sharing/time_sharing_task_handle.h"

namespace doris {
namespace vectorized {

std::atomic<int64_t> PrioritizedSplitRunner::_next_worker_id(0);

PrioritizedSplitRunner::PrioritizedSplitRunner(std::shared_ptr<TimeSharingTaskHandle> task_handle,
                                               int split_id,
                                               std::shared_ptr<SplitRunner> split_runner,
                                               std::shared_ptr<Ticker> ticker)
        : _task_handle(std::move(task_handle)),
          _split_id(split_id),
          _worker_id(_next_worker_id.fetch_add(1, std::memory_order_relaxed)),
          _split_runner(std::move(split_runner)),
          _ticker(ticker) {
    update_level_priority();
}

//PrioritizedSplitRunner::PrioritizedSplitRunner(CounterStats& globalCpuTimeMicros,
//                                               CounterStats& globalScheduledTimeMicros,
//                                               TimeStats& blockedQuantaWallTime,
//                                               TimeStats& unblockedQuantaWallTime)
//        : _created_nanos(std::chrono::steady_clock::now().time_since_epoch().count()),
//          _split_id(0),
//          _worker_id(_next_worker_id.fetch_add(1, std::memory_order_relaxed)),
//          _ticker(std::make_shared<SystemTicker>()),
//          _global_cpu_time_micros(globalCpuTimeMicros),
//          _global_scheduled_time_micros(globalScheduledTimeMicros),
//          _blocked_quanta_wall_time(blockedQuantaWallTime),
//          _unblocked_quanta_wall_time(unblockedQuantaWallTime) {
//}

Status PrioritizedSplitRunner::init() {
    return _split_runner->init();
}

std::shared_ptr<TimeSharingTaskHandle> PrioritizedSplitRunner::task_handle() const {
    return _task_handle;
}

SharedListenableFuture<Void> PrioritizedSplitRunner::finished_future() {
    return _finished_future;
}

bool PrioritizedSplitRunner::is_closed() const {
    return _closed.load();
}

void PrioritizedSplitRunner::close(const Status& status) {
    if (!_closed.exchange(true)) {
        _split_runner->close(status);
    }
}

int64_t PrioritizedSplitRunner::created_nanos() const {
    return _created_nanos;
}

bool PrioritizedSplitRunner::is_finished() {
    bool finished = _split_runner->is_finished();
    if (finished) {
        _finished_future.set_value({});
    }
    return finished || _closed.load() || _task_handle->is_closed();
}

Status PrioritizedSplitRunner::finished_status() {
    return _split_runner->finished_status();
}

int64_t PrioritizedSplitRunner::scheduled_nanos() const {
    return _scheduled_nanos.load();
}

Result<SharedListenableFuture<Void>> PrioritizedSplitRunner::process() {
    if (is_closed()) {
        SharedListenableFuture<Void> future;
        future.set_value({});
        return future;
    }

    auto start_nanos = std::chrono::steady_clock::now().time_since_epoch().count();
    int64_t expected = 0;
    _start.compare_exchange_strong(expected, start_nanos);
    _last_ready.compare_exchange_strong(expected, start_nanos);
    _process_calls.fetch_add(1);

    _wait_nanos.fetch_add(start_nanos - _last_ready.load());

    auto process_start_time = _ticker->read();
    auto blocked = _split_runner->process_for(SPLIT_RUN_QUANTA);
    if (!blocked.has_value()) {
        return unexpected(std::move(blocked).error());
    }
    auto process_end_time = _ticker->read();
    auto quanta_scheduled_nanos = process_end_time - process_start_time;

    _scheduled_nanos.fetch_add(quanta_scheduled_nanos);

    // _priority.store(_task_handle->add_scheduled_nanos(quanta_scheduled_nanos));

    {
        std::lock_guard<std::mutex> lock(_priority_mutex);
        _priority = _task_handle->add_scheduled_nanos(quanta_scheduled_nanos);
    }

    return blocked;
}

void PrioritizedSplitRunner::set_ready() {
    _last_ready.store(_ticker->read());
}

/**
 * Updates the (potentially stale) priority value cached in this object.
 * This should be called when this object is outside the queue.
 *
 * @return true if the level changed.
 */
//bool PrioritizedSplitRunner::update_level_priority() {
//    Priority new_priority = _task_handle->priority();
//    Priority old_priority = _priority.exchange(new_priority);
//    return new_priority.level() != old_priority.level();
//}

bool PrioritizedSplitRunner::update_level_priority() {
    std::lock_guard<std::mutex> lock(_priority_mutex);
    Priority new_priority = _task_handle->priority();
    Priority old_priority = _priority;
    _priority = new_priority;
    return new_priority.level() != old_priority.level();
}

//
//void PrioritizedSplitRunner::reset_level_priority() {
//     _priority.store(_task_handle->reset_level_priority());
//}

void PrioritizedSplitRunner::reset_level_priority() {
    std::lock_guard<std::mutex> lock(_priority_mutex);
    _priority = _task_handle->reset_level_priority();
}

int64_t PrioritizedSplitRunner::worker_id() const {
    return _worker_id;
}

int PrioritizedSplitRunner::split_id() const {
    return _split_id;
}

//Priority PrioritizedSplitRunner::priority() const {
//    return _priority.load();
//}

Priority PrioritizedSplitRunner::priority() const {
    std::lock_guard<std::mutex> lock(_priority_mutex);
    return _priority;
}

std::string PrioritizedSplitRunner::get_info() const {
    // const auto now = std::chrono::steady_clock::now();
    // const int64_t current_time = now.time_since_epoch().count();
    // const int64_t wall_time = (current_time - _start.load()) / 1'000'000; // 转换为毫秒

    // return fmt::format(
    //     "Split {:<15}-{} {} (start = {:.1f}, wall = {} ms, cpu = {} ms, wait = {} ms, calls = {})",
    //     _task_handle->get_task_id(),       // 假设返回字符串类型
    //     _split_id,                         // 整数类型
    //     _split_runner->get_info(),         // 需要确保SplitRunner有get_info()
    //     _start.load() / 1'000'000.0,       // 转换为毫秒并保留1位小数
    //     wall_time,                         // 已计算的耗时
    //     _global_cpu_time_micros.get() / 1000, // 微秒转毫秒
    //     _wait_nanos.load() / 1'000'000,    // 纳秒转毫秒
    //     _process_calls.load()              // 调用次数
    // );
    return "";
}

} // namespace vectorized
} // namespace doris
