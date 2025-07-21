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

#include "vec/exec/executor/tools/simulator/simulation_split.h"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include "vec/exec/executor/split_runner.h"
#include "vec/exec/executor/tools/simulator/scheduled_executor.h"
#include "vec/exec/executor/tools/simulator/simulation_task.h"

namespace doris {
namespace vectorized {

SimulationSplit::SimulationSplit(SimulationTask* task, std::chrono::nanoseconds scheduled_time)
        : _task(task), _scheduled_time(scheduled_time) {}

void SimulationSplit::set_killed() {
    int64_t current_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::high_resolution_clock::now().time_since_epoch())
                                   .count();
    int64_t last_ready_ts_nanos = _last_ready_ts_nanos.load();
    if (last_ready_ts_nanos != -1) {
        _waited_nanos.fetch_add(current_time - last_ready_ts_nanos);
    }

    _killed.store(true);

    _task->set_killed();
}

void SimulationSplit::interrupt() {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _interrupt = true;
    }
    _cv.notify_all();
}

Result<SharedListenableFuture<Void>> SimulationSplit::process_for(
        std::chrono::nanoseconds duration) {
    _calls++;

    int64_t quanta_start_nanos =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch())
                    .count();

    int64_t expected = -1;
    _start_ts_nanos.compare_exchange_strong(expected, quanta_start_nanos);

    expected = -1;
    _last_ready_ts_nanos.compare_exchange_strong(expected, quanta_start_nanos);

    _waited_nanos += quanta_start_nanos - _last_ready_ts_nanos.load();

    bool finished = process();

    int64_t quanta_end_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                       std::chrono::high_resolution_clock::now().time_since_epoch())
                                       .count();

    _processed_nanos += quanta_end_nanos - quanta_start_nanos;

    if (finished) {
        expected = -1;
        _end_ts_nanos.compare_exchange_strong(expected, quanta_end_nanos);

        if (!_killed) {
            _task->split_complete(shared_from_this());
        }

        return SharedListenableFuture<Void>::create_ready(Void {});
    }

    auto blocking_result = get_process_result();
    if (blocking_result.is_done()) {
        set_split_ready();
    } else if (blocking_result.is_error()) {
        set_killed();
    }

    return blocking_result;
}

LeafSplit::LeafSplit(SimulationTask* task, std::chrono::nanoseconds scheduled_time,
                     std::chrono::nanoseconds per_quanta)
        : SimulationSplit(task, scheduled_time), _per_quanta(per_quanta) {}

bool LeafSplit::process() {
    if (processed_nanos() >= scheduled_time_nanos()) {
        return true;
    }

    int64_t need_to_process_nanos =
            std::min(scheduled_time_nanos() - processed_nanos(), _per_quanta.count());
    if (need_to_process_nanos > 0) {
        //        std::this_thread::sleep_for(std::chrono::nanoseconds(need_to_process_nanos));
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait_for(lock, std::chrono::nanoseconds(need_to_process_nanos),
                     [this] { return _interrupt; });
        if (_interrupt) {
            set_killed();
            return true;
        }
    }

    return false;
}

SharedListenableFuture<Void> LeafSplit::get_process_result() {
    return SharedListenableFuture<Void>::create_ready(Void {});
}

IntermediateSplit::IntermediateSplit(SimulationTask* task, std::chrono::nanoseconds wall_time,
                                     int64_t num_quantas, std::chrono::nanoseconds per_quanta,
                                     std::chrono::nanoseconds between_quantas,
                                     std::chrono::nanoseconds scheduled_time)
        : SimulationSplit(task, scheduled_time),
          _wall_time(wall_time),
          _num_quantas(num_quantas),
          _per_quanta(per_quanta),
          _between_quantas(between_quantas),
          _executor(std::make_shared<ScheduledThreadPoolExecutor>(1)) {
    static_cast<void>(_wall_time);
}

bool IntermediateSplit::process() {
    if (_calls < _num_quantas) {
        //        std::this_thread::sleep_for(_per_quanta);
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait_for(lock, _per_quanta, [this] { return _interrupt; });
        if (_interrupt) {
            set_killed();
            return true;
        }
        return false;
    }

    return true;
}

SharedListenableFuture<Void> IntermediateSplit::get_process_result() {
    _future = SharedListenableFuture<Void>();

    _executor->schedule(
            [this]() {
                if (!_executor->is_shutdown()) {
                    set_split_ready();
                    _future.set_value(Void {});
                } else {
                    set_killed();
                    _future.set_error(Status::InternalError("scheduled executor is shutdown"));
                }
            },
            _between_quantas);

    return _future;
}

} // namespace vectorized
} // namespace doris
