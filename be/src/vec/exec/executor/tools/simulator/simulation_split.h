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
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include "vec/exec/executor/split_runner.h"

namespace doris {
namespace vectorized {

class SimulationTask;
class ScheduledExecutor;

class SimulationSplit : public SplitRunner, public std::enable_shared_from_this<SimulationSplit> {
public:
    Status init() override { return Status::OK(); }

    Result<SharedListenableFuture<Void>> process_for(std::chrono::nanoseconds duration) override;

    void close(const Status& status) override {}

    bool is_finished() override { return _end_ts_nanos.load() >= 0; }

    Status finished_status() override { return Status::OK(); }

    std::string get_info() const override { return ""; };

    virtual bool process() = 0;

    virtual SharedListenableFuture<Void> get_process_result() = 0;

    int64_t start_ts_nanos() const { return _start_ts_nanos.load(); }
    int64_t end_ts_nanos() const { return _end_ts_nanos.load(); }
    int64_t processed_nanos() const { return _processed_nanos.load(); }
    int64_t waited_nanos() const { return _waited_nanos.load(); }
    auto calls() const { return _calls.load(); }
    int64_t scheduled_time_nanos() const { return _scheduled_time.count(); }
    bool is_killed() const { return _killed.load(); }
    void set_killed();
    void interrupt();

protected:
    SimulationSplit(SimulationTask* task, std::chrono::nanoseconds scheduled);

    void set_split_ready() {
        _last_ready_ts_nanos.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::high_resolution_clock::now().time_since_epoch())
                        .count());
    }

    SimulationTask* _task;

    std::atomic<int> _calls {0};
    const int64_t _created_nanos {
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch())
                    .count()};
    std::atomic<int64_t> _processed_nanos {0};
    std::atomic<int64_t> _start_ts_nanos {-1};
    std::atomic<int64_t> _end_ts_nanos {-1};
    std::atomic<int64_t> _waited_nanos {0};
    std::atomic<int64_t> _last_ready_ts_nanos {-1};
    std::atomic<bool> _killed {false};

    const std::chrono::nanoseconds _scheduled_time;

    std::mutex _mutex;
    std::condition_variable _cv;
    bool _interrupt {false};
};

class LeafSplit : public SimulationSplit {
public:
    LeafSplit(SimulationTask* task, std::chrono::nanoseconds scheduled_time,
              std::chrono::nanoseconds per_quanta);

    bool process() override;

    SharedListenableFuture<Void> get_process_result() override;

private:
    std::chrono::nanoseconds _per_quanta;
};

class IntermediateSplit : public SimulationSplit {
    std::chrono::nanoseconds _wall_time;
    int64_t _num_quantas;
    std::chrono::nanoseconds _per_quanta;
    std::chrono::nanoseconds _between_quantas; // The blocking sleep time between quanta.
    std::shared_ptr<ScheduledExecutor> _executor;
    SharedListenableFuture<Void> _future;

public:
    IntermediateSplit(SimulationTask* task, std::chrono::nanoseconds wall_time, int64_t num_quantas,
                      std::chrono::nanoseconds per_quanta, std::chrono::nanoseconds between_quanta,
                      std::chrono::nanoseconds scheduled);

    void set_executor(std::shared_ptr<ScheduledExecutor> executor) { _executor = executor; }

    bool process() override;

    SharedListenableFuture<Void> get_process_result() override;
};

} // namespace vectorized
} // namespace doris