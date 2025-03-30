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
#include <memory>

#include "common/factory_creator.h"
#include "vec/exec/executor/listenable_future.h"
#include "vec/exec/executor/split_runner.h"
#include "vec/exec/executor/ticker.h"
#include "vec/exec/executor/time_sharing/priority.h"

namespace doris {
namespace vectorized {

class TimeSharingTaskHandle;

class PrioritizedSplitRunner : public std::enable_shared_from_this<PrioritizedSplitRunner> {
    ENABLE_FACTORY_CREATOR(PrioritizedSplitRunner);

public:
    static constexpr auto SPLIT_RUN_QUANTA = std::chrono::seconds(1);

    PrioritizedSplitRunner(std::shared_ptr<TimeSharingTaskHandle> task_handle, int split_id,
                           std::shared_ptr<SplitRunner> split_runner,
                           std::shared_ptr<Ticker> ticker);

    Status init();

    virtual ~PrioritizedSplitRunner() = default;

    std::shared_ptr<TimeSharingTaskHandle> task_handle() const;
    SharedListenableFuture<Void> finished_future();
    bool is_closed() const;
    void close(const Status& status);
    int64_t created_nanos() const;
    bool is_finished();
    Status finished_status();
    int64_t scheduled_nanos() const;
    Result<SharedListenableFuture<Void>> process();
    void set_ready();
    bool update_level_priority();
    void reset_level_priority();
    int64_t worker_id() const;
    int split_id() const;
    virtual Priority priority() const;

    std::string get_info() const;

private:
    static std::atomic<int64_t> _next_worker_id;

    const int64_t _created_nanos {std::chrono::steady_clock::now().time_since_epoch().count()};
    std::shared_ptr<TimeSharingTaskHandle> _task_handle;
    const int _split_id;
    const int64_t _worker_id;
    std::shared_ptr<SplitRunner> _split_runner;
    std::shared_ptr<Ticker> _ticker;
    SharedListenableFuture<Void> _finished_future {};

    std::atomic<bool> _closed {false};
    Priority _priority {0, 0};
    mutable std::mutex _priority_mutex;
    std::atomic<int64_t> _last_ready {0};
    std::atomic<int64_t> _start {0};
    std::atomic<int64_t> _scheduled_nanos {0};
    std::atomic<int64_t> _wait_nanos {0};
    std::atomic<int> _process_calls {0};
};

} // namespace vectorized
} // namespace doris
