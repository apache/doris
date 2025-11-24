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
#include <future>
#include <memory>
#include <optional>
#include <vector>

#include "vec/exec/executor/listenable_future.h"
#include "vec/exec/executor/split_runner.h"
#include "vec/exec/executor/task_id.h"

namespace doris {
namespace vectorized {

class TaskHandle;

class TaskExecutor {
public:
    virtual ~TaskExecutor() = default;

    virtual Status init() = 0;
    virtual Status start() = 0;
    virtual void stop() = 0;

    virtual Result<std::shared_ptr<TaskHandle>> create_task(
            const TaskId& task_id, std::function<double()> utilization_supplier,
            int initial_split_concurrency,
            std::chrono::nanoseconds split_concurrency_adjust_frequency,
            std::optional<int> max_drivers_per_task) = 0;

    virtual Status add_task(const TaskId& task_id, std::shared_ptr<TaskHandle> task_handle) = 0;

    virtual Status remove_task(std::shared_ptr<TaskHandle> task_handle) = 0;

    virtual Result<std::vector<SharedListenableFuture<Void>>> enqueue_splits(
            std::shared_ptr<TaskHandle> task_handle, bool intermediate,
            const std::vector<std::shared_ptr<SplitRunner>>& splits) = 0;

    virtual Status re_enqueue_split(std::shared_ptr<TaskHandle> task_handle, bool intermediate,
                                    const std::shared_ptr<SplitRunner>& split) = 0;
};

} // namespace vectorized
} // namespace doris
