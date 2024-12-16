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

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>

#include "common/status.h"

namespace doris {

// Any task that allow cancel should implement this class.
class TaskController {
    ENABLE_FACTORY_CREATOR(TaskController);

public:
    virtual Status cancel(Status cancel_reason) { return Status::OK(); }
    virtual Status running_time(int64_t* running_time_msecs) {
        *running_time_msecs = 0;
        return Status::OK();
    }
};

class WorkloadGroupContext {
    ENABLE_FACTORY_CREATOR(WorkloadGroupContext);

public:
    WorkloadGroupContext() = default;
    virtual ~WorkloadGroupContext() = default;

    WorkloadGroupPtr workload_group() { return _workload_group; }
    void set_workload_group(WorkloadGroupPtr wg) { _workload_group = wg; }

private:
    WorkloadGroupPtr _workload_group = nullptr;
};

// Every task should have its own resource context. And BE may adjust the resource
// context during running.
// ResourceContext contains many contexts or controller, the task could implements their
// own implementation.
class ResourceContext : public std::enable_shared_from_this<ResourceContext> {
    ENABLE_FACTORY_CREATOR(ResourceContext);

public:
    ResourceContext() {
        // These all default values, it may be reset.
        cpu_context_ = std::make_shared<CPUContext>();
        memory_context_ = std::make_shared<MemoryContext>();
        io_context_ = std::make_shared<IOContext>();
        reclaimer_ = std::make_shared<ResourceReclaimer>();
    }
    virtual ~ResourceContext() = default;

    // Only return the raw pointer to the caller, so that the caller should not save it to other variables.
    CPUContext* cpu_context() { return cpu_context_.get(); }
    MemoryContext* memory_context() { return memory_context_.get(); }
    IOContext* io_context() { return io_context_.get(); }
    WorkloadGroupContext* workload_group_context() { return workload_group_context_.get(); }
    TaskController* task_controller() { return task_controller_.get(); }

    void set_cpu_context(std::shared_ptr<CPUContext> cpu_context) { cpu_context_ = cpu_context; }
    void set_memory_context(std::shared_ptr<MemoryContext> memory_context) {
        memory_context_ = memory_context;
    }
    void set_io_context(std::shared_ptr<IOContext> io_context) { io_context_ = io_context; }
    void set_workload_group_context(std::shared_ptr<WorkloadGroupContext> wg_context) {
        workload_group_context_ = wg_context;
    }
    void set_task_controller(std::shared_ptr<TaskController> task_controller) {
        task_controller_ = task_controller;
    }

private:
    // The controller's init value is nullptr, it means the resource context will ignore this controller.
    std::shared_ptr<CPUContext> cpu_context_ = nullptr;
    std::shared_ptr<MemoryContext> memory_context_ = nullptr;
    std::shared_ptr<IOContext> io_context_ = nullptr;
    std::shared_ptr<WorkloadGroupContext> workload_group_context_ = nullptr;
    std::shared_ptr<TaskController> task_controller_ = nullptr;
};

} // namespace doris
