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

// Every task should have its own resource context. And BE may adjust the resource
// context during running.
// Workload group will hold the resource context and do some control work.
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

    // The caller should not hold the object, since it is a raw pointer.
    CPUContext* cpu_context() { return cpu_context_.get(); }
    MemoryContext* memory_context() { return memory_context_.get(); }
    IOContext* io_context() { return io_context_.get(); }
    TaskController* task_controller() { return task_controller_.get(); }

    void set_cpu_context(std::shared_ptr<CPUContext> cpu_context) { cpu_context_ = cpu_context; }
    void set_memory_context(std::shared_ptr<MemoryContext> memory_context) {
        memory_context_ = memory_context;
    }
    void set_io_context(std::shared_ptr<IOContext> io_context) { io_context_ = io_context; }
    void set_task_controller(std::shared_ptr<TaskController> task_controller) {
        task_controller_ = task_controller;
    }

    void set_workload_group(std::shared_ptr<WorkloadGroup> wg) {
        // update all child context's workload group property
        workload_group_ = wg;
    }

    std::shared_ptr<WorkloadGroup> workload_group() { return workload_group_.lock(); }

private:
    // The controller's init value is nullptr, it means the resource context will ignore this controller.
    std::shared_ptr<CPUContext> cpu_context_ = nullptr;
    std::shared_ptr<MemoryContext> memory_context_ = nullptr;
    std::shared_ptr<IOContext> io_context_ = nullptr;
    std::shared_ptr<TaskController> task_controller_ = nullptr;
    // Workload group will own resource context, so that resource context only have weak ptr for workload group.
    // TODO: should use atomic weak ptr to avoid the concurrent modification of the pointer.
    std::weak_ptr<WorkloadGroup> workload_group_ = nullptr;
};

} // namespace doris
