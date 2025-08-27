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

#include <gen_cpp/data.pb.h>

#include <memory>

#include "common/factory_creator.h"
#include "common/multi_version.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/cpu_context.h"
#include "runtime/workload_management/io_context.h"
#include "runtime/workload_management/memory_context.h"
#include "runtime/workload_management/task_controller.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"

// Every task should have its own resource context. And BE may adjust the resource
// context during running.
// ResourceContext contains many contexts or controller, the task could implements their
// own implementation.
class ResourceContext : public std::enable_shared_from_this<ResourceContext> {
    ENABLE_FACTORY_CREATOR(ResourceContext);

public:
    ResourceContext() {
        // These all default values, it may be reset.
        cpu_context_ = CPUContext::create_unique();
        memory_context_ = MemoryContext::create_unique();
        io_context_ = IOContext::create_unique();
        task_controller_ = TaskController::create_unique();

        cpu_context_->set_resource_ctx(this);
        memory_context_->set_resource_ctx(this);
        io_context_->set_resource_ctx(this);
        task_controller_->set_resource_ctx(this);
    }
    ~ResourceContext() = default;

    // Only return the raw pointer to the caller, so that the caller should not save it to other variables.
    CPUContext* cpu_context() const { return cpu_context_.get(); }
    MemoryContext* memory_context() const { return memory_context_.get(); }
    IOContext* io_context() const { return io_context_.get(); }
    TaskController* task_controller() const { return task_controller_.get(); }
    WorkloadGroupPtr workload_group() const { return _workload_group; }

    void set_cpu_context(std::unique_ptr<CPUContext> cpu_context) {
        cpu_context_ = std::move(cpu_context);
        cpu_context_->set_resource_ctx(this);
    }
    void set_memory_context(std::unique_ptr<MemoryContext> memory_context) {
        memory_context_ = std::move(memory_context);
        memory_context_->set_resource_ctx(this);
    }
    void set_io_context(std::unique_ptr<IOContext> io_context) {
        io_context_ = std::move(io_context);
        io_context_->set_resource_ctx(this);
    }
    void set_task_controller(std::unique_ptr<TaskController> task_controller) {
        task_controller_ = std::move(task_controller);
        task_controller_->set_resource_ctx(this);
    }
    void set_workload_group(WorkloadGroupPtr wg) { _workload_group = wg; }

    RuntimeProfile* profile() { return const_cast<RuntimeProfile*>(resource_profile_.get().get()); }

    void to_thrift_query_statistics(TQueryStatistics* statistics) const;

    std::string debug_string() { return resource_profile_.get()->pretty_print(); }
    void refresh_resource_profile() {
        std::unique_ptr<RuntimeProfile> resource_profile =
                std::make_unique<RuntimeProfile>("ResourceContext");

        RuntimeProfile* cpu_profile =
                resource_profile->create_child(cpu_context_->stats_profile()->name(), true, false);
        cpu_profile->merge(cpu_context_->stats_profile());
        RuntimeProfile* memory_profile = resource_profile->create_child(
                memory_context_->stats_profile()->name(), true, false);
        memory_profile->merge(memory_context_->stats_profile());
        RuntimeProfile* io_profile =
                resource_profile->create_child(io_context_->stats_profile()->name(), true, false);
        io_profile->merge(io_context_->stats_profile());

        resource_profile_.set(std::move(resource_profile));
    }

private:
    // The controller's init value is nullptr, it means the resource context will ignore this controller.
    std::unique_ptr<CPUContext> cpu_context_ = nullptr;
    std::unique_ptr<MemoryContext> memory_context_ = nullptr;
    std::unique_ptr<IOContext> io_context_ = nullptr;
    std::unique_ptr<TaskController> task_controller_ = nullptr;

    WorkloadGroupPtr _workload_group = nullptr;
    MultiVersion<RuntimeProfile> resource_profile_;
};

#include "common/compile_check_end.h"
} // namespace doris
