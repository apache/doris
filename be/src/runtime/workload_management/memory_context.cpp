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

#include "runtime/workload_management/memory_context.h"

#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {

void MemoryContext::set_mem_tracker(const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    mem_tracker_ = mem_tracker;
    user_set_mem_limit_ = mem_tracker_->limit();
    adjusted_mem_limit_ = mem_tracker_->limit();
    refresh_memory_limit_parent();
}

void MemoryContext::set_resource_ctx(ResourceContext* resource_ctx) {
    resource_ctx_ = resource_ctx;
    refresh_memory_limit_parent();
}

void MemoryContext::refresh_memory_limit_parent(bool reset_to_process) {
    // ResourceContext is also used for WG/IO policy evaluation without a task
    // tracker. WG and tracker initialization can happen in either order.
    if (!mem_tracker_) {
        return;
    }
    auto wg = resource_ctx_ ? resource_ctx_->workload_group() : nullptr;
    // AttachTask(tracker) borrows a shared task tracker in a temporary context
    // without a WG. It must not strip the original task's WG parent.
    if (!wg && !reset_to_process) {
        return;
    }
    std::shared_ptr<MemoryLimit> parent =
            wg ? std::static_pointer_cast<MemoryLimit>(wg) : MemoryLimit::process_memory_limit();
    mem_tracker_->set_memory_limit_parent(parent);
    // Write memory is a sibling: it must not acquire the query exec_mem_limit.
    // The public limiter constructor creates a task node without a write sibling;
    // create_shared() additionally creates the write tracker.
    if (auto write_tracker = mem_tracker_->write_tracker()) {
        write_tracker->set_memory_limit_parent(std::move(parent));
    }
}

std::string MemoryContext::debug_string() {
    return fmt::format("TaskId={}, Memory(Used={}, Limit={}, Peak={})",
                       print_id(resource_ctx_->task_controller()->task_id()),
                       PrettyPrinter::print_bytes(current_memory_bytes()),
                       PrettyPrinter::print_bytes(mem_limit()),
                       PrettyPrinter::print_bytes(peak_memory_bytes()));
}

} // namespace doris
