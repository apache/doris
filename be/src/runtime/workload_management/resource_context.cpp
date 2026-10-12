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

#include "runtime/workload_management/resource_context.h"

#include <fmt/format.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/check.h"
#include "common/config.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/query_task_controller.h"
#include "util/pretty_printer.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

namespace {

// The resource contexts created while memleak-check mode is on. Their addresses are kept
// disguised (bitwise inverted), so that LeakSanitizer does not take the registry for a
// reference: a context that nothing else references must still be reported as leaked.
struct LiveResourceContexts {
    std::mutex lock;
    std::unordered_set<uintptr_t> disguised_contexts;
};

LiveResourceContexts& live_resource_contexts() {
    // Never destroyed, so that a context released during static destruction can still
    // unregister itself.
    static auto* live = new LiveResourceContexts();
    return *live;
}

uintptr_t disguise(const ResourceContext* context) {
    return ~reinterpret_cast<uintptr_t>(context);
}

ResourceContext* undisguise(uintptr_t disguised_context) {
    return reinterpret_cast<ResourceContext*>(~disguised_context);
}

} // namespace

void ResourceContext::_track_if_leak_check_enabled() {
    if (!config::enable_graceful_exit_check) {
        return;
    }
    auto& live = live_resource_contexts();
    std::lock_guard<std::mutex> l(live.lock);
    live.disguised_contexts.insert(disguise(this));
    _tracked = true;
}

ResourceContext::~ResourceContext() {
    if (_tracked) {
        auto& live = live_resource_contexts();
        std::lock_guard<std::mutex> l(live.lock);
        DORIS_CHECK(live.disguised_contexts.erase(disguise(this)) == 1);
    }
}

std::string ResourceContext::describe_live_contexts(size_t max_contexts) {
    // Meant for shutdown, when no context is being created any more: a context that is still
    // inside std::make_shared has not been handed its weak_from_this() yet.
    //
    // Describe the contexts after the registry is unlocked, keeping each one alive meanwhile
    // with a reference of our own. A context whose last reference is being released right now
    // cannot be referenced any more and is left out.
    std::vector<std::shared_ptr<ResourceContext>> contexts;
    {
        auto& live = live_resource_contexts();
        std::lock_guard<std::mutex> l(live.lock);
        contexts.reserve(live.disguised_contexts.size());
        for (uintptr_t disguised_context : live.disguised_contexts) {
            if (auto reference = undisguise(disguised_context)->weak_from_this().lock()) {
                contexts.push_back(std::move(reference));
            }
        }
    }
    if (contexts.empty()) {
        return "";
    }
    fmt::memory_buffer out;
    fmt::format_to(std::back_inserter(out),
                   "{} resource contexts are still alive. LeakSanitizer reports a leaked context "
                   "as the memory block that holds it, so match the addresses below against the "
                   "ranges it prints under 'Objects leaked above'.",
                   contexts.size());
    for (size_t i = 0; i < contexts.size() && i < max_contexts; ++i) {
        // The reference taken above is not counted.
        fmt::format_to(std::back_inserter(out), "\n  {}",
                       contexts[i]->_describe(contexts[i].use_count() - 1));
    }
    if (contexts.size() > max_contexts) {
        fmt::format_to(std::back_inserter(out), "\n  ... and {} more",
                       contexts.size() - max_contexts);
    }
    return fmt::to_string(out);
}

std::string ResourceContext::_describe(long references_held_elsewhere) const {
    std::string mem_tracker = "none";
    if (auto tracker = memory_context_->mem_tracker()) {
        mem_tracker = fmt::format("{} (consumption={}, peak={})", tracker->label(),
                                  PrettyPrinter::print_bytes(tracker->consumption()),
                                  PrettyPrinter::print_bytes(tracker->peak_consumption()));
    }
    std::string workload_group = "none";
    if (_workload_group != nullptr) {
        workload_group = fmt::format("{}({})", _workload_group->name(), _workload_group->id());
    }
    // A query's context outlives its QueryContext when something else still holds it.
    std::string query_context;
    if (auto* query_task_controller = dynamic_cast<QueryTaskController*>(task_controller_.get())) {
        query_context = query_task_controller->is_query_context_alive()
                                ? ", query_context=alive"
                                : ", query_context=destroyed";
    }
    return fmt::format(
            "{}: references={}, task_id={}, finished={}, running_ms={}, mem_tracker={}, "
            "workload_group={}{}",
            fmt::ptr(this), references_held_elsewhere, print_id(task_controller_->task_id()),
            task_controller_->is_finished(), task_controller_->running_time(), mem_tracker,
            workload_group, query_context);
}

void ResourceContext::to_thrift_query_statistics(TQueryStatistics* statistics) const {
    DCHECK(statistics != nullptr);
    statistics->__set_scan_rows(io_context()->scan_rows());
    statistics->__set_scan_bytes(io_context()->scan_bytes());
    statistics->__set_cpu_ms(cpu_context()->cpu_cost_ms() / NANOS_PER_MILLIS);
    statistics->__set_returned_rows(io_context()->returned_rows());
    statistics->__set_process_rows(io_context()->process_rows());
    statistics->__set_max_peak_memory_bytes(memory_context()->max_peak_memory_bytes());
    statistics->__set_current_used_memory_bytes(memory_context()->current_memory_bytes());
    statistics->__set_shuffle_send_bytes(io_context()->shuffle_send_bytes());
    statistics->__set_shuffle_send_rows(io_context()->shuffle_send_rows());
    statistics->__set_scan_bytes_from_remote_storage(
            io_context()->scan_bytes_from_remote_storage());
    statistics->__set_scan_bytes_from_local_storage(io_context()->scan_bytes_from_local_storage());
    statistics->__set_bytes_write_into_cache(io_context()->bytes_write_into_cache());

    if (workload_group() != nullptr) {
        statistics->__set_workload_group_id(workload_group()->id());
    } else {
        statistics->__set_workload_group_id(-1);
    }

    statistics->__set_spill_write_bytes_to_local_storage(
            io_context_->spill_write_bytes_to_local_storage());
    statistics->__set_spill_read_bytes_from_local_storage(
            io_context_->spill_read_bytes_from_local_storage());

    if (auto* query_task_controller = dynamic_cast<QueryTaskController*>(task_controller())) {
        // Fill query task-level progress directly from task controller.
        statistics->__set_total_tasks_num(query_task_controller->get_total_task_num());
        statistics->__set_finished_tasks_num(query_task_controller->get_finished_task_num());
    }
}

} // namespace doris
