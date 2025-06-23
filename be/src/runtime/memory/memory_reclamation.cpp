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

#include "runtime/memory/memory_reclamation.h"

#include <unordered_map>

#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/jemalloc_control.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/workload_group/workload_group.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace doris {
#include "common/compile_check_begin.h"

int64_t MemoryReclamation::revoke_tasks_memory(
        int64_t need_free_mem, const std::vector<std::shared_ptr<ResourceContext>>& resource_ctxs,
        const std::string& revoke_reason, RuntimeProfile* profile, PriorityCmpFunc priority_cmp,
        std::vector<FilterFunc> filters, ActionFunc action) {
    if (need_free_mem <= 0) {
        return 0;
    }
    auto process_mem_stat = GlobalMemoryArbitrator::process_mem_log_str();
    auto process_mem_stat_to_client = fmt::format(
            "os physical memory {}. {}, limit {}. {}, low water mark {}.",
            PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES),
            GlobalMemoryArbitrator::process_memory_used_str(), MemInfo::mem_limit_str(),
            GlobalMemoryArbitrator::sys_mem_available_str(),
            PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES));
    RuntimeProfile::Counter* request_revoke_tasks_counter =
            ADD_COUNTER(profile, "RequestRevokeTasksNum", TUnit::UNIT);
    RuntimeProfile::Counter* revocable_tasks_counter =
            ADD_COUNTER(profile, "RevocableTasksNum", TUnit::UNIT);
    RuntimeProfile::Counter* this_time_revoked_tasks_counter =
            ADD_COUNTER(profile, "ThisTimeRevokedTasksNum", TUnit::UNIT);
    RuntimeProfile::Counter* skip_cancelling_tasks_counter =
            ADD_COUNTER(profile, "SkipCancellingTasksNum", TUnit::UNIT);
    RuntimeProfile::Counter* keep_wait_cancelling_tasks_counter =
            ADD_COUNTER(profile, "KeepWaitCancellingTasksNum", TUnit::UNIT);
    RuntimeProfile::Counter* freed_memory_counter =
            ADD_COUNTER(profile, "FreedMemory", TUnit::BYTES);
    RuntimeProfile::Counter* filter_cost_time = ADD_TIMER(profile, "FilterCostTime");
    RuntimeProfile::Counter* revoke_cost_time = ADD_TIMER(profile, "revokeCostTime");

    std::priority_queue<std::pair<int64_t, std::shared_ptr<ResourceContext>>>
            revocable_resource_ctxs;
    std::vector<std::string> this_time_revoked_tasks;
    std::vector<std::string> skip_cancelling_tasks;
    std::vector<std::string> keep_wait_cancelling_tasks;

    auto config_str = fmt::format(
            "need free memory: {}, request revoke tasks: {}, revoke reason: {}, priority compare "
            "function: {}, filter function: {}, action function: {}. {}",
            need_free_mem, resource_ctxs.size(), revoke_reason,
            priority_cmp_func_string(priority_cmp), filter_func_string(filters),
            action_func_string(action), process_mem_stat);
    LOG(INFO) << fmt::format("[MemoryGC] start revoke_tasks_memory, {}.", config_str);
    Defer defer {[&]() {
        LOG(INFO) << fmt::format(
                "[MemoryGC] end revoke_tasks_memory, {}. freed memory: {}, "
                "revocable tasks: {}, this time revoked tasks: {}, consist of: [{}], call revoke "
                "func cost(us): {}."
                " some tasks is being canceled and has not been completed yet, among them, skip "
                "canceling tasks: {}(cancel cost too long, not counted in freed memory), consist "
                "of: [{}], keep wait canceling tasks: {}(counted in freed memory), consist of: "
                "[{}], filter cost(us): {}.",
                config_str, freed_memory_counter->value(), revocable_tasks_counter->value(),
                this_time_revoked_tasks_counter->value(), join(this_time_revoked_tasks, " | "),
                revoke_cost_time->value(), skip_cancelling_tasks_counter->value(),
                join(skip_cancelling_tasks, " | "), keep_wait_cancelling_tasks_counter->value(),
                join(keep_wait_cancelling_tasks, " | "), filter_cost_time->value());
    }};

    {
        SCOPED_TIMER(filter_cost_time);
        for (auto resource_ctx : resource_ctxs) {
            bool is_filtered = false;
            for (auto filter : filters) {
                if (!FilterFuncImpl[filter](resource_ctx.get())) {
                    is_filtered = true;
                    break;
                }
            }
            if (is_filtered) {
                continue;
            }

            // skip cancelling tasks
            if (resource_ctx->task_controller()->is_cancelled()) {
                // for the query being canceled,
                // if (current time - cancel start time) < 3s (revoke_memory_max_tolerance_ms), the query memory is counted in `freed_memory`,
                // and the query memory is expected to be released soon.
                // if > 3s, the query memory will not be counted in `freed_memory`,
                // and the query may be blocked during the cancel process. skip this query and continue to cancel other queries.
                if (MonotonicMillis() - resource_ctx->task_controller()->cancelled_time() >
                    config::revoke_memory_max_tolerance_ms) {
                    skip_cancelling_tasks.push_back(
                            resource_ctx->task_controller()->debug_string());
                } else {
                    keep_wait_cancelling_tasks.push_back(
                            resource_ctx->task_controller()->debug_string());
                    COUNTER_UPDATE(freed_memory_counter,
                                   resource_ctx->memory_context()->current_memory_bytes());
                }
                is_filtered = true;
            }

            // TODO, if ActionFunc::SPILL, should skip spilling tasks.

            if (is_filtered) {
                continue;
            }
            int64_t weight = PriorityCmpFuncImpl[priority_cmp](resource_ctx.get());
            if (weight != -1) {
                revocable_resource_ctxs.emplace(weight, resource_ctx);
            }
        }
    }

    COUNTER_UPDATE(request_revoke_tasks_counter, resource_ctxs.size());
    COUNTER_UPDATE(revocable_tasks_counter, revocable_resource_ctxs.size());
    COUNTER_UPDATE(skip_cancelling_tasks_counter, skip_cancelling_tasks.size());
    COUNTER_UPDATE(keep_wait_cancelling_tasks_counter, keep_wait_cancelling_tasks.size());

    if (revocable_resource_ctxs.empty()) {
        return freed_memory_counter->value();
    }

    {
        SCOPED_TIMER(revoke_cost_time);
        while (!revocable_resource_ctxs.empty()) {
            auto resource_ctx = revocable_resource_ctxs.top().second;
            std::string task_revoke_reason = fmt::format(
                    "{} {} task: {}. because {}. in backend {}, {} execute again after enough "
                    "memory, details see be.INFO.",
                    action_func_string(action), priority_cmp_func_string(priority_cmp),
                    resource_ctx->memory_context()->debug_string(), revoke_reason,
                    BackendOptions::get_localhost(), process_mem_stat_to_client);
            if (ActionFuncImpl[action](resource_ctx.get(),
                                       Status::MemoryLimitExceeded(task_revoke_reason))) {
                this_time_revoked_tasks.push_back(resource_ctx->task_controller()->debug_string());
                COUNTER_UPDATE(freed_memory_counter,
                               resource_ctx->memory_context()->current_memory_bytes());
                COUNTER_UPDATE(this_time_revoked_tasks_counter, 1);
                if (freed_memory_counter->value() > need_free_mem) {
                    break;
                }
            }
            revocable_resource_ctxs.pop();
        }
    }
    return freed_memory_counter->value();
}

// step1: free process top memory query
// step2: free process top memory load, load retries are more expensive, so revoke at the end.
bool MemoryReclamation::revoke_process_memory(const std::string& revoke_reason) {
    MonotonicStopWatch watch;
    watch.start();
    int64_t freed_mem = 0;
    std::unique_ptr<RuntimeProfile> profile =
            std::make_unique<RuntimeProfile>("RevokeProcessMemory");

    LOG(INFO) << fmt::format(
            "[MemoryGC] start MemoryReclamation::revoke_process_memory, {}, need free size: {}.",
            GlobalMemoryArbitrator::process_mem_log_str(),
            PrettyPrinter::print_bytes(MemInfo::process_full_gc_size()));
    Defer defer {[&]() {
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "[MemoryGC] end MemoryReclamation::revoke_process_memory, {}, need free size: {}, "
                "free Memory {}. cost(us): {}, details: {}",
                GlobalMemoryArbitrator::process_mem_log_str(),
                PrettyPrinter::print_bytes(MemInfo::process_full_gc_size()),
                PrettyPrinter::print_bytes(freed_mem), watch.elapsed_time() / 1000, ss.str());
    }};

    // step1: start canceling from the query with the largest memory usage until the memory of process_full_gc_size is freed.
    VLOG_DEBUG << fmt::format(
            "[MemoryGC] before free top memory query in revoke process memory, Type:{}, Memory "
            "Tracker "
            "Summary: {}",
            MemTrackerLimiter::type_string(MemTrackerLimiter::Type::QUERY),
            MemTrackerLimiter::make_type_trackers_profile_str(MemTrackerLimiter::Type::QUERY));
    RuntimeProfile* free_top_query_profile =
            profile->create_child("FreeTopMemoryQuery", true, true);
    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    ExecEnv::GetInstance()->runtime_query_statistics_mgr()->get_tasks_resource_context(
            resource_ctxs);
    freed_mem +=
            revoke_tasks_memory(MemInfo::process_full_gc_size() - freed_mem, resource_ctxs,
                                revoke_reason, free_top_query_profile, PriorityCmpFunc::TOP_MEMORY,
                                {FilterFunc::IS_QUERY}, ActionFunc::CANCEL);
    if (freed_mem > MemInfo::process_full_gc_size()) {
        return true;
    }

    // step2: start canceling from the load with the largest memory usage until the memory of process_full_gc_size is freed.
    VLOG_DEBUG << fmt::format(
            "[MemoryGC] before free top memory load in revoke process memory, Type:{}, Memory "
            "Tracker "
            "Summary: {}",
            MemTrackerLimiter::type_string(MemTrackerLimiter::Type::LOAD),
            MemTrackerLimiter::make_type_trackers_profile_str(MemTrackerLimiter::Type::LOAD));
    RuntimeProfile* free_top_load_profile = profile->create_child("FreeTopMemoryLoad", true, true);
    freed_mem +=
            revoke_tasks_memory(MemInfo::process_full_gc_size() - freed_mem, resource_ctxs,
                                revoke_reason, free_top_load_profile, PriorityCmpFunc::TOP_MEMORY,
                                {FilterFunc::IS_LOAD}, ActionFunc::CANCEL);
    return freed_mem > MemInfo::process_full_gc_size();
}

void MemoryReclamation::je_purge_dirty_pages() {
#ifdef USE_JEMALLOC
    if (config::disable_memory_gc || !config::enable_je_purge_dirty_pages) {
        return;
    }
    std::unique_lock<std::mutex> l(doris::JemallocControl::je_purge_dirty_pages_lock);

    // Allow `purge_all_arena_dirty_pages` again after the process memory changes by 256M,
    // otherwise execute `decay_all_arena_dirty_pages`, because `purge_all_arena_dirty_pages` is very expensive.
    if (doris::JemallocControl::je_purge_dirty_pages_notify.load(std::memory_order_relaxed)) {
        doris::JemallocControl::je_purge_all_arena_dirty_pages();
        doris::JemallocControl::je_purge_dirty_pages_notify.store(false, std::memory_order_relaxed);
    } else {
        doris::JemallocControl::je_decay_all_arena_dirty_pages();
    }
#endif
}

#include "common/compile_check_end.h"
} // namespace doris
