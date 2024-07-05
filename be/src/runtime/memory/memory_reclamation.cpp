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

#include "runtime/memory/cache_manager.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace doris {

// step1: free all cache
// step2: free resource groups memory that enable overcommit
// step3: free global top overcommit query, if enable query memory overcommit
// TODO Now, the meaning is different from java minor gc + full gc, more like small gc + large gc.
bool MemoryReclamation::process_minor_gc(std::string mem_info) {
    MonotonicStopWatch watch;
    watch.start();
    int64_t freed_mem = 0;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");

    Defer defer {[&]() {
        MemInfo::notify_je_purge_dirty_pages();
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "[MemoryGC] end minor GC, free memory {}. cost(us): {}, details: {}",
                PrettyPrinter::print(freed_mem, TUnit::BYTES), watch.elapsed_time() / 1000,
                ss.str());
    }};

    freed_mem += CacheManager::instance()->for_each_cache_prune_stale(profile.get());
    MemInfo::notify_je_purge_dirty_pages();
    if (freed_mem > MemInfo::process_minor_gc_size()) {
        return true;
    }

    if (config::enable_workload_group_memory_gc) {
        RuntimeProfile* tg_profile = profile->create_child("WorkloadGroup", true, true);
        freed_mem += tg_enable_overcommit_group_gc(MemInfo::process_minor_gc_size() - freed_mem,
                                                   tg_profile, true);
        if (freed_mem > MemInfo::process_minor_gc_size()) {
            return true;
        }
    }

    if (config::enable_query_memory_overcommit) {
        VLOG_NOTICE << MemTrackerLimiter::type_detail_usage(
                "[MemoryGC] before free top memory overcommit query in minor GC",
                MemTrackerLimiter::Type::QUERY);
        RuntimeProfile* toq_profile =
                profile->create_child("FreeTopOvercommitMemoryQuery", true, true);
        freed_mem += MemTrackerLimiter::free_top_overcommit_query(
                MemInfo::process_minor_gc_size() - freed_mem, mem_info, toq_profile);
        if (freed_mem > MemInfo::process_minor_gc_size()) {
            return true;
        }
    }
    return false;
}

// step1: free all cache
// step2: free resource groups memory that enable overcommit
// step3: free global top memory query
// step4: free top overcommit load, load retries are more expensive, So cancel at the end.
// step5: free top memory load
bool MemoryReclamation::process_full_gc(std::string mem_info) {
    MonotonicStopWatch watch;
    watch.start();
    int64_t freed_mem = 0;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");

    Defer defer {[&]() {
        MemInfo::notify_je_purge_dirty_pages();
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "[MemoryGC] end full GC, free Memory {}. cost(us): {}, details: {}",
                PrettyPrinter::print(freed_mem, TUnit::BYTES), watch.elapsed_time() / 1000,
                ss.str());
    }};

    freed_mem += CacheManager::instance()->for_each_cache_prune_all(profile.get());
    MemInfo::notify_je_purge_dirty_pages();
    if (freed_mem > MemInfo::process_full_gc_size()) {
        return true;
    }

    if (config::enable_workload_group_memory_gc) {
        RuntimeProfile* tg_profile = profile->create_child("WorkloadGroup", true, true);
        freed_mem += tg_enable_overcommit_group_gc(MemInfo::process_full_gc_size() - freed_mem,
                                                   tg_profile, false);
        if (freed_mem > MemInfo::process_full_gc_size()) {
            return true;
        }
    }

    VLOG_NOTICE << MemTrackerLimiter::type_detail_usage(
            "[MemoryGC] before free top memory query in full GC", MemTrackerLimiter::Type::QUERY);
    RuntimeProfile* tmq_profile = profile->create_child("FreeTopMemoryQuery", true, true);
    freed_mem += MemTrackerLimiter::free_top_memory_query(
            MemInfo::process_full_gc_size() - freed_mem, mem_info, tmq_profile);
    if (freed_mem > MemInfo::process_full_gc_size()) {
        return true;
    }

    if (config::enable_query_memory_overcommit) {
        VLOG_NOTICE << MemTrackerLimiter::type_detail_usage(
                "[MemoryGC] before free top memory overcommit load in full GC",
                MemTrackerLimiter::Type::LOAD);
        RuntimeProfile* tol_profile =
                profile->create_child("FreeTopMemoryOvercommitLoad", true, true);
        freed_mem += MemTrackerLimiter::free_top_overcommit_load(
                MemInfo::process_full_gc_size() - freed_mem, mem_info, tol_profile);
        if (freed_mem > MemInfo::process_full_gc_size()) {
            return true;
        }
    }

    VLOG_NOTICE << MemTrackerLimiter::type_detail_usage(
            "[MemoryGC] before free top memory load in full GC", MemTrackerLimiter::Type::LOAD);
    RuntimeProfile* tml_profile = profile->create_child("FreeTopMemoryLoad", true, true);
    freed_mem += MemTrackerLimiter::free_top_memory_load(
            MemInfo::process_full_gc_size() - freed_mem, mem_info, tml_profile);
    return freed_mem > MemInfo::process_full_gc_size();
}

int64_t MemoryReclamation::tg_disable_overcommit_group_gc() {
    MonotonicStopWatch watch;
    watch.start();
    std::vector<WorkloadGroupPtr> task_groups;
    std::unique_ptr<RuntimeProfile> tg_profile = std::make_unique<RuntimeProfile>("WorkloadGroup");
    int64_t total_free_memory = 0;

    ExecEnv::GetInstance()->workload_group_mgr()->get_related_workload_groups(
            [](const WorkloadGroupPtr& workload_group) {
                return workload_group->is_mem_limit_valid() &&
                       !workload_group->enable_memory_overcommit();
            },
            &task_groups);
    if (task_groups.empty()) {
        return 0;
    }

    std::vector<WorkloadGroupPtr> task_groups_overcommit;
    for (const auto& workload_group : task_groups) {
        if (workload_group->memory_used() > workload_group->memory_limit()) {
            task_groups_overcommit.push_back(workload_group);
        }
    }
    if (task_groups_overcommit.empty()) {
        return 0;
    }

    LOG(INFO) << fmt::format(
            "[MemoryGC] start GC work load group that not enable overcommit, number of overcommit "
            "group: {}, "
            "if it exceeds the limit, try free size = (group used - group limit).",
            task_groups_overcommit.size());

    Defer defer {[&]() {
        if (total_free_memory > 0) {
            std::stringstream ss;
            tg_profile->pretty_print(&ss);
            LOG(INFO) << fmt::format(
                    "[MemoryGC] end GC work load group that not enable overcommit, number of "
                    "overcommit group: {}, free memory {}. cost(us): {}, details: {}",
                    task_groups_overcommit.size(),
                    PrettyPrinter::print(total_free_memory, TUnit::BYTES),
                    watch.elapsed_time() / 1000, ss.str());
        }
    }};

    for (const auto& workload_group : task_groups_overcommit) {
        auto used = workload_group->memory_used();
        total_free_memory += workload_group->gc_memory(used - workload_group->memory_limit(),
                                                       tg_profile.get(), false);
    }
    return total_free_memory;
}

int64_t MemoryReclamation::tg_enable_overcommit_group_gc(int64_t request_free_memory,
                                                         RuntimeProfile* profile,
                                                         bool is_minor_gc) {
    MonotonicStopWatch watch;
    watch.start();
    std::vector<WorkloadGroupPtr> task_groups;
    ExecEnv::GetInstance()->workload_group_mgr()->get_related_workload_groups(
            [](const WorkloadGroupPtr& workload_group) {
                return workload_group->is_mem_limit_valid() &&
                       workload_group->enable_memory_overcommit();
            },
            &task_groups);
    if (task_groups.empty()) {
        return 0;
    }

    int64_t total_exceeded_memory = 0;
    std::vector<int64_t> used_memorys;
    std::vector<int64_t> exceeded_memorys;
    for (const auto& workload_group : task_groups) {
        int64_t used_memory = workload_group->memory_used();
        int64_t exceeded = used_memory - workload_group->memory_limit();
        int64_t exceeded_memory = exceeded > 0 ? exceeded : 0;
        total_exceeded_memory += exceeded_memory;
        used_memorys.emplace_back(used_memory);
        exceeded_memorys.emplace_back(exceeded_memory);
    }

    int64_t total_free_memory = 0;
    bool gc_all_exceeded = request_free_memory >= total_exceeded_memory;
    std::string log_prefix = fmt::format(
            "work load group that enable overcommit, number of group: {}, request_free_memory:{}, "
            "total_exceeded_memory:{}",
            task_groups.size(), request_free_memory, total_exceeded_memory);
    if (gc_all_exceeded) {
        LOG(INFO) << fmt::format(
                "[MemoryGC] start GC {}, request more than exceeded, try free size = (group used - "
                "group limit).",
                log_prefix);
    } else {
        LOG(INFO) << fmt::format(
                "[MemoryGC] start GC {}, request less than exceeded, try free size = ((group used "
                "- group limit) / all group total_exceeded_memory) * request_free_memory.",
                log_prefix);
    }

    Defer defer {[&]() {
        if (total_free_memory > 0) {
            std::stringstream ss;
            profile->pretty_print(&ss);
            LOG(INFO) << fmt::format(
                    "[MemoryGC] end GC {}, free memory {}. cost(us): {}, details: {}", log_prefix,
                    PrettyPrinter::print(total_free_memory, TUnit::BYTES),
                    watch.elapsed_time() / 1000, ss.str());
        }
    }};

    for (int i = 0; i < task_groups.size(); ++i) {
        if (exceeded_memorys[i] == 0) {
            continue;
        }

        // todo: GC according to resource group priority
        auto tg_need_free_memory = int64_t(
                gc_all_exceeded ? exceeded_memorys[i]
                                : static_cast<double>(exceeded_memorys[i]) / total_exceeded_memory *
                                          request_free_memory); // exceeded memory as a weight
        auto workload_group = task_groups[i];
        total_free_memory += workload_group->gc_memory(tg_need_free_memory, profile, is_minor_gc);
    }
    return total_free_memory;
}

} // namespace doris
