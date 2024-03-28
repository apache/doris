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

#include "workload_group_manager.h"

#include <memory>
#include <mutex>
#include <unordered_map>

#include "pipeline/task_scheduler.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "util/mem_info.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

WorkloadGroupPtr WorkloadGroupMgr::get_or_create_workload_group(
        const WorkloadGroupInfo& workload_group_info) {
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        if (LIKELY(_workload_groups.count(workload_group_info.id))) {
            auto workload_group = _workload_groups[workload_group_info.id];
            workload_group->check_and_update(workload_group_info);
            return workload_group;
        }
    }

    auto new_task_group = std::make_shared<WorkloadGroup>(workload_group_info);
    std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
    if (_workload_groups.count(workload_group_info.id)) {
        auto workload_group = _workload_groups[workload_group_info.id];
        workload_group->check_and_update(workload_group_info);
        return workload_group;
    }
    _workload_groups[workload_group_info.id] = new_task_group;
    return new_task_group;
}

void WorkloadGroupMgr::get_related_workload_groups(
        const std::function<bool(const WorkloadGroupPtr& ptr)>& pred,
        std::vector<WorkloadGroupPtr>* task_groups) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, workload_group] : _workload_groups) {
        if (pred(workload_group)) {
            task_groups->push_back(workload_group);
        }
    }
}

WorkloadGroupPtr WorkloadGroupMgr::get_task_group_by_id(uint64_t tg_id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_workload_groups.find(tg_id) != _workload_groups.end()) {
        return _workload_groups.at(tg_id);
    }
    return nullptr;
}

void WorkloadGroupMgr::delete_workload_group_by_ids(std::set<uint64_t> used_wg_id) {
    int64_t begin_time = MonotonicMillis();
    // 1 get delete group without running queries
    std::vector<WorkloadGroupPtr> deleted_task_groups;
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
            uint64_t tg_id = iter->first;
            auto workload_group_ptr = iter->second;
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                workload_group_ptr->shutdown();
                // only when no query running in workload group, its resource can be released in BE
                if (workload_group_ptr->query_num() == 0) {
                    LOG(INFO) << "There is no query in wg " << tg_id << ", delete it.";
                    deleted_task_groups.push_back(workload_group_ptr);
                }
            }
        }
    }

    // 2 stop active thread
    for (auto& tg : deleted_task_groups) {
        // There is not lock here, but the tg may be released by another
        // thread, so that we should use shared ptr here, not use tg_id
        tg->try_stop_schedulers();
    }

    // 3 release resource in memory
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto& tg : deleted_task_groups) {
            _workload_groups.erase(tg->id());
        }
    }

    // 4 clear cgroup dir
    // NOTE(wb) currently we use rmdir to delete cgroup path,
    // this action may be failed until task file is cleared which means all thread are stopped.
    // So the first time to rmdir a cgroup path may failed.
    // Using cgdelete has no such issue.
    {
        std::lock_guard<std::shared_mutex> write_lock(_init_cg_ctl_lock);
        if (!_cg_cpu_ctl) {
            _cg_cpu_ctl = std::make_unique<CgroupV1CpuCtl>();
        }
        if (!_is_init_succ) {
            Status ret = _cg_cpu_ctl->init();
            if (ret.ok()) {
                _is_init_succ = true;
            } else {
                LOG(INFO) << "init workload group mgr cpu ctl failed, " << ret.to_string();
            }
        }
        if (_is_init_succ) {
            Status ret = _cg_cpu_ctl->delete_unused_cgroup_path(used_wg_id);
            if (!ret.ok()) {
                LOG(WARNING) << ret.to_string();
            }
        }
    }
    int64_t time_cost_ms = MonotonicMillis() - begin_time;
    LOG(INFO) << "finish clear unused workload group, time cost: " << time_cost_ms
              << "ms, deleted group size:" << deleted_task_groups.size();
}

struct WorkloadGroupMemInfo {
    int64_t total_mem_used = 0;
    int64_t weighted_mem_used = 0;
    bool is_low_wartermark = false;
    bool is_high_wartermark = false;
    double mem_used_ratio = 0;
};
void WorkloadGroupMgr::refresh_wg_memory_info() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    // workload group id -> workload group queries
    std::unordered_map<uint64_t, std::unordered_map<TUniqueId, std::weak_ptr<QueryContext>>>
            all_wg_queries;
    for (auto& [wg_id, wg] : _workload_groups) {
        all_wg_queries.insert({wg_id, wg->queries()});
    }

    int64_t all_queries_mem_used = 0;

    // calculate total memory used of each workload group and total memory used of all queries
    std::unordered_map<uint64_t, WorkloadGroupMemInfo> wgs_mem_info;
    for (auto& [wg_id, wg_queries] : all_wg_queries) {
        int64_t wg_total_mem_used = 0;
        for (const auto& [query_id, query_ctx_ptr] : wg_queries) {
            if (auto query_ctx = query_ctx_ptr.lock()) {
                wg_total_mem_used +=
                        query_ctx->is_cancelled() ? 0 : query_ctx->query_mem_tracker->consumption();
            }
        }
        all_queries_mem_used += wg_total_mem_used;
        wgs_mem_info[wg_id] = {wg_total_mem_used};
    }

    auto proc_vm_rss = PerfCounters::get_vm_rss();
    if (all_queries_mem_used <= 0) {
        return;
    }

    auto process_mem_used = doris::MemInfo::proc_mem_no_allocator_cache();
    auto sys_mem_available = doris::MemInfo::sys_mem_available();
    std::string debug_msg = fmt::format(
            "\nProcess Memory Summary: process_vm_rss: {}, process mem: {}, sys mem available: "
            "{}, all quries mem: {}",
            PrettyPrinter::print(proc_vm_rss, TUnit::BYTES),
            PrettyPrinter::print(process_mem_used, TUnit::BYTES),
            PrettyPrinter::print(sys_mem_available, TUnit::BYTES),
            PrettyPrinter::print(all_queries_mem_used, TUnit::BYTES));
    LOG_EVERY_N(INFO, 5) << debug_msg;

    if (proc_vm_rss < all_queries_mem_used) {
        all_queries_mem_used = proc_vm_rss;
    }

    // process memory used is actually bigger than all_queries_mem_used,
    // because memory of page cache, allocator cache, segment cache etc. are included
    // in process_mem_used.
    // we count these cache memories equally on workload groups.
    double ratio = (double)proc_vm_rss / (double)all_queries_mem_used;

    for (auto& wg : _workload_groups) {
        auto wg_mem_limit = wg.second->memory_limit();
        auto& wg_mem_info = wgs_mem_info[wg.first];
        wg_mem_info.weighted_mem_used = wg_mem_info.total_mem_used * ratio;
        wg_mem_info.mem_used_ratio = (double)wg_mem_info.weighted_mem_used / wg_mem_limit;

        wg.second->set_weighted_memory_used(wg_mem_info.total_mem_used, ratio);

        auto spill_low_water_mark = wg.second->spill_threshold_low_water_mark();
        auto spill_high_water_mark = wg.second->spill_threashold_high_water_mark();
        wg_mem_info.is_high_wartermark = (wg_mem_info.weighted_mem_used >
                                          ((double)wg_mem_limit * spill_high_water_mark / 100));
        wg_mem_info.is_low_wartermark = (wg_mem_info.weighted_mem_used >
                                         ((double)wg_mem_limit * spill_low_water_mark / 100));

        // calculate query weighted memory limit of task group
        const auto& wg_queries = all_wg_queries[wg.first];
        auto wg_query_count = wg_queries.size();
        int64_t query_weighted_mem_limit =
                wg_query_count ? (wg_mem_limit + wg_query_count) / wg_query_count : wg_mem_limit;

        debug_msg = "";
        if (wg_mem_info.is_high_wartermark || wg_mem_info.is_low_wartermark) {
            debug_msg = fmt::format(
                    "\nWorkload Group {}: mem limit: {}, mem used: {}, weighted mem used: {}, used "
                    "ratio: {}, query "
                    "count: {}, query_weighted_mem_limit: {}",
                    wg.second->name(), PrettyPrinter::print(wg_mem_limit, TUnit::BYTES),
                    PrettyPrinter::print(wg_mem_info.total_mem_used, TUnit::BYTES),
                    PrettyPrinter::print(wg_mem_info.weighted_mem_used, TUnit::BYTES),
                    wg_mem_info.mem_used_ratio, wg_query_count,
                    PrettyPrinter::print(query_weighted_mem_limit, TUnit::BYTES));

            debug_msg += "\n  Query Memory Summary:";
        }
        // check where queries need to revoke memory for task group
        for (const auto& query : wg_queries) {
            auto query_ctx = query.second.lock();
            if (!query_ctx) {
                continue;
            }
            auto query_consumption = query_ctx->query_mem_tracker->consumption();
            int64_t query_weighted_consumption = query_consumption * ratio;
            query_ctx->set_weighted_mem(query_weighted_mem_limit, query_weighted_consumption);

            bool need_revoke = false;
            if (wg_mem_info.is_high_wartermark) {
                need_revoke = true;
            } else if (wg_mem_info.is_low_wartermark) {
                need_revoke = query_weighted_consumption > query_weighted_mem_limit;
            }
            query_ctx->set_need_revoke(need_revoke);

            if (wg_mem_info.is_high_wartermark || wg_mem_info.is_low_wartermark) {
                debug_msg += fmt::format(
                        "\n    MemTracker Label={}, Parent Label={}, Used={}, WeightedUsed={}, "
                        "Peak={}",
                        query_ctx->query_mem_tracker->label(),
                        query_ctx->query_mem_tracker->parent_label(),
                        PrettyPrinter::print(query_consumption, TUnit::BYTES),
                        PrettyPrinter::print(query_weighted_consumption, TUnit::BYTES),
                        PrettyPrinter::print(query_ctx->query_mem_tracker->peak_consumption(),
                                             TUnit::BYTES));
            }
        }
        if (wg_mem_info.is_high_wartermark || wg_mem_info.is_low_wartermark) {
            LOG_EVERY_N(INFO, 3) << debug_msg;
        }
    }
}

void WorkloadGroupMgr::stop() {
    for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

} // namespace doris
