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

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "exec/schema_scanner/schema_scanner_helper.h"
#include "pipeline/task_scheduler.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "util/mem_info.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

PausedQuery::PausedQuery(std::shared_ptr<QueryContext> query_ctx, double cache_ratio)
        : query_ctx_(query_ctx),
          cache_ratio_(cache_ratio),
          query_id_(print_id(query_ctx->query_id())) {
    enqueue_at = std::chrono::system_clock::now();
}

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
    int old_wg_size = 0;
    int new_wg_size = 0;
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        old_wg_size = _workload_groups.size();
        for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
            uint64_t wg_id = iter->first;
            auto workload_group_ptr = iter->second;
            if (used_wg_id.find(wg_id) == used_wg_id.end()) {
                workload_group_ptr->shutdown();
                LOG(INFO) << "[topic_publish_wg] shutdown wg:" << wg_id;
            }
            // wg is shutdown and running rum = 0, its resource can be released in BE
            if (workload_group_ptr->can_be_dropped()) {
                LOG(INFO) << "[topic_publish_wg]There is no query in wg " << wg_id
                          << ", delete it.";
                deleted_task_groups.push_back(workload_group_ptr);
            }
        }
    }

    // 2 stop active thread
    for (auto& wg : deleted_task_groups) {
        // There is not lock here, but the tg may be released by another
        // thread, so that we should use shared ptr here, not use wg_id
        wg->try_stop_schedulers();
    }

    // 3 release resource in memory
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto& wg : deleted_task_groups) {
            _workload_groups.erase(wg->id());
        }
        new_wg_size = _workload_groups.size();
    }

    // 4 clear cgroup dir
    // NOTE(wb) currently we use rmdir to delete cgroup path,
    // this action may be failed until task file is cleared which means all thread are stopped.
    // So the first time to rmdir a cgroup path may failed.
    // Using cgdelete has no such issue.
    {
        if (config::doris_cgroup_cpu_path != "") {
            std::lock_guard<std::shared_mutex> write_lock(_clear_cgroup_lock);
            Status ret = CgroupCpuCtl::delete_unused_cgroup_path(used_wg_id);
            if (!ret.ok()) {
                LOG(WARNING) << "[topic_publish_wg]" << ret.to_string();
            }
        }
    }
    int64_t time_cost_ms = MonotonicMillis() - begin_time;
    LOG(INFO) << "[topic_publish_wg]finish clear unused workload group, time cost: " << time_cost_ms
              << " ms, deleted group size:" << deleted_task_groups.size()
              << ", before wg size=" << old_wg_size << ", after wg size=" << new_wg_size;
}

struct WorkloadGroupMemInfo {
    int64_t total_mem_used = 0;
    std::list<std::shared_ptr<MemTrackerLimiter>> tracker_snapshots =
            std::list<std::shared_ptr<MemTrackerLimiter>>();
};
void WorkloadGroupMgr::refresh_wg_weighted_memory_limit() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);

    // 1. make all workload groups memory snapshots(refresh workload groups total memory used at the same time)
    // and calculate total memory used of all queries.
    int64_t all_workload_groups_mem_usage = 0;
    std::unordered_map<uint64_t, WorkloadGroupMemInfo> wgs_mem_info;
    for (auto& [wg_id, wg] : _workload_groups) {
        wgs_mem_info[wg_id].total_mem_used =
                wg->make_memory_tracker_snapshots(&wgs_mem_info[wg_id].tracker_snapshots);
        all_workload_groups_mem_usage += wgs_mem_info[wg_id].total_mem_used;
    }
    if (all_workload_groups_mem_usage <= 0) {
        return;
    }

    // 2. calculate weighted memory limit ratio.
    // when construct workload group, mem_limit is equal to (process_memory_limit * group_limit_percent),
    // here, it is assumed that the available memory of workload groups is equal to process_memory_limit.
    //
    // but process_memory_usage is actually bigger than all_workload_groups_mem_usage,
    // because public_memory of page cache, allocator cache, segment cache etc. are included in process_memory_usage.
    // so actual available memory of the workload groups is equal to (process_memory_limit - public_memory)
    //
    // we will exclude this public_memory when calculate workload group mem_limit.
    // so a ratio is calculated to multiply the workload group mem_limit from the previous construction.
    auto process_memory_usage = GlobalMemoryArbitrator::process_memory_usage();
    auto process_memory_limit = MemInfo::mem_limit();
    double weighted_memory_limit_ratio = 1;
    // if all_workload_groups_mem_usage is greater than process_memory_usage, it means that the memory statistics
    // of the workload group are inaccurate.
    // the reason is that query/load/etc. tracked is virtual memory, and virtual memory is not used in time.
    //
    // At this time, weighted_memory_limit_ratio is equal to 1, and workload group mem_limit is still equal to
    // (process_memory_limit * group_limit_percent), this may cause query spill to occur earlier,
    // However, there is no good solution at present, but we cannot predict when these virtual memory will be used.
    if (all_workload_groups_mem_usage < process_memory_usage) {
        int64_t public_memory = process_memory_usage - all_workload_groups_mem_usage;
        weighted_memory_limit_ratio = 1 - (double)public_memory / (double)process_memory_limit;
        // Round the value from 1% to 100%.
        weighted_memory_limit_ratio = std::floor(weighted_memory_limit_ratio * 100) / 100;
    }

    std::string debug_msg = fmt::format(
            "\nProcess Memory Summary: {}, {}, all workload groups memory usage: {}, "
            "weighted_memory_limit_ratio: {}",
            doris::GlobalMemoryArbitrator::process_memory_used_details_str(),
            doris::GlobalMemoryArbitrator::sys_mem_available_details_str(),
            PrettyPrinter::print(all_workload_groups_mem_usage, TUnit::BYTES),
            weighted_memory_limit_ratio);
    // LOG_EVERY_T(INFO, 60) << debug_msg;
    LOG(INFO) << debug_msg;
    for (auto& wg : _workload_groups) {
        auto wg_mem_limit = wg.second->memory_limit();
        auto wg_weighted_mem_limit = int64_t(wg_mem_limit * weighted_memory_limit_ratio);
        wg.second->set_weighted_memory_limit(wg_weighted_mem_limit);
        auto all_query_ctxs = wg.second->queries();
        bool is_low_wartermark = false;
        bool is_high_wartermark = false;
        wg.second->check_mem_used(&is_low_wartermark, &is_high_wartermark);
        int64_t wg_high_water_mark_limit =
                (int64_t)(wg_mem_limit * wg.second->spill_threshold_high_water_mark() * 1.0 / 100);
        int64_t weighted_high_water_mark_limit =
                (int64_t)(wg_weighted_mem_limit * wg.second->spill_threshold_high_water_mark() *
                          1.0 / 100);
        std::string debug_msg;
        if (is_high_wartermark || is_low_wartermark) {
            debug_msg = fmt::format(
                    "\nWorkload Group {}: mem limit: {}, mem used: {}, weighted mem limit: {}, "
                    "high water mark mem limit: {}, used ratio: {}",
                    wg.second->name(),
                    PrettyPrinter::print(wg.second->memory_limit(), TUnit::BYTES),
                    PrettyPrinter::print(wgs_mem_info[wg.first].total_mem_used, TUnit::BYTES),
                    PrettyPrinter::print(wg_weighted_mem_limit, TUnit::BYTES),
                    PrettyPrinter::print(weighted_high_water_mark_limit, TUnit::BYTES),
                    (double)wgs_mem_info[wg.first].total_mem_used / wg_weighted_mem_limit);

            debug_msg += "\n  Query Memory Summary:";
            // check whether queries need to revoke memory for task group
            for (const auto& query_mem_tracker : wgs_mem_info[wg.first].tracker_snapshots) {
                debug_msg += fmt::format(
                        "\n    MemTracker Label={}, Used={}, MemLimit={}, "
                        "Peak={}",
                        query_mem_tracker->label(),
                        PrettyPrinter::print(query_mem_tracker->consumption(), TUnit::BYTES),
                        PrettyPrinter::print(query_mem_tracker->limit(), TUnit::BYTES),
                        PrettyPrinter::print(query_mem_tracker->peak_consumption(), TUnit::BYTES));
            }
        }

        // If the wg enable over commit memory, then it is no need to update query memlimit
        if (wg.second->enable_memory_overcommit()) {
            continue;
        }
        int32_t total_used_slot_count = 0;
        int32_t total_slot_count = wg.second->total_query_slot_count();
        // calculate total used slot count
        for (const auto& query : all_query_ctxs) {
            auto query_ctx = query.second.lock();
            if (!query_ctx) {
                continue;
            }
            total_used_slot_count += query_ctx->get_slot_count();
        }
        // calculate per query weighted memory limit
        debug_msg = "Query Memory Summary:";
        for (const auto& query : all_query_ctxs) {
            auto query_ctx = query.second.lock();
            if (!query_ctx) {
                continue;
            }
            int64_t query_weighted_mem_limit = 0;
            // If the query enable hard limit, then it should not use the soft limit
            if (query_ctx->enable_query_slot_hard_limit()) {
                if (total_slot_count < 1) {
                    LOG(WARNING)
                            << "query " << print_id(query_ctx->query_id())
                            << " enabled hard limit, but the slot count < 1, could not take affect";
                } else {
                    // If the query enable hard limit, then not use weighted info any more, just use the settings limit.
                    query_weighted_mem_limit = (int64_t)((wg_high_water_mark_limit *
                                                          query_ctx->get_slot_count() * 1.0) /
                                                         total_slot_count);
                }
            } else {
                // If low water mark is not reached, then use process memory limit as query memory limit.
                // It means it will not take effect.
                // If there are some query in paused list, then limit should take effect.
                if (!is_low_wartermark && wg.second->memory_sufficent()) {
                    query_weighted_mem_limit = wg_high_water_mark_limit;
                } else {
                    query_weighted_mem_limit =
                            total_used_slot_count > 0
                                    ? (int64_t)((wg_high_water_mark_limit + total_used_slot_count) *
                                                query_ctx->get_slot_count() * 1.0 /
                                                total_used_slot_count)
                                    : wg_high_water_mark_limit;
                }
            }
            debug_msg += fmt::format(
                    "\n    MemTracker Label={}, Used={}, Limit={}, Peak={}",
                    query_ctx->get_mem_tracker()->label(),
                    PrettyPrinter::print(query_ctx->get_mem_tracker()->consumption(), TUnit::BYTES),
                    PrettyPrinter::print(query_weighted_mem_limit, TUnit::BYTES),
                    PrettyPrinter::print(query_ctx->get_mem_tracker()->peak_consumption(),
                                         TUnit::BYTES));

            query_ctx->set_mem_limit(query_weighted_mem_limit);
        }
        // During memory insufficent stage, we already set every query's memlimit, so that the flag is useless any more.
        wg.second->update_memory_sufficent(true);
        // LOG_EVERY_T(INFO, 60) << debug_msg;
        LOG(INFO) << debug_msg;
    }
}

void WorkloadGroupMgr::get_wg_resource_usage(vectorized::Block* block) {
    int64_t be_id = ExecEnv::GetInstance()->master_info()->backend_id;
    int cpu_num = CpuInfo::num_cores();
    cpu_num = cpu_num <= 0 ? 1 : cpu_num;
    uint64_t total_cpu_time_ns_per_second = cpu_num * 1000000000ll;

    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    block->reserve(_workload_groups.size());
    for (const auto& [id, wg] : _workload_groups) {
        SchemaScannerHelper::insert_int64_value(0, be_id, block);
        SchemaScannerHelper::insert_int64_value(1, wg->id(), block);
        SchemaScannerHelper::insert_int64_value(2, wg->get_mem_used(), block);

        double cpu_usage_p =
                (double)wg->get_cpu_usage() / (double)total_cpu_time_ns_per_second * 100;
        cpu_usage_p = std::round(cpu_usage_p * 100.0) / 100.0;

        SchemaScannerHelper::insert_double_value(3, cpu_usage_p, block);

        SchemaScannerHelper::insert_int64_value(4, wg->get_local_scan_bytes_per_second(), block);
        SchemaScannerHelper::insert_int64_value(5, wg->get_remote_scan_bytes_per_second(), block);
    }
}

void WorkloadGroupMgr::add_paused_query(const std::shared_ptr<QueryContext>& query_ctx) {
    std::lock_guard<std::mutex> lock(_paused_queries_lock);
    DCHECK(query_ctx != nullptr);
    auto wg = query_ctx->workload_group();
    auto&& [it, inserted] = _paused_queries_list[wg].emplace(
            query_ctx, doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted);
    if (inserted) {
        LOG(INFO) << "workload group " << wg->debug_string()
                  << " insert one new paused query: " << it->query_id();
    }
}

/**
 * Strategy 1: A revocable query should not have any running task(PipelineTask).
 * strategy 2: If the workload group has any task exceed workload group memlimit, then set all queryctx's memlimit
 * strategy 3: If any query exceed process memlimit, then should clear all caches.
 * strategy 4: If any query exceed query's memlimit, then do spill disk or cancel it.
 * strategy 5: If any query exceed process's memlimit and cache is zero, then do spill disk or cancel it.
 */
void WorkloadGroupMgr::handle_paused_queries() {
    std::unique_lock<std::mutex> lock(_paused_queries_lock);
    for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
        auto& queries_list = it->second;
        const auto& wg = it->first;
        if (queries_list.empty()) {
            LOG(INFO) << "wg: " << wg->debug_string()
                      << " has no paused query, update it to memory sufficent";
            it = _paused_queries_list.erase(it);
            wg->update_memory_sufficent(true);
            continue;
        }

        bool is_low_wartermark = false;
        bool is_high_wartermark = false;

        wg->check_mem_used(&is_low_wartermark, &is_high_wartermark);

        // If the query is paused because its limit exceed the query itself's memlimit, then just spill disk.
        // The query's memlimit is set using slot mechanism and its value is set using the user settings, not
        // by weighted value. So if reserve failed, then it is actually exceed limit.
        for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
            auto query_ctx = query_it->query_ctx_.lock();
            // The query is finished during in paused list.
            if (query_ctx == nullptr) {
                query_it = queries_list.erase(query_it);
                continue;
            }
            if (query_ctx->is_cancelled()) {
                LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                          << "was canceled, remove from paused list";
                query_it = queries_list.erase(query_it);
                continue;
            }
            if (query_ctx->paused_reason().is<ErrorCode::QUERY_MEMORY_EXCEED>()) {
                bool spill_res = spill_or_cancel_query(query_ctx, query_ctx->paused_reason());
                if (!spill_res) {
                    ++query_it;
                    continue;
                } else {
                    query_it = queries_list.erase(query_it);
                    continue;
                }
            } else if (query_ctx->paused_reason().is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEED>()) {
                if (wg->memory_sufficent()) {
                    wg->update_memory_sufficent(false);
                    LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                              << " reserve memory failed due to workload group memory exceed, "
                                 "should set the workload group work in memory insufficent mode, "
                                 "so that other query will reduce their memory. wg: "
                              << wg->debug_string();
                }
                // Should not put the query back to task scheduler immediately, because when wg's memory not sufficient,
                // and then set wg's flag, other query may not free memory very quickly.
                if (query_it->elapsed_time() > 1000) {
                    // set wg's memory to insufficent, then add it back to task scheduler to run.
                    query_ctx->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                } else {
                    ++query_it;
                }
                continue;
            } else {
                // If wg's memlimit not exceed, but process memory exceed, it means cache or other metadata
                // used too much memory. Should clean all cache here.
                // 1. Check cache used, if cache is larger than > 0, then just return and wait for it to 0 to release some memory.
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted >
                            0.001 &&
                    doris::GlobalMemoryArbitrator::last_wg_trigger_cache_capacity_adjust_weighted >
                            0.001) {
                    doris::GlobalMemoryArbitrator::last_wg_trigger_cache_capacity_adjust_weighted =
                            0;
                    doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
                    LOG(INFO) << "There are some queries need process memory, so that set cache "
                                 "capacity "
                                 "to 0 now";
                }
                if (query_it->cache_ratio_ < 0.001) {
                    bool spill_res = spill_or_cancel_query(query_ctx, query_ctx->paused_reason());
                    if (!spill_res) {
                        ++query_it;
                        continue;
                    } else {
                        query_it = queries_list.erase(query_it);
                        continue;
                    }
                }
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted <
                            0.001 &&
                    query_it->cache_ratio_ > 0.001) {
                    query_ctx->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                ++query_it;
            }
        }

        // Finished deal with one workload group, and should deal with next one.
        ++it;
    }
}

bool WorkloadGroupMgr::spill_or_cancel_query(std::shared_ptr<QueryContext> query_ctx,
                                             Status paused_reason) {
    // TODO: If the query is an insert into select query, should consider memtable as revoke memory.
    size_t revocable_size = 0;
    size_t memory_usage = 0;
    bool has_running_task = false;
    query_ctx->get_revocable_info(&revocable_size, &memory_usage, &has_running_task);
    if (has_running_task) {
        LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                  << "is paused, but still has running task, skip it.";
        return false;
    }

    auto revocable_tasks = query_ctx->get_revocable_tasks();
    if (revocable_tasks.empty()) {
        if (paused_reason.is<ErrorCode::QUERY_MEMORY_EXCEED>()) {
            // Use MEM_LIMIT_EXCEEDED so that FE could parse the error code and do try logic
            query_ctx->cancel(doris::Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED>(
                    "query reserve memory failed, but could not find  memory that "
                    "could "
                    "release or spill to disk"));
        } else {
            query_ctx->cancel(doris::Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED>(
                    "The query reserved memory failed because process limit exceeded, and "
                    "there is no cache now. And could not find task to spill. Maybe you should set "
                    "the workload group's limit to a lower value."));
        }
    } else {
        SCOPED_ATTACH_TASK(query_ctx.get());
        // TODO, should spill the task that has max memory, not all
        for (auto* task : revocable_tasks) {
            auto st = task->revoke_memory();
            if (!st.ok()) {
                query_ctx->cancel(st);
                break;
            }
        }
        LOG(INFO) << "query: " << print_id(query_ctx->query_id()) << ", has "
                  << revocable_tasks.size()
                  << " tasks to revoke memory, revocable size: " << revocable_size;
    }
    return true;
}

void WorkloadGroupMgr::stop() {
    for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

void WorkloadGroupMgr::update_load_memtable_usage(
        const std::map<uint64_t, MemtableUsage>& wg_memtable_usages) {
    // Use readlock here, because it will not modify workload_groups
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (auto it = _workload_groups.begin(); it != _workload_groups.end(); ++it) {
        auto wg_usage = wg_memtable_usages.find(it->first);
        if (wg_usage != wg_memtable_usages.end()) {
            it->second->update_load_mem_usage(wg_usage->second.active_mem_usage,
                                              wg_usage->second.queue_mem_usage,
                                              wg_usage->second.flush_mem_usage);
        } else {
            // Not anything in memtable limiter, then set to 0
            it->second->update_load_mem_usage(0, 0, 0);
        }
    }
}

} // namespace doris
