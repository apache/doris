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

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "pipeline/task_scheduler.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "util/mem_info.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

PausedQuery::PausedQuery(std::shared_ptr<QueryContext> query_ctx, double cache_ratio,
                         bool any_wg_exceed_limit, int64_t reserve_size)
        : query_ctx_(query_ctx),
          cache_ratio_(cache_ratio),
          any_wg_exceed_limit_(any_wg_exceed_limit),
          reserve_size_(reserve_size),
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

void WorkloadGroupMgr::do_sweep() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (auto& [wg_id, wg] : _workload_groups) {
        wg->do_sweep();
    }
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
    bool has_wg_exceed_limit = false;
    for (auto& [wg_id, wg] : _workload_groups) {
        wgs_mem_info[wg_id].total_mem_used =
                wg->make_memory_tracker_snapshots(&wgs_mem_info[wg_id].tracker_snapshots);
        all_workload_groups_mem_usage += wgs_mem_info[wg_id].total_mem_used;
        if (wg->exceed_limit()) {
            has_wg_exceed_limit = true;
        }
    }
    doris::GlobalMemoryArbitrator::any_workload_group_exceed_limit = has_wg_exceed_limit;
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
    LOG_EVERY_T(INFO, 60) << debug_msg;
    for (auto& wg : _workload_groups) {
        update_queries_limit(wg.second, false);
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

void WorkloadGroupMgr::add_paused_query(const std::shared_ptr<QueryContext>& query_ctx,
                                        int64_t reserve_size) {
    std::lock_guard<std::mutex> lock(_paused_queries_lock);
    DCHECK(query_ctx != nullptr);
    auto wg = query_ctx->workload_group();
    auto&& [it, inserted] = _paused_queries_list[wg].emplace(
            query_ctx, doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted,
            doris::GlobalMemoryArbitrator::any_workload_group_exceed_limit, reserve_size);
    // Check if this is an invalid reserve, for example, if the reserve size is too large, larger than the query limit
    // if hard limit is enabled, then not need enable other queries hard limit.
    if (inserted) {
        query_ctx->set_memory_sufficient(false);
        LOG(INFO) << "workload group " << wg->debug_string()
                  << " insert one new paused query: " << query_ctx->debug_string();
    }
}

/**
 * 1. When Process's memory is lower than soft limit, then all workload group will be converted to hard limit (Exception: there is only one workload group).
 * 2. Reserve logic for workload group that is soft limit take no effect, it will always return success.
 * 3. QueryLimit for streamload,routineload,group commit, take no affect, it will always return success, but workload group's hard limit will take affect.
 * 4. See handle_non_overcommit_wg_paused_queries for hard limit logic.
 */
void WorkloadGroupMgr::handle_paused_queries() {
    handle_non_overcommit_wg_paused_queries();
    handle_overcommit_wg_paused_queries();
}

/**
 * Strategy 1: A revocable query should not have any running task(PipelineTask).
 * strategy 2: If the workload group has any task exceed workload group memlimit, then set all queryctx's memlimit
 * strategy 3: If any query exceed process memlimit, then should clear all caches.
 * strategy 4: If any query exceed query's memlimit, then do spill disk or cancel it.
 * strategy 5: If any query exceed process's memlimit and cache is zero, then do following:
 * 1. cancel other wg's(soft limit) query that exceed limit
 * 2. spill disk
 * 3. cancel it self.
 */
void WorkloadGroupMgr::handle_non_overcommit_wg_paused_queries() {
    const int64_t TIMEOUT_IN_QUEUE = 1000L * 10;
    std::unique_lock<std::mutex> lock(_paused_queries_lock);
    std::vector<std::weak_ptr<QueryContext>> resume_after_gc;
    for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
        auto& queries_list = it->second;
        const auto& wg = it->first;
        if (queries_list.empty()) {
            it = _paused_queries_list.erase(it);
            continue;
        }
        bool is_low_wartermark = false;
        bool is_high_wartermark = false;

        wg->check_mem_used(&is_low_wartermark, &is_high_wartermark);
        bool has_changed_hard_limit = false;
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
                          << " was canceled, remove from paused list";
                query_it = queries_list.erase(query_it);
                continue;
            }
            bool wg_changed_to_hard_limit = wg->has_changed_to_hard_limit();
            // Only deal with non overcommit workload group.
            if (wg->enable_memory_overcommit() && !wg_changed_to_hard_limit &&
                !query_ctx->paused_reason().is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
                // Soft limit wg will only reserve failed when process limit exceed. But in some corner case,
                // when reserve, the wg is hard limit, the query reserve failed, but when this loop run
                // the wg is converted to soft limit.
                // So that should resume the query.
                LOG(WARNING) << "query: " << print_id(query_ctx->query_id())
                             << " reserve memory failed, but workload group not converted to hard "
                                "limit, it should not happen, resume it again. paused reason: "
                             << query_ctx->paused_reason();
                query_ctx->set_memory_sufficient(true);
                query_it = queries_list.erase(query_it);
                continue;
            }

            if (query_ctx->paused_reason().is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
                CHECK(!wg->enable_memory_overcommit() || wg_changed_to_hard_limit);
                // Streamload, kafka load, group commit will never have query memory exceeded error because
                // their  query limit is very large.
                bool spill_res = handle_single_query(query_ctx, query_it->reserve_size_,
                                                     query_ctx->paused_reason());
                if (!spill_res) {
                    ++query_it;
                    continue;
                } else {
                    query_it = queries_list.erase(query_it);
                    continue;
                }
            } else if (query_ctx->paused_reason().is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
                CHECK(!wg->enable_memory_overcommit() || wg_changed_to_hard_limit);
                // check if the reserve is too large, if it is too large,
                // should set the query's limit only.
                // Check the query's reserve with expected limit.
                if (query_ctx->expected_mem_limit() <
                    query_ctx->get_mem_tracker()->consumption() + query_it->reserve_size_) {
                    query_ctx->set_mem_limit(query_ctx->expected_mem_limit());
                    query_ctx->set_memory_sufficient(true);
                    LOG(INFO) << "workload group memory reserve failed because "
                              << query_ctx->debug_string() << " reserve size "
                              << query_it->reserve_size_ << " is too large, set hard limit to "
                              << query_ctx->expected_mem_limit() << " and resume running.";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                if (!has_changed_hard_limit) {
                    update_queries_limit(wg, true);
                    has_changed_hard_limit = true;
                    LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                              << " reserve memory failed due to workload group memory exceed, "
                                 "should set the workload group work in memory insufficent mode, "
                                 "so that other query will reduce their memory. wg: "
                              << wg->debug_string();
                }
                // If there are a lot of memtable memory, then wait them flush finished.
                MemTableMemoryLimiter* memtable_limiter =
                        doris::ExecEnv::GetInstance()->memtable_memory_limiter();
                // Not use memlimit, should use high water mark.
                int64_t memtable_active_bytes = 0;
                int64_t memtable_queue_bytes = 0;
                int64_t memtable_flush_bytes = 0;
                wg->get_load_mem_usage(&memtable_active_bytes, &memtable_queue_bytes,
                                       &memtable_flush_bytes);
                // TODO: should add a signal in memtable limiter to prevent new batch
                // For example, streamload, it will not reserve many memory, but it will occupy many memtable memory.
                // TODO: 0.2 should be a workload group properties. For example, the group is optimized for load,then the value
                // should be larged, if the group is optimized for query, then the value should be smaller.
                int64_t max_wg_memtable_bytes = wg->load_buffer_limit();
                if (memtable_active_bytes + memtable_queue_bytes + memtable_flush_bytes >
                    max_wg_memtable_bytes) {
                    // There are many table in flush queue, just waiting them flush finished.
                    if (memtable_active_bytes < (int64_t)(max_wg_memtable_bytes * 0.6)) {
                        LOG_EVERY_T(INFO, 60)
                                << wg->name() << " load memtable size is: " << memtable_active_bytes
                                << ", " << memtable_queue_bytes << ", " << memtable_flush_bytes
                                << ", load buffer limit is: " << max_wg_memtable_bytes
                                << " wait for flush finished to release more memory";
                        continue;
                    } else {
                        // Flush some memtables(currently written) to flush queue.
                        memtable_limiter->flush_workload_group_memtables(
                                wg->id(),
                                memtable_active_bytes - (int64_t)(max_wg_memtable_bytes * 0.6));
                        LOG_EVERY_T(INFO, 60)
                                << wg->name() << " load memtable size is: " << memtable_active_bytes
                                << ", " << memtable_queue_bytes << ", " << memtable_flush_bytes
                                << ", flush some active memtable to revoke memory";
                        continue;
                    }
                }
                // Should not put the query back to task scheduler immediately, because when wg's memory not sufficient,
                // and then set wg's flag, other query may not free memory very quickly.
                if (query_it->elapsed_time() > TIMEOUT_IN_QUEUE) {
                    // set wg's memory to insufficent, then add it back to task scheduler to run.
                    LOG(INFO) << "query: " << print_id(query_ctx->query_id()) << " will be resume.";
                    query_ctx->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                } else {
                    ++query_it;
                }
                continue;
            } else {
                // PROCESS Reserve logic using hard limit, if reached here, should try to spill or cancel.
                // GC Logic also work at hard limit, so GC may cancel some query and could not spill here.
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
                    if (query_it->any_wg_exceed_limit_) {
                        if (wg->enable_memory_overcommit()) {
                            if (query_it->elapsed_time() > TIMEOUT_IN_QUEUE) {
                                resume_after_gc.push_back(query_ctx);
                                query_it = queries_list.erase(query_it);
                                continue;
                            } else {
                                ++query_it;
                                continue;
                            }
                        } else {
                            // current workload group is hard limit, should not wait other wg with
                            // soft limit, just cancel
                            resume_after_gc.push_back(query_ctx);
                            query_it = queries_list.erase(query_it);
                            continue;
                        }
                    } else {
                        // TODO: Find other exceed limit workload group and cancel query.
                        bool spill_res = handle_single_query(query_ctx, query_it->reserve_size_,
                                                             query_ctx->paused_reason());
                        if (!spill_res) {
                            ++query_it;
                            continue;
                        } else {
                            query_it = queries_list.erase(query_it);
                            continue;
                        }
                    }
                }
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted <
                            0.001 &&
                    query_it->cache_ratio_ > 0.001) {
                    LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                              << " will be resume after cache adjust.";
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
    // TODO minor GC to release some query
    if (!resume_after_gc.empty()) {
    }
    for (auto resume_it = resume_after_gc.begin(); resume_it != resume_after_gc.end();
         ++resume_it) {
        auto query_ctx = resume_it->lock();
        if (query_ctx != nullptr) {
            query_ctx->set_memory_sufficient(true);
        }
    }
}

// streamload, kafka routine load, group commit
// insert into select
// select

void WorkloadGroupMgr::handle_overcommit_wg_paused_queries() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    // If there is only one workload group and it is overcommit, then do nothing.
    // And should also start MinorGC logic.
    if (_workload_groups.size() == 1) {
        return;
    }
    // soft_limit - 10%, will change workload group to hard limit.
    // soft limit, process memory reserve failed.
    // hard limit, FullGC will kill query randomly.
    if (doris::GlobalMemoryArbitrator::is_exceed_soft_mem_limit(
                (int64_t)(MemInfo::mem_limit() * 0.1))) {
        for (auto& [wg_id, wg] : _workload_groups) {
            if (wg->enable_memory_overcommit() && !wg->has_changed_to_hard_limit()) {
                wg->change_to_hard_limit(true);
                LOG(INFO) << "Process memory usage + 10% will exceed soft limit, change all "
                             "workload "
                             "group with overcommit to hard limit now. "
                          << wg->debug_string();
            }
        }
    }
    // If current memory usage is below soft memlimit - 15%, then enable wg's overcommit
    if (!doris::GlobalMemoryArbitrator::is_exceed_soft_mem_limit(
                (int64_t)(MemInfo::mem_limit() * 0.15))) {
        for (auto& [wg_id, wg] : _workload_groups) {
            if (wg->enable_memory_overcommit() && wg->has_changed_to_hard_limit()) {
                wg->change_to_hard_limit(false);
                LOG(INFO) << "Process memory usage is lower than soft limit, enable all workload "
                             "group overcommit now. "
                          << wg->debug_string();
            }
        }
    }
}

// If the query could release some memory, for example, spill disk, flush memtable then the return value is true.
// If the query could not release memory, then cancel the query, the return value is true.
// If the query is not ready to do these tasks, it means just wait.
bool WorkloadGroupMgr::handle_single_query(std::shared_ptr<QueryContext> query_ctx,
                                           size_t size_to_reserve, Status paused_reason) {
    // TODO: If the query is an insert into select query, should consider memtable as revoke memory.
    size_t revocable_size = 0;
    size_t memory_usage = 0;
    bool has_running_task = false;
    const auto query_id = print_id(query_ctx->query_id());
    query_ctx->get_revocable_info(&revocable_size, &memory_usage, &has_running_task);
    if (has_running_task) {
        LOG(INFO) << "query: " << print_id(query_ctx->query_id())
                  << " is paused, but still has running task, skip it.";
        return false;
    }

    auto revocable_tasks = query_ctx->get_revocable_tasks();
    if (revocable_tasks.empty()) {
        if (paused_reason.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
            const auto limit = query_ctx->get_mem_limit();
            if ((memory_usage + size_to_reserve) < limit) {
                LOG(INFO) << "query: " << query_id << ", usage(" << memory_usage << " + "
                          << size_to_reserve << ") less than limit(" << limit << "), resume it.";
                query_ctx->set_memory_sufficient(true);
                return true;
            } else {
                // Use MEM_LIMIT_EXCEEDED so that FE could parse the error code and do try logic
                query_ctx->cancel(doris::Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED>(
                        "query({}) reserve memory failed, but could not find  memory that could "
                        "release or spill to disk(usage:{}, limit: {})",
                        query_id, memory_usage, query_ctx->get_mem_limit()));
            }
        } else {
            if (!GlobalMemoryArbitrator::is_exceed_hard_mem_limit()) {
                LOG(INFO) << "query: " << query_id
                          << ", process limit not exceeded now, resume this query"
                          << ", process memory info: "
                          << GlobalMemoryArbitrator::process_memory_used_details_str()
                          << ", wg info: " << query_ctx->workload_group()->memory_debug_string();
                query_ctx->set_memory_sufficient(true);
                return true;
            }

            LOG(INFO) << "query: " << query_id << ", process limit exceeded, info: "
                      << GlobalMemoryArbitrator::process_memory_used_details_str()
                      << ", wg info: " << query_ctx->workload_group()->memory_debug_string();
            query_ctx->cancel(doris::Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED>(
                    "The query({}) reserved memory failed because process limit exceeded, and "
                    "there is no cache now. And could not find task to spill. Maybe you should set "
                    "the workload group's limit to a lower value.",
                    query_id));
        }
    } else {
        SCOPED_ATTACH_TASK(query_ctx.get());
        RETURN_IF_ERROR(query_ctx->revoke_memory());
    }
    return true;
}

void WorkloadGroupMgr::update_queries_limit(WorkloadGroupPtr wg, bool enable_hard_limit) {
    auto wg_mem_limit = wg->memory_limit();
    auto wg_weighted_mem_limit = int64_t(wg_mem_limit * 1);
    wg->set_weighted_memory_limit(wg_weighted_mem_limit);
    auto all_query_ctxs = wg->queries();
    bool is_low_wartermark = false;
    bool is_high_wartermark = false;
    wg->check_mem_used(&is_low_wartermark, &is_high_wartermark);
    int64_t wg_high_water_mark_limit =
            (int64_t)(wg_mem_limit * wg->spill_threshold_high_water_mark() * 1.0 / 100);
    int64_t memtable_active_bytes = 0;
    int64_t memtable_queue_bytes = 0;
    int64_t memtable_flush_bytes = 0;
    wg->get_load_mem_usage(&memtable_active_bytes, &memtable_queue_bytes, &memtable_flush_bytes);
    int64_t memtable_usage = memtable_active_bytes + memtable_queue_bytes + memtable_flush_bytes;
    int64_t wg_high_water_mark_except_load = wg_high_water_mark_limit;
    if (memtable_usage > wg->load_buffer_limit()) {
        wg_high_water_mark_except_load = wg_high_water_mark_limit - wg->load_buffer_limit();
    } else {
        wg_high_water_mark_except_load =
                wg_high_water_mark_limit - memtable_usage - 10 * 1024 * 1024;
    }
    std::string debug_msg;
    if (is_high_wartermark || is_low_wartermark) {
        debug_msg = fmt::format(
                "\nWorkload Group {}: mem limit: {}, mem used: {}, weighted mem limit: {}, "
                "high water mark mem limit: {}, load memtable usage: {}, used ratio: {}",
                wg->name(), PrettyPrinter::print(wg->memory_limit(), TUnit::BYTES),
                PrettyPrinter::print(wg->total_mem_used(), TUnit::BYTES),
                PrettyPrinter::print(wg_weighted_mem_limit, TUnit::BYTES),
                PrettyPrinter::print(wg_high_water_mark_limit, TUnit::BYTES),
                PrettyPrinter::print(memtable_usage, TUnit::BYTES),
                (double)(wg->total_mem_used()) / wg_weighted_mem_limit);
    }

    // If the wg enable over commit memory, then it is no need to update query memlimit
    if (wg->enable_memory_overcommit() && !wg->has_changed_to_hard_limit()) {
        return;
    }
    int32_t total_used_slot_count = 0;
    int32_t total_slot_count = wg->total_query_slot_count();
    // calculate total used slot count
    for (const auto& query : all_query_ctxs) {
        auto query_ctx = query.second.lock();
        if (!query_ctx) {
            continue;
        }
        // Streamload kafka load group commit, not modify slot
        if (!query_ctx->is_pure_load_task()) {
            total_used_slot_count += query_ctx->get_slot_count();
        }
    }
    // calculate per query weighted memory limit
    debug_msg = "Query Memory Summary: \n";
    for (const auto& query : all_query_ctxs) {
        auto query_ctx = query.second.lock();
        if (!query_ctx) {
            continue;
        }
        int64_t query_weighted_mem_limit = 0;
        int64_t expected_query_weighted_mem_limit = 0;
        // If the query enable hard limit, then it should not use the soft limit
        if (query_ctx->enable_query_slot_hard_limit()) {
            if (total_slot_count < 1) {
                LOG(WARNING)
                        << "query " << print_id(query_ctx->query_id())
                        << " enabled hard limit, but the slot count < 1, could not take affect";
            } else {
                // If the query enable hard limit, then not use weighted info any more, just use the settings limit.
                query_weighted_mem_limit = (int64_t)((wg_high_water_mark_except_load *
                                                      query_ctx->get_slot_count() * 1.0) /
                                                     total_slot_count);
                expected_query_weighted_mem_limit = query_weighted_mem_limit;
            }
        } else {
            // If low water mark is not reached, then use process memory limit as query memory limit.
            // It means it will not take effect.
            // If there are some query in paused list, then limit should take effect.
            expected_query_weighted_mem_limit =
                    total_used_slot_count > 0
                            ? (int64_t)((wg_high_water_mark_except_load + total_used_slot_count) *
                                        query_ctx->get_slot_count() * 1.0 / total_used_slot_count)
                            : wg_high_water_mark_except_load;
            if (!is_low_wartermark && !enable_hard_limit) {
                query_weighted_mem_limit = wg_high_water_mark_except_load;
            } else {
                query_weighted_mem_limit = expected_query_weighted_mem_limit;
            }
        }
        debug_msg += query_ctx->debug_string() + "\n";
        // If the query is a pure load task, then should not modify its limit. Or it will reserve
        // memory failed and we did not hanle it.
        if (!query_ctx->is_pure_load_task()) {
            query_ctx->set_mem_limit(query_weighted_mem_limit);
            query_ctx->set_expected_mem_limit(expected_query_weighted_mem_limit);
        }
    }
    LOG(INFO) << debug_msg;
    //LOG_EVERY_T(INFO, 60) << debug_msg;
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
