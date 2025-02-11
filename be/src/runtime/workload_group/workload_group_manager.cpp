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

#include "common/config.h"
#include "common/status.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "pipeline/task_scheduler.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_metrics.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"
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

WorkloadGroupPtr WorkloadGroupMgr::get_group(uint64_t wg_id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_workload_groups.find(wg_id) != _workload_groups.end()) {
        return _workload_groups.at(wg_id);
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
    bool has_wg_exceed_limit = false;
    for (auto& [wg_id, wg] : _workload_groups) {
        all_workload_groups_mem_usage += wg->refresh_memory_usage();
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
        update_queries_limit_(wg.second, false);
    }
}

void WorkloadGroupMgr::get_wg_resource_usage(vectorized::Block* block) {
    int64_t be_id = ExecEnv::GetInstance()->cluster_info()->backend_id;
    int cpu_num = CpuInfo::num_cores();
    cpu_num = cpu_num <= 0 ? 1 : cpu_num;
    uint64_t total_cpu_time_ns_per_second = cpu_num * 1000000000ll;

    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    block->reserve(_workload_groups.size());
    for (const auto& [id, wg] : _workload_groups) {
        SchemaScannerHelper::insert_int64_value(0, be_id, block);
        SchemaScannerHelper::insert_int64_value(1, wg->id(), block);
        SchemaScannerHelper::insert_int64_value(2, wg->get_metrics()->get_memory_used(), block);

        double cpu_usage_p = (double)wg->get_metrics()->get_cpu_time_nanos_per_second() /
                             (double)total_cpu_time_ns_per_second * 100;
        cpu_usage_p = std::round(cpu_usage_p * 100.0) / 100.0;

        SchemaScannerHelper::insert_double_value(3, cpu_usage_p, block);
        SchemaScannerHelper::insert_int64_value(
                4, wg->get_metrics()->get_local_scan_bytes_per_second(), block);
        SchemaScannerHelper::insert_int64_value(
                5, wg->get_metrics()->get_remote_scan_bytes_per_second(), block);
        SchemaScannerHelper::insert_int64_value(6, wg->write_buffer_size(), block);
    }
}

void WorkloadGroupMgr::refresh_workload_group_metrics() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, wg] : _workload_groups) {
        wg->get_metrics()->refresh_metrics();
    }
}

void WorkloadGroupMgr::add_paused_query(const std::shared_ptr<QueryContext>& query_ctx,
                                        int64_t reserve_size, const Status& status) {
    DCHECK(query_ctx != nullptr);
    query_ctx->update_paused_reason(status);
    query_ctx->set_low_memory_mode();
    query_ctx->set_memory_sufficient(false);
    std::lock_guard<std::mutex> lock(_paused_queries_lock);
    auto wg = query_ctx->workload_group();
    auto&& [it, inserted] = _paused_queries_list[wg].emplace(
            query_ctx, doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted,
            doris::GlobalMemoryArbitrator::any_workload_group_exceed_limit, reserve_size);
    // Check if this is an invalid reserve, for example, if the reserve size is too large, larger than the query limit
    // if hard limit is enabled, then not need enable other queries hard limit.
    if (inserted) {
        LOG(INFO) << "Insert one new paused query: " << query_ctx->debug_string()
                  << ", workload group: " << wg->debug_string();
    }
}

/**
 * Strategy 1: A revocable query should not have any running task(PipelineTask).
 * strategy 2: If the workload group has any task exceed workload group memlimit, then set all queryctx's memlimit
 * strategy 3: If any query exceed process memlimit, then should clear all caches.
 * strategy 4: If any query exceed query's memlimit, then do spill disk or cancel it.
 * strategy 5: If any query exceed process's memlimit and cache is zero, then do following:
 */
void WorkloadGroupMgr::handle_paused_queries() {
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& [wg_id, wg] : _workload_groups) {
            std::unique_lock<std::mutex> lock(_paused_queries_lock);
            if (_paused_queries_list[wg].empty()) {
                // Add an empty set to wg that not contains paused queries.
            }
        }
    }

    std::unique_lock<std::mutex> lock(_paused_queries_lock);
    bool has_revoked_from_other_group = false;
    bool has_query_exceed_process_memlimit = false;
    for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
        auto& queries_list = it->second;
        auto query_count = queries_list.size();
        const auto& wg = it->first;

        if (query_count != 0) {
            LOG_EVERY_T(INFO, 1) << "Paused queries count of wg " << wg->name() << ": "
                                 << query_count;
        }

        bool has_changed_hard_limit = false;
        int64_t flushed_memtable_bytes = 0;
        // If the query is paused because its limit exceed the query itself's memlimit, then just spill disk.
        // The query's memlimit is set using slot mechanism and its value is set using the user settings, not
        // by weighted value. So if reserve failed, then it is actually exceed limit.
        for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
            auto query_ctx = query_it->query_ctx_.lock();
            // The query is finished during in paused list.
            if (query_ctx == nullptr) {
                LOG(INFO) << "Query: " << query_it->query_id() << " is nullptr, erase it.";
                query_it = queries_list.erase(query_it);
                continue;
            }
            if (query_ctx->is_cancelled()) {
                LOG(INFO) << "Query: " << print_id(query_ctx->query_id())
                          << " was canceled, remove from paused list";
                query_it = queries_list.erase(query_it);
                continue;
            }

            if (query_ctx->paused_reason().is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
                // Streamload, kafka load, group commit will never have query memory exceeded error because
                // their  query limit is very large.
                bool spill_res =
                        handle_single_query_(query_ctx, query_it->reserve_size_,
                                             query_it->elapsed_time(), query_ctx->paused_reason());
                if (!spill_res) {
                    ++query_it;
                    continue;
                } else {
                    VLOG_DEBUG << "Query: " << print_id(query_ctx->query_id())
                               << " remove from paused list";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
            } else if (query_ctx->paused_reason().is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
                // Only deal with non overcommit workload group.
                if (wg->enable_memory_overcommit()) {
                    // Soft limit wg will only reserve failed when process limit exceed. But in some corner case,
                    // when reserve, the wg is hard limit, the query reserve failed, but when this loop run
                    // the wg is converted to soft limit.
                    // So that should resume the query.
                    LOG(WARNING)
                            << "Query: " << print_id(query_ctx->query_id())
                            << " reserve memory failed because exceed workload group memlimit, it "
                               "should not happen, resume it again. paused reason: "
                            << query_ctx->paused_reason();
                    query_ctx->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                // check if the reserve is too large, if it is too large,
                // should set the query's limit only.
                // Check the query's reserve with expected limit.
                if (query_ctx->adjusted_mem_limit() <
                    query_ctx->get_mem_tracker()->consumption() + query_it->reserve_size_) {
                    query_ctx->set_mem_limit(query_ctx->adjusted_mem_limit());
                    query_ctx->set_memory_sufficient(true);
                    LOG(INFO) << "Workload group memory reserve failed because "
                              << query_ctx->debug_string() << " reserve size "
                              << PrettyPrinter::print_bytes(query_it->reserve_size_)
                              << " is too large, set hard limit to "
                              << PrettyPrinter::print_bytes(query_ctx->adjusted_mem_limit())
                              << " and resume running.";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                if (flushed_memtable_bytes <= 0) {
                    flushed_memtable_bytes =
                            flush_memtable_from_current_group_(wg, query_it->reserve_size_);
                }
                if (flushed_memtable_bytes > 0) {
                    // Flushed some memtable, just wait flush finished and not do anything more.
                    wg->enable_write_buffer_limit(true);
                    ++query_it;
                    continue;
                }
                if (!has_changed_hard_limit) {
                    update_queries_limit_(wg, true);
                    has_changed_hard_limit = true;
                    LOG(INFO) << "Query: " << print_id(query_ctx->query_id()) << " reserve memory("
                              << PrettyPrinter::print_bytes(query_it->reserve_size_)
                              << ") failed due to workload group memory exceed, "
                                 "should set the workload group work in memory insufficent mode, "
                                 "so that other query will reduce their memory."
                              << " Query mem limit: "
                              << PrettyPrinter::print_bytes(query_ctx->get_mem_limit())
                              << " mem usage: "
                              << PrettyPrinter::print_bytes(
                                         query_ctx->get_mem_tracker()->consumption())
                              << ", wg: " << wg->debug_string();
                }
                if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::NONE) {
                    // If not enable slot memory policy, then should spill directly
                    // Maybe there are another query that use too much memory, but we
                    // not encourage not enable slot memory.
                    // TODO should kill the query that exceed limit.
                    bool spill_res = handle_single_query_(query_ctx, query_it->reserve_size_,
                                                          query_it->elapsed_time(),
                                                          query_ctx->paused_reason());
                    if (!spill_res) {
                        ++query_it;
                        continue;
                    } else {
                        VLOG_DEBUG << "Query: " << print_id(query_ctx->query_id())
                                   << " remove from paused list";
                        query_it = queries_list.erase(query_it);
                        continue;
                    }
                } else {
                    // Should not put the query back to task scheduler immediately, because when wg's memory not sufficient,
                    // and then set wg's flag, other query may not free memory very quickly.
                    if (query_it->elapsed_time() > config::spill_in_paused_queue_timeout_ms) {
                        // set wg's memory to insufficent, then add it back to task scheduler to run.
                        LOG(INFO) << "Query: " << print_id(query_ctx->query_id())
                                  << " will be resume.";
                        query_ctx->set_memory_sufficient(true);
                        query_it = queries_list.erase(query_it);
                        continue;
                    } else {
                        ++query_it;
                        continue;
                    }
                }
            } else {
                has_query_exceed_process_memlimit = true;
                // If wg's memlimit not exceed, but process memory exceed, it means cache or other metadata
                // used too much memory. Should clean all cache here.
                // 1. Check cache used, if cache is larger than > 0, then just return and wait for it to 0 to release some memory.
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted >
                            0.05 &&
                    doris::GlobalMemoryArbitrator::last_wg_trigger_cache_capacity_adjust_weighted >
                            0.05) {
                    doris::GlobalMemoryArbitrator::last_wg_trigger_cache_capacity_adjust_weighted =
                            0.04;
                    doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
                    LOG(INFO) << "There are some queries need process memory, so that set cache "
                                 "capacity "
                                 "to 0 now";
                }
                // need to check config::disable_memory_gc here, if not, when config::disable_memory_gc == true,
                // cache is not adjusted, query_it->cache_ratio_ will always be 1, and this if branch will nenver
                // execute, this query will never be resumed, and will deadlock here
                if ((!config::disable_memory_gc && query_it->cache_ratio_ < 0.05) ||
                    config::disable_memory_gc) {
                    // 1. Check if could revoke some memory from memtable
                    if (flushed_memtable_bytes <= 0) {
                        flushed_memtable_bytes =
                                flush_memtable_from_current_group_(wg, query_it->reserve_size_);
                    }
                    if (flushed_memtable_bytes > 0) {
                        // Flushed some memtable, just wait flush finished and not do anything more.
                        ++query_it;
                        continue;
                    }
                    // TODO should wait here to check if the process has release revoked_size memory and then continue.
                    if (!has_revoked_from_other_group) {
                        int64_t revoked_size = revoke_memory_from_other_group_(
                                query_ctx, wg->enable_memory_overcommit(), query_it->reserve_size_);
                        if (revoked_size > 0) {
                            has_revoked_from_other_group = true;
                            query_ctx->set_memory_sufficient(true);
                            VLOG_DEBUG << "Query: " << print_id(query_ctx->query_id())
                                       << " is resumed after revoke memory from other group.";
                            query_it = queries_list.erase(query_it);
                            // Do not care if the revoked_size > reserve size, and try to run again.
                            continue;
                        } else {
                            bool spill_res = handle_single_query_(
                                    query_ctx, query_it->reserve_size_, query_it->elapsed_time(),
                                    query_ctx->paused_reason());
                            if (spill_res) {
                                VLOG_DEBUG << "Query: " << print_id(query_ctx->query_id())
                                           << " remove from paused list";
                                query_it = queries_list.erase(query_it);
                                continue;
                            } else {
                                ++query_it;
                                continue;
                            }
                        }
                    } else {
                        // If any query is cancelled during process limit stage, should resume other query and
                        // do not do any check now.
                        query_ctx->set_memory_sufficient(true);
                        VLOG_DEBUG << "Query: " << print_id(query_ctx->query_id())
                                   << " remove from paused list";
                        query_it = queries_list.erase(query_it);
                        continue;
                    }
                }
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted <
                            0.05 &&
                    query_it->cache_ratio_ > 0.05) {
                    LOG(INFO) << "Query: " << print_id(query_ctx->query_id())
                              << " will be resume after cache adjust.";
                    query_ctx->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                ++query_it;
            }
        }

        bool is_low_watermark = false;
        bool is_high_watermark = false;
        wg->check_mem_used(&is_low_watermark, &is_high_watermark);
        // Not need waiting flush memtable and below low watermark disable load buffer limit
        if (flushed_memtable_bytes <= 0 && !is_low_watermark) {
            wg->enable_write_buffer_limit(false);
        }

        if (queries_list.empty()) {
            it = _paused_queries_list.erase(it);
            continue;
        } else {
            // Finished deal with one workload group, and should deal with next one.
            ++it;
        }
    }

    if (has_query_exceed_process_memlimit) {
        // No query failed due to process exceed limit, so that enable cache now.
        doris::GlobalMemoryArbitrator::last_wg_trigger_cache_capacity_adjust_weighted = 1;
    }
}

// Return the expected free bytes if memtable could flush
int64_t WorkloadGroupMgr::flush_memtable_from_current_group_(WorkloadGroupPtr wg,
                                                             int64_t need_free_mem) {
    // If there are a lot of memtable memory, then wait them flush finished.
    MemTableMemoryLimiter* memtable_limiter =
            doris::ExecEnv::GetInstance()->memtable_memory_limiter();
    int64_t memtable_active_bytes = 0;
    int64_t memtable_queue_bytes = 0;
    int64_t memtable_flush_bytes = 0;
    DCHECK(memtable_limiter != nullptr) << "memtable limiter is nullptr";
    memtable_limiter->get_workload_group_memtable_usage(
            wg->id(), &memtable_active_bytes, &memtable_queue_bytes, &memtable_flush_bytes);
    // TODO: should add a signal in memtable limiter to prevent new batch
    // For example, streamload, it will not reserve many memory, but it will occupy many memtable memory.
    // TODO: 0.2 should be a workload group properties. For example, the group is optimized for load,then the value
    // should be larged, if the group is optimized for query, then the value should be smaller.
    int64_t max_wg_memtable_bytes = wg->write_buffer_limit();
    if (memtable_active_bytes + memtable_queue_bytes + memtable_flush_bytes >
        max_wg_memtable_bytes) {
        // There are many table in flush queue, just waiting them flush finished.
        if (memtable_active_bytes < (int64_t)(max_wg_memtable_bytes * 0.6)) {
            LOG_EVERY_T(INFO, 60) << wg->name()
                                  << " load memtable size is: " << memtable_active_bytes << ", "
                                  << memtable_queue_bytes << ", " << memtable_flush_bytes
                                  << ", load buffer limit is: " << max_wg_memtable_bytes
                                  << " wait for flush finished to release more memory";
            return memtable_queue_bytes + memtable_flush_bytes;
        } else {
            // Flush some memtables(currently written) to flush queue.
            memtable_limiter->flush_workload_group_memtables(
                    wg->id(), memtable_active_bytes - (int64_t)(max_wg_memtable_bytes * 0.6));
            LOG_EVERY_T(INFO, 60) << wg->name()
                                  << " load memtable size is: " << memtable_active_bytes << ", "
                                  << memtable_queue_bytes << ", " << memtable_flush_bytes
                                  << ", flush some active memtable to revoke memory";
            return memtable_queue_bytes + memtable_flush_bytes + memtable_active_bytes -
                   (int64_t)(max_wg_memtable_bytes * 0.6);
        }
    }
    return 0;
}

int64_t WorkloadGroupMgr::revoke_memory_from_other_group_(std::shared_ptr<QueryContext> requestor,
                                                          bool hard_limit, int64_t need_free_mem) {
    int64_t total_freed_mem = 0;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("RevokeMemory");
    // 1. memtable like memory
    // 2. query exceed workload group limit
    int64_t freed_mem = revoke_overcommited_memory_(requestor, need_free_mem, profile.get());
    total_freed_mem += freed_mem;
    // The revoke process may kill current requestor, so should return now.
    if (need_free_mem - total_freed_mem < 0 || requestor->is_cancelled()) {
        return total_freed_mem;
    }
    if (hard_limit) {
        freed_mem = cancel_top_query_in_overcommit_group_(need_free_mem - total_freed_mem,
                                                          doris::QUERY_MIN_MEMORY, profile.get());
    } else {
        freed_mem = cancel_top_query_in_overcommit_group_(
                need_free_mem - total_freed_mem, requestor->get_mem_tracker()->consumption(),
                profile.get());
    }
    total_freed_mem += freed_mem;
    // The revoke process may kill current requestor, so should return now.
    if (need_free_mem - total_freed_mem < 0 || requestor->is_cancelled()) {
        return total_freed_mem;
    }
    return total_freed_mem;
}

// Revoke memory from workload group that exceed it's limit. For example, if the wg's limit is 10g, but used 12g
// then should revoke 2g from the group.
int64_t WorkloadGroupMgr::revoke_overcommited_memory_(std::shared_ptr<QueryContext> requestor,
                                                      int64_t need_free_mem,
                                                      RuntimeProfile* profile) {
    int64_t total_freed_mem = 0;
    // 1. check memtable usage, and try to free them.
    int64_t freed_mem = revoke_memtable_from_overcommited_groups_(need_free_mem, profile);
    total_freed_mem += freed_mem;
    // The revoke process may kill current requestor, so should return now.
    if (need_free_mem - total_freed_mem < 0 || requestor->is_cancelled()) {
        return total_freed_mem;
    }
    // 2. Cancel top usage query, one by one
    using WorkloadGroupMem = std::pair<WorkloadGroupPtr, int64_t>;
    auto cmp = [](WorkloadGroupMem left, WorkloadGroupMem right) {
        return left.second < right.second;
    };
    std::priority_queue<WorkloadGroupMem, std::vector<WorkloadGroupMem>, decltype(cmp)> heap(cmp);
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
            if (requestor->workload_group() != nullptr &&
                iter->second->id() == requestor->workload_group()->id()) {
                continue;
            }
            heap.emplace(iter->second, iter->second->memory_used());
        }
    }
    while (!heap.empty() && need_free_mem - total_freed_mem > 0 && !requestor->is_cancelled()) {
        auto [wg, sort_mem] = heap.top();
        heap.pop();
        freed_mem = wg->free_overcommited_memory(need_free_mem - total_freed_mem, profile);
        total_freed_mem += freed_mem;
    }
    return total_freed_mem;
}

// If the memtable is too large, then flush them and wait for finished.
int64_t WorkloadGroupMgr::revoke_memtable_from_overcommited_groups_(int64_t need_free_mem,
                                                                    RuntimeProfile* profile) {
    return 0;
}

// 1. Sort all memory limiter in all overcommit wg, and cancel the top usage task that with most memory.
// 2. Maybe not valid because it's memory not exceed limit.
int64_t WorkloadGroupMgr::cancel_top_query_in_overcommit_group_(int64_t need_free_mem,
                                                                int64_t lower_bound,
                                                                RuntimeProfile* profile) {
    return 0;
}

// streamload, kafka routine load, group commit
// insert into select
// select

// If the query could release some memory, for example, spill disk, then the return value is true.
// If the query could not release memory, then cancel the query, the return value is true.
// If the query is not ready to do these tasks, it means just wait, then return value is false.
bool WorkloadGroupMgr::handle_single_query_(const std::shared_ptr<QueryContext>& query_ctx,
                                            size_t size_to_reserve, int64_t time_in_queue,
                                            Status paused_reason) {
    size_t revocable_size = 0;
    size_t memory_usage = 0;
    bool has_running_task = false;
    const auto query_id = print_id(query_ctx->query_id());
    query_ctx->get_revocable_info(&revocable_size, &memory_usage, &has_running_task);
    if (has_running_task) {
        LOG(INFO) << "Query: " << print_id(query_ctx->query_id())
                  << " is paused, but still has running task, skip it.";
        return false;
    }

    const auto wg = query_ctx->workload_group();
    auto revocable_tasks = query_ctx->get_revocable_tasks();
    if (revocable_tasks.empty()) {
        const auto limit = query_ctx->get_mem_limit();
        const auto reserved_size = query_ctx->query_mem_tracker()->reserved_consumption();
        if (paused_reason.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
            // During waiting time, another operator in the query may finished and release
            // many memory and we could run.
            if ((memory_usage + size_to_reserve) < limit) {
                LOG(INFO) << "Query: " << query_id << ", usage("
                          << PrettyPrinter::print_bytes(memory_usage) << " + " << size_to_reserve
                          << ") less than limit(" << PrettyPrinter::print_bytes(limit)
                          << "), resume it.";
                query_ctx->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue >= config::spill_in_paused_queue_timeout_ms) {
                // if cannot find any memory to release, then let the query continue to run as far as possible
                // or cancelled by gc if memory is really not enough.
                auto log_str = fmt::format(
                        "Query {} memory limit is exceeded, but could "
                        "not find memory that could release or spill to disk, disable reserve "
                        "memory and resume it. Query memory usage: "
                        "{}, limit: {}, reserved "
                        "size: {}, try to reserve: {}, wg info: {}. {}",
                        query_id, PrettyPrinter::print_bytes(memory_usage),
                        PrettyPrinter::print_bytes(limit),
                        PrettyPrinter::print_bytes(reserved_size),
                        PrettyPrinter::print_bytes(size_to_reserve), wg->memory_debug_string(),
                        doris::ProcessProfile::instance()
                                ->memory_profile()
                                ->process_memory_detail_str());
                LOG_LONG_STRING(INFO, log_str);
                query_ctx->disable_reserve_memory();
                query_ctx->set_memory_sufficient(true);
                return true;
            } else {
                return false;
            }
        } else if (paused_reason.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
            if (!wg->exceed_limit()) {
                LOG(INFO) << "Query: " << query_id
                          << " paused caused by WORKLOAD_GROUP_MEMORY_EXCEEDED, now resume it.";
                query_ctx->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue > config::spill_in_paused_queue_timeout_ms) {
                // if cannot find any memory to release, then let the query continue to run as far as possible
                // or cancelled by gc if memory is really not enough.
                auto log_str = fmt::format(
                        "Query {} workload group memory is exceeded"
                        ", and there is no cache now. And could not find task to spill, disable "
                        "reserve memory and resume it. "
                        "Query memory usage: {}, limit: {}, reserved "
                        "size: {}, try to reserve: {}, wg info: {}."
                        " Maybe you should set the workload group's limit to a lower value. {}",
                        query_id, PrettyPrinter::print_bytes(memory_usage),
                        PrettyPrinter::print_bytes(limit),
                        PrettyPrinter::print_bytes(reserved_size),
                        PrettyPrinter::print_bytes(size_to_reserve), wg->memory_debug_string(),
                        doris::ProcessProfile::instance()
                                ->memory_profile()
                                ->process_memory_detail_str());
                LOG_LONG_STRING(INFO, log_str);
                query_ctx->disable_reserve_memory();
                query_ctx->set_memory_sufficient(true);
                return true;
            } else {
                return false;
            }
        } else {
            // Should not consider about process memory. For example, the query's limit is 100g, workload
            // group's memlimit is 10g, process memory is 20g. The query reserve will always failed in wg
            // limit, and process is always have memory, so that it will resume and failed reserve again.
            const size_t test_memory_size = std::max<size_t>(size_to_reserve, 32L * 1024 * 1024);
            if (!GlobalMemoryArbitrator::is_exceed_soft_mem_limit(test_memory_size)) {
                LOG(INFO) << "Query: " << query_id
                          << ", process limit not exceeded now, resume this query"
                          << ", process memory info: "
                          << GlobalMemoryArbitrator::process_memory_used_details_str()
                          << ", wg info: " << wg->debug_string();
                query_ctx->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue > config::spill_in_paused_queue_timeout_ms) {
                // if cannot find any memory to release, then let the query continue to run as far as possible
                // or cancelled by gc if memory is really not enough.
                auto log_str = fmt::format(
                        "Query {} process memory is exceeded"
                        ", and there is no cache now. And could not find task to spill, disable "
                        "reserve memory and resume it. "
                        "Query memory usage: {}, limit: {}, reserved "
                        "size: {}, try to reserve: {}, wg info: {}."
                        " Maybe you should set the workload group's limit to a lower value. {}",
                        query_id, PrettyPrinter::print_bytes(memory_usage),
                        PrettyPrinter::print_bytes(limit),
                        PrettyPrinter::print_bytes(reserved_size),
                        PrettyPrinter::print_bytes(size_to_reserve), wg->memory_debug_string(),
                        doris::ProcessProfile::instance()
                                ->memory_profile()
                                ->process_memory_detail_str());
                LOG_LONG_STRING(INFO, log_str);
                query_ctx->disable_reserve_memory();
                query_ctx->set_memory_sufficient(true);
            } else {
                return false;
            }
        }
    } else {
        SCOPED_ATTACH_TASK(query_ctx.get());
        auto status = query_ctx->revoke_memory();
        if (!status.ok()) {
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(query_ctx->query_id(), status);
        }
    }
    return true;
}

void WorkloadGroupMgr::update_queries_limit_(WorkloadGroupPtr wg, bool enable_hard_limit) {
    auto wg_mem_limit = wg->memory_limit();
    auto all_query_ctxs = wg->queries();
    bool is_low_watermark = false;
    bool is_high_watermark = false;
    wg->check_mem_used(&is_low_watermark, &is_high_watermark);
    int64_t wg_high_water_mark_limit =
            (int64_t)(wg_mem_limit * wg->memory_high_watermark() * 1.0 / 100);
    int64_t memtable_usage = wg->write_buffer_size();
    int64_t wg_high_water_mark_except_load = wg_high_water_mark_limit;
    if (memtable_usage > wg->write_buffer_limit()) {
        wg_high_water_mark_except_load = wg_high_water_mark_limit - wg->write_buffer_limit();
    } else {
        wg_high_water_mark_except_load =
                wg_high_water_mark_limit - memtable_usage - 10 * 1024 * 1024;
    }
    std::string debug_msg;
    if (is_high_watermark || is_low_watermark) {
        debug_msg = fmt::format(
                "\nWorkload Group {}: mem limit: {}, mem used: {}, "
                "high water mark mem limit: {}, load memtable usage: {}, used ratio: {}",
                wg->name(), PrettyPrinter::print(wg->memory_limit(), TUnit::BYTES),
                PrettyPrinter::print(wg->total_mem_used(), TUnit::BYTES),
                PrettyPrinter::print(wg_high_water_mark_limit, TUnit::BYTES),
                PrettyPrinter::print(memtable_usage, TUnit::BYTES),
                (double)(wg->total_mem_used()) / wg_mem_limit);
    }

    // If reached low watermark and wg is not enable memory overcommit, then enable load buffer limit
    if (is_low_watermark && !wg->enable_memory_overcommit()) {
        wg->enable_write_buffer_limit(true);
    }
    // Both enable overcommit and not enable overcommit, if user set slot memory policy
    // then we will replace the memtracker's memlimit with
    if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::NONE) {
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
        if (is_low_watermark) {
            query_ctx->set_low_memory_mode();
        }
        int64_t query_weighted_mem_limit = 0;
        int64_t expected_query_weighted_mem_limit = 0;
        // If the query enable hard limit, then it should not use the soft limit
        if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::FIXED) {
            if (total_slot_count < 1) {
                LOG(WARNING)
                        << "Query " << print_id(query_ctx->query_id())
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
            if (!is_low_watermark && !enable_hard_limit) {
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
            query_ctx->set_adjusted_mem_limit(expected_query_weighted_mem_limit);
        }
    }
    LOG_EVERY_T(INFO, 60) << debug_msg;
}

void WorkloadGroupMgr::stop() {
    for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

} // namespace doris
