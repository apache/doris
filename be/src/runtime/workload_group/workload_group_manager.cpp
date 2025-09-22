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

#include "common/compile_check_begin.h"

const static std::string INTERNAL_NORMAL_WG_NAME = "normal";
const static uint64_t INTERNAL_NORMAL_WG_ID = 1;

PausedQuery::PausedQuery(std::shared_ptr<ResourceContext> resource_ctx, double cache_ratio,
                         int64_t reserve_size)
        : resource_ctx_(resource_ctx),
          cache_ratio_(cache_ratio),
          reserve_size_(reserve_size),
          query_id_(print_id(resource_ctx->task_controller()->task_id())) {
    enqueue_at = std::chrono::system_clock::now();
}

WorkloadGroupMgr::~WorkloadGroupMgr() = default;

WorkloadGroupMgr::WorkloadGroupMgr() = default;

WorkloadGroupPtr WorkloadGroupMgr::get_or_create_workload_group(
        const WorkloadGroupInfo& fe_wg_info) {
    std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
    // 1. update internal wg's id
    if (fe_wg_info.name == INTERNAL_NORMAL_WG_NAME) {
        // normal wg's id maybe not equal to BE's id, so that need update it
        reset_workload_group_id(INTERNAL_NORMAL_WG_NAME, fe_wg_info.id);
    }

    // 2. check and update wg
    if (LIKELY(_workload_groups.count(fe_wg_info.id))) {
        auto workload_group = _workload_groups[fe_wg_info.id];
        workload_group->check_and_update(fe_wg_info);
        return workload_group;
    }

    auto new_task_group = WorkloadGroup::create_shared(fe_wg_info);
    _workload_groups[fe_wg_info.id] = new_task_group;
    return new_task_group;
}

WorkloadGroupPtr WorkloadGroupMgr::get_group(std::vector<uint64_t>& id_list) {
    WorkloadGroupPtr ret_wg = nullptr;
    int wg_cout = 0;
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& wg_id : id_list) {
            if (_workload_groups.find(wg_id) != _workload_groups.end()) {
                wg_cout++;
                ret_wg = _workload_groups.at(wg_id);
            }
        }
    }

    if (wg_cout > 1) {
        std::stringstream ss;
        ss << "Unexpected error: find too much wg in BE; input id=";

        for (auto& id : id_list) {
            ss << id << ",";
        }

        ss << " be wg: ";
        for (auto& wg_pair : _workload_groups) {
            ss << wg_pair.second->debug_string() << ", ";
        }
        LOG(ERROR) << ss.str();
    }
    DCHECK(wg_cout <= 1);

    if (ret_wg == nullptr) {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& wg_pair : _workload_groups) {
            if (wg_pair.second->name() == INTERNAL_NORMAL_WG_NAME) {
                ret_wg = wg_pair.second;
                break;
            }
        }
    }

    if (ret_wg == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "not even find normal wg in BE");
    }
    return ret_wg;
}

void WorkloadGroupMgr::reset_workload_group_id(std::string workload_group_name, uint64_t new_id) {
    WorkloadGroupPtr wg_ptr = nullptr;
    uint64_t old_wg_id = -1;
    for (auto& wg_pair : _workload_groups) {
        uint64_t wg_id = wg_pair.first;
        WorkloadGroupPtr wg = wg_pair.second;
        if (workload_group_name == wg->name() && wg_id != new_id) {
            wg_ptr = wg_pair.second;
            old_wg_id = wg_id;
            break;
        }
    }
    if (wg_ptr) {
        _workload_groups.erase(old_wg_id);
        wg_ptr->set_id(new_id);
        _workload_groups[wg_ptr->id()] = wg_ptr;
        LOG(INFO) << "workload group's id changed, before: " << old_wg_id
                  << ", after:" << wg_ptr->id();
    }
}

void WorkloadGroupMgr::delete_workload_group_by_ids(std::set<uint64_t> used_wg_id) {
    int64_t begin_time = MonotonicMillis();
    // 1 get delete group without running queries
    std::vector<WorkloadGroupPtr> deleted_task_groups;
    size_t old_wg_size = 0;
    size_t new_wg_size = 0;
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        old_wg_size = _workload_groups.size();
        for (auto& _workload_group : _workload_groups) {
            uint64_t wg_id = _workload_group.first;
            auto workload_group_ptr = _workload_group.second;
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
        if (!config::doris_cgroup_cpu_path.empty()) {
            std::lock_guard<std::shared_mutex> write_lock(_clear_cgroup_lock);
            Status ret = CgroupCpuCtl::delete_unused_cgroup_path(used_wg_id);
            if (!ret.ok()) {
                LOG(WARNING) << "[topic_publish_wg]" << ret.to_string();
            }
        }
    }
    int64_t time_cost_ms = MonotonicMillis() - begin_time;
    if (deleted_task_groups.size() > 0) {
        LOG(INFO) << "[topic_publish_wg]finish clear unused workload group, time cost: "
                  << time_cost_ms << " ms, deleted group size:" << deleted_task_groups.size()
                  << ", before wg size=" << old_wg_size << ", after wg size=" << new_wg_size;
    }
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

void WorkloadGroupMgr::refresh_workload_group_memory_state() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);

    // 1. make all workload groups memory snapshots(refresh workload groups total memory used at the same time)
    // and calculate total memory used of all queries.
    int64_t all_workload_groups_mem_usage = 0;
    for (auto& [wg_id, wg] : _workload_groups) {
        all_workload_groups_mem_usage += wg->refresh_memory_usage();
    }
    if (all_workload_groups_mem_usage <= 0) {
        return;
    }

    std::string debug_msg =
            fmt::format("\nProcess Memory Summary: {}, {}, all workload groups memory usage: {}",
                        doris::GlobalMemoryArbitrator::process_memory_used_details_str(),
                        doris::GlobalMemoryArbitrator::sys_mem_available_details_str(),
                        PrettyPrinter::print(all_workload_groups_mem_usage, TUnit::BYTES));
    LOG_EVERY_T(INFO, 60) << debug_msg;
    for (auto& wg : _workload_groups) {
        update_queries_limit_(wg.second, false);
    }
}

void WorkloadGroupMgr::get_wg_resource_usage(vectorized::Block* block) {
    int64_t be_id = ExecEnv::GetInstance()->cluster_info()->backend_id;
    int cpu_num = CpuInfo::num_cores();
    cpu_num = cpu_num <= 0 ? 1 : cpu_num;
    uint64_t total_cpu_time_ns_per_second = cpu_num * 1000000000LL;

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
    }
}

void WorkloadGroupMgr::refresh_workload_group_metrics() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, wg] : _workload_groups) {
        wg->get_metrics()->refresh_metrics();
    }
}

void WorkloadGroupMgr::add_paused_query(const std::shared_ptr<ResourceContext>& resource_ctx,
                                        int64_t reserve_size, const Status& status) {
    DCHECK(resource_ctx != nullptr);
    resource_ctx->task_controller()->update_paused_reason(status);
    resource_ctx->task_controller()->set_low_memory_mode(true);
    resource_ctx->task_controller()->set_memory_sufficient(false);
    std::lock_guard<std::mutex> lock(_paused_queries_lock);
    auto wg = resource_ctx->workload_group();
    auto&& [it, inserted] = _paused_queries_list[wg].emplace(
            resource_ctx,
            doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted,
            reserve_size);
    // Check if this is an invalid reserve, for example, if the reserve size is too large, larger than the query limit
    // if hard limit is enabled, then not need enable other queries hard limit.
    if (inserted) {
        LOG(INFO) << "Insert one new paused query: "
                  << resource_ctx->task_controller()->debug_string()
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
    for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
        auto& queries_list = it->second;
        for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
            auto resource_ctx = query_it->resource_ctx_.lock();
            // The query is finished during in paused list.
            if (resource_ctx == nullptr) {
                LOG(INFO) << "Query: " << query_it->query_id() << " is nullptr, erase it.";
                query_it = queries_list.erase(query_it);
                continue;
            }
            // If there are any tasks that is cancelled and canceled time is less than 15 seconds, just break.
            // because it may release memory and other tasks may not be cancelled and spill disk.
            if (resource_ctx->task_controller()->is_cancelled() &&
                resource_ctx->task_controller()->cancel_elapsed_millis() <
                        config::wait_cancel_release_memory_ms) {
                return;
            }
            ++query_it;
        }
        if (queries_list.empty()) {
            it = _paused_queries_list.erase(it);
            continue;
        } else {
            // Finished deal with one workload group, and should deal with next one.
            ++it;
        }
    }

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
        bool exceed_low_watermark = false;
        bool exceed_high_watermark = false;
        wg->check_mem_used(&exceed_low_watermark, &exceed_high_watermark);
        // If the query is paused because its limit exceed the query itself's memlimit, then just spill disk.
        // The query's memlimit is set using slot mechanism and its value is set using the user settings, not
        // by weighted value. So if reserve failed, then it is actually exceed limit.
        for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
            auto resource_ctx = query_it->resource_ctx_.lock();
            // The query is finished during in paused list.
            if (resource_ctx == nullptr) {
                LOG(INFO) << "Query: " << query_it->query_id() << " is nullptr, erase it.";
                query_it = queries_list.erase(query_it);
                continue;
            }

            if (resource_ctx->task_controller()
                        ->paused_reason()
                        .is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
                // Streamload, kafka load, group commit will never have query memory exceeded error because
                // their  query limit is very large.
                bool spill_res = handle_single_query_(
                        resource_ctx, query_it->reserve_size_, query_it->elapsed_time(),
                        resource_ctx->task_controller()->paused_reason());
                if (!spill_res) {
                    ++query_it;
                    continue;
                } else {
                    VLOG_DEBUG << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                               << " remove from paused list";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
            } else if (resource_ctx->task_controller()
                               ->paused_reason()
                               .is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
                // here query is paused because of WORKLOAD_GROUP_MEMORY_EXCEEDED,
                // wg of the current query may not actually exceed the limit,
                // just (wg consumption + current query expected reserve memory > wg memory limit)
                // if the current query memory consumption + expected reserve memory exceeds the limit,
                // it may be that the expected reserve memory is too large,
                // wg memory is insufficient at this time,
                // so the current query should try to release memory by itself,
                // but here we did not directly try to spill this query,
                // set the query's limit only, and then wake up the current query to continue execution.
                //
                // if the expected reserve memory estimate is correct, high probability,
                // query will enter the pause state again, the reason is expected to be QUERY_MEMORY_EXCEEDED,
                // and handle_single_query_ will be called to spill.
                //
                // Of course, if the actual required memory is less than the reserved memory,
                // or if there is enough memory when continuing to execute,
                // it will run successfully without spilling.
                if (resource_ctx->memory_context()->adjusted_mem_limit() <
                    resource_ctx->memory_context()->current_memory_bytes() +
                            query_it->reserve_size_) {
                    // The query not exceed the query limit, but exceed the expected query limit when the workload
                    // group memory is not enough, use the litter memory limit to let the query exceed query limit.
                    resource_ctx->memory_context()->set_mem_limit(
                            resource_ctx->memory_context()->adjusted_mem_limit());
                    resource_ctx->task_controller()->set_memory_sufficient(true);
                    LOG(INFO) << "Workload group memory reserve failed because "
                              << resource_ctx->task_controller()->debug_string() << " reserve size "
                              << PrettyPrinter::print_bytes(query_it->reserve_size_)
                              << " is too large, set hard limit to "
                              << PrettyPrinter::print_bytes(
                                         resource_ctx->memory_context()->adjusted_mem_limit())
                              << " and resume running.";
                    query_it = queries_list.erase(query_it);
                    continue;
                }

                // when running here, current query adjusted_mem_limit < query memory consumption + reserve_size,
                // which means that the current query itself has not exceeded the memory limit.
                //
                // this means that there must be queries in the wg of the current query whose memory exceeds
                // adjusted_mem_limit, but these queries may not have entered the paused state,
                // so these queries may not modify the mem limit and continue to execute
                // when (adjusted_mem_limit < consumption + reserve_size_) is judged above.
                //
                // so call `update_queries_limit_` to force the update of the mem_limit of all queries
                // in the wg of the current query to the adjusted_mem_limit,
                // hoping that these queries that exceed limit will release memory.
                if (!has_changed_hard_limit) {
                    update_queries_limit_(wg, true);
                    has_changed_hard_limit = true;
                    LOG(INFO) << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                              << " reserve memory("
                              << PrettyPrinter::print_bytes(query_it->reserve_size_)
                              << ") failed due to workload group memory exceed, "
                                 "should set the workload group work in memory insufficent mode, "
                                 "so that other query will reduce their memory."
                              << " Query mem limit: "
                              << PrettyPrinter::print_bytes(
                                         resource_ctx->memory_context()->mem_limit())
                              << " mem usage: "
                              << PrettyPrinter::print_bytes(
                                         resource_ctx->memory_context()->current_memory_bytes())
                              << ", wg: " << wg->debug_string();
                }
                if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::NONE) {
                    // we not encourage not enable slot memory.
                    //
                    // If not enable slot memory policy, then should spill directly
                    // Maybe there are another query that use too much memory, if these queries
                    // exceed the memory limit, they will enter the paused state
                    // due to `QUERY_MEMORY_EXCEEDED` and will also try to spill.
                    //
                    // TODO should kill the query that exceed limit.
                    bool spill_res = handle_single_query_(
                            resource_ctx, query_it->reserve_size_, query_it->elapsed_time(),
                            resource_ctx->task_controller()->paused_reason());
                    if (!spill_res) {
                        ++query_it;
                        continue;
                    } else {
                        VLOG_DEBUG
                                << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                                << " remove from paused list";
                        query_it = queries_list.erase(query_it);
                        continue;
                    }
                } else {
                    // Should not put the query back to task scheduler immediately, because when wg's memory not sufficient,
                    // and then set wg's flag, other query may not free memory very quickly.
                    // If the workload group's memusage is less than low watermark then dispatch the query to run.
                    if (query_it->elapsed_time() > config::spill_in_paused_queue_timeout_ms ||
                        !exceed_low_watermark) {
                        // set wg's memory to sufficient, then add it back to task scheduler to run.
                        LOG(INFO) << "Query: "
                                  << print_id(resource_ctx->task_controller()->task_id())
                                  << " has waited in paused query queue for "
                                  << query_it->elapsed_time() << " ms. Resume it.";
                        resource_ctx->task_controller()->set_memory_sufficient(true);
                        query_it = queries_list.erase(query_it);
                        continue;
                    } else {
                        ++query_it;
                        continue;
                    }
                }
            } else {
                if (revoking_memory_from_other_query_) {
                    // Previously, we have revoked memory from other query, and the cancel stage finished.
                    // So, resume all queries now.
                    resource_ctx->task_controller()->set_memory_sufficient(true);
                    VLOG_DEBUG << "Query " << print_id(resource_ctx->task_controller()->task_id())
                               << " is blocked due to process memory not enough, but already "
                                  "cancelled some queries, resumt it now.";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                has_query_exceed_process_memlimit = true;
                // If wg's memlimit not exceed, but process memory exceed, it means cache or other metadata
                // used too much memory. Should clean all cache here.
                // Clear all cache not part of cache, because the cache thread already try to release cache step
                // by step. And it is not useful.
                //
                // here query is paused because of PROCESS_MEMORY_EXCEEDED,
                // normally, before process memory exceeds, daemon thread `refresh_cache_capacity` will
                // adjust the cache capacity to 0.
                // but at this time, process may not actually exceed the limit,
                // just (process memory + current query expected reserve memory > process memory limit)
                // so the behavior at this time is the same as the process memory limit exceed, clear all cache.
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted >
                            0.05 &&
                    doris::GlobalMemoryArbitrator::
                                    last_memory_exceeded_cache_capacity_adjust_weighted > 0.05) {
                    doris::GlobalMemoryArbitrator::
                            last_memory_exceeded_cache_capacity_adjust_weighted = 0.04;
                    doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
                    LOG(INFO) << "There are some queries need process memory, so that set cache "
                                 "capacity to 0 now";
                }

                // `cache_ratio_ < 0.05` means that the cache has been cleared
                // before the query enters the paused state.
                // but the query is still paused because of process memory exceed,
                // so here we will try to continue to release other memory.
                //
                // need to check config::disable_memory_gc here, if not, when config::disable_memory_gc == true,
                // cache is not adjusted, query_it->cache_ratio_ will always be 1, and this if branch will nenver
                // execute, this query will never be resumed, and will deadlock here.
                if (query_it->cache_ratio_ < 0.05 || config::disable_memory_gc) {
                    // If workload group's memory usage > min memory, then it means the workload group use too much memory
                    // in memory contention state. Should just spill
                    if (wg->total_mem_used() > wg->min_memory_limit()) {
                        auto revocable_tasks =
                                resource_ctx->task_controller()->get_revocable_tasks();
                        if (revocable_tasks.empty()) {
                            Status status = Status::MemoryLimitExceeded(
                                    "Workload group memory usage {} > min memory {}, but no "
                                    "revocable tasks",
                                    wg->total_mem_used(), wg->min_memory_limit());
                            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                                    resource_ctx->task_controller()->task_id(), status);
                            revoking_memory_from_other_query_ = true;
                            // If any query is cancelled, then skip others because it will release many memory and
                            // other query may not need release memory.
                            return;
                        } else {
                            SCOPED_ATTACH_TASK(resource_ctx);
                            auto status = resource_ctx->task_controller()->revoke_memory();
                            if (!status.ok()) {
                                ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                                        resource_ctx->task_controller()->task_id(), status);
                                revoking_memory_from_other_query_ = true;
                                return;
                            }
                            query_it = queries_list.erase(query_it);
                            continue;
                        }
                    }

                    // Other workload groups many use a lot of memory, should revoke memory from other workload groups
                    // by cancelling their queries.
                    int64_t revoked_size = revoke_memory_from_other_groups_();
                    if (revoked_size > 0) {
                        // Revoke memory from other workload groups will cancel some queries, wait them cancel finished
                        // and then check it again.
                        revoking_memory_from_other_query_ = true;
                        return;
                    }

                    // TODO revoke from memtable
                }
                // `cache_ratio_ > 0.05` means that the cache has not been cleared
                // when the query enters the paused state.
                // `last_affected_cache_capacity_adjust_weighted < 0.05` means that
                // the cache has been cleared at this time.
                // this means that the cache has been cleaned after the query enters the paused state.
                // assuming that some memory has been released, wake up the query to continue execution.
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted <
                            0.05 &&
                    query_it->cache_ratio_ > 0.05) {
                    LOG(INFO) << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                              << " will be resume after cache adjust.";
                    resource_ctx->task_controller()->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                ++query_it;
            }
        }

        // even if wg has no query in the paused state, the following code will still be executed
        // because `handle_paused_queries` adds a <wg, empty set> to `_paused_queries_list` at the beginning.
        if (queries_list.empty()) {
            it = _paused_queries_list.erase(it);
            continue;
        } else {
            // Finished deal with one workload group, and should deal with next one.
            ++it;
        }
    }
    // Attention: has to be here. It means, no query is at cancelling state and all query blocked by process
    // not enough has been resumed.
    revoking_memory_from_other_query_ = false;

    if (!has_query_exceed_process_memlimit &&
        doris::GlobalMemoryArbitrator::last_memory_exceeded_cache_capacity_adjust_weighted < 0.05) {
        // No query paused due to process exceed limit, so that enable cache now.
        doris::GlobalMemoryArbitrator::last_memory_exceeded_cache_capacity_adjust_weighted =
                doris::GlobalMemoryArbitrator::
                        last_periodic_refreshed_cache_capacity_adjust_weighted.load(
                                std::memory_order_relaxed);
        doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
        LOG(INFO) << "No query was paused due to insufficient process memory, so that set cache "
                     "capacity to last_periodic_refreshed_cache_capacity_adjust_weighted now";
    }
}

// Find the workload group that could revoke lot of memory:
// 1. workload group = max(total used memory - min memory that should reserved for it)
// 2. revoke 10% memory of the workload group that exceeded. For example, if the workload group exceed 10g,
//    then revoke 1g memory.
// 3. After revoke memory, go to the loop and wait for the query to be cancelled and check again.
int64_t WorkloadGroupMgr::revoke_memory_from_other_groups_() {
    MonotonicStopWatch watch;
    watch.start();
    std::unique_ptr<RuntimeProfile> profile =
            std::make_unique<RuntimeProfile>("RevokeMemoryFromOtherGroups");

    WorkloadGroupPtr max_wg = nullptr;
    int64_t max_exceeded_memory = 0;
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& workload_group : _workload_groups) {
            int64_t min_memory_limit = workload_group.second->min_memory_limit();
            int64_t total_used_memory = workload_group.second->total_mem_used();
            if (total_used_memory <= min_memory_limit) {
                // min memory is reserved for this workload group, if it used less than min memory,
                // then not revoke memory from it.
                continue;
            }
            if (total_used_memory - min_memory_limit > max_exceeded_memory) {
                max_wg = workload_group.second;
                max_exceeded_memory = total_used_memory - min_memory_limit;
            }
        }
    }
    if (max_wg == nullptr) {
        return 0;
    }
    if (max_exceeded_memory < 1 << 27) {
        LOG(INFO) << "The workload group that exceed most memory is :"
                  << max_wg->memory_debug_string() << ", max_exceeded_memory: "
                  << PrettyPrinter::print(max_exceeded_memory, TUnit::BYTES)
                  << " less than 128MB, no need to revoke memory";
        return 0;
    }
    int64_t freed_mem = static_cast<int64_t>((double)max_exceeded_memory * 0.1);
    // Revoke 10% of memory from the workload group that exceed most memory
    max_wg->revoke_memory(freed_mem, "exceed_memory", profile.get());
    std::stringstream ss;
    profile->pretty_print(&ss);
    LOG(INFO) << fmt::format(
            "[MemoryGC] process memory not enough, revoke memory from workload_group: {}, "
            "free memory {}. cost(us): {}, details: {}",
            max_wg->memory_debug_string(), PrettyPrinter::print_bytes(freed_mem),
            watch.elapsed_time() / 1000, ss.str());
    return freed_mem;
}

// streamload, kafka routine load, group commit
// insert into select
// select

// If the query could release some memory, for example, spill disk, then the return value is true.
// If the query could not release memory, then cancel the query, the return value is true.
// If the query is not ready to do these tasks, it means just wait, then return value is false.
bool WorkloadGroupMgr::handle_single_query_(const std::shared_ptr<ResourceContext>& requestor,
                                            size_t size_to_reserve, int64_t time_in_queue,
                                            Status paused_reason) {
    size_t revocable_size = 0;
    size_t memory_usage = 0;
    bool has_running_task = false;
    const auto query_id = print_id(requestor->task_controller()->task_id());
    requestor->task_controller()->get_revocable_info(&revocable_size, &memory_usage,
                                                     &has_running_task);
    if (has_running_task) {
        LOG(INFO) << "Query: " << print_id(requestor->task_controller()->task_id())
                  << " is paused, but still has running task, skip it.";
        return false;
    }

    const auto wg = requestor->workload_group();
    auto revocable_tasks = requestor->task_controller()->get_revocable_tasks();
    if (revocable_tasks.empty()) {
        const auto limit = requestor->memory_context()->mem_limit();
        const auto reserved_size = requestor->memory_context()->reserved_consumption();
        if (paused_reason.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
            // During waiting time, another operator in the query may finished and release
            // many memory and we could run.
            if ((memory_usage + size_to_reserve) < limit) {
                LOG(INFO) << "Query: " << query_id << ", usage("
                          << PrettyPrinter::print_bytes(memory_usage) << " + " << size_to_reserve
                          << ") less than limit(" << PrettyPrinter::print_bytes(limit)
                          << "), resume it.";
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue >= config::spill_in_paused_queue_timeout_ms) {
                // if cannot find any memory to release, then let the query continue to run as far as possible.
                // after `disable_reserve_memory`, the query will not enter the paused state again,
                // if the memory is really insufficient, Allocator will throw an exception
                // of query memory limit exceed and the query will be canceled,
                // or it will be canceled by memory gc when the process memory exceeds the limit.
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
                // Disable reserve memory will enable query level memory check, if the query
                // need a lot of memory than the memory limit, it will be killed.
                // Do not need set memlimit = ajusted_mem_limit because workload group refresher thread
                // will update automatically.
                requestor->task_controller()->disable_reserve_memory();
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else {
                return false;
            }
        } else if (paused_reason.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
            if (!wg->exceed_limit()) {
                LOG(INFO) << "Query: " << query_id
                          << " paused caused by WORKLOAD_GROUP_MEMORY_EXCEEDED, now resume it.";
                requestor->task_controller()->set_memory_sufficient(true);
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
                requestor->task_controller()->disable_reserve_memory();
                requestor->task_controller()->set_memory_sufficient(true);
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
                requestor->task_controller()->set_memory_sufficient(true);
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
                requestor->task_controller()->disable_reserve_memory();
                requestor->task_controller()->set_memory_sufficient(true);
            } else {
                return false;
            }
        }
    } else {
        SCOPED_ATTACH_TASK(requestor);
        auto status = requestor->task_controller()->revoke_memory();
        if (!status.ok()) {
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    requestor->task_controller()->task_id(), status);
        }
    }
    return true;
}

void WorkloadGroupMgr::update_queries_limit_(WorkloadGroupPtr wg, bool enable_hard_limit) {
    auto wg_mem_limit = wg->memory_limit();
    auto all_resource_ctxs = wg->resource_ctxs();
    bool exceed_low_watermark = false;
    bool exceed_high_watermark = false;
    wg->check_mem_used(&exceed_low_watermark, &exceed_high_watermark);
    int64_t wg_high_water_mark_limit =
            (int64_t)(static_cast<double>(wg_mem_limit) * wg->memory_high_watermark() * 1.0 / 100);
    int64_t wg_high_water_mark_except_load = wg_high_water_mark_limit;
    std::string debug_msg;
    if (exceed_high_watermark || exceed_low_watermark) {
        debug_msg = fmt::format(
                "\nWorkload Group {}: mem limit: {}, mem used: {}, "
                "high water mark mem limit: {}, used ratio: {}",
                wg->name(), PrettyPrinter::print(wg->memory_limit(), TUnit::BYTES),
                PrettyPrinter::print(wg->total_mem_used(), TUnit::BYTES),
                PrettyPrinter::print(wg_high_water_mark_limit, TUnit::BYTES),
                (double)(wg->total_mem_used()) / static_cast<double>(wg_mem_limit));
    }

    int32_t total_used_slot_count = 0;
    int32_t total_slot_count = wg->total_query_slot_count();
    // calculate total used slot count
    for (const auto& resource_ctx_pair : all_resource_ctxs) {
        auto resource_ctx = resource_ctx_pair.second.lock();
        if (!resource_ctx) {
            continue;
        }
        // Streamload kafka load group commit, not modify slot
        if (!resource_ctx->task_controller()->is_pure_load_task()) {
            total_used_slot_count += resource_ctx->task_controller()->get_slot_count();
        }
    }
    // calculate per query weighted memory limit
    debug_msg += "\nQuery Memory Summary: \n";
    for (const auto& resource_ctx_pair : all_resource_ctxs) {
        auto resource_ctx = resource_ctx_pair.second.lock();
        if (!resource_ctx) {
            continue;
        }
        if (exceed_low_watermark) {
            resource_ctx->task_controller()->set_low_memory_mode(true);
        }
        int64_t query_weighted_mem_limit = 0;
        int64_t expected_query_weighted_mem_limit = 0;
        if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::NONE) {
            query_weighted_mem_limit = resource_ctx->memory_context()->user_set_mem_limit();
            // If the policy is NONE, we use the query's memory limit. but the query's memory limit
            // should not be greater than the workload group's memory limit.
            if (query_weighted_mem_limit > wg_mem_limit) {
                query_weighted_mem_limit = wg_mem_limit;
            }
            expected_query_weighted_mem_limit = query_weighted_mem_limit;
        } else if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::FIXED) {
            // TODO, `Policy::FIXED` expects `all_query_used_slot_count < wg_total_slot_count`,
            // which is controlled when query is submitted
            // DCEHCK(total_used_slot_count <= total_slot_count);
            if (total_slot_count < 1) {
                LOG(WARNING)
                        << "Query " << print_id(resource_ctx->task_controller()->task_id())
                        << " enabled hard limit, but the slot count < 1, could not take affect";
                continue;
            } else {
                // If the query enable hard limit, then not use weighted info any more, just use the settings limit.
                query_weighted_mem_limit =
                        (int64_t)((static_cast<double>(wg_high_water_mark_except_load) *
                                   resource_ctx->task_controller()->get_slot_count() * 1.0) /
                                  total_slot_count);
                expected_query_weighted_mem_limit = query_weighted_mem_limit;
            }
        } else if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::DYNAMIC) {
            // If low water mark is not reached, then use process memory limit as query memory limit.
            // It means it will not take effect.
            // If there are some query in paused list, then limit should take effect.
            // numerator `+ total_used_slot_count` ensures that the result is greater than 1.
            expected_query_weighted_mem_limit =
                    total_used_slot_count > 0
                            ? (int64_t)(static_cast<double>(wg_high_water_mark_except_load +
                                                            total_used_slot_count) *
                                        resource_ctx->task_controller()->get_slot_count() * 1.0 /
                                        total_used_slot_count)
                            : wg_high_water_mark_except_load;
            if (!exceed_low_watermark && !enable_hard_limit) {
                query_weighted_mem_limit = wg_high_water_mark_except_load;
            } else {
                query_weighted_mem_limit = expected_query_weighted_mem_limit;
            }
        }
        debug_msg += resource_ctx->task_controller()->debug_string() + "\n";
        // If the query is a pure load task, then should not modify its limit. Or it will reserve
        // memory failed and we did not hanle it.
        if (!resource_ctx->task_controller()->is_pure_load_task()) {
            // If user's set mem limit is less than query weighted mem limit, then should not modify its limit.
            // Use user settings.
            if (resource_ctx->memory_context()->user_set_mem_limit() > query_weighted_mem_limit) {
                resource_ctx->memory_context()->set_mem_limit(query_weighted_mem_limit);
            }
            resource_ctx->memory_context()->set_adjusted_mem_limit(
                    expected_query_weighted_mem_limit);
        }
    }
    LOG_EVERY_T(INFO, 60) << debug_msg;
}

void WorkloadGroupMgr::stop() {
    for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

Status WorkloadGroupMgr::create_internal_wg() {
    TWorkloadGroupInfo twg_info;
    twg_info.__set_id(INTERNAL_NORMAL_WG_ID);
    twg_info.__set_name(INTERNAL_NORMAL_WG_NAME);
    twg_info.__set_max_memory_percent(100); // The normal wg will occupy all memory by default.
    twg_info.__set_min_memory_percent(0);   // The normal wg will occupy all memory by default.
    twg_info.__set_max_cpu_percent(100);
    twg_info.__set_min_cpu_percent(0);
    twg_info.__set_version(0);

    WorkloadGroupInfo wg_info = WorkloadGroupInfo::parse_topic_info(twg_info);
    auto normal_wg = std::make_shared<WorkloadGroup>(wg_info);

    RETURN_IF_ERROR(normal_wg->upsert_task_scheduler(&wg_info));

    {
        std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
        _workload_groups[normal_wg->id()] = normal_wg;
    }

    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris
