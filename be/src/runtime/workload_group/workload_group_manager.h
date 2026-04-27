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

#include <stdint.h>

#include <map>
#include <set>
#include <shared_mutex>
#include <unordered_map>

#include "common/be_mock_util.h"
#include "runtime/workload_group/workload_group.h"

namespace doris {

class CgroupCpuCtl;

class Block;

class TaskScheduler;
class MultiCoreTaskQueue;

// In doris, query includes all tasks such as query, load, compaction, schemachange, etc.
class PausedQuery {
public:
    // Use weak ptr to save resource ctx, to make sure if the query is cancelled
    // the resource will be released
    std::weak_ptr<ResourceContext> resource_ctx_;
    std::chrono::system_clock::time_point enqueue_at;
    size_t last_mem_usage {0};
    double cache_ratio_ {0.0};
    int64_t reserve_size_ {0};

    PausedQuery(std::shared_ptr<ResourceContext> resource_ctx_, double cache_ratio,
                int64_t reserve_size);

    int64_t elapsed_time() const {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now - enqueue_at).count();
    }

    std::string query_id() const { return query_id_; }

    bool operator<(const PausedQuery& other) const { return query_id_ < other.query_id_; }

    bool operator==(const PausedQuery& other) const { return query_id_ == other.query_id_; }

private:
    std::string query_id_;
};

class WorkloadGroupMgr {
public:
    WorkloadGroupMgr();
    MOCK_FUNCTION ~WorkloadGroupMgr();

    void delete_workload_group_by_ids(std::set<uint64_t> id_set);

    WorkloadGroupPtr get_group(std::vector<uint64_t>& id_list);

    // This method is used during workload group listener to update internal workload group's id.
    // This method does not acquire locks, so it should be called in a locked context.
    void reset_workload_group_id(std::string workload_group_name, uint64_t new_id);

    void do_sweep();

    void stop();

    void refresh_workload_group_memory_state();

    void get_wg_resource_usage(Block* block);

    void refresh_workload_group_metrics();

    void update_memtable_flush_threads();

    MOCK_FUNCTION void add_paused_query(const std::shared_ptr<ResourceContext>& resource_ctx,
                                        int64_t reserve_size, const Status& status);

    void handle_paused_queries();

    friend class WorkloadGroupListener;
    friend class ExecEnv;

private:
    using PausedQuerySet = std::set<PausedQuery>;
    using PausedQueryIterator = PausedQuerySet::iterator;

    struct RecentlyCancelledQueries {
        bool has_query = false;
        std::set<WorkloadGroupPtr> workload_groups;
    };

    Status create_internal_wg();

    WorkloadGroupPtr get_or_create_workload_group(const WorkloadGroupInfo& workload_group_info);

    // ==================== handle_paused_queries() sub-routines ====================
    //
    // handle_paused_queries() is the top-level entry called periodically. It runs
    // four phases in order:
    //   Phase 1 (cleanup):  cleanup_paused_queries_()
    //   Phase 2 (revoke):   resume_paused_queries_after_revoke_()
    //   Phase 3 (process):  process_paused_queries_()
    //
    // All Phase 1–3 helpers below are called with _paused_queries_lock held.

    // Phase 1 — Scan every paused query and build a summary of recently cancelled
    // queries. Queries cancelled longer than `wait_cancel_release_memory_ms` ago are
    // erased from the list. Empty per-WG entries are pruned.
    // Returns a RecentlyCancelledQueries struct recording:
    //   - has_query: true if any recently cancelled query exists (process-level signal)
    //   - workload_groups: which WGs contain a recently cancelled query (WG-level signal)
    RecentlyCancelledQueries cleanup_paused_queries_();

    // Phase 2 — If a previous round cancelled a query and set
    // `revoking_memory_from_other_query_`, check whether the cancelled query has
    // finished releasing memory. If still releasing → return true (caller should
    // exit early). If done → resume all remaining paused queries, reset the flag,
    // and return true (caller should exit, work is complete for this round).
    // Returns false when the revoking flag was not set, meaning Phase 3 should run.
    bool resume_paused_queries_after_revoke_(const RecentlyCancelledQueries& recently_cancelled);

    // Helper for Phase 2 — Resume every non-cancelled paused query by calling
    // set_memory_sufficient(true), then erase all entries from `_paused_queries_list`.
    // Cancelled queries are simply erased without resuming.
    void resume_all_paused_queries_();

    // Phase 3 — Iterate over each workload group's paused queries and attempt to
    // resolve them (spill, cancel, resume, or keep waiting). Delegates per-WG work
    // to `process_workload_group_paused_queries_()`. If any per-WG handler signals
    // early termination (returns true), this function returns immediately.
    void process_paused_queries_(const RecentlyCancelledQueries& recently_cancelled);

    // Process all paused queries belonging to a single workload group. For each
    // non-cancelled query, dispatches to the appropriate handler based on the
    // paused reason (QUERY_MEMORY_EXCEEDED / WORKLOAD_GROUP_MEMORY_EXCEEDED /
    // PROCESS_MEMORY_EXCEEDED).
    // Returns true if the caller should stop processing further workload groups
    // (e.g., a query was cancelled and we need to wait for memory release).
    bool process_workload_group_paused_queries_(const WorkloadGroupPtr& wg,
                                                PausedQuerySet& queries_list,
                                                const RecentlyCancelledQueries& recently_cancelled);

    // Try to lock the weak_ptr in the PausedQuery entry. If the query context has
    // already been destroyed (query finished while paused), erase the entry and
    // advance the iterator. Returns nullptr in that case; otherwise returns the
    // locked shared_ptr.
    std::shared_ptr<ResourceContext> get_resource_ctx_or_erase_(PausedQuerySet& queries_list,
                                                                PausedQueryIterator& query_it);

    // Handle a query paused due to QUERY_MEMORY_EXCEEDED. Delegates to
    // release_query_memory_() with stop_after_release=false, meaning other queries
    // in the same WG will continue to be processed even after this one is resolved.
    // Returns true if the caller should stop processing (query was cancelled).
    bool handle_query_memory_exceeded_(PausedQuerySet& queries_list, PausedQueryIterator& query_it,
                                       const std::shared_ptr<ResourceContext>& resource_ctx);

    // Handle a query paused due to WORKLOAD_GROUP_MEMORY_EXCEEDED. The logic is:
    //   1. If a recently cancelled query exists in the SAME WG, skip (wait for it
    //      to release WG-level memory).
    //   2. If adjusted_mem_limit < current_usage + reserve_size, lower the query's
    //      hard limit to adjusted_mem_limit and resume it — the query will likely
    //      re-pause with QUERY_MEMORY_EXCEEDED and spill then.
    //   3. Otherwise, force-update all queries in the WG to use adjusted limits
    //      (once per WG via has_changed_hard_limit), hoping over-limit queries
    //      will voluntarily release memory.
    //   4. If slot_memory_policy is NONE, spill directly via release_query_memory_.
    //   5. If slot_memory_policy is active, wait until timeout or low watermark
    //      recovery before resuming.
    // Returns true if the caller should stop processing further queries/WGs.
    bool handle_workload_group_memory_exceeded_(
            const WorkloadGroupPtr& wg, PausedQuerySet& queries_list, PausedQueryIterator& query_it,
            const std::shared_ptr<ResourceContext>& resource_ctx, bool& has_changed_hard_limit,
            bool exceed_low_watermark, const RecentlyCancelledQueries& recently_cancelled);

    // Handle a query paused due to PROCESS_MEMORY_EXCEEDED. The logic is:
    //   1. If any recently cancelled query exists globally, skip (wait for
    //      process-level memory release).
    //   2. If the WG's usage exceeds its min_memory_limit, spill via
    //      release_query_memory_(stop_after_release=true) — one spill per round.
    //   3. Otherwise, try to revoke memory from other overcommitted WGs by
    //      cancelling their largest queries.
    // Returns true if the caller should stop processing further queries/WGs.
    bool handle_process_memory_exceeded_(const WorkloadGroupPtr& wg, PausedQuerySet& queries_list,
                                         PausedQueryIterator& query_it,
                                         const std::shared_ptr<ResourceContext>& resource_ctx,
                                         const RecentlyCancelledQueries& recently_cancelled);

    // Common helper: attempt to release memory for a single paused query by calling
    // handle_single_query_() (which triggers spill or cancel). On success, erases the
    // query from the paused list. If the query was cancelled (not spilled), sets
    // revoking_memory_from_other_query_ and returns true to signal early termination.
    // `stop_after_release`: if true, return true even on successful spill (used by
    // PROCESS_MEMORY_EXCEEDED to limit one spill per round); if false, return false
    // to let the caller continue processing remaining queries.
    bool release_query_memory_(PausedQuerySet& queries_list, PausedQueryIterator& query_it,
                               const std::shared_ptr<ResourceContext>& resource_ctx,
                               bool stop_after_release);

    // Attempt to resolve a single paused query: if revocable memory exists, trigger
    // spill; if under limit, resume; if no memory can be freed, cancel the query or
    // disable reserve memory and resume. Returns true if the query was acted upon
    // (spilled/cancelled/resumed), false if it should keep waiting (e.g., still has
    // running tasks).
    bool handle_single_query_(const std::shared_ptr<ResourceContext>& requestor,
                              size_t size_to_reserve, int64_t time_in_queue, Status paused_reason);

    // Find the most overcommitted workload group (usage - min_memory_limit is
    // largest) and cancel its biggest query to reclaim ~10% of the excess memory.
    // Returns the amount of memory expected to be freed, or 0 if no WG qualifies.
    int64_t revoke_memory_from_other_groups_();

    // Recalculate and apply per-query memory limits for all queries in a workload
    // group based on the slot memory policy (NONE/FIXED/DYNAMIC). When
    // `enable_hard_limit` is true, limits are tightened to the slot-weighted value
    // even if the WG is below the low watermark (used during memory pressure).
    void update_queries_limit_(WorkloadGroupPtr wg, bool enable_hard_limit);

    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, WorkloadGroupPtr> _workload_groups;

    std::shared_mutex _clear_cgroup_lock;

    // Save per group paused query list, it should be a global structure, not per
    // workload group, because we need do some coordinate work globally.
    std::mutex _paused_queries_lock;
    std::map<WorkloadGroupPtr, PausedQuerySet> _paused_queries_list;
    // If any query is cancelled when process memory is not enough, we set this to true.
    // When there is not query in cancel state, this var is set to false.
    bool revoking_memory_from_other_query_ = false;
};

} // namespace doris
