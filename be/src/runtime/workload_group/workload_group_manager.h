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

#include <shared_mutex>
#include <unordered_map>

#include "common/be_mock_util.h"
#include "runtime/workload_group/workload_group.h"
#include "util/time.h"

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
    // Monotonic ms. For a query paused for process memory this is the start of its bounded wait,
    // which may predate this entry: see TaskController::start_process_memory_wait().
    int64_t enqueue_at {0};
    size_t last_mem_usage {0};
    double cache_ratio_ {0.0};
    int64_t reserve_size_ {0};

    PausedQuery(std::shared_ptr<ResourceContext> resource_ctx_, double cache_ratio,
                int64_t reserve_size, int64_t enqueue_at_ms);

    int64_t elapsed_time() const { return MonotonicMillis() - enqueue_at; }

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

    void destroy_schedulers();

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
    Status create_internal_wg();

    WorkloadGroupPtr get_or_create_workload_group(const WorkloadGroupInfo& workload_group_info);

    using PausedQuerySet = std::set<PausedQuery>;
    using PausedQueryIterator = PausedQuerySet::iterator;

    bool handle_process_memory_exceeded_(const WorkloadGroupPtr& wg, PausedQuerySet& queries_list,
                                         PausedQueryIterator& query_it,
                                         const std::shared_ptr<ResourceContext>& resource_ctx,
                                         bool has_recently_cancelled_query);
    void resume_process_paused_query_(const WorkloadGroupPtr& wg, PausedQuerySet& queries_list,
                                      PausedQueryIterator& query_it,
                                      const std::shared_ptr<ResourceContext>& resource_ctx);
    bool release_query_memory_(PausedQuerySet& queries_list, PausedQueryIterator& query_it,
                               const std::shared_ptr<ResourceContext>& resource_ctx,
                               bool stop_after_release);
    bool handle_single_query_(const std::shared_ptr<ResourceContext>& requestor,
                              size_t size_to_reserve, int64_t time_in_queue, Status paused_reason);
    bool resolve_process_memory_exceeded_query_(const std::shared_ptr<ResourceContext>& requestor,
                                                size_t size_to_reserve, int64_t time_in_queue,
                                                size_t memory_usage, bool has_running_task);
    void spill_query_(const std::shared_ptr<ResourceContext>& requestor);
    int64_t revoke_memory_from_other_groups_(int64_t reserve_size);
    void update_queries_limit_(WorkloadGroupPtr wg, bool enable_hard_limit);

    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, WorkloadGroupPtr> _workload_groups;

    std::shared_mutex _clear_cgroup_lock;

    // Save per group paused query list, it should be a global structure, not per
    // workload group, because we need do some coordinate work globally.
    std::mutex _paused_queries_lock;
    std::map<WorkloadGroupPtr, std::set<PausedQuery>> _paused_queries_list;
    // If any query is cancelled when process memory is not enough, we set this to true.
    // When there is not query in cancel state, this var is set to false.
    bool revoking_memory_from_other_query_ = false;
};

} // namespace doris
