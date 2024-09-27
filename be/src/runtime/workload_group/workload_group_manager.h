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

#include "workload_group.h"

namespace doris {

class CgroupCpuCtl;

namespace vectorized {
class Block;
class QueryContext;
} // namespace vectorized

namespace pipeline {
class TaskScheduler;
class MultiCoreTaskQueue;
} // namespace pipeline

class PausedQuery {
public:
    // Use weak ptr to save query ctx, to make sure if the query is cancelled
    // the resource will be released
    std::weak_ptr<QueryContext> query_ctx_;
    std::chrono::system_clock::time_point enqueue_at;
    size_t last_mem_usage {0};
    double cache_ratio_ {0.0};
    bool any_wg_exceed_limit_ {false};
    int64_t reserve_size_ {0};

    PausedQuery(std::shared_ptr<QueryContext> query_ctx, double cache_ratio,
                bool any_wg_exceed_limit, int64_t reserve_size);

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

class MemtableUsage {
public:
    int64_t active_mem_usage = 0;
    int64_t queue_mem_usage = 0;
    int64_t flush_mem_usage = 0;
};

class WorkloadGroupMgr {
public:
    WorkloadGroupMgr() = default;
    ~WorkloadGroupMgr() = default;

    WorkloadGroupPtr get_or_create_workload_group(const WorkloadGroupInfo& workload_group_info);

    void get_related_workload_groups(const std::function<bool(const WorkloadGroupPtr& ptr)>& pred,
                                     std::vector<WorkloadGroupPtr>* task_groups);

    void delete_workload_group_by_ids(std::set<uint64_t> id_set);

    WorkloadGroupPtr get_task_group_by_id(uint64_t tg_id);

    void stop();

    std::atomic<bool> _enable_cpu_hard_limit = false;

    bool enable_cpu_soft_limit() { return !_enable_cpu_hard_limit.load(); }

    bool enable_cpu_hard_limit() { return _enable_cpu_hard_limit.load(); }

    void refresh_wg_weighted_memory_limit();

    void get_wg_resource_usage(vectorized::Block* block);

    void add_paused_query(const std::shared_ptr<QueryContext>& query_ctx, int64_t reserve_size);

    void handle_paused_queries();

    void update_load_memtable_usage(const std::map<uint64_t, MemtableUsage>& wg_memtable_usages);

private:
    bool handle_single_query(std::shared_ptr<QueryContext> query_ctx, size_t size_to_reserve,
                             Status paused_reason);
    void handle_non_overcommit_wg_paused_queries();
    void handle_overcommit_wg_paused_queries();
    void change_query_to_hard_limit(WorkloadGroupPtr wg, bool enable_hard_limit);

private:
    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, WorkloadGroupPtr> _workload_groups;

    std::shared_mutex _clear_cgroup_lock;

    // Save per group paused query list, it should be a global structure, not per
    // workload group, because we need do some coordinate work globally.
    std::mutex _paused_queries_lock;
    std::map<WorkloadGroupPtr, std::set<PausedQuery>> _paused_queries_list;
};

} // namespace doris
