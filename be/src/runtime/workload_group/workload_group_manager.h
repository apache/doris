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
} // namespace vectorized

namespace pipeline {
class TaskScheduler;
class MultiCoreTaskQueue;
} // namespace pipeline

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

private:
    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, WorkloadGroupPtr> _workload_groups;

    std::shared_mutex _clear_cgroup_lock;
};

} // namespace doris
