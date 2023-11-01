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

#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "task_group.h"

namespace doris {
class ExecEnv;
class QueryContext;
class CgroupCpuCtl;

namespace vectorized {
class SimplifiedScanScheduler;
}

namespace pipeline {
class TaskScheduler;
class MultiCoreTaskQueue;
} // namespace pipeline

namespace taskgroup {

class TaskGroupManager {
public:
    TaskGroupManager() = default;
    ~TaskGroupManager() = default;

    TaskGroupPtr get_or_create_task_group(const TaskGroupInfo& task_group_info);

    void get_resource_groups(const std::function<bool(const TaskGroupPtr& ptr)>& pred,
                             std::vector<TaskGroupPtr>* task_groups);

    Status create_and_get_task_scheduler(uint64_t wg_id, std::string wg_name, int cpu_hard_limit,
                                         ExecEnv* exec_env, QueryContext* query_ctx_ptr);

    void delete_task_group_by_ids(std::set<uint64_t> id_set);

    void stop();

private:
    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, TaskGroupPtr> _task_groups;

    // map for workload group id and task scheduler pool
    // used for cpu hard limit
    std::mutex _task_scheduler_lock;
    std::map<uint64_t, std::unique_ptr<doris::pipeline::TaskScheduler>> _tg_sche_map;
    std::map<uint64_t, std::unique_ptr<vectorized::SimplifiedScanScheduler>> _tg_scan_sche_map;
    std::map<uint64_t, std::unique_ptr<CgroupCpuCtl>> _cgroup_ctl_map;
};

} // namespace taskgroup
} // namespace doris
