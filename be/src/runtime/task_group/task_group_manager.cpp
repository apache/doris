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

#include "task_group_manager.h"

#include <memory>
#include <mutex>

#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/task_group/task_group.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::taskgroup {

TaskGroupPtr TaskGroupManager::get_or_create_task_group(const TaskGroupInfo& task_group_info) {
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        if (LIKELY(_task_groups.count(task_group_info.id))) {
            auto task_group = _task_groups[task_group_info.id];
            task_group->check_and_update(task_group_info);
            return task_group;
        }
    }

    auto new_task_group = std::make_shared<TaskGroup>(task_group_info);
    std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
    if (_task_groups.count(task_group_info.id)) {
        auto task_group = _task_groups[task_group_info.id];
        task_group->check_and_update(task_group_info);
        return task_group;
    }
    _task_groups[task_group_info.id] = new_task_group;
    return new_task_group;
}

void TaskGroupManager::get_resource_groups(const std::function<bool(const TaskGroupPtr& ptr)>& pred,
                                           std::vector<TaskGroupPtr>* task_groups) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, task_group] : _task_groups) {
        if (pred(task_group)) {
            task_groups->push_back(task_group);
        }
    }
}

Status TaskGroupManager::create_and_get_task_scheduler(uint64_t tg_id, std::string tg_name,
                                                       int cpu_hard_limit, ExecEnv* exec_env,
                                                       QueryContext* query_ctx_ptr) {
    std::lock_guard<std::mutex> lock(_task_scheduler_lock);
    // step 1: init cgroup cpu controller
    CgroupCpuCtl* cg_cu_ctl_ptr = nullptr;
    if (_cgroup_ctl_map.find(tg_id) == _cgroup_ctl_map.end()) {
        std::unique_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_unique<CgroupV1CpuCtl>(tg_id);
        Status ret = cgroup_cpu_ctl->init();
        if (ret.ok()) {
            cg_cu_ctl_ptr = cgroup_cpu_ctl.get();
            _cgroup_ctl_map.emplace(tg_id, std::move(cgroup_cpu_ctl));
        } else {
            return Status::Error<INTERNAL_ERROR, false>("cgroup init failed, gid={}", tg_id);
        }
    }

    // step 2: init task scheduler
    if (_tg_sche_map.find(tg_id) == _tg_sche_map.end()) {
        int32_t executors_size = config::pipeline_executor_size;
        if (executors_size <= 0) {
            executors_size = CpuInfo::num_cores();
        }
        auto task_queue = std::make_shared<pipeline::MultiCoreTaskQueue>(executors_size);

        auto pipeline_task_scheduler = std::make_unique<pipeline::TaskScheduler>(
                exec_env, exec_env->get_global_block_scheduler(), std::move(task_queue),
                "Exec_" + tg_name, cg_cu_ctl_ptr);
        Status ret = pipeline_task_scheduler->start();
        if (ret.ok()) {
            _tg_sche_map.emplace(tg_id, std::move(pipeline_task_scheduler));
        } else {
            return Status::Error<INTERNAL_ERROR, false>("task scheduler start failed, gid={}",
                                                        tg_id);
        }
    }

    // step 3: init scan scheduler
    if (_tg_scan_sche_map.find(tg_id) == _tg_scan_sche_map.end()) {
        auto scan_scheduler =
                std::make_unique<vectorized::SimplifiedScanScheduler>(tg_name, cg_cu_ctl_ptr);
        Status ret = scan_scheduler->start();
        if (ret.ok()) {
            _tg_scan_sche_map.emplace(tg_id, std::move(scan_scheduler));
        } else {
            return Status::Error<INTERNAL_ERROR, false>("scan scheduler start failed, gid={}",
                                                        tg_id);
        }
    }

    // step 4 set exec/scan task scheudler to query ctx
    pipeline::TaskScheduler* task_sche = _tg_sche_map.at(tg_id).get();
    query_ctx_ptr->set_task_scheduler(task_sche);

    vectorized::SimplifiedScanScheduler* scan_task_sche = _tg_scan_sche_map.at(tg_id).get();
    query_ctx_ptr->set_scan_task_scheduler(scan_task_sche);

    // step 5 update cgroup cpu if needed
    _cgroup_ctl_map.at(tg_id)->update_cpu_hard_limit(cpu_hard_limit);

    return Status::OK();
}

void TaskGroupManager::stop() {
    for (auto& task_sche : _tg_sche_map) {
        task_sche.second->stop();
    }
    for (auto& task_sche : _tg_scan_sche_map) {
        task_sche.second->stop();
    }
}

} // namespace doris::taskgroup
