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

#include <memory>
#include <mutex>

#include "pipeline/task_scheduler.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

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
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
            uint64_t tg_id = iter->first;
            auto workload_group_ptr = iter->second;
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                workload_group_ptr->shutdown();
                // only when no query running in workload group, its resource can be released in BE
                if (workload_group_ptr->query_num() == 0) {
                    LOG(INFO) << "There is no query in wg " << tg_id << ", delete it.";
                    deleted_task_groups.push_back(workload_group_ptr);
                }
            }
        }
    }

    // 2 stop active thread
    for (auto& tg : deleted_task_groups) {
        // There is not lock here, but the tg may be released by another
        // thread, so that we should use shared ptr here, not use tg_id
        tg->try_stop_schedulers();
    }

    // 3 release resource in memory
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto& tg : deleted_task_groups) {
            _workload_groups.erase(tg->id());
        }
    }

    // 4 clear cgroup dir
    // NOTE(wb) currently we use rmdir to delete cgroup path,
    // this action may be failed until task file is cleared which means all thread are stopped.
    // So the first time to rmdir a cgroup path may failed.
    // Using cgdelete has no such issue.
    {
        std::lock_guard<std::shared_mutex> write_lock(_init_cg_ctl_lock);
        if (!_cg_cpu_ctl) {
            _cg_cpu_ctl = std::make_unique<CgroupV1CpuCtl>();
        }
        if (!_is_init_succ) {
            Status ret = _cg_cpu_ctl->init();
            if (ret.ok()) {
                _is_init_succ = true;
            } else {
                LOG(INFO) << "init workload group mgr cpu ctl failed, " << ret.to_string();
            }
        }
        if (_is_init_succ) {
            Status ret = _cg_cpu_ctl->delete_unused_cgroup_path(used_wg_id);
            if (!ret.ok()) {
                LOG(WARNING) << ret.to_string();
            }
        }
    }
    int64_t time_cost_ms = MonotonicMillis() - begin_time;
    LOG(INFO) << "finish clear unused workload group, time cost: " << time_cost_ms
              << "ms, deleted group size:" << deleted_task_groups.size();
}

void WorkloadGroupMgr::stop() {
    for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

} // namespace doris
