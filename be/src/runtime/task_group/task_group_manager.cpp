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
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/task_group/task_group.h"
#include "util/threadpool.h"
#include "util/time.h"
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

void TaskGroupManager::get_related_taskgroups(
        const std::function<bool(const TaskGroupPtr& ptr)>& pred,
        std::vector<TaskGroupPtr>* task_groups) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, task_group] : _task_groups) {
        if (pred(task_group)) {
            task_groups->push_back(task_group);
        }
    }
}

TaskGroupPtr TaskGroupManager::get_task_group_by_id(uint64_t tg_id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_task_groups.find(tg_id) != _task_groups.end()) {
        return _task_groups.at(tg_id);
    }
    return nullptr;
}

void TaskGroupManager::delete_task_group_by_ids(std::set<uint64_t> used_wg_id) {
    int64_t begin_time = MonotonicMillis();
    // 1 get delete group without running queries
    std::set<uint64_t> deleted_tg_ids;
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto iter = _task_groups.begin(); iter != _task_groups.end(); iter++) {
            uint64_t tg_id = iter->first;
            auto* task_group_ptr = iter->second.get();
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                task_group_ptr->shutdown();
                // only when no query running in task group, its resource can be released in BE
                if (task_group_ptr->query_num() == 0) {
                    LOG(INFO) << "There is no query in wg " << tg_id << ", delete it.";
                    deleted_tg_ids.insert(tg_id);
                }
            }
        }
    }

    // 2 stop active thread
    for (uint64_t tg_id : deleted_tg_ids) {
        _task_groups.at(tg_id)->try_stop_schedulers();
    }

    // 3 release resource in memory
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (uint64_t tg_id : deleted_tg_ids) {
            _task_groups.erase(tg_id);
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
                LOG(INFO) << "init task group mgr cpu ctl failed, " << ret.to_string();
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
    LOG(INFO) << "finish clear unused task group, time cost: " << time_cost_ms
              << "ms, deleted group size:" << deleted_tg_ids.size();
}

void TaskGroupManager::stop() {
    for (auto iter = _task_groups.begin(); iter != _task_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

} // namespace doris::taskgroup
