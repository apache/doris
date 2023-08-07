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

TaskGroupManager::TaskGroupManager() = default;
TaskGroupManager::~TaskGroupManager() = default;

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

} // namespace doris::taskgroup
