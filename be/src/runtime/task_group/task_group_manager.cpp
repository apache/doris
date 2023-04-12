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

#include <charconv>

namespace doris::taskgroup {

TaskGroupManager::TaskGroupManager() = default;
TaskGroupManager::~TaskGroupManager() = default;

TaskGroupManager* TaskGroupManager::instance() {
    static TaskGroupManager tgm;
    return &tgm;
}

TaskGroupPtr TaskGroupManager::get_task_group(uint64_t id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_task_groups.count(id)) {
        return _task_groups[id];
    }
    return nullptr;
}

TaskGroupPtr TaskGroupManager::get_or_create_task_group(const TaskGroupInfo& task_group_info) {
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        if (_task_groups.count(task_group_info._id)) {
            return _task_groups[task_group_info._id];
        }
    }

    auto new_task_group =
            std::make_shared<TaskGroup>(task_group_info._id, task_group_info._name,
                                        task_group_info._cpu_share, task_group_info._version);
    std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
    if (_task_groups.count(task_group_info._id)) {
        return _task_groups[task_group_info._id];
    }
    _task_groups[task_group_info._id] = new_task_group;
    return new_task_group;
}

Status TaskGroupManager::parse_group_info(const TPipelineResourceGroup& resource_group,
                                          TaskGroupInfo* task_group_info) {
    if (!check_group_info(resource_group)) {
        std::stringstream ss;
        ss << "incomplete resource group parameters: ";
        resource_group.printTo(ss);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    auto iter = resource_group.properties.find(CPU_SHARE);
    uint64_t share = 0;
    std::from_chars(iter->second.c_str(), iter->second.c_str() + iter->second.size(), share);

    task_group_info->_id = resource_group.id;
    task_group_info->_name = resource_group.name;
    task_group_info->_version = resource_group.version;
    task_group_info->_cpu_share = share;
    return Status::OK();
}

bool TaskGroupManager::check_group_info(const TPipelineResourceGroup& resource_group) {
    return resource_group.__isset.id && resource_group.__isset.version &&
           resource_group.__isset.name && resource_group.__isset.properties &&
           resource_group.properties.count(CPU_SHARE) > 0;
}

} // namespace doris::taskgroup
