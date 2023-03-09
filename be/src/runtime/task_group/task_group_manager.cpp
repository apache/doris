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

namespace doris::taskgroup {

TaskGroupManager::TaskGroupManager() {
    _create_default_task_group();
    _create_poc_task_group();
}
TaskGroupManager::~TaskGroupManager() = default;

TaskGroupPtr TaskGroupManager::get_or_create_task_group(uint64_t id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_task_groups.count(id)) {
        return _task_groups[id];
    } else {
        return _task_groups[DEFAULT_RG_ID];
    }
}

void TaskGroupManager::_create_default_task_group() {
    _task_groups[DEFAULT_RG_ID] =
            std::make_shared<TaskGroup>(DEFAULT_RG_ID, "default_rs", DEFAULT_CPU_SHARE);
}

void TaskGroupManager::_create_poc_task_group() {
    _task_groups[POC_RG_ID] =
            std::make_shared<TaskGroup>(POC_RG_ID, "poc_rs", POC_RG_CPU_SHARE);
}

}