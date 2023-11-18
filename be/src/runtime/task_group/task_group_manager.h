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

#include "task_group.h"

namespace doris {
class ExecEnv;
namespace taskgroup {

class TaskGroupManager {
public:
    TaskGroupManager();
    ~TaskGroupManager();

    TaskGroupPtr get_or_create_task_group(const TaskGroupInfo& task_group_info);

    void get_resource_groups(const std::function<bool(const TaskGroupPtr& ptr)>& pred,
                             std::vector<TaskGroupPtr>* task_groups);

private:
    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, TaskGroupPtr> _task_groups;
};

} // namespace taskgroup
} // namespace doris
