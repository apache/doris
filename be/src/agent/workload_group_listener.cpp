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

#include "agent/workload_group_listener.h"

#include "runtime/task_group/task_group.h"
#include "runtime/task_group/task_group_manager.h"
#include "util/mem_info.h"
#include "util/parse_util.h"

namespace doris {

void WorkloadGroupListener::handle_topic_info(const std::vector<TopicInfo>& topic_info_list) {
    std::set<uint64_t> current_wg_ids;
    for (const TopicInfo& topic_info : topic_info_list) {
        if (!topic_info.__isset.workload_group_info) {
            continue;
        }

        // 1 parse topicinfo to group info
        taskgroup::TaskGroupInfo task_group_info;
        Status ret = taskgroup::TaskGroupInfo::parse_topic_info(topic_info.workload_group_info,
                                                                &task_group_info);
        if (!ret.ok()) {
            LOG(INFO) << "parse topic info failed, tg_id=" << task_group_info.id
                      << ", reason:" << ret.to_string();
            continue;
        }
        current_wg_ids.insert(task_group_info.id);

        // 2 update task group
        auto tg = _exec_env->task_group_manager()->get_or_create_task_group(task_group_info);

        // 3 set cpu soft hard limit switch
        _exec_env->task_group_manager()->_enable_cpu_hard_limit.store(
                task_group_info.enable_cpu_hard_limit);

        // 4 create and update task scheduler
        Status ret2 = _exec_env->task_group_manager()->upsert_cg_task_scheduler(&task_group_info,
                                                                                _exec_env);
        if (!ret2.ok()) {
            LOG(WARNING) << "upsert task sche failed, tg_id=" << task_group_info.id
                         << ", reason=" << ret2.to_string();
        }

        LOG(INFO) << "update task group finish, tg info=" << tg->debug_string()
                  << ", enable_cpu_hard_limit="
                  << (_exec_env->task_group_manager()->enable_cpu_hard_limit() ? "true" : "false")
                  << ", cgroup cpu_shares=" << task_group_info.cgroup_cpu_shares
                  << ", cgroup cpu_hard_limit=" << task_group_info.cgroup_cpu_hard_limit
                  << ", enable_cgroup_cpu_soft_limit="
                  << (config::enable_cgroup_cpu_soft_limit ? "true" : "false");
    }

    _exec_env->task_group_manager()->delete_task_group_by_ids(current_wg_ids);
}
} // namespace doris