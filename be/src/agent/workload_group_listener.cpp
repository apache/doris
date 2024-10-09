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

#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/mem_info.h"
#include "util/parse_util.h"

namespace doris {

void WorkloadGroupListener::handle_topic_info(const std::vector<TopicInfo>& topic_info_list) {
    std::set<uint64_t> current_wg_ids;
    bool is_set_workload_group_info = false;
    int list_size = topic_info_list.size();
    for (const TopicInfo& topic_info : topic_info_list) {
        if (!topic_info.__isset.workload_group_info) {
            continue;
        }
        is_set_workload_group_info = true;

        // 1 parse topic info to group info
        WorkloadGroupInfo workload_group_info =
                WorkloadGroupInfo::parse_topic_info(topic_info.workload_group_info);
        // it means FE has this wg, but may parse failed, so we should not delete it.
        if (workload_group_info.id != 0) {
            current_wg_ids.insert(workload_group_info.id);
        }
        if (!workload_group_info.valid) {
            LOG(INFO) << "[topic_publish_wg]parse topic info failed, wg_id="
                      << workload_group_info.id << ", reason: [tworkload_group_info.__isset.id: "
                      << topic_info.workload_group_info.__isset.id
                      << ", tworkload_group_info.__isset.version: "
                      << topic_info.workload_group_info.__isset.version << "]";
            continue;
        }

        // 2 update workload group
        auto wg =
                _exec_env->workload_group_mgr()->get_or_create_workload_group(workload_group_info);

        // 3 set cpu soft hard limit switch
        _exec_env->workload_group_mgr()->_enable_cpu_hard_limit.store(
                workload_group_info.enable_cpu_hard_limit);

        // 4 create and update task scheduler
        wg->upsert_task_scheduler(&workload_group_info, _exec_env);

        // 5 upsert io throttle
        wg->upsert_scan_io_throttle(&workload_group_info);

        LOG(INFO) << "[topic_publish_wg]update workload group finish, wg info="
                  << wg->debug_string() << ", enable_cpu_hard_limit="
                  << (_exec_env->workload_group_mgr()->enable_cpu_hard_limit() ? "true" : "false")
                  << ", cgroup cpu_shares=" << workload_group_info.cgroup_cpu_shares
                  << ", cgroup cpu_hard_limit=" << workload_group_info.cgroup_cpu_hard_limit
                  << ", cgroup home path=" << config::doris_cgroup_cpu_path
                  << ", list size=" << list_size << ", thread info=" << wg->thread_debug_info();
    }

    // NOTE(wb) when is_set_workload_group_info=false, it means FE send a empty workload group list
    // this should not happens, because FE should has at least one normal group.
    // just log it if that happens
    if (!is_set_workload_group_info) {
        LOG(INFO) << "[topic_publish_wg]unexpected error happens, no workload group info is "
                     "set, list size="
                  << list_size;
    }
    _exec_env->workload_group_mgr()->delete_workload_group_by_ids(current_wg_ids);
}
} // namespace doris