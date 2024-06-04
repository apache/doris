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

#include "agent/workload_sched_policy_listener.h"

#include "runtime/workload_management/workload_action.h"
#include "runtime/workload_management/workload_condition.h"
#include "runtime/workload_management/workload_sched_policy.h"
#include "runtime/workload_management/workload_sched_policy_mgr.h"

namespace doris {

void WorkloadschedPolicyListener::handle_topic_info(const std::vector<TopicInfo>& topic_info_list) {
    std::map<uint64_t, std::shared_ptr<WorkloadSchedPolicy>> policy_map;
    for (const TopicInfo& topic_info : topic_info_list) {
        if (!topic_info.__isset.workload_sched_policy) {
            continue;
        }

        TWorkloadSchedPolicy tpolicy = topic_info.workload_sched_policy;
        // some metric or action can not exec in be, then need skip
        bool need_skip_current_policy = false;

        std::vector<std::unique_ptr<WorkloadCondition>> cond_ptr_list;
        for (TWorkloadCondition& cond : tpolicy.condition_list) {
            std::unique_ptr<WorkloadCondition> cond_ptr =
                    WorkloadConditionFactory::create_workload_condition(&cond);
            if (cond_ptr == nullptr) {
                need_skip_current_policy = true;
                break;
            }
            cond_ptr_list.push_back(std::move(cond_ptr));
        }
        if (need_skip_current_policy) {
            continue;
        }

        std::vector<std::unique_ptr<WorkloadAction>> action_ptr_list;
        for (TWorkloadAction& action : tpolicy.action_list) {
            std::unique_ptr<WorkloadAction> action_ptr =
                    WorkloadActionFactory::create_workload_action(&action);
            if (action_ptr == nullptr) {
                need_skip_current_policy = true;
                break;
            }
            action_ptr_list.push_back(std::move(action_ptr));
        }
        if (need_skip_current_policy) {
            continue;
        }

        std::set<int64_t> wg_id_set;
        for (int64_t wg_id : tpolicy.wg_id_list) {
            wg_id_set.insert(wg_id);
        }

        std::shared_ptr<WorkloadSchedPolicy> policy_ptr = std::make_shared<WorkloadSchedPolicy>();
        policy_ptr->init(tpolicy.id, tpolicy.name, tpolicy.version, tpolicy.enabled,
                         tpolicy.priority, wg_id_set, std::move(cond_ptr_list),
                         std::move(action_ptr_list));
        policy_map.emplace(tpolicy.id, std::move(policy_ptr));
    }
    size_t new_policy_size = policy_map.size();
    _exec_env->workload_sched_policy_mgr()->update_workload_sched_policy(std::move(policy_map));
    LOG(INFO) << "[workload_schedule]finish update workload schedule policy, size="
              << new_policy_size;
}
} // namespace doris