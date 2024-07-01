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

#include "runtime/workload_management/workload_sched_policy.h"

namespace doris {

void WorkloadSchedPolicy::init(int64_t id, std::string name, int version, bool enabled,
                               int priority, std::set<int64_t> wg_id_set,
                               std::vector<std::unique_ptr<WorkloadCondition>> condition_list,
                               std::vector<std::unique_ptr<WorkloadAction>> action_list) {
    _id = id;
    _name = name;
    _version = version;
    _enabled = enabled;
    _priority = priority;
    _condition_list = std::move(condition_list);
    _action_list = std::move(action_list);
    _wg_id_set = wg_id_set;

    _first_action_type = _action_list[0]->get_action_type();
    if (_first_action_type != WorkloadActionType::MOVE_QUERY_TO_GROUP &&
        _first_action_type != WorkloadActionType::CANCEL_QUERY) {
        for (int i = 1; i < _action_list.size(); i++) {
            WorkloadActionType cur_action_type = _action_list[i]->get_action_type();
            // one policy can not both contains move and cancel
            if (cur_action_type == WorkloadActionType::MOVE_QUERY_TO_GROUP ||
                cur_action_type == WorkloadActionType::CANCEL_QUERY) {
                _first_action_type = cur_action_type;
                break;
            }
        }
    }
}

bool WorkloadSchedPolicy::is_match(WorkloadQueryInfo* query_info_ptr) {
    if (!_enabled) {
        return false;
    }

    // 1 when policy has no group(_wg_id_set.size() < 0), it should match all query
    // 2 when policy has group, it can only match the query which has the same group
    if (_wg_id_set.size() > 0 && (query_info_ptr->wg_id <= 0 ||
                                  _wg_id_set.find(query_info_ptr->wg_id) == _wg_id_set.end())) {
        return false;
    }

    auto& metric_val_map = query_info_ptr->metric_map;
    for (auto& cond : _condition_list) {
        if (metric_val_map.find(cond->get_workload_metric_type()) == metric_val_map.end()) {
            return false;
        }

        std::string val = metric_val_map.at(cond->get_workload_metric_type());
        if (!cond->eval(val)) {
            return false;
        }
    }
    return true;
}

void WorkloadSchedPolicy::exec_action(WorkloadQueryInfo* query_info) {
    for (int i = 0; i < _action_list.size(); i++) {
        query_info->policy_id = this->_id;
        query_info->policy_name = this->_name;
        _action_list[i]->exec(query_info);
    }
}

} // namespace doris