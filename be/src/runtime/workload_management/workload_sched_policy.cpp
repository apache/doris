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

#include "runtime/workload_management/resource_context.h"
#include "util/time.h"

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

    if (!_action_list.empty()) {
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
}

bool WorkloadSchedPolicy::is_match(WorkloadAction::RuntimeContext* action_runtime_ctx) const {
    if (!_enabled) {
        return false;
    }

    // 1 when policy has no group(_wg_id_set.size() < 0), it should match all query
    // 2 when policy has group, it can only match the query which has the same group
    if (!_wg_id_set.empty() &&
        (action_runtime_ctx->resource_ctx->workload_group() == nullptr ||
         _wg_id_set.find(action_runtime_ctx->resource_ctx->workload_group()->id()) ==
                 _wg_id_set.end())) {
        return false;
    }

    std::string cond_eval_msg;
    for (const auto& cond : _condition_list) {
        std::string val;
        switch (cond->get_workload_metric_type()) {
        case WorkloadMetricType::QUERY_TIME: {
            val = std::to_string(
                    action_runtime_ctx->resource_ctx->task_controller()->running_time());
            break;
        }
        case WorkloadMetricType::SCAN_BYTES: {
            val = std::to_string(action_runtime_ctx->resource_ctx->io_context()->scan_bytes());
            break;
        }
        case WorkloadMetricType::SCAN_ROWS: {
            val = std::to_string(action_runtime_ctx->resource_ctx->io_context()->scan_rows());
            break;
        }
        case WorkloadMetricType::QUERY_MEMORY_BYTES: {
            val = std::to_string(
                    action_runtime_ctx->resource_ctx->memory_context()->current_memory_bytes());
            break;
        }
        default:
            return false;
        }

        if (!cond->eval(val)) {
            return false;
        }
        cond_eval_msg += cond->get_metric_string() + ":" + val + "(" +
                         cond->get_metric_value_string() + "), ";
    }
    cond_eval_msg = cond_eval_msg.substr(0, cond_eval_msg.size() - 2);
    action_runtime_ctx->cond_eval_msg = cond_eval_msg;
    return true;
}

void WorkloadSchedPolicy::exec_action(WorkloadAction::RuntimeContext* action_runtime_ctx) {
    for (auto& action : _action_list) {
        action_runtime_ctx->policy_id = this->_id;
        action_runtime_ctx->policy_name = this->_name;
        action->exec(action_runtime_ctx);
    }
}

} // namespace doris