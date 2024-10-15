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

#include <vector>

#include "runtime/workload_management/workload_action.h"
#include "runtime/workload_management/workload_condition.h"

namespace doris {

class WorkloadSchedPolicy {
public:
    WorkloadSchedPolicy() = default;
    ~WorkloadSchedPolicy() = default;

    void init(int64_t id, std::string name, int version, bool enabled, int priority,
              std::set<int64_t> wg_id_set,
              std::vector<std::unique_ptr<WorkloadCondition>> condition_list,
              std::vector<std::unique_ptr<WorkloadAction>> action_list);

    bool enabled() { return _enabled; }
    int priority() { return _priority; }

    bool is_match(WorkloadQueryInfo* query_info);

    WorkloadActionType get_first_action_type() { return _first_action_type; }

    void exec_action(WorkloadQueryInfo* query_info);

    int version() { return _version; }

private:
    int64_t _id;
    std::string _name;
    int _version;
    bool _enabled;
    int _priority;
    std::set<int64_t> _wg_id_set;

    std::vector<std::unique_ptr<WorkloadCondition>> _condition_list;
    std::vector<std::unique_ptr<WorkloadAction>> _action_list;

    WorkloadActionType _first_action_type;
};
} // namespace doris