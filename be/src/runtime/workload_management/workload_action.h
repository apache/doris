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

#include "runtime/workload_management/workload_query_info.h"

namespace doris {

enum WorkloadActionType { MOVE_QUERY_TO_GROUP = 0, CANCEL_QUERY = 1 };

class WorkloadAction {
public:
    WorkloadAction() = default;
    virtual ~WorkloadAction() = default;

    virtual void exec(WorkloadQueryInfo* query_info) = 0;

    virtual WorkloadActionType get_action_type() = 0;
};

class WorkloadActionCancelQuery : public WorkloadAction {
public:
    void exec(WorkloadQueryInfo* query_info) override;

    WorkloadActionType get_action_type() override { return CANCEL_QUERY; }
};

//todo(wb) implement it
class WorkloadActionMoveQuery : public WorkloadAction {
public:
    WorkloadActionMoveQuery(std::string wg_name) : _wg_name(wg_name) {}
    void exec(WorkloadQueryInfo* query_info) override;

    WorkloadActionType get_action_type() override { return MOVE_QUERY_TO_GROUP; }

private:
    std::string _wg_name;
};

class WorkloadActionFactory {
public:
    static std::unique_ptr<WorkloadAction> create_workload_action(TWorkloadAction* action) {
        if (TWorkloadActionType::type::CANCEL_QUERY == action->action) {
            return std::make_unique<WorkloadActionCancelQuery>();
        }
        LOG(ERROR) << "not find a action " << action->action;
        return nullptr;
    }
};

} // namespace doris