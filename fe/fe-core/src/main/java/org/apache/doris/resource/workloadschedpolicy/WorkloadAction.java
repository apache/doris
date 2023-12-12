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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.common.UserException;

public interface WorkloadAction {

    void exec(WorkloadQueryInfo queryInfo);

    WorkloadActionType getWorkloadActionType();

    // NOTE(wb) currently createPolicyAction is also used when replay meta, it better not contains heavy check
    static WorkloadAction createWorkloadAction(String actionCmd, String actionCmdArgs)
            throws UserException {
        if (WorkloadActionType.cancel_query.toString().equals(actionCmd)) {
            return WorkloadActionCancelQuery.createWorkloadAction();
        } else if (WorkloadActionType.move_query_to_group.toString().equals(actionCmd)) {
            return WorkloadActionMoveQueryToGroup.createWorkloadAction(actionCmdArgs);
        } else if (WorkloadActionType.set_session_variable.toString().equals(actionCmd)) {
            return WorkloadActionSetSessionVar.createWorkloadAction(actionCmdArgs);
        }
        throw new UserException("invalid action type " + actionCmd);
    }

}
