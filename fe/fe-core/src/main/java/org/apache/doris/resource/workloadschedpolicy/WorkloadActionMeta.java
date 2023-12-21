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

import com.google.gson.annotations.SerializedName;

public class WorkloadActionMeta {

    @SerializedName(value = "action")
    public WorkloadActionType action;

    @SerializedName(value = "actionArgs")
    public String actionArgs;

    public WorkloadActionMeta(String workloadAction, String actionArgs) throws UserException {
        this.action = getWorkloadActionType(workloadAction);
        this.actionArgs = actionArgs;
    }

    static WorkloadActionType getWorkloadActionType(String strType) throws UserException {
        if (WorkloadActionType.CANCEL_QUERY.toString().equalsIgnoreCase(strType)) {
            return WorkloadActionType.CANCEL_QUERY;
        } else if (WorkloadActionType.MOVE_QUERY_TO_GROUP.toString().equalsIgnoreCase(strType)) {
            return WorkloadActionType.MOVE_QUERY_TO_GROUP;
        } else if (WorkloadActionType.SET_SESSION_VARIABLE.toString().equalsIgnoreCase(strType)) {
            return WorkloadActionType.SET_SESSION_VARIABLE;
        }
        throw new UserException("invalid action type " + strType);
    }
}
