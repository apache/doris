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

public class WorkloadActionMoveQueryToGroup implements WorkloadAction {
    private long dstWgId;

    public WorkloadActionMoveQueryToGroup(long wgId) {
        this.dstWgId = wgId;
    }

    @Override
    public void exec(WorkloadQueryInfo queryInfo) {
    }

    @Override
    public WorkloadActionType getWorkloadActionType() {
        return WorkloadActionType.MOVE_QUERY_TO_GROUP;
    }

    public static WorkloadActionMoveQueryToGroup createWorkloadAction(String groupIdStr) {
        Long wgId = Long.parseLong(groupIdStr);
        return new WorkloadActionMoveQueryToGroup(wgId);
    }
}
