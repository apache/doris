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

import org.apache.doris.common.publish.WorkloadMoveActionPublisherThread;
import org.apache.doris.thrift.TWorkloadMoveQueryToGroupAction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkloadActionMoveQueryToGroup implements WorkloadAction {

    private static final Logger LOG = LogManager.getLogger(WorkloadActionMoveQueryToGroup.class);

    private long dstWgId;

    public WorkloadActionMoveQueryToGroup(long dstWgId) {
        this.dstWgId = dstWgId;
    }

    @Override
    public void exec(WorkloadQueryInfo queryInfo) {
        LOG.info("try move query {} to group {}", queryInfo.queryId, dstWgId);
        TWorkloadMoveQueryToGroupAction moveQueryToGroupAction = new TWorkloadMoveQueryToGroupAction();
        moveQueryToGroupAction.setQueryId(queryInfo.tUniqueId);
        moveQueryToGroupAction.setWorkloadGroupId(dstWgId);
        WorkloadMoveActionPublisherThread.putMoveAction(moveQueryToGroupAction);
    }

    @Override
    public WorkloadActionType getWorkloadActionType() {
        return WorkloadActionType.move_query_to_group;
    }

    public static WorkloadActionMoveQueryToGroup createWorkloadAction(String groupId) {
        long wgId = Long.parseLong(groupId);
        return new WorkloadActionMoveQueryToGroup(wgId);
    }

}
