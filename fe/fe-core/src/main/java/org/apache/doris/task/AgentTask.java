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

package org.apache.doris.task;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

public abstract class AgentTask {
    protected long signature;
    protected long backendId;
    protected TTaskType taskType;

    protected long dbId;
    protected long tableId;
    protected long partitionId;
    protected long indexId;
    protected long tabletId;

    protected TResourceInfo resourceInfo;

    protected int failedTimes;
    protected String errorMsg;
    // some of process may use this member to check if the task is finished.
    // some of are not.
    // so whether the task is finished depends on caller's logic, not the value of this member.
    protected boolean isFinished = false;
    protected long createTime;

    public AgentTask(TResourceInfo resourceInfo, long backendId, TTaskType taskType,
                     long dbId, long tableId, long partitionId, long indexId, long tabletId, long signature, long createTime) {
        this.backendId = backendId;
        this.signature = signature;
        this.taskType = taskType;

        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;

        this.resourceInfo = resourceInfo;

        this.failedTimes = 0;
        this.createTime = createTime;
    }
    
    public AgentTask(TResourceInfo resourceInfo, long backendId, TTaskType taskType,
            long dbId, long tableId, long partitionId, long indexId, long tabletId) {
        this(resourceInfo, backendId, taskType, dbId, tableId, partitionId, indexId, tabletId, tabletId, -1);
    }

    public AgentTask(TResourceInfo resourceInfo, long backendId, TTaskType taskType,
                     long dbId, long tableId, long partitionId, long indexId, long tabletId, long signature) {
        this(resourceInfo, backendId, taskType, dbId, tableId, partitionId, indexId, tabletId, signature, -1);
    }

    public long getSignature() {
        return this.signature;
    }

    public long getBackendId() {
        return this.backendId;
    }

    public TTaskType getTaskType() {
        return this.taskType;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public TResourceInfo getResourceInfo() {
        return resourceInfo;
    }

    public void failed() {
        ++this.failedTimes;
    }

    public int getFailedTimes() {
        return this.failedTimes;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setFinished(boolean isFinished) {
        this.isFinished = isFinished;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public boolean shouldResend(long currentTimeMillis) {
        return createTime == -1 || currentTimeMillis - createTime > Config.agent_task_resend_wait_time_ms;
    }

    @Override
    public String toString() {
        return "[" + taskType + "], signature: " + signature + ", backendId: " + backendId + ", tablet id: " + tabletId;
    }
}
