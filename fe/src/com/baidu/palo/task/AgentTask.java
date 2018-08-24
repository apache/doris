// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TTaskType;

public abstract class AgentTask {
    private long signature;
    private long backendId;
    private TTaskType taskType;

    protected long dbId;
    protected long tableId;
    protected long partitionId;
    protected long indexId;
    protected long tabletId;

    protected TResourceInfo resourceInfo;

    protected int failedTimes;

    public AgentTask(TResourceInfo resourceInfo, long backendId, long signature, TTaskType taskType,
                     long dbId, long tableId, long partitionId, long indexId, long tabletId) {
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

    @Override
    public String toString() {
        return "[" + taskType + "], signature: " + signature + ", backendId: " + backendId + ", tablet id: " + tabletId;
    }
}
