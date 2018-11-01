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

package org.apache.doris.clone;

import org.apache.doris.task.CloneTask;

public class CloneJob {
    public enum JobPriority {
        HIGH,
        NORMAL,
        LOW
    }

    public enum JobState {
        PENDING,
        RUNNING,
        FINISHED,
        CANCELLED
    }
    
    public enum JobType {
        SUPPLEMENT,
        MIGRATION,
        CATCHUP
    }
    
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long destBackendId;
    private JobState state;
    private JobType type; 
    private JobPriority priority;
    private long createTimeMs;
    private long cloneStartTimeMs;
    private long cloneFinishTimeMs;
    private long timeoutMs;
    private String failMsg;
    private CloneTask cloneTask;

    public CloneJob(long dbId, long tableId, long partitionId, long indexId, long tabletId,
                    long destBackendId, JobType type, JobPriority priority, long timeoutMs) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.destBackendId = destBackendId;
        this.state = JobState.PENDING;
        this.type = type;
        this.priority = priority;
        this.createTimeMs = System.currentTimeMillis();
        this.cloneStartTimeMs = -1;
        this.cloneFinishTimeMs = -1;
        this.timeoutMs = timeoutMs;
        this.failMsg = "";
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

    public long getDestBackendId() {
        return destBackendId;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public JobType getType() {
        return type;
    }

    public JobPriority getPriority() {
        return priority;
    }

    public void setPriority(JobPriority priority) {
        this.priority = priority;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }
    
    public void setCreateTimeMs(long createTimeMs) {
        this.createTimeMs = createTimeMs;
    }

    public long getCloneStartTimeMs() {
        return cloneStartTimeMs;
    }

    public void setCloneStartTimeMs(long cloneStartTimeMs) {
        this.cloneStartTimeMs = cloneStartTimeMs;
    }

    public long getCloneFinishTimeMs() {
        return cloneFinishTimeMs;
    }

    public void setCloneFinishTimeMs(long cloneFinishTimeMs) {
        this.cloneFinishTimeMs = cloneFinishTimeMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public String getFailMsg() {
        return failMsg;
    }

    public void setFailMsg(String failMsg) {
        this.failMsg = failMsg;
    }
    
    public void setCloneTask(CloneTask task) {
        this.cloneTask = task;
    }
    
    public CloneTask getCloneTask() {
        return this.cloneTask;
    }

    @Override
    public String toString() {
        return "CloneJob [dbId=" + dbId + ", tableId=" + tableId + ", partitionId=" + partitionId
                + ", indexId=" + indexId + ", tabletId=" + tabletId + ", destBackendId=" + destBackendId
                + ", state=" + state + ", type=" + type + ", priority=" + priority
                + ", createTimeMs=" + createTimeMs + ", cloneStartTimeMs=" + cloneStartTimeMs
                + ", cloneFinishTimeMs=" + cloneFinishTimeMs + ", timeoutMs=" + timeoutMs + ", failMsg=" + failMsg
                + "]";
    }

}
