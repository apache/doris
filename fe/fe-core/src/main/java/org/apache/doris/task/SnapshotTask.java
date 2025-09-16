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

import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TypesConstants;

public class SnapshotTask extends AgentTask {
    private long jobId;
    private long version;
    private int schemaHash;
    private long timeoutMs;
    private boolean isRestoreTask;
    private Long refTabletId;

    // Set to true if this task for AdminCopyTablet.
    // Otherwise, it is for Backup/Restore operation.
    private boolean isCopyTabletTask = false;
    private MarkedCountDownLatch<Long, Long> countDownLatch;
    // Only for copy tablet task.
    // Save the snapshot path.
    private String resultSnapshotPath;

    public SnapshotTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId, long tableId,
            long partitionId, long indexId, long tabletId, long version, int schemaHash, long timeoutMs,
            boolean isRestoreTask) {
        super(resourceInfo, backendId, TTaskType.MAKE_SNAPSHOT, dbId, tableId, partitionId, indexId, tabletId,
                signature);

        this.jobId = jobId;

        this.version = version;
        this.schemaHash = schemaHash;

        this.timeoutMs = timeoutMs;

        this.isRestoreTask = isRestoreTask;
    }

    public void setIsCopyTabletTask(boolean value) {
        this.isCopyTabletTask = value;
    }

    public boolean isCopyTabletTask() {
        return isCopyTabletTask;
    }

    public void setCountDownLatch(MarkedCountDownLatch<Long, Long> countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void countDown(long backendId, long tabletId) {
        this.countDownLatch.markedCountDown(backendId, tabletId);
    }

    public long getJobId() {
        return jobId;
    }

    public long getVersion() {
        return version;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public boolean isRestoreTask() {
        return isRestoreTask;
    }

    public void setResultSnapshotPath(String resultSnapshotPath) {
        this.resultSnapshotPath = resultSnapshotPath;
    }

    public String getResultSnapshotPath() {
        return resultSnapshotPath;
    }

    public void setRefTabletId(long refTabletId) {
        assert refTabletId > 0;
        this.refTabletId = refTabletId;
    }

    public TSnapshotRequest toThrift() {
        TSnapshotRequest request = new TSnapshotRequest(tabletId, schemaHash);
        request.setListFiles(true);
        request.setPreferredSnapshotVersion(TypesConstants.TPREFER_SNAPSHOT_REQ_VERSION);
        request.setTimeout(timeoutMs / 1000);
        request.setIsCopyTabletTask(isCopyTabletTask);
        if (refTabletId != null) {
            request.setRefTabletId(refTabletId);
        }
        if (version > 0L) {
            request.setVersion(version);
        }
        return request;
    }
}
