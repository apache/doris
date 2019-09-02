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

import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TTaskType;

public class SnapshotTask extends AgentTask {
    private static final int PREFERRED_SNAPSHOT_VERSION = 3;
    
    private long jobId;

    private long version;
    private long versionHash;

    private int schemaHash;

    private long timeoutMs;

    private boolean isRestoreTask;

    public SnapshotTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId,
            long dbId, long tableId, long partitionId, long indexId, long tabletId,
            long version, long versionHash, int schemaHash, long timeoutMs, boolean isRestoreTask) {
        super(resourceInfo, backendId, TTaskType.MAKE_SNAPSHOT, dbId, tableId, partitionId, indexId, tabletId,
                signature);

        this.jobId = jobId;

        this.version = version;
        this.versionHash = versionHash;
        this.schemaHash = schemaHash;

        this.timeoutMs = timeoutMs;

        this.isRestoreTask = isRestoreTask;
    }

    public long getJobId() {
        return jobId;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionHash() {
        return versionHash;
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

    public TSnapshotRequest toThrift() {
        TSnapshotRequest request = new TSnapshotRequest(tabletId, schemaHash);
        request.setVersion(version);
        request.setVersion_hash(versionHash);
        request.setList_files(true);
        request.setPreferred_snapshot_version(PREFERRED_SNAPSHOT_VERSION);
        request.setTimeout(timeoutMs / 1000);
        return request;
    }
}
