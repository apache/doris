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

import org.apache.doris.thrift.TAlterTabletReqV2;
import org.apache.doris.thrift.TTaskType;

/*
 * This is to replace the old CreateRollupTask.
 * This task only do data transformation from base tablet to rollup tablet.
 * The rollup tablet has already been created before.
 */
public class CreateRollupTaskV2 extends AgentTask {

    private long baseTabletId;
    private long rollupReplicaId;
    private int baseSchemaHash;
    private int rollupSchemaHash;
    private long version;
    private long versionHash;
    private long jobId;

    public CreateRollupTaskV2(long backendId, long dbId, long tableId,
            long partitionId, long rollupIndexId, long baseIndexId, long rollupTabletId,
            long baseTabletId, long rollupReplicaId, int rollupSchemaHash, int baseSchemaHash,
            long version, long versionHash, long jobId) {
        super(null, backendId, TTaskType.ALTER, dbId, tableId, partitionId, rollupIndexId, rollupTabletId);

        this.baseTabletId = baseTabletId;
        this.rollupReplicaId = rollupReplicaId;

        this.rollupSchemaHash = rollupSchemaHash;
        this.baseSchemaHash = baseSchemaHash;

        this.version = version;
        this.versionHash = versionHash;
        this.jobId = jobId;
    }

    public long getBaseTabletId() {
        return baseTabletId;
    }

    public long getRollupReplicaId() {
        return rollupReplicaId;
    }

    public int getRollupSchemaHash() {
        return rollupSchemaHash;
    }

    public int getBaseSchemaHash() {
        return baseSchemaHash;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionHash() {
        return versionHash;
    }

    public long getJobId() {
        return jobId;
    }

    public TAlterTabletReqV2 toThrift() {
        TAlterTabletReqV2 req = new TAlterTabletReqV2(baseTabletId, signature, baseSchemaHash, rollupSchemaHash);
        req.setAlter_version(version);
        req.setAlter_version_hash(versionHash);
        return req;
    }
}
