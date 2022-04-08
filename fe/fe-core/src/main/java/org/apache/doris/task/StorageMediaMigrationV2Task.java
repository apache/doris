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

import org.apache.doris.thrift.TStorageMigrationReqV2;
import org.apache.doris.thrift.TTaskType;

/*
 * This task is used for migration replica process
 * The task will do data transformation from base replica to new replica.
 * The new replica should be created before.
 */
public class StorageMediaMigrationV2Task extends AgentTask {

    private long baseTabletId;
    private long newReplicaId;
    private int baseSchemaHash;
    private int newSchemaHash;
    private long version;
    private long jobId;

    public StorageMediaMigrationV2Task(long backendId, long dbId, long tableId,
                                       long partitionId, long rollupIndexId, long baseIndexId, long rollupTabletId,
                                       long baseTabletId, long newReplicaId, int newSchemaHash, int baseSchemaHash,
                                       long version, long jobId) {
        super(null, backendId, TTaskType.STORAGE_MEDIUM_MIGRATE_V2, dbId, tableId, partitionId, rollupIndexId, rollupTabletId);

        this.baseTabletId = baseTabletId;
        this.newReplicaId = newReplicaId;

        this.newSchemaHash = newSchemaHash;
        this.baseSchemaHash = baseSchemaHash;

        this.version = version;
        this.jobId = jobId;
    }

    public long getBaseTabletId() {
        return baseTabletId;
    }

    public long getNewReplicaId() {
        return newReplicaId;
    }

    public int getNewSchemaHash() {
        return newSchemaHash;
    }

    public int getBaseSchemaHash() {
        return baseSchemaHash;
    }

    public long getVersion() {
        return version;
    }

    public long getJobId() {
        return jobId;
    }

    public TStorageMigrationReqV2 toThrift() {
        TStorageMigrationReqV2 req = new TStorageMigrationReqV2(baseTabletId, signature, baseSchemaHash, newSchemaHash);
        req.setMigrationVersion(version);
        return req;
    }
}
