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

import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TCloneReq;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTaskType;

import java.util.List;

public class CloneTask extends AgentTask {

    private int schemaHash;
    private List<TBackend> srcBackends;
    private TStorageMedium storageMedium;

    long committedVersion;
    long committedVersionHash;

    public CloneTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                     long tabletId, int schemaHash, List<TBackend> srcBackends, TStorageMedium storageMedium,
                     long committedVersion, long committedVersionHash) {
        super(null, backendId, TTaskType.CLONE, dbId, tableId, partitionId, indexId, tabletId);
        this.schemaHash = schemaHash;
        this.srcBackends = srcBackends;
        this.storageMedium = storageMedium;
        this.committedVersion = committedVersion;
        this.committedVersionHash = committedVersionHash;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCommittedVersion() {
        return committedVersion;
    }

    public long getCommittedVersionHash() {
        return committedVersionHash;
    }

    public TCloneReq toThrift() {
        TCloneReq request = new TCloneReq(tabletId, schemaHash, srcBackends);
        request.setStorage_medium(storageMedium);
        request.setCommitted_version(committedVersion);
        request.setCommitted_version_hash(committedVersionHash);
        return request;
    }
}
