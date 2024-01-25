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
    // these versions are for distinguishing the old clone task(VERSION_1) and the new clone task(VERSION_2)
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

    private int schemaHash;
    private long replicaId;
    private List<TBackend> srcBackends;
    private TStorageMedium storageMedium;
    private TBackend destBackend;

    private long visibleVersion;

    private long srcPathHash = -1;
    private long destPathHash = -1;

    private int timeoutS;

    private int taskVersion = VERSION_1;

    public CloneTask(TBackend destBackend, long backendId, long dbId, long tableId, long partitionId,
            long indexId, long tabletId, long replicaId, int schemaHash, List<TBackend> srcBackends,
            TStorageMedium storageMedium, long visibleVersion, int timeoutS) {
        super(null, backendId, TTaskType.CLONE, dbId, tableId, partitionId, indexId, tabletId);
        this.destBackend = destBackend;
        this.replicaId = replicaId;
        this.schemaHash = schemaHash;
        this.srcBackends = srcBackends;
        this.storageMedium = storageMedium;
        this.visibleVersion = visibleVersion;
        this.timeoutS = timeoutS;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }

    public void setPathHash(long srcPathHash, long destPathHash) {
        this.srcPathHash = srcPathHash;
        this.destPathHash = destPathHash;
        this.taskVersion = VERSION_2;
    }

    public int getTaskVersion() {
        return taskVersion;
    }

    public TCloneReq toThrift() {
        TCloneReq request = new TCloneReq(tabletId, schemaHash, srcBackends);
        request.setReplicaId(replicaId);
        request.setStorageMedium(storageMedium);
        request.setVersion(visibleVersion);
        request.setTaskVersion(taskVersion);
        request.setPartitionId(partitionId);
        if (taskVersion == VERSION_2) {
            request.setSrcPathHash(srcPathHash);
            request.setDestPathHash(destPathHash);
        }
        request.setTimeoutS(timeoutS);

        return request;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId)
                .append(", replica id: ").append(replicaId)
                .append(", schema hash: ").append(schemaHash);
        sb.append(", storageMedium: ").append(storageMedium.name());
        sb.append(", visible version: ").append(visibleVersion);
        sb.append(", src backend: ").append(srcBackends.get(0).getHost())
                .append(", src path hash: ").append(srcPathHash);
        sb.append(", dest backend id: ").append(backendId)
                .append(", dest backend: ").append(destBackend.getHost())
                .append(", dest path hash: ").append(destPathHash);
        return sb.toString();
    }
}
