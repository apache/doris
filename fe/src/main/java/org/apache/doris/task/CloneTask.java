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
    private List<TBackend> srcBackends;
    private TStorageMedium storageMedium;

    private long visibleVersion;
    private long visibleVersionHash;

    private long srcPathHash = -1;
    private long destPathHash = -1;

    private int taskVersion = VERSION_1;

    public CloneTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                     long tabletId, int schemaHash, List<TBackend> srcBackends, TStorageMedium storageMedium,
                     long visibleVersion, long visibleVersionHash) {
        super(null, backendId, TTaskType.CLONE, dbId, tableId, partitionId, indexId, tabletId);
        this.schemaHash = schemaHash;
        this.srcBackends = srcBackends;
        this.storageMedium = storageMedium;
        this.visibleVersion = visibleVersion;
        this.visibleVersionHash = visibleVersionHash;
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

    public long getVisibleVersionHash() {
        return visibleVersionHash;
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
        request.setStorage_medium(storageMedium);
        request.setCommitted_version(visibleVersion);
        request.setCommitted_version_hash(visibleVersionHash);
        request.setTask_version(taskVersion);
        if (taskVersion == VERSION_2) {
            request.setSrc_path_hash(srcPathHash);
            request.setDest_path_hash(destPathHash);
        }

        return request;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId).append(", schema hash: ").append(schemaHash);
        sb.append(", storageMedium: ").append(storageMedium.name());
        sb.append(", visible version(hash): ").append(visibleVersion).append("-").append(visibleVersionHash);
        sb.append(", src backend: ").append(srcBackends.get(0).getHost()).append(", src path hash: ").append(srcPathHash);
        sb.append(", dest backend: ").append(backendId).append(", dest path hash: ").append(destPathHash);
        return sb.toString();
    }
}
