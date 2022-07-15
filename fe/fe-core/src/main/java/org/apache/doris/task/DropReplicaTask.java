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

import org.apache.doris.thrift.TDropTabletReq;
import org.apache.doris.thrift.TTaskType;

public class DropReplicaTask extends AgentTask {
    private int schemaHash; // set -1L as unknown
    private long replicaId;
    // It is used to distinguish whether the purpose of the drop replica is to drop table/partition,
    // or whether the table/partition is just a drop replica.
    // The former can safely delete all remote data,
    // but the latter can be shared by other bes because the table is still there
    private boolean isDropTableOrPartition;

    public DropReplicaTask(long backendId, long tabletId, long replicaId, int schemaHash,
                           boolean isDropTableOrPartition) {
        super(null, backendId, TTaskType.DROP, -1L, -1L, -1L, -1L, tabletId);
        this.schemaHash = schemaHash;
        this.replicaId = replicaId;
        this.isDropTableOrPartition = isDropTableOrPartition;
    }

    public TDropTabletReq toThrift() {
        TDropTabletReq request = new TDropTabletReq(tabletId);
        if (this.schemaHash != -1) {
            request.setSchemaHash(schemaHash);
        }
        request.setReplicaId(replicaId);
        request.setIsDropTableOrPartition(isDropTableOrPartition);
        return request;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getReplicaId() {
        return replicaId;
    }
}
