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

import org.apache.doris.catalog.Database;

import java.util.Set;

public class CloneTabletInfo {
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private short replicationNum;
    private short onlineReplicaNum;
    private long tabletSizeB;
    private Set<Long> backendIds;
    private Database.DbState dbState;

    public CloneTabletInfo(long dbId, long tableId, long partitionId, long indexId, long tabletId, short replicationNum,
                           short onlineReplicaNum, long tabletSizeB, Set<Long> backendIds) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.replicationNum = replicationNum;
        this.onlineReplicaNum = onlineReplicaNum;
        this.tabletSizeB = tabletSizeB;
        this.backendIds = backendIds;
        this.dbState = Database.DbState.NORMAL;
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

    public short getReplicationNum() {
        return replicationNum;
    }

    public short getOnlineReplicaNum() {
        return onlineReplicaNum;
    }

    public long getTabletSizeB() {
        return tabletSizeB;
    }

    public Set<Long> getBackendIds() {
        return backendIds;
    }

    @Override
    public String toString() {
        return "TabletInfo [dbId=" + dbId + ", tableId=" + tableId + ", partitionId=" + partitionId + ", indexId="
                + indexId + ", tabletId=" + tabletId + ", replicationNum=" + replicationNum + ", onlineReplicaNum="
                + onlineReplicaNum + ", tabletSizeB=" + tabletSizeB + ", backendIds=" + backendIds + "]";
    }

    public Database.DbState getDbState() {
        return dbState;
    }

    public void setDbState(Database.DbState dbState) {
        this.dbState = dbState;
    }
}
