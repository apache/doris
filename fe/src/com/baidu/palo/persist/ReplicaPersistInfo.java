// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.persist;

import com.baidu.palo.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplicaPersistInfo implements Writable {
    // required
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;

    private long replicaId;
    private long backendId;
    
    private long version;
    private long versionHash;
    private long dataSize;
    private long rowCount;

    public static ReplicaPersistInfo createForAdd(long dbId, long tableId, long partitionId, long indexId,
                                                  long tabletId, long backendId, long replicaId, long version,
                                                  long versionHash, long dataSize, long rowCount) {
        return new ReplicaPersistInfo(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                      replicaId, version, versionHash, dataSize, rowCount);
    }

    /*
     * this for delete stmt operation
     */
    public static ReplicaPersistInfo createForCondDelete(long indexId, long tabletId, long replicaId, long version,
                                                         long versionHash, long dataSize, long rowCount) {
        return new ReplicaPersistInfo(-1L, -1L, -1L, indexId, tabletId, -1L,
                                      replicaId, version, versionHash, dataSize, rowCount);
    }

    /*
     * this for remove replica from meta
     */
    public static ReplicaPersistInfo createForDelete(long dbId, long tableId, long partitionId, long indexId,
                                                     long tabletId, long backendId) {
        return new ReplicaPersistInfo(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                      -1L, -1L, -1L, -1L, -1L);
    }

    public static ReplicaPersistInfo createForClone(long dbId, long tableId, long partitionId, long indexId,
                                                    long tabletId, long backendId, long replicaId, long version,
                                                    long versionHash, long dataSize, long rowCount) {
        return new ReplicaPersistInfo(dbId, tableId, partitionId, indexId, tabletId, backendId, replicaId,
                                      version, versionHash, dataSize, rowCount);
    }

    public static ReplicaPersistInfo createForLoad(long tableId, long partitionId, long indexId, long tabletId,
                                                   long replicaId, long version, long versionHash, long dataSize,
                                                   long rowCount) {
        return new ReplicaPersistInfo(-1L, tableId, partitionId, indexId, tabletId, -1L,
                                      replicaId, version, versionHash, dataSize, rowCount);
    }

    public static ReplicaPersistInfo createForRollup(long indexId, long tabletId, long backendId, long version,
                                                     long versionHash, long dataSize, long rowCount) {
        return new ReplicaPersistInfo(-1L, -1L, -1L, indexId, tabletId, backendId, -1L,
                                      version, versionHash, dataSize, rowCount);
    }

    public static ReplicaPersistInfo createForSchemaChange(long partitionId, long indexId, long tabletId,
                                                           long backendId, long version, long versionHash,
                                                           long dataSize, long rowCount) {
        return new ReplicaPersistInfo(-1L, -1L, partitionId, indexId, tabletId, backendId, -1L, version,
                                      versionHash, dataSize, rowCount);
    }
    
    public static ReplicaPersistInfo createForClearRollupInfo(long dbId, long tableId, long partitionId, long indexId) {
        return new ReplicaPersistInfo(dbId, tableId, partitionId, indexId, -1L, -1L, -1L, -1L, -1L, -1L, -1L);
    }

    public ReplicaPersistInfo() {
    }
    
    private ReplicaPersistInfo(long dbId, long tableId, long partitionId, long indexId, long tabletId, long backendId,
                               long replicaId, long version, long versionHash, long dataSize, long rowCount) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.backendId = backendId;

        this.replicaId = replicaId;

        this.version = version;
        this.versionHash = versionHash;
        this.dataSize = dataSize;
        this.rowCount = rowCount;
    }

    public void setReplicaId(long replicaId) {
        this.replicaId = replicaId;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setVersionHash(long versionHash) {
        this.versionHash = versionHash;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
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

    public long getReplicaId() {
        return replicaId;
    }

    public long getBackendId() {
        return backendId;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionHash() {
        return versionHash;
    }

    public long getDataSize() {
        return dataSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public static ReplicaPersistInfo read(DataInput in) throws IOException {
        ReplicaPersistInfo replicaInfo = new ReplicaPersistInfo();
        replicaInfo.readFields(in);
        return replicaInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        out.writeLong(indexId);
        out.writeLong(tabletId);
        out.writeLong(backendId);
        out.writeLong(replicaId);
        out.writeLong(version);
        out.writeLong(versionHash);
        out.writeLong(dataSize);
        out.writeLong(rowCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        tabletId = in.readLong();
        backendId = in.readLong();
        replicaId = in.readLong();
        version = in.readLong();
        versionHash = in.readLong();
        dataSize = in.readLong();
        rowCount = in.readLong();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof ReplicaPersistInfo)) {
            return false;
        }
        
        ReplicaPersistInfo info = (ReplicaPersistInfo) obj;
        
        return backendId == info.backendId
                && replicaId == info.replicaId
                && tabletId == info.tabletId
                && indexId == info.indexId
                && partitionId == info.partitionId
                && tableId == info.tableId
                && dbId == info.dbId
                && version == info.version
                && versionHash == info.versionHash
                && dataSize == info.dataSize
                && rowCount == info.rowCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" partition id: ").append(partitionId);
        sb.append(" index id: ").append(indexId);
        sb.append(" index id: ").append(indexId);
        sb.append(" tablet id: ").append(tabletId);
        sb.append(" backend id: ").append(backendId);
        sb.append(" replica id: ").append(replicaId);
        sb.append(" version: ").append(version);
        sb.append(" version hash: ").append(versionHash);
        sb.append(" data size: ").append(dataSize);
        sb.append(" row count: ").append(rowCount);

        return sb.toString();
    }
}
