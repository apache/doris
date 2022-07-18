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

package org.apache.doris.persist;

import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplicaPersistInfo implements Writable {

    public enum ReplicaOperationType {
        ADD(0),
        CROND_DELETE(1),
        DELETE(2),
        CLONE(3),
        LOAD(4),
        ROLLUP(5),
        SCHEMA_CHANGE(6),
        CLEAR_ROLLUPINFO(7),
        // this default op is used for upgrate to femeta_45, add default op to solve this scenario
        // the old image and old persist log does not have op field, so the op field is null when upgrate to fe meta 45
        // then fe will dump image and want to write op type to image,
        // op type is null and then throw null pointer exception
        // add the default op, when read from image and op type == null ,set op type to default op to skip the exception
        DEFAULT_OP(8),
        TABLET_INFO(9);

        private final int value;

        ReplicaOperationType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static ReplicaOperationType findByValue(int value) {
            switch (value) {
                case 0:
                    return ADD;
                case 1:
                    return CROND_DELETE;
                case 2:
                    return DELETE;
                case 3:
                    return CLONE;
                case 4:
                    return LOAD;
                case 5:
                    return ROLLUP;
                case 6:
                    return SCHEMA_CHANGE;
                case 7:
                    return CLEAR_ROLLUPINFO;
                case 8:
                    return DEFAULT_OP;
                case 9:
                    return TABLET_INFO;
                default:
                    return null;
            }
        }
    }

    // required
    private ReplicaOperationType opType;
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;

    private long replicaId;
    private long backendId;

    private long version;
    @Deprecated
    private long versionHash = 0L;
    private int schemaHash = -1;
    private long dataSize;
    private long remoteDataSize;
    private long rowCount;

    private long lastFailedVersion = -1L;
    @Deprecated
    private long lastFailedVersionHash = 0L;
    private long lastSuccessVersion = -1L;
    @Deprecated
    private long lastSuccessVersionHash = 0L;

    public static ReplicaPersistInfo createForAdd(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long backendId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion,
            long lastSuccessVersion) {

        return new ReplicaPersistInfo(ReplicaOperationType.ADD,
                dbId, tableId, partitionId, indexId, tabletId, backendId,
                replicaId, version, schemaHash, dataSize, remoteDataSize, rowCount,
                lastFailedVersion, lastSuccessVersion);
    }

    /*
     * this for delete stmt operation
     */
    public static ReplicaPersistInfo createForCondDelete(long indexId, long tabletId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion, long lastSuccessVersion) {

        return new ReplicaPersistInfo(ReplicaOperationType.CROND_DELETE,
                -1L, -1L, -1L, indexId, tabletId, -1L,
                replicaId, version, schemaHash, dataSize, remoteDataSize, rowCount,
                lastFailedVersion, lastSuccessVersion);
    }

    /*
     * this for remove replica from meta
     */
    public static ReplicaPersistInfo createForDelete(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long backendId) {
        return new ReplicaPersistInfo(ReplicaOperationType.DELETE,
                dbId, tableId, partitionId, indexId, tabletId, backendId,
                -1L, -1L, -1, -1L, -1L, -1L, -1L, -1L);
    }

    public static ReplicaPersistInfo createForClone(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long backendId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion, long lastSuccessVersion) {

        return new ReplicaPersistInfo(ReplicaOperationType.CLONE,
                dbId, tableId, partitionId, indexId, tabletId, backendId, replicaId,
                version, schemaHash, dataSize, remoteDataSize, rowCount,
                lastFailedVersion, lastSuccessVersion);
    }

    // for original batch load, the last success version = version, last success version hash = version hash
    // last failed version = -1
    public static ReplicaPersistInfo createForLoad(long tableId, long partitionId, long indexId, long tabletId,
            long replicaId, long version, int schemaHash, long dataSize, long remoteDataSize, long rowCount) {

        return new ReplicaPersistInfo(ReplicaOperationType.LOAD,
                -1L, tableId, partitionId, indexId, tabletId, -1L,
                replicaId, version, schemaHash, dataSize, remoteDataSize,
                rowCount, -1L, version);
    }

    public static ReplicaPersistInfo createForRollup(long indexId, long tabletId, long backendId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion, long lastSuccessVersion) {

        return new ReplicaPersistInfo(ReplicaOperationType.ROLLUP,
                -1L, -1L, -1L, indexId, tabletId, backendId, -1L,
                version, schemaHash, dataSize, remoteDataSize, rowCount,
                lastFailedVersion, lastSuccessVersion);
    }

    public static ReplicaPersistInfo createForSchemaChange(long partitionId, long indexId, long tabletId,
            long backendId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion,
            long lastSuccessVersion) {

        return new ReplicaPersistInfo(ReplicaOperationType.SCHEMA_CHANGE,
                -1L, -1L, partitionId, indexId, tabletId, backendId, -1L, version,
                schemaHash, dataSize, remoteDataSize, rowCount, lastFailedVersion,
                lastSuccessVersion);
    }

    public static ReplicaPersistInfo createForClearRollupInfo(long dbId, long tableId, long partitionId, long indexId) {
        return new ReplicaPersistInfo(ReplicaOperationType.CLEAR_ROLLUPINFO,
                dbId, tableId, partitionId, indexId, -1L, -1L, -1L, -1L, -1, -1L, -1L, -1L, -1L, -1L);
    }

    public static ReplicaPersistInfo createForReport(long dbId, long tblId, long partitionId, long indexId,
            long tabletId, long backendId, long replicaId) {
        return new ReplicaPersistInfo(ReplicaOperationType.TABLET_INFO, dbId, tblId, partitionId,
                indexId, tabletId, backendId, replicaId, -1L, -1, -1L, -1L, -1L, -1L, -1L);
    }


    private ReplicaPersistInfo() {
    }

    private ReplicaPersistInfo(ReplicaOperationType opType, long dbId, long tableId, long partitionId,
            long indexId, long tabletId, long backendId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount, long lastFailedVersion,
            long lastSuccessVersion) {
        this.opType = opType;
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.backendId = backendId;
        this.replicaId = replicaId;
        this.version = version;
        this.schemaHash = schemaHash;
        this.dataSize = dataSize;
        this.remoteDataSize = remoteDataSize;
        this.rowCount = rowCount;

        this.lastFailedVersion = lastFailedVersion;
        this.lastSuccessVersion = lastSuccessVersion;
    }

    public ReplicaOperationType getOpType() {
        return opType;
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

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getDataSize() {
        return dataSize;
    }

    public long getRemoteDataSize() {
        return remoteDataSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getLastFailedVersion() {
        return lastFailedVersion;
    }

    public long getLastSuccessVersion() {
        return lastSuccessVersion;
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

        out.writeInt(opType.value);
        out.writeLong(lastFailedVersion);
        out.writeLong(lastFailedVersionHash);
        out.writeLong(lastSuccessVersion);
        out.writeLong(lastSuccessVersionHash);

        out.writeInt(schemaHash);
    }

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
        opType = ReplicaOperationType.DEFAULT_OP;
        opType = ReplicaOperationType.findByValue(in.readInt());
        if (opType == null) {
            throw new IOException("could not parse operation type from replica info");
        }
        lastFailedVersion = in.readLong();
        lastFailedVersionHash = in.readLong();
        lastSuccessVersion = in.readLong();
        lastSuccessVersionHash = in.readLong();
        schemaHash = in.readInt();
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
                && dataSize == info.dataSize
                && rowCount == info.rowCount
                && lastFailedVersion == info.lastFailedVersion
                && lastSuccessVersion == info.lastSuccessVersion;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" partition id: ").append(partitionId);
        sb.append(" index id: ").append(indexId);
        sb.append(" tablet id: ").append(tabletId);
        sb.append(" backend id: ").append(backendId);
        sb.append(" replica id: ").append(replicaId);
        sb.append(" version: ").append(version);
        sb.append(" schema hash: ").append(schemaHash);
        sb.append(" data size: ").append(dataSize);
        sb.append(" row count: ").append(rowCount);
        sb.append(" last failed version: ").append(lastFailedVersion);
        sb.append(" last success version: ").append(lastSuccessVersion);

        return sb.toString();
    }
}
