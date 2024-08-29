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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplicaPersistInfo implements Writable, GsonPostProcessable {

    public enum ReplicaOperationType {
        ADD(0),
        CROND_DELETE(1), //Deprecated
        DELETE(2),
        CLONE(3),   //Deprecated
        LOAD(4),    //Deprecated
        ROLLUP(5),  //Deprecated
        SCHEMA_CHANGE(6), //Deprecated
        CLEAR_ROLLUPINFO(7), //Deprecated
        // this default op is used for upgrate to femeta_45, add default op to solve this scenario
        // the old image and old persist log does not have op field, so the op field is null when upgrate to fe meta 45
        // then fe will dump image and want to write op type to image,
        // op type is null and then throw null pointer exception
        // add the default op, when read from image and op type == null ,set op type to default op to skip the exception
        DEFAULT_OP(8),   //Deprecated
        TABLET_INFO(9),  //Deprecated
        UPDATE(10);

        @SerializedName("v")
        private final int value;

        ReplicaOperationType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static ReplicaOperationType findByValue(int value) {
            for (ReplicaOperationType type : ReplicaOperationType.values()) {
                if (type.getValue() == value) {
                    return type;
                }
            }
            return null;
        }
    }

    // required
    @SerializedName("op")
    private ReplicaOperationType opType;
    @SerializedName("dbid")
    private long dbId;
    @SerializedName("tbid")
    private long tableId;
    @SerializedName("pid")
    private long partitionId;
    @SerializedName("ind")
    private long indexId;
    @SerializedName("tblid")
    private long tabletId;

    @SerializedName("repid")
    private long replicaId;
    @SerializedName("beid")
    private long backendId;

    @SerializedName("ver")
    private long version;
    @Deprecated
    @SerializedName("verh")
    private long versionHash = 0L;
    @SerializedName("sh")
    private int schemaHash = -1;
    @SerializedName("ds")
    private long dataSize;
    private long remoteDataSize;
    @SerializedName("rc")
    private long rowCount;

    @SerializedName("lfv")
    private long lastFailedVersion = -1L;
    @Deprecated
    @SerializedName("lfvh")
    private long lastFailedVersionHash = 0L;
    @SerializedName("lsv")
    private long lastSuccessVersion = -1L;
    @Deprecated
    @SerializedName("lsvh")
    private long lastSuccessVersionHash = 0L;

    @SerializedName("bad")
    private boolean isBad = false;

    public static ReplicaPersistInfo createForAdd(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long backendId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion,
            long lastSuccessVersion) {

        return new ReplicaPersistInfo(ReplicaOperationType.ADD,
                dbId, tableId, partitionId, indexId, tabletId, backendId,
                replicaId, version, schemaHash, dataSize, remoteDataSize, rowCount,
                lastFailedVersion, lastSuccessVersion, false);
    }

    /*
    /*
     * this for remove replica from meta
     */
    public static ReplicaPersistInfo createForDelete(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long backendId) {
        return new ReplicaPersistInfo(ReplicaOperationType.DELETE,
                dbId, tableId, partitionId, indexId, tabletId, backendId,
                -1L, -1L, -1, -1L, -1L, -1L, -1L, -1L, false);
    }

    public static ReplicaPersistInfo createForUpdate(long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long backendId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount,
            long lastFailedVersion, long lastSuccessVersion, boolean isBad) {

        return new ReplicaPersistInfo(ReplicaOperationType.UPDATE,
                dbId, tableId, partitionId, indexId, tabletId, backendId, replicaId,
                version, schemaHash, dataSize, remoteDataSize, rowCount,
                lastFailedVersion, lastSuccessVersion, isBad);
    }

    private ReplicaPersistInfo() {
    }

    private ReplicaPersistInfo(ReplicaOperationType opType, long dbId, long tableId, long partitionId,
            long indexId, long tabletId, long backendId, long replicaId, long version,
            int schemaHash, long dataSize, long remoteDataSize, long rowCount, long lastFailedVersion,
            long lastSuccessVersion, boolean isBad) {
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
        this.isBad = isBad;
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

    public boolean isBad() {
        return isBad;
    }

    public static ReplicaPersistInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_137) {
            ReplicaPersistInfo replicaInfo = new ReplicaPersistInfo();
            replicaInfo.readFields(in);
            return replicaInfo;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), ReplicaPersistInfo.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (opType == null) {
            throw new IOException("could not parse operation type from replica info");
        }
    }

    @Deprecated
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
