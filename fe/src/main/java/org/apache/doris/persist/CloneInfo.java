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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
// (cmy 2015-07-22)
// donot use anymore. use ReplicaPersistInfo instead.
// remove later
public class CloneInfo implements Writable {
    public enum CloneType {
        CLONE,
        DELETE 
    }
    
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long version;
    private long versionHash;
    private long dataSize;
    private long rowCount;
    private long backendId;
    private CloneType type;

    
    public CloneInfo() {
        this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, CloneType.CLONE);
    }
    
    public CloneInfo(long dbId, long tableId, long partitionId, long indexId, long tabletId,
                     long replicaId, CloneType type) {
        this(dbId, tableId, partitionId, indexId, tabletId, replicaId, 0, 0, 0, 0, 0, type);
    }
    
    public CloneInfo(long dbId, long tableId, long partitionId, long indexId, long tabletId,
                     long replicaId, long version, long versionHash, long dataSize, long rowCount,
                     long backendId, CloneType type) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.replicaId = replicaId;
        this.version = version;
        this.versionHash = versionHash;
        this.dataSize = dataSize;
        this.rowCount = rowCount;
        this.backendId = backendId;
        this.type = type;
    }
    
    public long getDbId() {
        return this.dbId;
    }
    
    public long getTableId() {
        return this.tableId;
    }

    public long getPartitionId() {
        return this.partitionId;
    }
    
    public long getIndexId() {
        return this.indexId;
    }
    
    public long getTabletId() {
        return this.tabletId;
    }
    
    public long getReplicaId() {
        return this.replicaId;
    }
    
    public long getVersion() {
        return this.version;
    }
    
    public long getVersionHash() {
        return this.versionHash;
    }
    
    public long getDataSize() {
        return this.dataSize;
    }
    
    public long getRowCount() {
        return this.rowCount;
    }
    
    public long getBackendId() {
        return this.backendId;
    }
    
    public CloneType getType() {
        return this.type;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        out.writeLong(indexId);
        out.writeLong(tabletId);
        out.writeLong(replicaId);
        out.writeLong(version);
        out.writeLong(versionHash);
        out.writeLong(dataSize);
        out.writeLong(rowCount);
        out.writeLong(backendId);
        Text.writeString(out, type.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        tabletId = in.readLong();
        replicaId = in.readLong();
        version = in.readLong();
        versionHash = in.readLong();
        dataSize = in.readLong();
        rowCount = in.readLong();
        backendId = in.readLong();
        type = CloneType.valueOf(Text.readString(in));
    }
    
    public static CloneInfo read(DataInput in) throws IOException {
        CloneInfo cloneInfo = new CloneInfo();
        cloneInfo.readFields(in);
        return cloneInfo;
    }
}
