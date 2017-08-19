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

package com.baidu.palo.catalog;

import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * This class represents the olap replica related metadata.
 */
public class Replica implements Writable {
    private static final Logger LOG = LogManager.getLogger(Replica.class);
    public static final VersionComparator<Replica> VERSION_DESC_COMPARATOR = new VersionComparator<Replica>();

    public enum ReplicaState {
        NORMAL,
        ROLLUP,
        SCHEMA_CHANGE,
        CLONE
    }
    
    private long id;
    private long backendId;
    private long version;
    private long versionHash;
    private long dataSize;
    private long rowCount;
    private ReplicaState state;
    
    public Replica() {
    }
    
    public Replica(long replicaId, long backendId, ReplicaState state) {
        this(replicaId, backendId, -1, 0, -1, -1, state);
    }
    
    public Replica(long replicaId, long backendId, ReplicaState state, long version, long versionHash) {
        this(replicaId, backendId, version, versionHash, -1, -1, state);
    }

    public Replica(long replicaId, long backendId, long version, long versionHash,
                       long dataSize, long rowCount, ReplicaState state) {
        this.id = replicaId;
        this.backendId = backendId;
        this.version = version;
        this.versionHash = versionHash;
        this.dataSize = dataSize;
        this.rowCount = rowCount;
        this.state = state;
        if (this.state == null) {
            this.state = ReplicaState.NORMAL;
        }
    }
    
    public long getVersion() {
        return this.version;
    }
    
    public long getVersionHash() {
        return this.versionHash;
    }

    public long getId() {
        return this.id;
    }

    public long getBackendId() {
        return this.backendId;
    }
    
    public long getDataSize() {
        return dataSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void updateInfo(long newVersion, long newVersionHash, long newDataSize, long newRowCount) {
        if (newVersion < this.version) {
            LOG.warn("replica[" + id + "] new version is lower than meta version. " + newVersion + " vs " + version);
        }
        this.version = newVersion;
        this.versionHash = newVersionHash;
        this.dataSize = newDataSize;
        this.rowCount = newRowCount;

        LOG.debug("update {}", this.toString());
    }

    public boolean checkVersionCatchUp(long committedVersion, long committedVersionHash) {
        if (this.version < committedVersion
                || (this.version == committedVersion && this.versionHash != committedVersionHash)) {
            LOG.debug("replica version does not catch up with version: {}-{}. replica: {}",
                      committedVersion, committedVersionHash, this);
            return false;
        }
        return true;
    }

    public void setState(ReplicaState replicaState) {
        this.state = replicaState;
    }
    
    public ReplicaState getState() {
        return this.state;
    }
    
    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer("replicaId=");
        strBuffer.append(id);
        strBuffer.append(", BackendId=");
        strBuffer.append(backendId);
        strBuffer.append(", version=");
        strBuffer.append(version);
        strBuffer.append(", versionHash=");
        strBuffer.append(versionHash);
        strBuffer.append(", dataSize=");
        strBuffer.append(dataSize);
        strBuffer.append(", rowCount=");
        strBuffer.append(rowCount);
        return strBuffer.toString();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(backendId);
        out.writeLong(version);
        out.writeLong(versionHash);
        out.writeLong(dataSize);
        out.writeLong(rowCount);
        Text.writeString(out, state.name());
    }
     
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        backendId = in.readLong();
        version = in.readLong();
        versionHash = in.readLong();
        dataSize = in.readLong();
        rowCount = in.readLong();
        state = ReplicaState.valueOf(Text.readString(in));
    }
    
    public static Replica read(DataInput in) throws IOException {
        Replica replica = new Replica();
        replica.readFields(in);
        return replica;
    }
    
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Replica)) {
            return false;
        }
        
        Replica replica = (Replica) obj;
        return (id == replica.id) 
                && (backendId == replica.backendId) 
                && (version == replica.version)
                && (versionHash == replica.versionHash)
                && (dataSize == replica.dataSize)
                && (rowCount == replica.rowCount) 
                && (state.equals(replica.state));
    }

    private static class VersionComparator<T extends Replica> implements Comparator<T> {
        public VersionComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getVersion() < replica2.getVersion()) {
                return 1;
            } else if (replica1.getVersion() == replica2.getVersion()) {
                return 0;
            } else {
                return -1;
            }
        }
    }
}
