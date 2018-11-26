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

package org.apache.doris.catalog;

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

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
    
    public enum ReplicaStatus {
        OK, // health
        DEAD, // backend is not available
        VERSION_ERROR, // missing version
        MISSING // replica does not exist
    }
    
    private long id;
    private long backendId;
    private long version;
    private long versionHash;
    private long dataSize;
    private long rowCount;
    private ReplicaState state;
    
    private long lastFailedVersion = -1L;
    private long lastFailedVersionHash = 0L;
    // not serialized, not very important
    private long lastFailedTimestamp = 0;
    private long lastSuccessVersion = -1L;
    private long lastSuccessVersionHash = 0L;

	private AtomicLong versionCount = new AtomicLong(-1);

    private long pathHash = -1;
    
    public Replica() {
    }
    
    // the new replica's version is -1 and last failed version is -1
    public Replica(long replicaId, long backendId, ReplicaState state) {
        this(replicaId, backendId, -1, 0, -1, -1, state, -1, 0, -1, 0);
    }
    
    public Replica(long replicaId, long backendId, ReplicaState state, long version, long versionHash) {
        this(replicaId, backendId, version, versionHash, -1, -1, state, -1L, 0L, version, versionHash);
    }

    public Replica(long replicaId, long backendId, long version, long versionHash,
                       long dataSize, long rowCount, ReplicaState state, 
                       long lastFailedVersion, long lastFailedVersionHash,
                       long lastSuccessVersion, long lastSuccessVersionHash) {
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
        this.lastFailedVersion = lastFailedVersion;
        this.lastFailedVersionHash = lastFailedVersionHash;
        if (this.lastFailedVersion > 0) {
            this.lastFailedTimestamp = System.currentTimeMillis();
        }
        if (lastSuccessVersion < this.version) {
            this.lastSuccessVersion = this.version;
            this.lastSuccessVersionHash = this.versionHash;
        } else {
            this.lastSuccessVersion = lastSuccessVersion;
            this.lastSuccessVersionHash = lastSuccessVersionHash;
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
    public long getLastFailedVersion() {
        return lastFailedVersion;
    }
    
    public long getLastFailedVersionHash() {
        return lastFailedVersionHash;
    }
    
    public long getLastFailedTimestamp() {
        return lastFailedTimestamp;
    }
    
    public long getLastSuccessVersion() {
        return lastSuccessVersion;
    }
    
    public long getLastSuccessVersionHash() {
        return lastSuccessVersionHash;
    }

    public long getPathHash() {
        return pathHash;
    }

    public void setPathHash(long pathHash) {
        this.pathHash = pathHash;
    }

    // only update data size and row num
    public synchronized void updateStat(long dataSize, long rowNum) {
        this.dataSize = dataSize;
        this.rowCount = rowNum;
    }

    public synchronized void updateVersionInfo(long newVersion, long newVersionHash, long newDataSize, long newRowCount) {
        updateReplicaInfo(newVersion, newVersionHash, this.lastFailedVersion, this.lastFailedVersionHash, 
                this.lastSuccessVersion, this.lastSuccessVersionHash, newDataSize, newRowCount);
    }
    
    public synchronized void updateVersionInfo(long newVersion, long newVersionHash, 
            long lastFailedVersion, long lastFailedVersionHash, 
            long lastSuccessVersion, long lastSuccessVersionHash) {
        updateReplicaInfo(newVersion, newVersionHash, lastFailedVersion, lastFailedVersionHash, 
                lastSuccessVersion, lastSuccessVersionHash, dataSize, rowCount);
    }
    
    /* last failed version:  LFV
     * last success version: LSV
     * version:              V
     * 
     * Case 1:
     *      If LFV > LSV, set LSV back to V, which indicates that version between LSV and LFV is invalid.
     *      Clone task will clone the version between LSV and LFV
     *      
     * Case 2:
     *      LFV changed, set LSV back to V. This is just same as Case 1. Cause LFV must large than LSV.
     * 
     * Case 3:
     *      LFV remains unchanged, just update LSV, and then check if it falls into Case 1.
     *      
     * Case 4:
     *      V is larger or equal to LFV, reset LFV. And if V is less than LSV, just set V to LSV. This may
     *      happen when a clone task finished and report version V, but the LSV is already larger than V,
     *      And we know that version between V and LSV is valid, so move V forward to LSV.
     */
    private void updateReplicaInfo(long newVersion, long newVersionHash, 
            long lastFailedVersion, long lastFailedVersionHash, 
            long lastSuccessVersion, long lastSuccessVersionHash, 
            long newDataSize, long newRowCount) {
        if (newVersion < this.version) {
            LOG.warn("replica[" + id + "] new version is lower than meta version. " + newVersion + " vs " + version);
            // yiguolei: could not find any reason why new version less than this.version should run???
            return;
        }
        this.version = newVersion;
        this.versionHash = newVersionHash;
        this.dataSize = newDataSize;
        this.rowCount = newRowCount;
        // just check it
        if (lastSuccessVersion <= this.version) {
            lastSuccessVersion = this.version;
            lastSuccessVersionHash = this.versionHash;
        }

        // case 1:
        if (this.lastSuccessVersion <= this.lastFailedVersion) {
            this.lastSuccessVersion = this.version;
            this.lastSuccessVersionHash = this.versionHash;
        }
        
        // TODO: this case is unknown, add log to observe
        if (this.version > lastFailedVersion && lastFailedVersion > 0) {
            LOG.info("current version {} is larger than last failed version {} , " 
                        + "last failed version hash {}, maybe a fatal error or be report version, print a stack here ", 
                    this.version, lastFailedVersion, lastFailedVersionHash, new Exception());
        }
        
        if (lastFailedVersion != this.lastFailedVersion
                || this.lastFailedVersionHash != lastFailedVersionHash) {
            // Case 2:
            if (lastFailedVersion > this.lastFailedVersion) {
                this.lastFailedVersion = lastFailedVersion;
                this.lastFailedVersionHash = lastFailedVersionHash;
                this.lastFailedTimestamp = System.currentTimeMillis();
            }

            this.lastSuccessVersion = this.version;
            this.lastSuccessVersionHash = this.versionHash;
        } else {
            // Case 3:
            if (lastSuccessVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = lastSuccessVersion;
                this.lastSuccessVersionHash = lastSuccessVersionHash;
            }
            if (lastFailedVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = this.version;
                this.lastSuccessVersionHash = this.versionHash;
            }
        }
        
        // Case 4:
        if (this.version > this.lastFailedVersion 
                || this.version == this.lastFailedVersion && this.versionHash == this.lastFailedVersionHash
                || this.version == this.lastFailedVersion && this.lastFailedVersionHash == 0 && this.versionHash != 0) {
            this.lastFailedVersion = -1;
            this.lastFailedVersionHash = 0;
            this.lastFailedTimestamp = -1;
            if (this.version < this.lastSuccessVersion) {
                this.version = this.lastSuccessVersion;
                this.versionHash = this.lastSuccessVersionHash;
            }
        }

        LOG.debug("update {}", this.toString()); 
    }
    
    public synchronized void updateLastFailedVersion(long lastFailedVersion, long lastFailedVersionHash) {
        updateReplicaInfo(this.version, this.versionHash, lastFailedVersion, lastFailedVersionHash, 
                this.lastSuccessVersion, this.lastSuccessVersionHash, dataSize, rowCount);
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

    public long getVersionCount() {
		return versionCount.get();
	}

    public void setVersionCount(long versionCount) {
		this.versionCount.set(versionCount);
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
        strBuffer.append(", lastFailedVersion=");
        strBuffer.append(lastFailedVersion);
        strBuffer.append(", lastFailedVersionHash=");
        strBuffer.append(lastFailedVersionHash);
        strBuffer.append(", lastSuccessVersion=");
        strBuffer.append(lastSuccessVersion);
        strBuffer.append(", lastSuccessVersionHash=");
        strBuffer.append(lastSuccessVersionHash);
        strBuffer.append(", lastFailedTimestamp=");
        strBuffer.append(lastFailedTimestamp);
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
        
        out.writeLong(lastFailedVersion);
        out.writeLong(lastFailedVersionHash);
        out.writeLong(lastSuccessVersion);
        out.writeLong(lastSuccessVersionHash);
        
    }
     
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        backendId = in.readLong();
        version = in.readLong();
        versionHash = in.readLong();
        dataSize = in.readLong();
        rowCount = in.readLong();
        state = ReplicaState.valueOf(Text.readString(in));
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_45) {
            lastFailedVersion = in.readLong();
            lastFailedVersionHash = in.readLong();
            lastSuccessVersion = in.readLong();
            lastSuccessVersionHash = in.readLong();
        }
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
                && (state.equals(replica.state))
                && (lastFailedVersion == replica.lastFailedVersion)
                && (lastFailedVersionHash == replica.lastFailedVersionHash)
                && (lastSuccessVersion == replica.lastSuccessVersion)
                && (lastSuccessVersionHash == replica.lastSuccessVersionHash);
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
