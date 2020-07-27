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

import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE,
        CLONE,
        ALTER, // replica is under rollup or schema change
        DECOMMISSION; // replica is ready to be deleted

        public boolean canLoad() {
            return this == NORMAL || this == SCHEMA_CHANGE || this == ALTER;
        }

        public boolean canQuery() {
            return this == NORMAL || this == SCHEMA_CHANGE;
        }
    }
    
    public enum ReplicaStatus {
        OK, // health
        DEAD, // backend is not available
        VERSION_ERROR, // missing version
        MISSING, // replica does not exist
        SCHEMA_ERROR, // replica's schema hash does not equal to index's schema hash
        BAD // replica is broken.
    }
    
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "backendId")
    private long backendId;
    // the version could be queried
    @SerializedName(value = "version")
    private volatile long version;
    @SerializedName(value = "versionHash")
    private long versionHash;
    private int schemaHash = -1;
    @SerializedName(value = "dataSize")
    private volatile long dataSize = 0;
    @SerializedName(value = "rowCount")
    private volatile long rowCount = 0;
    @SerializedName(value = "state")
    private volatile ReplicaState state;

    // the last load failed version
    @SerializedName(value = "lastFailedVersion")
    private long lastFailedVersion = -1L;
    @SerializedName(value = "lastFailedVersionHash")
    private long lastFailedVersionHash = 0L;
    // not serialized, not very important
    private long lastFailedTimestamp = 0;
    // the last load successful version
    @SerializedName(value = "lastSuccessVersion")
    private long lastSuccessVersion = -1L;
    @SerializedName(value = "lastSuccessVersionHash")
    private long lastSuccessVersionHash = 0L;

	private volatile long versionCount = -1;

    private long pathHash = -1;

    // bad means this Replica is unrecoverable and we will delete it
    private boolean bad = false;

    /*
     * If set to true, with means this replica need to be repaired. explicitly.
     * This can happen when this replica is created by a balance clone task, and
     * when task finished, the version of this replica is behind the partition's visible version.
     * So this replica need a further repair.
     * If we do not do this, this replica will be treated as version stale, and will be removed,
     * so that the balance task is failed, which is unexpected.
     * 
     * furtherRepairSetTime set alone with needFurtherRepair.
     * This is an insurance, in case that further repair task always fail. If 20 min passed
     * since we set needFurtherRepair to true, the 'needFurtherRepair' will be set to false.
     */
    private boolean needFurtherRepair = false;
    private long furtherRepairSetTime = -1;
    private static final long FURTHER_REPAIR_TIMEOUT_MS = 20 * 60 * 1000L; // 20min

    // if this watermarkTxnId is set, which means before deleting a replica,
    // we should ensure that all txns on this replicas are finished.
    private long watermarkTxnId = -1;

    public Replica() {
    }
    
    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public Replica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        this(replicaId, backendId, -1, 0, schemaHash, 0L, 0L, state, -1, 0, -1, 0);
    }
    
    // for create tablet and restore
    public Replica(long replicaId, long backendId, ReplicaState state, long version, long versionHash, int schemaHash) {
        this(replicaId, backendId, version, versionHash, schemaHash, 0L, 0L, state, -1L, 0L, version, versionHash);
    }

    public Replica(long replicaId, long backendId, long version, long versionHash, int schemaHash,
                       long dataSize, long rowCount, ReplicaState state, 
                       long lastFailedVersion, long lastFailedVersionHash,
                       long lastSuccessVersion, long lastSuccessVersionHash) {
        this.id = replicaId;
        this.backendId = backendId;
        this.version = version;
        this.versionHash = versionHash;
        this.schemaHash = schemaHash;

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

    public int getSchemaHash() {
        return schemaHash;
    }

    // for compatibility
    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
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

    public boolean isBad() {
        return bad;
    }

    public boolean setBad(boolean bad) {
        if (this.bad == bad) {
            return false;
        }
        this.bad = bad;
        return true;
    }

    public boolean needFurtherRepair() {
        if (needFurtherRepair && System.currentTimeMillis() - this.furtherRepairSetTime < FURTHER_REPAIR_TIMEOUT_MS) {
            return true;
        }
        return false;
    }

    public void setNeedFurtherRepair(boolean needFurtherRepair) {
        this.needFurtherRepair = needFurtherRepair;
        this.furtherRepairSetTime = System.currentTimeMillis();
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
    
    public void updateVersionInfoForRecovery(
            long newVersion, long newVersionHash,
            long lastFailedVersion, long lastFailedVersionHash,
            long lastSuccessVersion, long lastSuccessVersionHash) {

        LOG.warn("update replica {} on backend {}'s version for recovery. version: {}-{}:{}-{}."
                + " last failed version: {}-{}:{}-{}, last success version: {}-{}:{}-{}",
                this.id, this.backendId, this.version, this.versionHash, newVersion, newVersionHash,
                this.lastFailedVersion, this.lastFailedVersionHash, lastFailedVersion, lastFailedVersionHash,
                this.lastSuccessVersion, this.lastSuccessVersionHash, lastSuccessVersion, lastSuccessVersionHash);

        this.version = newVersion;
        this.versionHash = newVersionHash;
        this.lastFailedVersion = lastFailedVersion;
        this.lastFailedVersionHash = lastFailedVersionHash;
        this.lastSuccessVersion = lastSuccessVersion;
        this.lastSuccessVersionHash = lastSuccessVersionHash;
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
     *    
     * Case 5:
     *      This is a bug case, I don't know why, may be some previous version introduce it. It looks like
     *      the V(hash) equals to LSV(hash), and V equals to LFV, but LFV hash is 0 or some unknown number.
     *      We just reset the LFV(hash) to recovery this replica. 
     */
    private void updateReplicaInfo(long newVersion, long newVersionHash, 
            long lastFailedVersion, long lastFailedVersionHash, 
            long lastSuccessVersion, long lastSuccessVersionHash, 
            long newDataSize, long newRowCount) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("before update: {}", this.toString());
        }

        if (newVersion < this.version) {
            // This case means that replica meta version has been updated by ReportHandler before
            // For example, the publish version daemon has already sent some publish verison tasks to one be to publish version 2, 3, 4, 5, 6,
            // and the be finish all publish version tasks, the be's replica version is 6 now, but publish version daemon need to wait
            // for other be to finish most of publish version tasks to update replica version in fe.
            // At the moment, the replica version in fe is 4, when ReportHandler sync tablet, it find reported replica version in be is 6 and then
            // set version to 6 for replica in fe. And then publish version daemon try to finish txn, and use visible version(5)
            // to update replica. Finally, it find the newer version(5) is lower than replica version(6) in fe.
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica {} on backend {}'s new version {} is lower than meta version {},"
                        + "not to continue to update replica", id, backendId, newVersion, this.version);
            }
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
            LOG.debug("current version {} is larger than last failed version {}, "
                        + "last failed version hash {}, maybe a fatal error or be report version, print a stack here ", 
                    this.version, lastFailedVersion, lastFailedVersionHash, new Exception());
        }
        
        if (lastFailedVersion != this.lastFailedVersion) {
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
        if (this.version >= this.lastFailedVersion) {
            this.lastFailedVersion = -1;
            this.lastFailedVersionHash = 0;
            this.lastFailedTimestamp = -1;
            if (this.version < this.lastSuccessVersion) {
                this.version = this.lastSuccessVersion;
                this.versionHash = this.lastSuccessVersionHash;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("after update {}", this.toString());
        }
    }
    
    public synchronized void updateLastFailedVersion(long lastFailedVersion, long lastFailedVersionHash) {
        updateReplicaInfo(this.version, this.versionHash, lastFailedVersion, lastFailedVersionHash, 
                this.lastSuccessVersion, this.lastSuccessVersionHash, dataSize, rowCount);
    }

    /*
     * Check whether the replica's version catch up with the expected version.
     * If ignoreAlter is true, and state is ALTER, and replica's version is PARTITION_INIT_VERSION, just return true, ignore the version.
     *      This is for the case that when altering table, the newly created replica's version is PARTITION_INIT_VERSION,
     *      but we need to treat it as a "normal" replica which version is supposed to be "catch-up".
     *      But if state is ALTER but version larger than PARTITION_INIT_VERSION, which means this replica
     *      is already updated by load process, so we need to consider its version.
     */
    public boolean checkVersionCatchUp(long expectedVersion, long expectedVersionHash, boolean ignoreAlter) {
        if (ignoreAlter && state == ReplicaState.ALTER && version == Partition.PARTITION_INIT_VERSION
                && versionHash == Partition.PARTITION_INIT_VERSION_HASH) {
            return true;
        }
        
        if (expectedVersion == Partition.PARTITION_INIT_VERSION
                && expectedVersionHash == Partition.PARTITION_INIT_VERSION_HASH) {
            // no data is loaded into this replica, just return true
            return true;
        }

        if (this.version < expectedVersion) {
            LOG.debug("replica version does not catch up with version: {}-{}. replica: {}",
                      expectedVersion, expectedVersionHash, this);
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
		return versionCount;
	}

    public void setVersionCount(long versionCount) {
		this.versionCount = versionCount;
	}
    
    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer("[replicaId=");
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
        strBuffer.append(", schemaHash=");
        strBuffer.append(schemaHash);
        strBuffer.append(", state=");
        strBuffer.append(state.name());
        strBuffer.append("]");
        return strBuffer.toString();
    }

    @Override
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
    
    @Override
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

    public void setWatermarkTxnId(long watermarkTxnId) {
        this.watermarkTxnId = watermarkTxnId;
    }

    public long getWatermarkTxnId() {
        return watermarkTxnId;
    }
}
