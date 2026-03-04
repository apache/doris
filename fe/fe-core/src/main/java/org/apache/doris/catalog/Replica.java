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

import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;

/**
 * This class represents the olap replica related metadata.
 */
public abstract class Replica {
    private static final Logger LOG = LogManager.getLogger(Replica.class);
    public static final LastSuccessVersionComparator<Replica> LAST_SUCCESS_VERSION_COMPARATOR =
            new LastSuccessVersionComparator<Replica>();
    public static final IdComparator<Replica> ID_COMPARATOR = new IdComparator<Replica>();

    public enum ReplicaState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE,
        CLONE,
        ALTER, // replica is under rollup or schema change
        DECOMMISSION, // replica is ready to be deleted
        COMPACTION_TOO_SLOW; // replica version count is too large


        public boolean canLoad() {
            return this == NORMAL || this == SCHEMA_CHANGE || this == ALTER || this == COMPACTION_TOO_SLOW;
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
        BAD, // replica is broken.
        DROP,  // user force drop replica on this backend
    }

    public static class ReplicaContext {
        public long replicaId;
        public long backendId;
        public ReplicaState state;
        public long version;
        public int schemaHash;
        public long dbId;
        public long tableId;
        public long partitionId;
        public long indexId;
        public Replica originReplica;
    }

    @SerializedName(value = "id")
    private long id;
    // the version could be queried
    @SerializedName(value = "v", alternate = {"version"})
    protected volatile long version;
    private int schemaHash = -1;
    @SerializedName(value = "ds", alternate = {"dataSize"})
    private volatile long dataSize = 0;
    @SerializedName(value = "rc", alternate = {"rowCount"})
    private volatile long rowCount = 0;
    @SerializedName(value = "st", alternate = {"state"})
    private volatile ReplicaState state;

    @Setter
    @Getter
    @SerializedName(value = "lis", alternate = {"localInvertedIndexSize"})
    private long localInvertedIndexSize = 0L;
    @Setter
    @Getter
    @SerializedName(value = "lss", alternate = {"localSegmentSize"})
    private long localSegmentSize = 0L;

    public Replica() {
    }

    public Replica(ReplicaContext context) {
        this(context.replicaId, context.backendId, context.state, context.version, context.schemaHash);
    }

    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public Replica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        this(replicaId, backendId, -1, schemaHash, 0L, 0L, 0L, state, -1, -1);
    }

    // for create tablet and restore
    public Replica(long replicaId, long backendId, ReplicaState state, long version, int schemaHash) {
        this(replicaId, backendId, version, schemaHash, 0L, 0L, 0L, state, -1L, version);
    }

    public Replica(long replicaId, long backendId, long version, int schemaHash,
                       long dataSize, long remoteDataSize, long rowCount, ReplicaState state,
                       long lastFailedVersion,
                       long lastSuccessVersion) {
        this.id = replicaId;
        this.version = version;
        this.schemaHash = schemaHash;

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

    public long getBackendIdWithoutException() {
        try {
            return getBackendId();
        } catch (UserException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getBackendIdWithoutException: ", e);
            }
            return -1;
        }
    }

    public abstract long getBackendId() throws UserException;

    protected long getBackendIdValue() {
        return -1L;
    }

    public void setBackendId(long backendId) {
        if (backendId != -1) {
            throw new UnsupportedOperationException("setBackendId is not supported in Replica");
        }
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getRemoteDataSize() {
        return 0;
    }

    public void setRemoteDataSize(long remoteDataSize) {
        if (remoteDataSize > 0) {
            throw new UnsupportedOperationException("setRemoteDataSize is not supported in Replica");
        }
    }

    public long getRemoteInvertedIndexSize() {
        return 0L;
    }

    public void setRemoteInvertedIndexSize(long remoteInvertedIndexSize) {
        if (remoteInvertedIndexSize > 0) {
            throw new UnsupportedOperationException("setRemoteInvertedIndexSize is not supported in Replica");
        }
    }

    public long getRemoteSegmentSize() {
        return 0L;
    }

    public void setRemoteSegmentSize(long remoteSegmentSize) {
        if (remoteSegmentSize > 0) {
            throw new UnsupportedOperationException("setRemoteSegmentSize is not supported in Replica");
        }
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getSegmentCount() {
        return 0;
    }

    public void setSegmentCount(long segmentCount) {
        if (segmentCount > 0) {
            throw new UnsupportedOperationException("setSegmentCount is not supported in Replica");
        }
    }

    public long getRowsetCount() {
        return 0;
    }

    public void setRowsetCount(long rowsetCount) {
        if (rowsetCount > 0) {
            throw new UnsupportedOperationException("setRowsetCount is not supported in Replica");
        }
    }

    public long getLastFailedVersion() {
        return -1;
    }

    public long getLastFailedTimestamp() {
        return 0;
    }

    public long getLastSuccessVersion() {
        return 1;
    }

    public long getPathHash() {
        return -1;
    }

    public void setPathHash(long pathHash) {
        if (pathHash != -1) {
            throw new UnsupportedOperationException("setPathHash is not supported in Replica");
        }
    }

    public boolean isBad() {
        return false;
    }

    public boolean setBad(boolean bad) {
        if (bad) {
            throw new UnsupportedOperationException("setBad is not supported in Replica");
        }
        return false;
    }

    public TUniqueId getCooldownMetaId() {
        return null;
    }

    public void setCooldownMetaId(TUniqueId cooldownMetaId) {
        throw new UnsupportedOperationException("setCooldownMetaId is not supported in Replica");
    }

    public long getCooldownTerm() {
        return -1;
    }

    public void setCooldownTerm(long cooldownTerm) {
        throw new UnsupportedOperationException("setCooldownTerm is not supported in Replica");
    }

    public boolean needFurtherRepair() {
        return false;
    }

    public void setNeedFurtherRepair(boolean needFurtherRepair) {
        if (needFurtherRepair) {
            throw new UnsupportedOperationException("setNeedFurtherRepair is not supported in Replica");
        }
    }

    public void incrFurtherRepairCount() {
        throw new UnsupportedOperationException("incrFurtherRepairCount is not supported in Replica");
    }

    public int getLeftFurtherRepairCount() {
        return 0;
    }

    public long getFurtherRepairWatermarkTxnTd() {
        return -1;
    }

    public void setFurtherRepairWatermarkTxnTd(long furtherRepairWatermarkTxnTd) {
        if (furtherRepairWatermarkTxnTd != -1) {
            throw new UnsupportedOperationException("setFurtherRepairWatermarkTxnTd is not supported in Replica");
        }
    }

    public void updateWithReport(TTabletInfo backendReplica) {
        updateVersion(backendReplica.getVersion());
        setDataSize(backendReplica.getDataSize());
        setRemoteDataSize(backendReplica.getRemoteDataSize());
        setRowCount(backendReplica.getRowCount());
        setTotalVersionCount(backendReplica.getTotalVersionCount());
        setVisibleVersionCount(
                backendReplica.isSetVisibleVersionCount() ? backendReplica.getVisibleVersionCount()
                        : backendReplica.getTotalVersionCount());
    }

    public synchronized void updateVersion(long newVersion) {
        updateReplicaVersion(newVersion, getLastFailedVersion(), getLastSuccessVersion());
    }

    public synchronized void updateVersionWithFailed(
            long newVersion, long lastFailedVersion, long lastSuccessVersion) {
        updateReplicaVersion(newVersion, lastFailedVersion, lastSuccessVersion);
    }

    public synchronized void adminUpdateVersionInfo(Long version, Long lastFailedVersion, Long lastSuccessVersion,
            long updateTime) {
        throw new UnsupportedOperationException("adminUpdateVersionInfo is not supported in Replica");
    }

    protected void updateReplicaVersion(long newVersion, long lastFailedVersion, long lastSuccessVersion) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("before update: {}, newVersion: {}, lastFailedVersion: {}, lastSuccessVersion: {}",
                    this.toString(), newVersion, lastFailedVersion, lastSuccessVersion);
        }

        if (newVersion < this.version) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica {} on backend {}'s new version {} is lower than meta version {},"
                        + "not to continue to update replica", getId(), getBackendIdValue(), newVersion, this.version);
            }
            return;
        }

        this.version = newVersion;
    }

    public synchronized void updateLastFailedVersion(long lastFailedVersion) {
        updateReplicaVersion(this.version, lastFailedVersion, getLastSuccessVersion());
    }

    public void updateVersionForRestore(long version) {
        throw new UnsupportedOperationException("updateVersionForRestore is not supported in Replica");
    }

    /*
     * Check whether the replica's version catch up with the expected version.
     * If ignoreAlter is true, and state is ALTER, and replica's version is
     *  PARTITION_INIT_VERSION, just return true, ignore the version.
     *      This is for the case that when altering table,
     *      the newly created replica's version is PARTITION_INIT_VERSION,
     *      but we need to treat it as a "normal" replica which version is supposed to be "catch-up".
     *      But if state is ALTER but version larger than PARTITION_INIT_VERSION, which means this replica
     *      is already updated by load process, so we need to consider its version.
     */
    public abstract boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter);

    public void setState(ReplicaState replicaState) {
        this.state = replicaState;
    }

    public ReplicaState getState() {
        return this.state;
    }

    public boolean tooSlow() {
        return state == ReplicaState.COMPACTION_TOO_SLOW;
    }

    public boolean tooBigVersionCount() {
        return false;
    }

    public boolean isNormal() {
        return state == ReplicaState.NORMAL;
    }

    public long getTotalVersionCount() {
        return -1;
    }

    public void setTotalVersionCount(long totalVersionCount) {
        if (totalVersionCount > 0) {
            throw new UnsupportedOperationException("setTotalVersionCount is not supported in Replica");
        }
    }

    public long getVisibleVersionCount() {
        return -1;
    }

    public void setVisibleVersionCount(long visibleVersionCount) {
        if (visibleVersionCount > 0) {
            throw new UnsupportedOperationException("setVisibleVersionCount is not supported in Replica");
        }
    }

    public boolean checkVersionRegressive(long newVersion) {
        throw new UnsupportedOperationException("checkVersionRegressive is not supported in Replica");
    }

    @Override
    public String toString() {
        StringBuilder strBuffer = new StringBuilder("[replicaId=");
        strBuffer.append(id);
        strBuffer.append(", BackendId=");
        strBuffer.append(getBackendIdValue());
        strBuffer.append(", version=");
        strBuffer.append(version);
        strBuffer.append(", dataSize=");
        strBuffer.append(dataSize);
        strBuffer.append(", rowCount=");
        strBuffer.append(rowCount);
        strBuffer.append(", lastFailedVersion=");
        strBuffer.append(getLastFailedVersion());
        strBuffer.append(", lastSuccessVersion=");
        strBuffer.append(getLastSuccessVersion());
        strBuffer.append(", lastFailedTimestamp=");
        strBuffer.append(getLastFailedTimestamp());
        strBuffer.append(", schemaHash=");
        strBuffer.append(schemaHash);
        strBuffer.append(", state=");
        strBuffer.append(state.name());
        strBuffer.append(", isBad=");
        strBuffer.append(isBad());
        strBuffer.append("]");
        return strBuffer.toString();
    }

    public String toStringSimple(boolean checkBeAlive) {
        StringBuilder strBuffer = new StringBuilder("[replicaId=");
        strBuffer.append(id);
        strBuffer.append(", backendId=");
        strBuffer.append(getBackendIdValue());
        if (checkBeAlive) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(getBackendIdValue());
            if (backend == null) {
                strBuffer.append(", backend=null");
            } else {
                strBuffer.append(", backendAlive=");
                strBuffer.append(backend.isAlive());
                if (backend.isDecommissioned()) {
                    strBuffer.append(", backendDecommission=true");
                }
            }
        }
        strBuffer.append(", version=");
        strBuffer.append(version);
        if (getLastFailedVersion() > 0) {
            strBuffer.append(", lastFailedVersion=");
            strBuffer.append(getLastFailedVersion());
            strBuffer.append(", lastSuccessVersion=");
            strBuffer.append(getLastSuccessVersion());
            strBuffer.append(", lastFailedTimestamp=");
            strBuffer.append(getLastFailedTimestamp());
        }
        if (isBad()) {
            strBuffer.append(", isBad=true");
            Backend backend = Env.getCurrentSystemInfo().getBackend(getBackendIdValue());
            if (backend != null && getPathHash() != -1) {
                DiskInfo diskInfo = backend.getDisks().values().stream()
                        .filter(disk -> disk.getPathHash() == getPathHash())
                        .findFirst().orElse(null);
                if (diskInfo == null) {
                    strBuffer.append(", disk with path hash " + getPathHash() + " not exists");
                } else if (diskInfo.getState() == DiskInfo.DiskState.OFFLINE) {
                    strBuffer.append(", disk " + diskInfo.getRootPath() + " is bad");
                }
            }
        }
        strBuffer.append(", state=");
        strBuffer.append(state.name());
        strBuffer.append("]");

        return strBuffer.toString();
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
                && (version == replica.version)
                && (dataSize == replica.dataSize)
                && (rowCount == replica.rowCount)
                && (state.equals(replica.state));
    }

    private static class LastSuccessVersionComparator<T extends Replica> implements Comparator<T> {
        public LastSuccessVersionComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getLastSuccessVersion() < replica2.getLastSuccessVersion()) {
                return 1;
            } else if (replica1.getLastSuccessVersion() == replica2.getLastSuccessVersion()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    private static class IdComparator<T extends Replica> implements Comparator<T> {
        public IdComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getId() < replica2.getId()) {
                return -1;
            } else if (replica1.getId() == replica2.getId()) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public void setPreWatermarkTxnId(long preWatermarkTxnId) {
        if (preWatermarkTxnId != -1) {
            throw new UnsupportedOperationException("setPreWatermarkTxnId is not supported in Replica");
        }
    }

    public long getPreWatermarkTxnId() {
        return -1;
    }

    public void setPostWatermarkTxnId(long postWatermarkTxnId) {
        if (postWatermarkTxnId != -1) {
            throw new UnsupportedOperationException("setPostWatermarkTxnId is not supported in Replica");
        }
    }

    public long getPostWatermarkTxnId() {
        return -1;
    }

    public void setUserDropTime(long userDropTime) {
        if (userDropTime > 0) {
            throw new UnsupportedOperationException("setUserDropTime is not supported in Replica");
        }
    }

    public boolean isUserDrop() {
        return false;
    }

    public void setScaleInDropTimeStamp(long scaleInDropTime) {
        if (scaleInDropTime > 0) {
            throw new UnsupportedOperationException("setScaleInDropTimeStamp is not supported in Replica");
        }
    }

    public boolean isScaleInDrop() {
        return false;
    }

    public boolean isAlive() {
        return getState() != ReplicaState.CLONE
                && getState() != ReplicaState.DECOMMISSION
                && !isBad();
    }

    public boolean isScheduleAvailable() {
        return Env.getCurrentSystemInfo().checkBackendScheduleAvailable(getBackendIdValue())
                && !isUserDrop();
    }

    public void setLastReportVersion(long version) {
        if (version > 0) {
            throw new UnsupportedOperationException("setLastReportVersion is not supported in Replica");
        }
    }

    public long getLastReportVersion() {
        return 0;
    }
}
