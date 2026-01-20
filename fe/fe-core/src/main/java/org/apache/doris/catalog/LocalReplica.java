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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(LocalReplica.class);

    @SerializedName(value = "bid", alternate = {"backendId"})
    private long backendId;

    @SerializedName(value = "rds", alternate = {"remoteDataSize"})
    private volatile long remoteDataSize = 0;
    @SerializedName(value = "ris", alternate = {"remoteInvertedIndexSize"})
    private long remoteInvertedIndexSize = 0L;
    @SerializedName(value = "rss", alternate = {"remoteSegmentSize"})
    private long remoteSegmentSize = 0L;

    // the last load failed version
    @SerializedName(value = "lfv", alternate = {"lastFailedVersion"})
    private long lastFailedVersion = -1L;
    // not serialized, not very important
    private long lastFailedTimestamp = 0;
    // the last load successful version
    @SerializedName(value = "lsv", alternate = {"lastSuccessVersion"})
    private long lastSuccessVersion = -1L;

    private volatile long totalVersionCount = -1;
    private volatile long visibleVersionCount = -1;

    private long pathHash = -1;

    // bad means this Replica is unrecoverable, and we will delete it
    private boolean bad = false;

    // A replica version should increase monotonically,
    // but backend may missing some versions due to disk failure or bugs.
    // FE should found these and mark the replica as missing versions.
    // If backend's report version < fe version, record the backend's report version as `regressiveVersion`,
    // and if time exceed 5min, fe should mark this replica as missing versions.
    private long regressiveVersion = -1;
    private long regressiveVersionTimestamp = 0;

    /*
     * This can happen when this replica is created by a balance clone task, and
     * when task finished, the version of this replica is behind the partition's visible version.
     * So this replica need a further repair.
     * If we do not do this, this replica will be treated as version stale, and will be removed,
     * so that the balance task is failed, which is unexpected.
     *
     * furtherRepairSetTime and leftFurtherRepairCount are set alone with needFurtherRepair.
     * This is an insurance, in case that further repair task always fail. If 20 min passed
     * since we set needFurtherRepair to true, the 'needFurtherRepair' will be set to false.
     */
    private long furtherRepairSetTime = -1;
    private int leftFurtherRepairCount = 0;

    // During full clone, the replica's state is CLONE, it will not load the data.
    // After full clone finished, even if the replica's version = partition's visible version,
    //
    // notice: furtherRepairWatermarkTxnTd is used to clone a replica, protected it from be removed.
    //
    private long furtherRepairWatermarkTxnTd = -1;

    /* Decommission a backend B, steps are as follow:
     * 1. wait peer backends catchup with B;
     * 2. B change state to DECOMMISSION, set preWatermarkTxnId. B can load data now.
     * 3. wait txn before preWatermarkTxnId finished, set postWatermarkTxnId. B can't load data now.
     * 4. wait txn before postWatermarkTxnId finished, delete B.
     *
     * notice: preWatermarkTxnId and postWatermarkTxnId are used to delete this replica.
     *
     */
    private long preWatermarkTxnId = -1;
    private long postWatermarkTxnId = -1;

    private long userDropTime = -1;

    private long scaleInDropTime = -1;

    private long lastReportVersion = 0;

    private TUniqueId cooldownMetaId;
    private long cooldownTerm = -1;

    public LocalReplica() {
        super();
    }

    public LocalReplica(ReplicaContext context) {
        this(context.replicaId, context.backendId, context.state, context.version, context.schemaHash);
    }

    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public LocalReplica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        this(replicaId, backendId, -1, schemaHash, 0L, 0L, 0L, state, -1, -1);
    }

    // for create tablet and restore
    public LocalReplica(long replicaId, long backendId, ReplicaState state, long version, int schemaHash) {
        this(replicaId, backendId, version, schemaHash, 0L, 0L, 0L, state, -1L, version);
    }

    public LocalReplica(long replicaId, long backendId, long version, int schemaHash, long dataSize,
            long remoteDataSize, long rowCount, ReplicaState state, long lastFailedVersion, long lastSuccessVersion) {
        super(replicaId, backendId, version, schemaHash, dataSize, remoteDataSize, rowCount, state, lastFailedVersion,
                lastSuccessVersion);
        this.backendId = backendId;
        this.lastFailedVersion = lastFailedVersion;
        if (this.lastFailedVersion > 0) {
            this.lastFailedTimestamp = System.currentTimeMillis();
        }
        if (lastSuccessVersion < this.version) {
            this.lastSuccessVersion = this.version;
        } else {
            this.lastSuccessVersion = lastSuccessVersion;
        }
        this.remoteDataSize = remoteDataSize;
    }

    @Override
    public long getBackendId() {
        return this.backendId;
    }

    @Override
    protected long getBackendIdValue() {
        return this.backendId;
    }

    // just for ut
    @Override
    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    @Override
    public long getLastFailedVersion() {
        return lastFailedVersion;
    }

    @Override
    public long getLastFailedTimestamp() {
        return lastFailedTimestamp;
    }

    @Override
    public long getLastSuccessVersion() {
        return lastSuccessVersion;
    }

    @Override
    public long getPathHash() {
        return pathHash;
    }

    @Override
    public void setPathHash(long pathHash) {
        this.pathHash = pathHash;
    }

    @Override
    public boolean isBad() {
        return bad;
    }

    @Override
    public boolean setBad(boolean bad) {
        if (this.bad == bad) {
            return false;
        }
        this.bad = bad;
        return true;
    }

    @Override
    public long getRemoteDataSize() {
        return remoteDataSize;
    }

    @Override
    public void setRemoteDataSize(long remoteDataSize) {
        this.remoteDataSize = remoteDataSize;
    }

    @Override
    public long getRemoteInvertedIndexSize() {
        return remoteInvertedIndexSize;
    }

    @Override
    public void setRemoteInvertedIndexSize(long remoteInvertedIndexSize) {
        this.remoteInvertedIndexSize = remoteInvertedIndexSize;
    }

    @Override
    public long getRemoteSegmentSize() {
        return remoteSegmentSize;
    }

    @Override
    public void setRemoteSegmentSize(long remoteSegmentSize) {
        this.remoteSegmentSize = remoteSegmentSize;
    }

    @Override
    public TUniqueId getCooldownMetaId() {
        return cooldownMetaId;
    }

    @Override
    public void setCooldownMetaId(TUniqueId cooldownMetaId) {
        this.cooldownMetaId = cooldownMetaId;
    }

    @Override
    public long getCooldownTerm() {
        return cooldownTerm;
    }

    @Override
    public void setCooldownTerm(long cooldownTerm) {
        this.cooldownTerm = cooldownTerm;
    }

    @Override
    public boolean needFurtherRepair() {
        return leftFurtherRepairCount > 0
                && System.currentTimeMillis() < furtherRepairSetTime
                + Config.tablet_further_repair_timeout_second * 1000;
    }

    @Override
    public void setNeedFurtherRepair(boolean needFurtherRepair) {
        if (needFurtherRepair) {
            furtherRepairSetTime = System.currentTimeMillis();
            leftFurtherRepairCount = Config.tablet_further_repair_max_times;
        } else {
            leftFurtherRepairCount = 0;
            furtherRepairSetTime = -1;
        }
    }

    @Override
    public void incrFurtherRepairCount() {
        leftFurtherRepairCount--;
    }

    @Override
    public int getLeftFurtherRepairCount() {
        return leftFurtherRepairCount;
    }

    @Override
    public long getFurtherRepairWatermarkTxnTd() {
        return furtherRepairWatermarkTxnTd;
    }

    @Override
    public void setFurtherRepairWatermarkTxnTd(long furtherRepairWatermarkTxnTd) {
        this.furtherRepairWatermarkTxnTd = furtherRepairWatermarkTxnTd;
    }

    @Override
    public synchronized void adminUpdateVersionInfo(Long version, Long lastFailedVersion, Long lastSuccessVersion,
            long updateTime) {
        long oldLastFailedVersion = this.lastFailedVersion;
        if (version != null) {
            this.version = version;
        }
        if (lastSuccessVersion != null) {
            this.lastSuccessVersion = lastSuccessVersion;
        }
        if (lastFailedVersion != null) {
            if (this.lastFailedVersion < lastFailedVersion) {
                this.lastFailedTimestamp = updateTime;
            }
            this.lastFailedVersion = lastFailedVersion;
        }
        if (this.lastFailedVersion < this.version) {
            this.lastFailedVersion = -1;
            this.lastFailedTimestamp = -1;
        }
        if (this.lastFailedVersion > 0
                && this.lastSuccessVersion > this.lastFailedVersion) {
            this.lastSuccessVersion = this.version;
        }
        if (this.lastSuccessVersion < this.version) {
            this.lastSuccessVersion = this.version;
        }
        if (oldLastFailedVersion < 0 && this.lastFailedVersion > 0) {
            LOG.info("change replica last failed version from '< 0' to '> 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        } else if (oldLastFailedVersion > 0 && this.lastFailedVersion < 0) {
            LOG.info("change replica last failed version from '> 0' to '< 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        }
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
    @Override
    protected void updateReplicaVersion(long newVersion, long lastFailedVersion, long lastSuccessVersion) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("before update: {}", this.toString());
        }

        if (newVersion < this.version) {
            // This case means that replica meta version has been updated by ReportHandler before
            // For example, the publish version daemon has already sent some publish version tasks
            // to one be to publish version 2, 3, 4, 5, 6, and the be finish all publish version tasks,
            // the be's replica version is 6 now, but publish version daemon need to wait
            // for other be to finish most of publish version tasks to update replica version in fe.
            // At the moment, the replica version in fe is 4, when ReportHandler sync tablet,
            // it find reported replica version in be is 6 and then set version to 6 for replica in fe.
            // And then publish version daemon try to finish txn, and use visible version(5)
            // to update replica. Finally, it find the newer version(5) is lower than replica version(6) in fe.
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica {} on backend {}'s new version {} is lower than meta version {},"
                        + "not to continue to update replica", getId(), getBackendIdValue(), newVersion, this.version);
            }
            return;
        }

        this.version = newVersion;

        long oldLastFailedVersion = this.lastFailedVersion;

        // just check it
        if (lastSuccessVersion <= this.version) {
            lastSuccessVersion = this.version;
        }

        // case 1:
        if (this.lastSuccessVersion <= this.lastFailedVersion) {
            this.lastSuccessVersion = this.version;
        }

        // TODO: this case is unknown, add log to observe
        if (this.version > lastFailedVersion && lastFailedVersion > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("current version {} is larger than last failed version {}, "
                                + "maybe a fatal error or be report version, print a stack here ",
                        this.version, lastFailedVersion, new Exception());
            }
        }

        if (lastFailedVersion != this.lastFailedVersion) {
            // Case 2:
            if (lastFailedVersion > this.lastFailedVersion || lastFailedVersion < 0) {
                this.lastFailedVersion = lastFailedVersion;
                this.lastFailedTimestamp = lastFailedVersion > 0 ? System.currentTimeMillis() : -1L;
            }

            this.lastSuccessVersion = this.version;
        } else {
            // Case 3:
            if (lastSuccessVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = lastSuccessVersion;
            }
            if (lastFailedVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = this.version;
            }
        }

        // Case 4:
        if (this.version >= this.lastFailedVersion) {
            this.lastFailedVersion = -1;
            this.lastFailedTimestamp = -1;
            if (this.version < this.lastSuccessVersion) {
                this.version = this.lastSuccessVersion;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("after update {}", this.toString());
        }

        if (oldLastFailedVersion < 0 && this.lastFailedVersion > 0) {
            LOG.info("change replica last failed version from '< 0' to '> 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        } else if (oldLastFailedVersion > 0 && this.lastFailedVersion < 0) {
            LOG.info("change replica last failed version from '> 0' to '< 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        }

    }

    /*
     * If a replica is overwritten by a restore job, we need to reset version and lastSuccessVersion to
     * the restored replica version
     */
    @Override
    public void updateVersionForRestore(long version) {
        this.version = version;
        this.lastSuccessVersion = version;
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
    @Override
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        if (ignoreAlter && getState() == ReplicaState.ALTER && version == Partition.PARTITION_INIT_VERSION) {
            return true;
        }

        if (expectedVersion == Partition.PARTITION_INIT_VERSION) {
            // no data is loaded into this replica, just return true
            return true;
        }

        if (this.version < expectedVersion) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica version does not catch up with version: {}. replica: {}",
                        expectedVersion, this);
            }
            return false;
        }
        return true;
    }

    @Override
    public boolean tooBigVersionCount() {
        return visibleVersionCount >= Config.min_version_count_indicate_replica_compaction_too_slow;
    }

    @Override
    public long getTotalVersionCount() {
        return totalVersionCount;
    }

    @Override
    public void setTotalVersionCount(long totalVersionCount) {
        this.totalVersionCount = totalVersionCount;
    }

    @Override
    public long getVisibleVersionCount() {
        return visibleVersionCount;
    }

    @Override
    public void setVisibleVersionCount(long visibleVersionCount) {
        this.visibleVersionCount = visibleVersionCount;
    }

    @Override
    public boolean checkVersionRegressive(long newVersion) {
        if (newVersion >= version) {
            regressiveVersion = -1;
            regressiveVersionTimestamp = -1;
            return false;
        }

        if (DebugPointUtil.isEnable("Replica.regressive_version_immediately")) {
            return true;
        }

        if (newVersion != regressiveVersion) {
            regressiveVersion = newVersion;
            regressiveVersionTimestamp = System.currentTimeMillis();
        }

        return System.currentTimeMillis() - regressiveVersionTimestamp >= 5 * 60 * 1000L;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof LocalReplica)) {
            return false;
        }
        LocalReplica replica = (LocalReplica) obj;
        return (backendId == replica.backendId)
                && (lastFailedVersion == replica.lastFailedVersion)
                && (lastSuccessVersion == replica.lastSuccessVersion);
    }

    @Override
    public void setPreWatermarkTxnId(long preWatermarkTxnId) {
        this.preWatermarkTxnId = preWatermarkTxnId;
    }

    @Override
    public long getPreWatermarkTxnId() {
        return preWatermarkTxnId;
    }

    @Override
    public void setPostWatermarkTxnId(long postWatermarkTxnId) {
        this.postWatermarkTxnId = postWatermarkTxnId;
    }

    @Override
    public long getPostWatermarkTxnId() {
        return postWatermarkTxnId;
    }

    @Override
    public void setUserDropTime(long userDropTime) {
        this.userDropTime = userDropTime;
    }

    @Override
    public boolean isUserDrop() {
        if (userDropTime > 0) {
            if (System.currentTimeMillis() - userDropTime < Config.manual_drop_replica_valid_second * 1000L) {
                return true;
            }
            userDropTime = -1;
        }

        return false;
    }

    @Override
    public void setScaleInDropTimeStamp(long scaleInDropTime) {
        this.scaleInDropTime = scaleInDropTime;
    }

    @Override
    public boolean isScaleInDrop() {
        if (this.scaleInDropTime > 0) {
            if (System.currentTimeMillis() - this.scaleInDropTime
                    < Config.manual_drop_replica_valid_second * 1000L) {
                return true;
            }
            this.scaleInDropTime = -1;
        }
        return false;
    }

    @Override
    public void setLastReportVersion(long version) {
        this.lastReportVersion = version;
    }

    @Override
    public long getLastReportVersion() {
        return lastReportVersion;
    }
}
