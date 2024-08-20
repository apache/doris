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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.SchedException.SubCode;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * TabletSchedCtx contains all information which is created during tablet scheduler processing.
 */
public class TabletSchedCtx implements Comparable<TabletSchedCtx> {
    private static final Logger LOG = LogManager.getLogger(TabletSchedCtx.class);

    /*
     * SCHED_FAILED_COUNTER_THRESHOLD:
     *    threshold of times a tablet failed to be scheduled
     *
     * MIN_ADJUST_PRIORITY_INTERVAL_MS:
     *    min interval time of adjusting a tablet's priority
     *
     * MAX_NOT_BEING_SCHEDULED_INTERVAL_MS:
     *    max gap time of a tablet NOT being scheduled.
     *
     * These 3 params is for adjusting priority.
     * If a tablet being scheduled failed for more than SCHED_FAILED_COUNTER_THRESHOLD times, its priority
     * will be downgraded. And the interval between adjustment is larger than MIN_ADJUST_PRIORITY_INTERVAL_MS,
     * to avoid being downgraded too soon.
     * And if a tablet is not being scheduled longer than MAX_NOT_BEING_SCHEDULED_INTERVAL_MS, its priority
     * will be upgraded, to avoid starvation.
     *
     */
    private static final int SCHED_FAILED_COUNTER_THRESHOLD = 5;
    private static final long MIN_ADJUST_PRIORITY_INTERVAL_MS = 5 * 60 * 1000L; // 5 min
    private static final long MAX_NOT_BEING_SCHEDULED_INTERVAL_MS = 30 * 60 * 1000L; // 30 min

    /*
     * A clone task timeout is between Config.min_clone_task_timeout_sec and Config.max_clone_task_timeout_sec,
     * estimated by tablet size / MIN_CLONE_SPEED_MB_PER_SECOND.
     * We set a relatively small default value, so that the calculated timeout will be larger
     * to ensure that the clone task can be completed even in the case of poor network environment.
     */
    private static final long MIN_CLONE_SPEED_MB_PER_SECOND = 1; // 1MB/sec

    /*
     * If a clone task is failed to run more than RUNNING_FAILED_COUNTER_THRESHOLD, it will be removed
     * from the tablet scheduler.
     */
    private static final int RUNNING_FAILED_COUNTER_THRESHOLD = 3;

    public static final int FINISHED_COUNTER_THRESHOLD = 4;

    private static VersionCountComparator VERSION_COUNTER_COMPARATOR = new VersionCountComparator();

    public enum Type {
        BALANCE, REPAIR
    }

    public enum BalanceType {
        BE_BALANCE, DISK_BALANCE
    }

    public enum Priority {
        LOW,
        NORMAL,
        HIGH,
        VERY_HIGH;
    }

    public enum State {
        PENDING, // tablet is not being scheduled
        RUNNING, // tablet is being scheduled
        FINISHED, // task is finished
        CANCELLED, // task is failed
        UNEXPECTED // other unexpected errors
    }

    private Type type;
    private BalanceType balanceType;

    // we change the dynamic priority based on how many times it fails to be scheduled
    private int failedSchedCounter = 0;
    // clone task failed counter
    private int failedRunningCounter = 0;
    // When finish a tablet ctx, it will check the tablet's health status.
    // If the tablet is unhealthy, it will add a new ctx.
    // The new ctx's finishedCounter = old ctx's finishedCounter + 1.
    private int finishedCounter = 0;

    // last time this tablet being scheduled
    private long lastSchedTime = 0;

    // last time this tablet being visited.
    // being visited means:
    // 1. being visited in TabletScheduler.schedulePendingTablets()
    // 2. being visited in finishCloneTask()
    //
    // This time is used to observer when this tablet being visited, for debug tracing.
    // It does not same as 'lastSchedTime', which is used for adjusting priority.
    private long lastVisitedTime = -1;

    // an approximate timeout of this task, only be set when sending clone task.
    private long taskTimeoutMs = 0;

    private State state;
    private TabletHealth tabletHealth;

    private long decommissionTime = -1;

    private long dbId;
    private long tblId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private int schemaHash;
    private TStorageMedium storageMedium;

    private long createTime = -1;
    private long finishedTime = -1;

    private Tablet tablet = null;
    private long visibleVersion = -1;
    private long committedVersion = -1;

    private long tabletSize = 0;

    private Replica srcReplica = null;
    private long srcPathHash = -1;
    // for disk balance to keep src path, and avoid take slot on selectAlternativeTablets
    private Replica tempSrcReplica = null;
    private long destBackendId = -1;
    private long destPathHash = -1;
    private long destOldVersion = -1;
    // for disk balance to set migration task's datadir
    private String destPath = null;
    private String errMsg = null;

    private CloneTask cloneTask = null;
    private StorageMediaMigrationTask storageMediaMigrationTask = null;

    // statistics gathered from clone task report
    // the total size of clone files and the total cost time in ms.
    private long copySize = 0;
    private long copyTimeMs = 0;

    private Set<Long> colocateBackendsSet = null;
    private int tabletOrderIdx = -1;

    private SystemInfoService infoService;

    // replicaAlloc is only set for REPAIR task
    private ReplicaAllocation replicaAlloc;
    // tag is only set for BALANCE task, used to identify which workload group this Balance job is in
    private Tag tag;

    private SubCode schedFailedCode;

    private boolean isUniqKeyMergeOnWrite = false;

    public TabletSchedCtx(Type type, long dbId, long tblId, long partId,
            long idxId, long tabletId, ReplicaAllocation replicaAlloc, long createTime) {
        this.type = type;
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partId;
        this.indexId = idxId;
        this.tabletId = tabletId;
        this.createTime = createTime;
        this.infoService = Env.getCurrentSystemInfo();
        this.state = State.PENDING;
        this.replicaAlloc = replicaAlloc;
        this.balanceType = BalanceType.BE_BALANCE;
        this.schedFailedCode = SubCode.NONE;
        this.tabletHealth = new TabletHealth();
    }

    public ReplicaAllocation getReplicaAlloc() {
        return replicaAlloc;
    }

    public void setReplicaAlloc(ReplicaAllocation replicaAlloc) {
        this.replicaAlloc = replicaAlloc;
    }

    public void setTag(Tag tag) {
        this.tag = tag;
    }

    public Tag getTag() {
        return tag;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public void setBalanceType(BalanceType type) {
        this.balanceType = type;
    }

    public BalanceType getBalanceType() {
        return balanceType;
    }

    public Priority getPriority() {
        return tabletHealth.priority;
    }

    public void setPriority(Priority priority) {
        this.tabletHealth.priority = priority;
    }

    public void setTabletHealth(TabletHealth tabletHealth) {
        this.tabletHealth = tabletHealth;
    }

    public void setIsUniqKeyMergeOnWrite(boolean isUniqKeyMergeOnWrite) {
        this.isUniqKeyMergeOnWrite = isUniqKeyMergeOnWrite;
    }

    public int getFinishedCounter() {
        return finishedCounter;
    }

    public void setFinishedCounter(int finishedCounter) {
        this.finishedCounter = finishedCounter;
    }

    public void increaseFailedSchedCounter() {
        ++failedSchedCounter;
    }

    public int getFailedSchedCounter() {
        return failedSchedCounter;
    }

    public void resetFailedSchedCounter() {
        failedSchedCounter = 0;
    }

    public void increaseFailedRunningCounter() {
        ++failedRunningCounter;
    }

    public boolean isExceedFailedRunningLimit() {
        return failedRunningCounter >= RUNNING_FAILED_COUNTER_THRESHOLD;
    }

    public boolean onSchedFailedAndCheckExceedLimit(SubCode code) {
        schedFailedCode = code;
        failedSchedCounter++;
        if (code == SubCode.WAITING_DECOMMISSION) {
            failedSchedCounter = 0;
            if (decommissionTime < 0) {
                decommissionTime = System.currentTimeMillis();
            }
            return System.currentTimeMillis() > decommissionTime + Config.decommission_tablet_wait_time_seconds * 1000L;
        } else {
            decommissionTime = -1;
            if (code == SubCode.WAITING_SLOT && type != Type.BALANCE) {
                return failedSchedCounter > 30 * 1000 / Config.tablet_schedule_interval_ms;
            } else {
                return failedSchedCounter > 10;
            }
        }
    }

    public void setLastSchedTime(long lastSchedTime) {
        this.lastSchedTime = lastSchedTime;
    }

    public void setLastVisitedTime(long lastVisitedTime) {
        this.lastVisitedTime = lastVisitedTime;
    }

    public long getLastVisitedTime() {
        return lastVisitedTime;
    }

    public void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public void setDecommissionTime(long decommissionTime) {
        this.decommissionTime = decommissionTime;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void setTabletStatus(TabletStatus tabletStatus) {
        this.tabletHealth.status = tabletStatus;
    }

    public TabletStatus getTabletStatus() {
        return tabletHealth.status;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
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

    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getCommittedVersion() {
        return visibleVersion;
    }

    public void setTablet(Tablet tablet) {
        this.tablet = tablet;
    }

    public Tablet getTablet() {
        return tablet;
    }

    // database lock should be held.
    public List<Replica> getReplicas() {
        return tablet.getReplicas();
    }

    public void setVersionInfo(long visibleVersion, long committedVersion) {
        this.visibleVersion = visibleVersion;
        this.committedVersion = committedVersion;
    }

    public void setDest(Long destBeId, long destPathHash) {
        this.destBackendId = destBeId;
        this.destPathHash = destPathHash;
    }

    public void setDest(Long destBeId, long destPathHash, String destPath) {
        setDest(destBeId, destPathHash);
        this.destPath = destPath;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public SubCode getSchedFailedCode() {
        return schedFailedCode;
    }

    public void setSchedFailedCode(SubCode code) {
        schedFailedCode = code;
    }

    public CloneTask getCloneTask() {
        return cloneTask;
    }

    public long getCopySize() {
        return copySize;
    }

    public long getCopyTimeMs() {
        return copyTimeMs;
    }

    public long getSrcBackendId() {
        if (srcReplica != null) {
            return srcReplica.getBackendId();
        } else {
            return -1;
        }
    }

    public long getSrcPathHash() {
        return srcPathHash;
    }

    public void setSrc(Replica srcReplica) {
        this.srcReplica = srcReplica;
        this.srcPathHash = srcReplica.getPathHash();
    }

    public void setTempSrc(Replica srcReplica) {
        this.tempSrcReplica = srcReplica;
    }

    public long getTempSrcBackendId() {
        if (tempSrcReplica != null) {
            return tempSrcReplica.getBackendId();
        }
        return -1;
    }

    public long getTempSrcPathHash() {
        if (tempSrcReplica != null) {
            return tempSrcReplica.getPathHash();
        }
        return -1;
    }

    public long getDestBackendId() {
        return destBackendId;
    }

    public long getDestPathHash() {
        return destPathHash;
    }

    public String getDestPath() {
        return destPath;
    }

    // database lock should be held.
    public long getTabletSize() {
        return tabletSize;
    }

    public void updateTabletSize() {
        tabletSize = 0;
        tablet.getReplicas().stream().forEach(
                replica -> tabletSize = Math.max(tabletSize, replica.getDataSize()));
    }

    /*
     * check if existing replicas are on same BE or Host.
     * database lock should be held.
     */
    public boolean filterDestBE(long beId) {
        Backend backend = infoService.getBackend(beId);
        if (backend == null) {
            // containsBE() is currently only used for choosing dest backend to do clone task.
            // return true so that it won't choose this backend.
            if (LOG.isDebugEnabled()) {
                LOG.debug("desc backend {} does not exist, skip. tablet: {}", beId, tabletId);
            }
            return true;
        }
        String host = backend.getHost();
        for (Replica replica : tablet.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // BE has been dropped, skip it
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} does not exist, skip. tablet: {}",
                            replica.getBackendId(), tabletId);
                }
                continue;
            }
            if (!Config.allow_replica_on_same_host && !FeConstants.runningUnitTest && host.equals(be.getHost())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} is on same host {}, skip. tablet: {}",
                            replica.getBackendId(), host, tabletId);
                }
                return true;
            }

            if (replica.getBackendId() == beId) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} is same as dest backend {}, skip. tablet: {}",
                            replica.getBackendId(), beId, tabletId);
                }
                return true;
            }
        }
        return false;
    }

    public void setColocateGroupBackendIds(Set<Long> backendsSet) {
        this.colocateBackendsSet = backendsSet;
    }

    public Set<Long> getColocateBackendsSet() {
        return colocateBackendsSet;
    }

    public void setTabletOrderIdx(int idx) {
        this.tabletOrderIdx = idx;
    }

    public int getTabletOrderIdx() {
        return tabletOrderIdx;
    }

    public boolean compactionRecovered() {
        Replica chosenReplica = null;
        long maxVersionCount = Integer.MIN_VALUE;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getVisibleVersionCount() > maxVersionCount) {
                maxVersionCount = replica.getVisibleVersionCount();
                chosenReplica = replica;
            }
        }
        boolean recovered = false;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.isAlive() && replica.tooSlow() && (!replica.equals(chosenReplica)
                    || !replica.tooBigVersionCount())) {
                if (chosenReplica != null) {
                    chosenReplica.setState(ReplicaState.NORMAL);
                    recovered = true;
                }
            }
        }
        return recovered;
    }

    // table lock should be held.
    // If exceptBeId != -1, should not choose src replica with same BE id as exceptBeId
    public void chooseSrcReplica(Map<Long, PathSlot> backendsWorkingSlots, long exceptBeId) throws SchedException {
        /*
         * get all candidate source replicas
         * 1. source replica should be healthy.
         * 2. slot of this source replica is available.
         */
        List<Replica> candidates = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            if (exceptBeId != -1 && replica.getBackendId() == exceptBeId) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} is same as except backend {}, skip. tablet: {}",
                            replica.getBackendId(), exceptBeId, tabletId);
                }
                continue;
            }

            if (replica.isBad() || replica.tooSlow()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica {} is bad({}) or too slow({}), skip. tablet: {}",
                            replica.getId(), replica.isBad(), replica.tooSlow(), tabletId);
                }
                continue;
            }

            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAlive()) {
                // backend which is in decommission can still be the source backend
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} does not exist or is not alive, skip. tablet: {}",
                            replica.getBackendId(), tabletId);
                }
                continue;
            }

            if (replica.getLastFailedVersion() > 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica {} has failed version {}, skip. tablet: {}",
                            replica.getId(), replica.getLastFailedVersion(), tabletId);
                }
                continue;
            }

            if (!replica.checkVersionCatchUp(visibleVersion, false)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica {} version {} has not catch up to visible version {}, skip. tablet: {}",
                            replica.getId(), replica.getVersion(), visibleVersion, tabletId);
                }
                continue;
            }

            candidates.add(replica);
        }

        if (candidates.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to find copy source replica");
        }

        // choose a replica which slot is available from candidates.
        // sort replica by version count asc, so that we prefer to choose replicas with fewer versions
        Collections.sort(candidates, VERSION_COUNTER_COMPARATOR);
        for (Replica srcReplica : candidates) {
            PathSlot slot = backendsWorkingSlots.get(srcReplica.getBackendId());
            if (slot == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} does not have working slot, skip. tablet: {}",
                            srcReplica.getBackendId(), tabletId);
                }
                continue;
            }

            long srcPathHash = slot.takeSlot(srcReplica.getPathHash());
            if (srcPathHash == -1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica's backend {} does not have available slot, skip. tablet: {}",
                            srcReplica.getBackendId(), tabletId);
                }
                continue;
            }
            setSrc(srcReplica);
            return;
        }
        throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                "waiting for source replica's slot");
    }

    /*
     * Same rules as choosing source replica for supplement.
     * But we need to check that we can not choose the same replica as dest replica,
     * because replica info is changing all the time.
     */
    public void chooseSrcReplicaForVersionIncomplete(Map<Long, PathSlot> backendsWorkingSlots)
            throws SchedException {
        chooseSrcReplica(backendsWorkingSlots, destBackendId);
        Preconditions.checkState(srcReplica.getBackendId() != destBackendId,
                "wrong be id: " + destBackendId);
    }

    /*
     * Rules to choose a destination replica for version incomplete
     * 1. replica's last failed version > 0
     * 2. better to choose a replica which has a lower last failed version
     * 3. best to choose a replica if its last success version > last failed version
     * 4. if these is replica which need further repair, choose that replica.
     *
     * database lock should be held.
     */
    public void chooseDestReplicaForVersionIncomplete(Map<Long, PathSlot> backendsWorkingSlots)
            throws SchedException {
        List<Replica> decommissionCand = Lists.newArrayList();
        List<Replica> colocateCand = Lists.newArrayList();
        List<Replica> notColocateCand = Lists.newArrayList();
        List<Replica> furtherRepairs = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            if (replica.isBad()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica {} is bad, skip. tablet: {}",
                            replica.getId(), tabletId);
                }
                continue;
            }

            if (!replica.isScheduleAvailable()) {
                if (Env.getCurrentSystemInfo().checkBackendScheduleAvailable(replica.getBackendId())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("replica's backend {} does not exist or is not scheduler available, skip. tablet: {}",
                                replica.getBackendId(), tabletId);
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("user drop replica {}, skip. tablet: {}",
                                replica, tabletId);
                    }
                }
                continue;
            }

            // not enough version completed replicas, then try add back the decommission replica.
            if (replica.getState() == ReplicaState.DECOMMISSION) {
                decommissionCand.add(replica);
                continue;
            }

            if (replica.getLastFailedVersion() <= 0
                    && replica.getVersion() >= visibleVersion) {

                if (tabletHealth.status == TabletStatus.NEED_FURTHER_REPAIR && replica.needFurtherRepair()) {
                    furtherRepairs.add(replica);
                }

                // skip healthy replica
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica {} version {} is healthy, visible version {}, "
                                    + "replica state {}, skip. tablet: {}",
                            replica.getId(), replica.getVersion(), visibleVersion, replica.getState(), tabletId);
                }
                continue;
            }

            if (colocateBackendsSet != null && colocateBackendsSet.contains(replica.getBackendId())) {
                colocateCand.add(replica);
            } else {
                notColocateCand.add(replica);
            }
        }

        List<Replica> candidates = null;
        if (!colocateCand.isEmpty()) {
            candidates = colocateCand;
        } else if (!notColocateCand.isEmpty()) {
            candidates = notColocateCand;
        } else {
            candidates = decommissionCand;
        }

        if (candidates.isEmpty()) {
            if (furtherRepairs.isEmpty()) {
                throw new SchedException(Status.UNRECOVERABLE, "unable to choose copy dest replica");
            }

            boolean allCatchup = true;
            for (Replica replica : furtherRepairs) {
                if (checkFurtherRepairFinish(replica, visibleVersion)) {
                    replica.setNeedFurtherRepair(false);
                    replica.setFurtherRepairWatermarkTxnTd(-1);
                } else {
                    allCatchup = false;
                }
            }

            throw new SchedException(Status.FINISHED,
                    allCatchup ? "further repair all catchup" : "further repair waiting catchup");
        }

        Replica chosenReplica = null;
        for (Replica replica : candidates) {
            PathSlot slot = backendsWorkingSlots.get(replica.getBackendId());
            if (slot == null || !slot.hasAvailableSlot(replica.getPathHash())) {
                if (!replica.needFurtherRepair()) {
                    throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                            "dest replica " + replica + " has no slot");
                }

                continue;
            }

            if (replica.needFurtherRepair()) {
                chosenReplica = replica;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("replica {} need further repair, choose it. tablet: {}",
                            replica.getId(), tabletId);
                }
                break;
            }

            if (chosenReplica == null) {
                chosenReplica = replica;
            } else if (replica.getLastSuccessVersion() > replica.getLastFailedVersion()) {
                chosenReplica = replica;
                break;
            } else if (replica.getLastFailedVersion() < chosenReplica.getLastFailedVersion()) {
                // its better to select a low last failed version replica
                chosenReplica = replica;
            }
        }

        // check if the dest replica has available slot
        // it should not happen cause it just check hasAvailableSlot yet.
        PathSlot slot = backendsWorkingSlots.get(chosenReplica.getBackendId());
        if (slot == null) {
            throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                    "backend of dest replica is missing");
        }
        long destPathHash = slot.takeSlot(chosenReplica.getPathHash());
        if (destPathHash == -1) {
            throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                    "unable to take slot of dest path");
        }

        if (chosenReplica.getState() == ReplicaState.DECOMMISSION) {
            // Since this replica is selected as the repair object of VERSION_INCOMPLETE,
            // it means that this replica needs to be able to accept loading data.
            // So if this replica was previously set to DECOMMISSION, this state needs to be reset to NORMAL.
            // It may happen as follows:
            // 1. A tablet of colocation table is in COLOCATION_REDUNDANT state
            // 2. The tablet is being scheduled and set one of replica as
            //    DECOMMISSION in TabletScheduler.deleteReplicaInternal()
            // 3. The tablet will then be scheduled again
            // 4. But at that time, the BE node of the replica that was
            //    set to the DECOMMISSION state in step 2 is returned to the colocation group.
            //    So the tablet's health status becomes VERSION_INCOMPLETE.
            //
            // If we do not reset this replica state to NORMAL, the tablet's health status will be in VERSION_INCOMPLETE
            // forever, because the replica in the DECOMMISSION state will not receive the load task.
            chosenReplica.setPreWatermarkTxnId(-1);
            chosenReplica.setPostWatermarkTxnId(-1);
            chosenReplica.setState(ReplicaState.NORMAL);
            setDecommissionTime(-1);
            LOG.info("choose replica {} on backend {} of tablet {} as dest replica for version incomplete,"
                    + " and change state from DECOMMISSION to NORMAL",
                    chosenReplica.getId(), chosenReplica.getBackendId(), tabletId);
        }
        setDest(chosenReplica.getBackendId(), chosenReplica.getPathHash());
    }

    private boolean checkFurtherRepairFinish(Replica replica, long version) {
        if (replica.getVersion() < version || replica.getLastFailedVersion() > 0) {
            return false;
        }

        long furtherRepairWatermarkTxnTd = replica.getFurtherRepairWatermarkTxnTd();
        if (furtherRepairWatermarkTxnTd < 0) {
            return true;
        }

        try {
            if (Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(
                        furtherRepairWatermarkTxnTd, dbId, tblId, partitionId)) {
                LOG.info("replica {} of tablet {} has catchup with further repair watermark id {}",
                        replica, tabletId, furtherRepairWatermarkTxnTd);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            LOG.warn("replica {} of tablet {} check catchup with further repair watermark id {} failed",
                    replica, tabletId, furtherRepairWatermarkTxnTd, e);
            return true;
        }
    }

    public void releaseResource(TabletScheduler tabletScheduler) {
        releaseResource(tabletScheduler, false);
    }

    /*
     * release all resources before finishing this task.
     * if reserveTablet is true, the tablet object in this ctx will not be set to null after calling reset().
     */
    public void releaseResource(TabletScheduler tabletScheduler, boolean reserveTablet) {
        if (srcReplica != null) {
            Preconditions.checkState(srcPathHash != -1);
            PathSlot slot = tabletScheduler.getBackendsWorkingSlots().get(srcReplica.getBackendId());
            if (slot != null) {
                if (type == Type.REPAIR) {
                    slot.freeSlot(srcPathHash);
                } else {
                    slot.freeBalanceSlot(srcPathHash);
                }
            }
        }

        if (destPathHash != -1) {
            PathSlot slot = tabletScheduler.getBackendsWorkingSlots().get(destBackendId);
            if (slot != null) {
                if (type == Type.REPAIR) {
                    slot.freeSlot(destPathHash);
                } else {
                    slot.freeBalanceSlot(destPathHash);
                }
            }
        }

        if (storageMediaMigrationTask != null) {
            AgentTaskQueue.removeTask(storageMediaMigrationTask.getBackendId(),
                    TTaskType.STORAGE_MEDIUM_MIGRATE, storageMediaMigrationTask.getSignature());
        }
        if (cloneTask != null) {
            AgentTaskQueue.removeTask(cloneTask.getBackendId(), TTaskType.CLONE, cloneTask.getSignature());

            // clear all CLONE replicas
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db != null) {
                Table table = db.getTableNullable(tblId);
                if (table != null && table.writeLockIfExist()) {
                    try {
                        List<Replica> cloneReplicas = Lists.newArrayList();
                        tablet.getReplicas().stream().filter(r -> r.getState() == ReplicaState.CLONE).forEach(r -> {
                            cloneReplicas.add(r);
                        });

                        for (Replica cloneReplica : cloneReplicas) {
                            tablet.deleteReplica(cloneReplica);
                        }

                    } finally {
                        table.writeUnlock();
                    }
                }
            }
        }

        reset(reserveTablet);
    }

    // reset to save memory after state is done
    private void reset(boolean reserveTablet) {
        /*
         * If state is PENDING, these fields will be reset when being rescheduled.
         * if state is FINISHED/CANCELLED/TIMEOUT, leave these fields for show.
         */
        if (state == State.PENDING) {
            if (!reserveTablet) {
                this.tablet = null;
            }
            this.srcReplica = null;
            this.srcPathHash = -1;
            this.destBackendId = -1;
            this.destPathHash = -1;
            this.destPath = null;
            this.cloneTask = null;
            this.storageMediaMigrationTask = null;
        }
    }

    public void deleteReplica(Replica replica) {
        tablet.deleteReplicaByBackendId(replica.getBackendId());
    }

    public StorageMediaMigrationTask createStorageMediaMigrationTask() throws SchedException {
        storageMediaMigrationTask = new StorageMediaMigrationTask(getSrcBackendId(), getTabletId(),
                getSchemaHash(), getStorageMedium());
        if (destPath == null || destPath.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE,
                "backend " + srcReplica.getBackendId() + ", dest path is empty");
        }
        storageMediaMigrationTask.setDataDir(destPath);
        this.taskTimeoutMs = getApproximateTimeoutMs();
        this.state = State.RUNNING;
        return storageMediaMigrationTask;
    }

    // database lock should be held.
    public CloneTask createCloneReplicaAndTask() throws SchedException {
        Backend srcBe = infoService.getBackend(srcReplica.getBackendId());
        if (srcBe == null) {
            throw new SchedException(Status.SCHEDULE_FAILED,
                "src backend " + srcReplica.getBackendId() + " does not exist");
        }

        Backend destBe = infoService.getBackend(destBackendId);
        if (destBe == null) {
            throw new SchedException(Status.SCHEDULE_FAILED,
                "dest backend " + destBackendId + " does not exist");
        }

        taskTimeoutMs = getApproximateTimeoutMs();

        // create the clone task and clone replica.
        // we use visible version in clone task, but set the clone replica's last failed version to
        // committed version.
        // because the clone task can only clone the data with visible version.
        // so after clone is finished, the clone replica's version is visible version, but its last
        // failed version is committed version, which is larger than visible version.
        // So after clone, this clone replica is still version incomplete, and it will be repaired in
        // another clone task.
        // That is, we may need to use 2 clone tasks to create a new replica. It is inefficient,
        // but there is no other way now.

        // if this is a balance task, or this is a repair task with
        // REPLICA_MISSING/REPLICA_RELOCATING,
        // we create a new replica with state CLONE
        Replica replica = null;
        if (tabletHealth.status == TabletStatus.REPLICA_MISSING
                || tabletHealth.status == TabletStatus.REPLICA_RELOCATING || type == Type.BALANCE
                || tabletHealth.status == TabletStatus.COLOCATE_MISMATCH
                || tabletHealth.status == TabletStatus.REPLICA_MISSING_FOR_TAG) {
            replica = new Replica(
                    Env.getCurrentEnv().getNextId(), destBackendId,
                    -1 /* version */, schemaHash,
                    -1 /* data size */, -1, -1 /* row count */,
                    ReplicaState.CLONE,
                    committedVersion, /* use committed version as last failed version */
                    -1 /* last success version */);

            // addReplica() method will add this replica to tablet inverted index too.
            tablet.addReplica(replica);
        } else {
            // tabletStatus is VERSION_INCOMPLETE || NEED_FURTHER_REPAIR
            Preconditions.checkState(type == Type.REPAIR, type);
            // double check
            replica = tablet.getReplicaByBackendId(destBackendId);
            if (replica == null) {
                throw new SchedException(Status.SCHEDULE_FAILED, "dest replica does not exist on BE " + destBackendId);
            }

            if (replica.getPathHash() != destPathHash) {
                throw new SchedException(Status.SCHEDULE_FAILED, "dest replica's path hash is changed. "
                        + "current: " + replica.getPathHash() + ", scheduled: " + destPathHash);
            }
        }

        TBackend tSrcBe = new TBackend(srcBe.getHost(), srcBe.getBePort(), srcBe.getHttpPort());
        TBackend tDestBe = new TBackend(destBe.getHost(), destBe.getBePort(), destBe.getHttpPort());

        cloneTask = new CloneTask(tDestBe, destBackendId, dbId, tblId, partitionId, indexId, tabletId,
                replica.getId(), schemaHash, Lists.newArrayList(tSrcBe), storageMedium,
                visibleVersion, (int) (taskTimeoutMs / 1000));
        destOldVersion = replica.getVersion();
        cloneTask.setPathHash(srcPathHash, destPathHash);
        LOG.info("create clone task to repair replica, tabletId={}, replica={}, visible version {}, tablet status {}",
                tabletId, replica, visibleVersion, tabletHealth.status);

        this.state = State.RUNNING;
        return cloneTask;
    }

    // for storage migration or cloning a new replica
    public long getDestEstimatedCopingSize() {
        if ((cloneTask != null && tabletHealth.status != TabletStatus.VERSION_INCOMPLETE)
                || storageMediaMigrationTask != null) {
            return Math.max(getTabletSize(), 10L);
        } else {
            return 0;
        }
    }

    // timeout is between MIN_CLONE_TASK_TIMEOUT_MS and MAX_CLONE_TASK_TIMEOUT_MS
    private long getApproximateTimeoutMs() {
        long tabletSize = getTabletSize();
        long timeoutMs = tabletSize / 1024 / 1024 / MIN_CLONE_SPEED_MB_PER_SECOND * 1000;
        timeoutMs = Math.max(timeoutMs, Config.min_clone_task_timeout_sec * 1000);
        timeoutMs = Math.min(timeoutMs, Config.max_clone_task_timeout_sec * 1000);
        return timeoutMs;
    }

    /*
     * 1. Check if the tablet is already healthy. If yes, ignore the clone task report, and take it as FINISHED.
     * 2. If not, check the reported clone replica, and try to make it effective.
     *
     * Throw SchedException if error happens
     * 1. SCHEDULE_FAILED: will keep the tablet RUNNING.
     * 2. UNRECOVERABLE: will remove the tablet from runningTablets.
     */
    public void finishCloneTask(CloneTask cloneTask, TFinishTaskRequest request)
            throws SchedException {
        Preconditions.checkState(state == State.RUNNING, state);
        Preconditions.checkArgument(cloneTask.getTaskVersion() == CloneTask.VERSION_2);
        setLastVisitedTime(System.currentTimeMillis());

        // check if clone task success
        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            throw new SchedException(Status.RUNNING_FAILED, request.getTaskStatus().getErrorMsgs().get(0));
        }

        if (!request.isSetFinishTabletInfos() || request.getFinishTabletInfos().isEmpty()) {
            throw new SchedException(Status.RUNNING_FAILED, "tablet info is not set in task report request");
        }

        // check task report
        if (dbId != cloneTask.getDbId() || tblId != cloneTask.getTableId()
                || partitionId != cloneTask.getPartitionId() || indexId != cloneTask.getIndexId()
                || tabletId != cloneTask.getTabletId() || destBackendId != cloneTask.getBackendId()) {
            String msg = String.format("clone task does not match the tablet info"
                    + ". clone task %d-%d-%d-%d-%d-%d"
                    + ", tablet info: %d-%d-%d-%d-%d-%d",
                    cloneTask.getDbId(), cloneTask.getTableId(), cloneTask.getPartitionId(),
                    cloneTask.getIndexId(), cloneTask.getTabletId(), cloneTask.getBackendId(),
                    dbId, tblId, partitionId, indexId, tablet.getId(), destBackendId);
            throw new SchedException(Status.UNRECOVERABLE, msg);
        }

        // 1. check the tablet status first
        Database db = Env.getCurrentInternalCatalog().getDbOrException(dbId,
                s -> new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                        "db " + dbId + " does not exist"));
        OlapTable olapTable = (OlapTable) db.getTableOrException(tblId,
                s -> new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                        "tbl " + tabletId + " does not exist"));
        olapTable.writeLockOrException(new SchedException(Status.UNRECOVERABLE, "table "
                + olapTable.getName() + " does not exist"));
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                        "partition does not exist");
            }

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE, "index does not exist");
            }

            if (schemaHash != olapTable.getSchemaHashByIndexId(indexId)) {
                throw new SchedException(Status.UNRECOVERABLE, "schema hash is not consistent. index's: "
                        + olapTable.getSchemaHashByIndexId(indexId)
                        + ", task's: " + schemaHash);
            }

            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet does not exist");
            }

            List<Long> aliveBeIds = infoService.getAllBackendIds(true);
            ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo().getReplicaAllocation(partitionId);
            TabletStatus status = tablet.getHealth(infoService, visibleVersion, replicaAlloc, aliveBeIds).status;
            if (status == TabletStatus.HEALTHY) {
                throw new SchedException(Status.FINISHED, "tablet is healthy");
            }

            // tablet is unhealthy, go on

            // Here we do not check if the clone version is equal to the partition's visible version.
            // Because in case of high frequency loading, clone version always lags behind the visible version,
            // But we will check if the clone replica's version is larger than or equal to the task's visible version.
            // (which is 'visibleVersion[Hash]' saved)
            // We should discard the clone replica with stale version.
            TTabletInfo reportedTablet = request.getFinishTabletInfos().get(0);
            if (reportedTablet.getVersion() < visibleVersion) {
                String msg = String.format("the clone replica's version is stale. %d, task visible version: %d",
                        reportedTablet.getVersion(), visibleVersion);
                throw new SchedException(Status.RUNNING_FAILED, msg);
            }

            // check if replica exist
            Replica replica = tablet.getReplicaByBackendId(destBackendId);
            if (replica == null) {
                throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                        "replica does not exist. backend id: " + destBackendId);
            }

            replica.updateWithReport(reportedTablet);
            if (replica.getLastFailedVersion() > partition.getCommittedVersion()
                    && reportedTablet.getVersion() >= partition.getCommittedVersion()
                    //&& !(reportedTablet.isSetVersionMiss() && reportedTablet.isVersionMiss()
                    && !(reportedTablet.isSetUsed() && !reportedTablet.isUsed())) {
                LOG.info("change replica {} of tablet {} 's last failed version to -1", replica, tabletId);
                replica.updateLastFailedVersion(-1L);
            }
            if (reportedTablet.isSetPathHash()) {
                replica.setPathHash(reportedTablet.getPathHash());
            }

            if (type == Type.BALANCE) {
                replica.setNeedFurtherRepair(true);
                try {
                    long furtherRepairWatermarkTxnTd = Env.getCurrentGlobalTransactionMgr()
                            .getNextTransactionId();
                    replica.setFurtherRepairWatermarkTxnTd(furtherRepairWatermarkTxnTd);
                    LOG.info("new replica {} of tablet {} set further repair watermark id {}",
                            replica, tabletId, furtherRepairWatermarkTxnTd);
                } catch (Exception e) {
                    LOG.warn("new replica {} set further repair watermark id failed", replica, e);
                }
            }

            // isCatchup should check the txns during ReplicaState CLONE finished.
            // Because when replica's state = CLONE, it will not load txns.
            // Even if this replica version = partition visible version, but later if the txns during CLONE
            // change from prepare to committed or visible, this replica will be fall behind and be removed
            // in REDUNDANT detection.
            //
            boolean isCatchup = checkFurtherRepairFinish(replica, partition.getVisibleVersion());
            replica.incrFurtherRepairCount();
            if (isCatchup || replica.getLeftFurtherRepairCount() <= 0) {
                replica.setNeedFurtherRepair(false);
            }
            if (!replica.needFurtherRepair()) {
                replica.setFurtherRepairWatermarkTxnTd(-1);
            }

            ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tblId, partitionId, indexId,
                    tabletId, destBackendId, replica.getId(),
                    reportedTablet.getVersion(),
                    reportedTablet.getSchemaHash(),
                    reportedTablet.getDataSize(),
                    reportedTablet.getRemoteDataSize(),
                    reportedTablet.getRowCount(),
                    replica.getLastFailedVersion(),
                    replica.getLastSuccessVersion());

            if (replica.getState() == ReplicaState.CLONE) {
                replica.setState(ReplicaState.NORMAL);
                Env.getCurrentEnv().getEditLog().logAddReplica(info);
            } else {
                // if in VERSION_INCOMPLETE, replica is not newly created, thus the state is not CLONE
                // so we keep it state unchanged, and log update replica
                Env.getCurrentEnv().getEditLog().logUpdateReplica(info);
            }

            state = State.FINISHED;
            LOG.info("clone finished: {}, replica {}, replica old version {}, need further repair {}, is catchup {}",
                    this, replica, destOldVersion, replica.needFurtherRepair(), isCatchup);
        } finally {
            olapTable.writeUnlock();
        }

        if (request.isSetCopySize()) {
            this.copySize = request.getCopySize();
        }

        if (request.isSetCopyTimeMs()) {
            this.copyTimeMs = request.getCopyTimeMs();
        }
    }

    public boolean isTimeout() {
        if (state != TabletSchedCtx.State.RUNNING) {
            return false;
        }

        Preconditions.checkState(lastSchedTime != 0 && taskTimeoutMs != 0, lastSchedTime + "-" + taskTimeoutMs);
        return System.currentTimeMillis() - lastSchedTime > taskTimeoutMs;
    }

    public List<String> getBrief() {
        List<String> result = Lists.newArrayList();
        result.add(String.valueOf(tabletId));
        result.add(type.name());
        result.add(storageMedium == null ? FeConstants.null_string : storageMedium.name());
        result.add(tabletHealth.status == null ? FeConstants.null_string : tabletHealth.status.name());
        result.add(state.name());
        result.add(schedFailedCode.name());
        result.add(tabletHealth.priority == null ? FeConstants.null_string : tabletHealth.priority.name());
        // show the real priority value, higher this value, higher sched priority. Add 10 hour to make it
        // to be a positive value.
        result.add(String.valueOf((System.currentTimeMillis() - getCompareValue()) / 1000 + 10 * 3600L));
        result.add(srcReplica == null ? "-1" : String.valueOf(srcReplica.getBackendId()));
        result.add(String.valueOf(srcPathHash));
        result.add(String.valueOf(destBackendId));
        result.add(String.valueOf(destPathHash));
        result.add(String.valueOf(taskTimeoutMs));
        result.add(TimeUtils.longToTimeString(createTime));
        result.add(TimeUtils.longToTimeString(lastSchedTime));
        result.add(TimeUtils.longToTimeString(lastVisitedTime));
        result.add(TimeUtils.longToTimeString(finishedTime));
        Pair<Double, String> tabletSizeDesc = DebugUtil.getByteUint(tabletSize);
        result.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tabletSizeDesc.first) + " " + tabletSizeDesc.second);
        result.add(copyTimeMs > 0 ? String.valueOf(copySize / copyTimeMs / 1000.0) : FeConstants.null_string);
        result.add(String.valueOf(failedSchedCounter));
        result.add(String.valueOf(failedRunningCounter));
        result.add(String.valueOf(visibleVersion));
        result.add(String.valueOf(committedVersion));
        result.add(Strings.nullToEmpty(errMsg));
        return result;
    }

    /*
     * First compared by dynamic priority. higher priority rank ahead.
     * If priority is equals, compared by last visit time, earlier visit time rank ahead.
     */
    @Override
    public int compareTo(TabletSchedCtx o) {
        return Long.compare(getCompareValue(), o.getCompareValue());
    }

    // smaller compare value, higher priority
    private long getCompareValue() {
        long value = createTime;
        if (lastVisitedTime > 0) {
            value = lastVisitedTime;
        }

        value += (Priority.VERY_HIGH.ordinal() - tabletHealth.priority.ordinal() + 1) * 60  * 1000L;
        value += 5000L * (failedSchedCounter / 10);

        if (schedFailedCode == SubCode.WAITING_DECOMMISSION) {
            value += 5 * 1000L;
        }

        long baseTime = Config.tablet_schedule_high_priority_second * 1000L;
        // repair tasks always prior than balance
        if (type == Type.BALANCE) {
            value += 10 * baseTime;
        } else {
            int replicaNum = replicaAlloc.getTotalReplicaNum();
            if (tabletHealth.aliveAndVersionCompleteNum < replicaNum && !tabletHealth.noPathForNewReplica) {
                if (tabletHealth.aliveAndVersionCompleteNum < (replicaNum / 2 + 1)) {
                    value -= 3 * baseTime;
                    if (tabletHealth.hasRecentLoadFailed) {
                        value -= 3 * baseTime;
                    }
                }
                if (tabletHealth.hasAliveAndVersionIncomplete) {
                    value -= 1 * baseTime;
                    if (isUniqKeyMergeOnWrite) {
                        value -= 1 * baseTime;
                    }
                }
            }
        }

        if (tabletHealth.status == TabletStatus.NEED_FURTHER_REPAIR) {
            value -= 1 * baseTime;
        }

        return value;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId);
        if (tabletHealth.status != null) {
            sb.append(", status: ").append(tabletHealth.status.name());
        }
        if (state != null) {
            sb.append(", state: ").append(state.name());
        }
        if (type != null) {
            sb.append(", type: ").append(type.name());
        }
        if (type == Type.BALANCE && balanceType != null) {
            sb.append(", balance: ").append(balanceType.name());
        }
        sb.append(", priority: ").append(tabletHealth.priority.name());
        sb.append(", tablet size: ").append(tabletSize);
        if (srcReplica != null) {
            sb.append(", from backend: ").append(srcReplica.getBackendId());
            sb.append(", src path hash: ").append(srcPathHash);
        }
        if (destPathHash != -1) {
            sb.append(", to backend: ").append(destBackendId);
            sb.append(", dest path hash: ").append(destPathHash);
        }
        sb.append(", visible version: ").append(visibleVersion);
        sb.append(", committed version: ").append(committedVersion);
        if (errMsg != null) {
            sb.append(". err: ").append(errMsg);
        }
        return sb.toString();
    }

    // Comparator to sort the replica with version count, asc
    public static class VersionCountComparator implements Comparator<Replica> {
        @Override
        public int compare(Replica r1, Replica r2) {
            long verCount1 = r1.getVisibleVersionCount() == -1 ? Long.MAX_VALUE : r1.getVisibleVersionCount();
            long verCount2 = r2.getVisibleVersionCount() == -1 ? Long.MAX_VALUE : r2.getVisibleVersionCount();
            if (verCount1 < verCount2) {
                return -1;
            } else if (verCount1 > verCount2) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * call this when releaseTabletCtx()
     */
    public void resetReplicaState() {
        setDecommissionTime(-1);
        if (tablet != null) {
            for (Replica replica : tablet.getReplicas()) {
                // To address issue: https://github.com/apache/doris/issues/9422
                // the DECOMMISSION state is set in TabletScheduler and not persist to meta.
                // So it is reasonable to reset this state if we failed to scheduler this tablet.
                // That is, if the TabletScheduler cannot process the tablet, then it should reset
                // any intermediate state it set during the scheduling process.
                if (replica.getState() == ReplicaState.DECOMMISSION) {
                    replica.setState(ReplicaState.NORMAL);
                    replica.setPreWatermarkTxnId(-1);
                    replica.setPostWatermarkTxnId(-1);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("reset replica {} on backend {} of tablet {} state from DECOMMISSION to NORMAL",
                                replica.getId(), replica.getBackendId(), tabletId);
                    }
                }
            }
        }
    }
}
