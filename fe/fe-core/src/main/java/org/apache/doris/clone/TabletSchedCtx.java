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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
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
     *  A clone task timeout is between Config.min_clone_task_timeout_sec and Config.max_clone_task_timeout_sec,
     *  estimated by tablet size / MIN_CLONE_SPEED_MB_PER_SECOND.
     */
    private static final long MIN_CLONE_SPEED_MB_PER_SECOND = 5; // 5MB/sec

    /*
     * If a clone task is failed to run more than RUNNING_FAILED_COUNTER_THRESHOLD, it will be removed
     * from the tablet scheduler.
     */
    private static final int RUNNING_FAILED_COUNTER_THRESHOLD = 3;
    
    public enum Type {
        BALANCE, REPAIR
    }

    public enum Priority {
        LOW,
        NORMAL,
        HIGH,
        VERY_HIGH;
        
        // VERY_HIGH can only be downgraded to NORMAL
        // LOW can only be upgraded to HIGH
        public Priority adjust(Priority origPriority, boolean isUp) {
            switch (this) {
                case VERY_HIGH:
                    return isUp ? VERY_HIGH : HIGH;
                case HIGH:
                    return isUp ? (origPriority == LOW ? HIGH : VERY_HIGH) : NORMAL;
                case NORMAL:
                    return isUp ? HIGH : (origPriority == Priority.VERY_HIGH ? NORMAL : LOW);
                default:
                    return isUp ? NORMAL : LOW;
            }
        }
        
    }
    
    public enum State {
        PENDING, // tablet is not being scheduled
        RUNNING, // tablet is being scheduled
        FINISHED, // task is finished
        CANCELLED, // task is failed
        TIMEOUT, // task is timeout
        UNEXPECTED // other unexpected errors
    }
    
    private Type type;

    /*
     * origPriority is the origin priority being set when this tablet being added to scheduler.
     * dynamicPriority will be set during tablet schedule processing, it will not be prior than origin priority.
     * And dynamic priority is also used in priority queue compare in tablet scheduler.
     */
    private Priority origPriority;
    private Priority dynamicPriority;
    
    // we change the dynamic priority based on how many times it fails to be scheduled
    private int failedSchedCounter = 0;
    // clone task failed counter
    private int failedRunningCounter = 0;
    
    // last time this tablet being scheduled
    private long lastSchedTime = 0;
    // last time the dynamic priority being adjusted
    private long lastAdjustPrioTime = 0;
    
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
    private TabletStatus tabletStatus;
    
    private String cluster;
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
    private long visibleVersionHash = -1;
    private long committedVersion = -1;
    private long committedVersionHash = -1;
    
    private Replica srcReplica = null;
    private long srcPathHash = -1;
    private long destBackendId = -1;
    private long destPathHash = -1;
    private String errMsg = null;
    
    private CloneTask cloneTask = null;
    
    // statistics gathered from clone task report
    // the total size of clone files and the total cost time in ms.
    private long copySize = 0;
    private long copyTimeMs = 0;

    private Set<Long> colocateBackendsSet = null;
    private int tabletOrderIdx = -1;

    private SystemInfoService infoService;
    
    public TabletSchedCtx(Type type, String cluster, long dbId, long tblId, long partId,
            long idxId, long tabletId, long createTime) {
        this.type = type;
        this.cluster = cluster;
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partId;
        this.indexId = idxId;
        this.tabletId = tabletId;
        this.createTime = createTime;
        this.infoService = Catalog.getCurrentSystemInfo();
        this.state = State.PENDING;
    }
    
    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public Priority getOrigPriority() {
        return origPriority;
    }
    
    public void setOrigPriority(Priority origPriority) {
        this.origPriority = origPriority;
        // reset dynamic priority along with the origin priority being set.
        this.dynamicPriority = origPriority;
        this.failedSchedCounter = 0;
        this.lastSchedTime = 0;
        this.lastAdjustPrioTime = 0;
    }
    
    public Priority getDynamicPriority() {
        return dynamicPriority;
    }
    
    public void increaseFailedSchedCounter() {
        ++failedSchedCounter;
    }
    
    public int getFailedSchedCounter() {
        return failedSchedCounter;
    }

    public void increaseFailedRunningCounter() {
        ++failedRunningCounter;
    }
    
    public int getFailedRunningCounter() {
        return failedRunningCounter;
    }
    
    public void setLastSchedTime(long lastSchedTime) {
        this.lastSchedTime = lastSchedTime;
    }
    
    public void setLastVisitedTime(long lastVisitedTime) {
        this.lastVisitedTime = lastVisitedTime;
    }

    public void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public State getState() {
        return state;
    }
    
    public void setState(State state) {
        this.state = state;
    }
    
    public void setTabletStatus(TabletStatus tabletStatus) {
        this.tabletStatus = tabletStatus;
    }
    
    public TabletStatus getTabletStatus() {
        return tabletStatus;
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
    
    public String getCluster() {
        return cluster;
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public long getCommittedVersion() {
        return visibleVersion;
    }
    
    public long getCommittedVersionHash() {
        return visibleVersionHash;
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
    
    public void setVersionInfo(long visibleVersion, long visibleVersionHash,
            long committedVersion, long committedVersionHash) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionHash = visibleVersionHash;
        this.committedVersion = committedVersion;
        this.committedVersionHash = committedVersionHash;
    }
    
    public void setDest(Long destBeId, long destPathHash) {
        this.destBackendId = destBeId;
        this.destPathHash = destPathHash;
    }
    
    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
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

    public long getDestBackendId() {
        return destBackendId;
    }

    public long getDestPathHash() {
        return destPathHash;
    }

    // database lock should be held.
    public long getTabletSize() {
        long max = Long.MIN_VALUE;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getDataSize() > max) {
                max = replica.getDataSize();
            }
        }
        return max;
    }
    
    /*
     * check if existing replicas are on same BE.
     * database lock should be held.
     */
    public boolean containsBE(long beId) {
        String host = infoService.getBackend(beId).getHost();
        for (Replica replica : tablet.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // BE has been dropped, skip it
                continue;
            }
            if (host.equals(be.getHost())) {
                return true;
            }
            // actually there is no need to check BE id anymore, because if hosts are not same, BE ids are
            // not same either. But for psychological comfort, leave this check here.
            if (replica.getBackendId() == beId) {
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

    // database lock should be held.
    public void chooseSrcReplica(Map<Long, PathSlot> backendsWorkingSlots) throws SchedException {
        /*
         * get all candidate source replicas
         * 1. source replica should be healthy.
         * 2. slot of this source replica is available. 
         */
        List<Replica> candidates = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            if (replica.isBad()) {
                continue;
            }

            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAlive()) {
                // backend which is in decommission can still be the source backend
                continue;
            }
            
            if (replica.getLastFailedVersion() > 0) {
                continue;
            }
            
            if (!replica.checkVersionCatchUp(visibleVersion, visibleVersionHash, false)) {
                continue;
            }
            
            candidates.add(replica);
        }
        
        if (candidates.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to find source replica");
        }
        
        // choose a replica which slot is available from candidates.
        for (Replica srcReplica : candidates) {
            PathSlot slot = backendsWorkingSlots.get(srcReplica.getBackendId());
            if (slot == null) {
                continue;
            }
            
            long srcPathHash = slot.takeSlot(srcReplica.getPathHash());
            if (srcPathHash != -1) {
                setSrc(srcReplica);
                return;
            }
        }
        
        throw new SchedException(Status.SCHEDULE_FAILED, "unable to find source slot");
    }
    
    /*
     * Same rules as choosing source replica for supplement.
     * But we need to check that we can not choose the same replica as dest replica,
     * because replica info is changing all the time.
     */
    public void chooseSrcReplicaForVersionIncomplete(Map<Long, PathSlot> backendsWorkingSlots)
            throws SchedException {
        chooseSrcReplica(backendsWorkingSlots);
        if (srcReplica.getBackendId() == destBackendId) {
            throw new SchedException(Status.SCHEDULE_FAILED, "the chosen source replica is in dest backend");
        }
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
        Replica chosenReplica = null;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.isBad()) {
                continue;
            }

            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAvailable()) {
                continue;
            }

            if (replica.getLastFailedVersion() <= 0
                    && ((replica.getVersion() == visibleVersion && replica.getVersionHash() == visibleVersionHash)
                            || replica.getVersion() > visibleVersion)) {
                // skip healthy replica
                continue;
            }

            if (replica.needFurtherRepair()) {
                chosenReplica = replica;
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
        
        if (chosenReplica == null) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to choose dest replica");
        }
        
        // check if the dest replica has available slot
        PathSlot slot = backendsWorkingSlots.get(chosenReplica.getBackendId());
        if (slot == null) {
            throw new SchedException(Status.SCHEDULE_FAILED, "backend of dest replica is missing");
        }
        
        long destPathHash = slot.takeSlot(chosenReplica.getPathHash());
        if (destPathHash == -1) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to take slot of dest path");
        }
        
        setDest(chosenReplica.getBackendId(), chosenReplica.getPathHash());
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
        
        if (cloneTask != null) {
            AgentTaskQueue.removeTask(cloneTask.getBackendId(), TTaskType.CLONE, cloneTask.getSignature());

            // clear all CLONE replicas
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db != null) {
                Table table = db.getTable(tblId);
                if (table != null) {
                    table.writeLock();
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
            this.cloneTask = null;
        }
    }
    
    public void deleteReplica(Replica replica) {
        tablet.deleteReplicaByBackendId(replica.getBackendId());
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
                "dest backend " + srcReplica.getBackendId() + " does not exist");
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
        TBackend tSrcBe = new TBackend(srcBe.getHost(), srcBe.getBePort(), srcBe.getHttpPort());
        cloneTask = new CloneTask(destBackendId, dbId, tblId, partitionId, indexId,
                tabletId, schemaHash, Lists.newArrayList(tSrcBe), storageMedium,
                visibleVersion, visibleVersionHash, (int) (taskTimeoutMs / 1000));
        cloneTask.setPathHash(srcPathHash, destPathHash);
        
        // if this is a balance task, or this is a repair task with REPLICA_MISSING/REPLICA_RELOCATING or REPLICA_MISSING_IN_CLUSTER,
        // we create a new replica with state CLONE
        if (tabletStatus == TabletStatus.REPLICA_MISSING || tabletStatus == TabletStatus.REPLICA_MISSING_IN_CLUSTER
                || tabletStatus == TabletStatus.REPLICA_RELOCATING || type == Type.BALANCE
                || tabletStatus == TabletStatus.COLOCATE_MISMATCH) {
            Replica cloneReplica = new Replica(
                    Catalog.getCurrentCatalog().getNextId(), destBackendId,
                    -1 /* version */, 0 /* version hash */, schemaHash,
                    -1 /* data size */, -1 /* row count */,
                    ReplicaState.CLONE,
                    committedVersion, committedVersionHash, /* use committed version as last failed version */
                    -1 /* last success version */, 0 /* last success version hash */);

            // addReplica() method will add this replica to tablet inverted index too.
            tablet.addReplica(cloneReplica);
        } else if (tabletStatus == TabletStatus.VERSION_INCOMPLETE) {
            Preconditions.checkState(type == Type.REPAIR, type);
            // double check
            Replica replica = tablet.getReplicaByBackendId(destBackendId);
            if (replica == null) {
                throw new SchedException(Status.SCHEDULE_FAILED, "dest replica does not exist on BE " + destBackendId);
            }

            if (replica.getPathHash() != destPathHash) {
                throw new SchedException(Status.SCHEDULE_FAILED, "dest replica's path hash is changed. "
                        + "current: " + replica.getPathHash() + ", scheduled: " + destPathHash);
            }
        }
        
        this.state = State.RUNNING;
        return cloneTask;
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
            throw new SchedException(Status.RUNNING_FAILED, msg);
        }

        // 1. check the tablet status first
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db does not exist");
        }
        OlapTable olapTable = (OlapTable) db.getTable(tblId);
        if (olapTable == null) {
            throw new SchedException(Status.UNRECOVERABLE, "tbl does not exist");
        }

        olapTable.writeLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
            }
            
            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
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
            
            List<Long> aliveBeIdsInCluster = infoService.getClusterBackendIds(db.getClusterName(), true);
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partitionId);
            Pair<TabletStatus, TabletSchedCtx.Priority> pair = tablet.getHealthStatusWithPriority(
                    infoService, db.getClusterName(), visibleVersion, visibleVersionHash, replicationNum,
                    aliveBeIdsInCluster);
            if (pair.first == TabletStatus.HEALTHY) {
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
                String msg = String.format("the clone replica's version is stale. %d-%d, task visible version: %d-%d",
                        reportedTablet.getVersion(), reportedTablet.getVersionHash(),
                        visibleVersion, visibleVersionHash);
                throw new SchedException(Status.RUNNING_FAILED, msg);
            }
            
            // check if replica exist
            Replica replica = tablet.getReplicaByBackendId(destBackendId);
            if (replica == null) {
                throw new SchedException(Status.UNRECOVERABLE,
                        "replica does not exist. backend id: " + destBackendId);
            }
            
            replica.updateVersionInfo(reportedTablet.getVersion(), reportedTablet.getVersionHash(),
                    reportedTablet.getDataSize(), reportedTablet.getRowCount());
            if (reportedTablet.isSetPathHash()) {
                replica.setPathHash(reportedTablet.getPathHash());
            }
            
            if (this.type == Type.BALANCE) {
                long partitionVisibleVersion = partition.getVisibleVersion();
                if (replica.getVersion() < partitionVisibleVersion) {
                    // see comment 'needFurtherRepair' of Replica for explanation.
                    // no need to persist this info. If FE restart, just do it again.
                    replica.setNeedFurtherRepair(true);
                }
            } else {
                replica.setNeedFurtherRepair(false);
            }

            ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tblId, partitionId, indexId,
                    tabletId, destBackendId, replica.getId(),
                    reportedTablet.getVersion(),
                    reportedTablet.getVersionHash(),
                    reportedTablet.getSchemaHash(),
                    reportedTablet.getDataSize(),
                    reportedTablet.getRowCount(),
                    replica.getLastFailedVersion(),
                    replica.getLastFailedVersionHash(),
                    replica.getLastSuccessVersion(),
                    replica.getLastSuccessVersionHash());

            if (replica.getState() == ReplicaState.CLONE) {
                replica.setState(ReplicaState.NORMAL);
                Catalog.getCurrentCatalog().getEditLog().logAddReplica(info);
            } else {
                // if in VERSION_INCOMPLETE, replica is not newly created, thus the state is not CLONE
                // so we keep it state unchanged, and log update replica
                Catalog.getCurrentCatalog().getEditLog().logUpdateReplica(info);
            }

            state = State.FINISHED;
            LOG.info("clone finished: {}", this);
        } catch (SchedException e) {
            // if failed to too many times, remove this task
            ++failedRunningCounter;
            if (failedRunningCounter > RUNNING_FAILED_COUNTER_THRESHOLD) {
                throw new SchedException(Status.UNRECOVERABLE, e.getMessage());
            }
            throw e;
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
    
    /*
     * we try to adjust the priority based on schedule history
     * 1. If failed counter is larger than FAILED_COUNTER_THRESHOLD, which means this tablet is being scheduled
     *    at least FAILED_TIME_THRESHOLD times and all are failed. So we downgrade its priority.
     *    Also reset the failedCounter, or it will be downgraded forever.
     *    
     * 2. Else, if it has been a long time since last time the tablet being scheduled, we upgrade its
     *    priority to let it more available to be scheduled.
     *    
     * The time gap between adjustment should be larger than MIN_ADJUST_PRIORITY_INTERVAL_MS, to avoid
     * being downgraded too fast.
     *    
     * eg:
     *    A tablet has been scheduled for 5 times and all were failed. its priority will be downgraded. And if it is
     *    scheduled for 5 times and all are failed again, it will be downgraded again, until to the LOW.
     *    And than, because of LOW, this tablet can not be scheduled for a long time, and it will be upgraded
     *    to NORMAL, if still not being scheduled, it will be upgraded up to VERY_HIGH.
     *    
     * return true if dynamic priority changed
     */
    public boolean adjustPriority(TabletSchedulerStat stat) {
        long currentTime = System.currentTimeMillis();
        if (lastAdjustPrioTime == 0) {
            // skip the first time we adjust this priority
            lastAdjustPrioTime = currentTime;
            return false;
        } else {
            if (currentTime - lastAdjustPrioTime < MIN_ADJUST_PRIORITY_INTERVAL_MS) {
                return false;
            }
        }
        
        boolean isDowngrade = false;
        boolean isUpgrade = false;
        
        if (failedSchedCounter > SCHED_FAILED_COUNTER_THRESHOLD) {
            isDowngrade = true;
        } else {
            long lastTime = lastSchedTime == 0 ? createTime : lastSchedTime;
            if (currentTime - lastTime > MAX_NOT_BEING_SCHEDULED_INTERVAL_MS) {
                isUpgrade = true;
            }
        }
        
        Priority originDynamicPriority = dynamicPriority;
        if (isDowngrade) {
            dynamicPriority = dynamicPriority.adjust(origPriority, false /* downgrade */);
            failedSchedCounter = 0;
            if (originDynamicPriority != dynamicPriority) {
                LOG.debug("downgrade dynamic priority from {} to {}, origin: {}, tablet: {}",
                    originDynamicPriority.name(), dynamicPriority.name(), origPriority.name(), tabletId);
                stat.counterTabletPrioDowngraded.incrementAndGet();
                return true;
            }
        } else if (isUpgrade) {
            dynamicPriority = dynamicPriority.adjust(origPriority, true /* upgrade */);
            // no need to set lastSchedTime, lastSchedTime is set each time we schedule this tablet
            if (originDynamicPriority != dynamicPriority) {
                LOG.debug("upgrade dynamic priority from {} to {}, origin: {}, tablet: {}",
                    originDynamicPriority.name(), dynamicPriority.name(), origPriority.name(), tabletId);
                stat.counterTabletPrioUpgraded.incrementAndGet();
                return true;
            }
        }
        return false;
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
        result.add(tabletStatus == null ? FeConstants.null_string : tabletStatus.name());
        result.add(state.name());
        result.add(origPriority.name());
        result.add(dynamicPriority.name());
        result.add(srcReplica == null ? "-1" : String.valueOf(srcReplica.getBackendId()));
        result.add(String.valueOf(srcPathHash));
        result.add(String.valueOf(destBackendId));
        result.add(String.valueOf(destPathHash));
        result.add(String.valueOf(taskTimeoutMs));
        result.add(TimeUtils.longToTimeString(createTime));
        result.add(TimeUtils.longToTimeString(lastSchedTime));
        result.add(TimeUtils.longToTimeString(lastVisitedTime));
        result.add(TimeUtils.longToTimeString(finishedTime));
        result.add(copyTimeMs > 0 ? String.valueOf(copySize / copyTimeMs / 1000.0) : FeConstants.null_string);
        result.add(String.valueOf(failedSchedCounter));
        result.add(String.valueOf(failedRunningCounter));
        result.add(TimeUtils.longToTimeString(lastAdjustPrioTime));
        result.add(String.valueOf(visibleVersion));
        result.add(String.valueOf(visibleVersionHash));
        result.add(String.valueOf(committedVersion));
        result.add(String.valueOf(committedVersionHash));
        result.add(Strings.nullToEmpty(errMsg));
        return result;
    }
    
    /*
     * First compared by dynamic priority. higher priority rank ahead.
     * If priority is equals, compared by last visit time, earlier visit time rank ahead.
     */
    @Override
    public int compareTo(TabletSchedCtx o) {
        if (dynamicPriority.ordinal() < o.dynamicPriority.ordinal()) {
            return 1;
        } else if (dynamicPriority.ordinal() > o.dynamicPriority.ordinal()) {
            return -1;
        } else {
            if (lastVisitedTime < o.lastVisitedTime) {
                return -1;
            } else if (lastVisitedTime > o.lastVisitedTime) {
                return 1;
            } else {
                return 0;
            }
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId).append(", status: ").append(tabletStatus.name());
        sb.append(", state: ").append(state.name()).append(", type: ").append(type.name());
        if (srcReplica != null) {
            sb.append(". from backend: ").append(srcReplica.getBackendId());
            sb.append(", src path hash: ").append(srcPathHash);
        }
        if (destPathHash != -1) {
            sb.append(". to backend: ").append(destBackendId);
            sb.append(", dest path hash: ").append(destPathHash);
        }
        if (errMsg != null) {
            sb.append(". err: ").append(errMsg);
        }
        return sb.toString();
    }
}
