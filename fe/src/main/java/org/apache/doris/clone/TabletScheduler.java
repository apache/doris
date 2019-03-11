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
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletSchedCtx.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TFinishTaskRequest;

import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * TabletScheduler saved the tablets produced by TabletChecker and try to schedule them.
 * It also try to balance the cluster load.
 * 
 * We are expecting an efficient way to recovery the entire cluster and make it balanced.
 * Case 1:
 *  A Backend is down. All tablets which has replica on this BE should be repaired as soon as possible.
 *  
 * Case 1.1:
 *  As Backend is down, some tables should be repaired in high priority. So the clone task should be able
 *  to preempted.
 *  
 * Case 2:
 *  A new Backend is added to the cluster. Replicas should be transfer to that host to balance the cluster load.
 */
public class TabletScheduler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(TabletScheduler.class);

    // handle at most BATCH_NUM tablets in one loop
    private static final int MIN_BATCH_NUM = 10;

    // the minimum interval of updating cluster statistics and priority of tablet info
    private static final long STAT_UPDATE_INTERVAL_MS = 20 * 1000; // 20s

    private static final long SCHEDULE_INTERVAL_MS = 5000; // 5s

    public static final int BALANCE_SLOT_NUM_FOR_PATH = 2;

    // if the number of scheduled tablets in TabletScheduler exceed this threshold,
    // skip checking.
    public static final int MAX_SCHEDULING_TABLETS = 5000;
    // if the number of balancing tablets in TabletScheduler exceed this threshold,
    // no more balance check
    public static final int MAX_BALANCING_TABLETS = 100;

    /*
     * Tablet is added to pendingTablets as well it's id in allTabletIds.
     * TabletScheduler will take tablet from pendingTablets but will not remove it's id from allTabletIds when
     * handling a tablet.
     * Tablet' id can only be removed after the clone task is done(timeout, cancelled or finished).
     * So if a tablet's id is still in allTabletIds, TabletChecker can not add tablet to TabletScheduler.
     * 
     * pendingTablets + runningTablets = allTabletIds
     * 
     * pendingTablets, allTabletIds, runningTablets and schedHistory are protected by 'synchronized' 
     */
    private PriorityQueue<TabletSchedCtx> pendingTablets = new PriorityQueue<>();
    private Set<Long> allTabletIds = Sets.newHashSet();
    // contains all tabletCtxs which state are RUNNING
    private Map<Long, TabletSchedCtx> runningTablets = Maps.newHashMap();
    // save the latest 1000 scheduled tablet info
    private Queue<TabletSchedCtx> schedHistory = EvictingQueue.create(1000);

    // be id -> #working slots
    private Map<Long, PathSlot> backendsWorkingSlots = Maps.newConcurrentMap();
    // cluster name -> load statistic
    private Map<String, ClusterLoadStatistic> statisticMap = Maps.newConcurrentMap();
    private long lastStatUpdateTime = 0;
    
    private long lastSlotAdjustTime = 0;

    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;
    private TabletSchedulerStat stat;
    
    // result of adding a tablet to pendingTablets
    public enum AddResult {
        ADDED, // success to add
        ALREADY_IN, // already added, skip
        LIMIT_EXCEED // number of pending tablets exceed the limit
    }

    public TabletScheduler(Catalog catalog, SystemInfoService infoService, TabletInvertedIndex invertedIndex,
            TabletSchedulerStat stat) {
        super("tablet scheduler", SCHEDULE_INTERVAL_MS);
        this.catalog = catalog;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
        this.stat = stat;
    }

    public TabletSchedulerStat getStat() {
        return stat;
    }

    /*
     * update working slots at the beginning of each round
     */
    private boolean updateWorkingSlots() {
        ImmutableMap<Long, Backend> backends = infoService.getBackendsInCluster(null);
        for (Backend backend : backends.values()) {
            if (!backend.hasPathHash() && backend.isAlive()) {
                // when upgrading, backend may not get path info yet. so return false and wait for next round.
                // and we should check if backend is alive. If backend is dead when upgrading, this backend
                // will never report its path hash, and tablet scheduler is blocked.
                LOG.info("not all backends have path info");
                return false;
            }
        }

        // update exist backends
        Set<Long> deletedBeIds = Sets.newHashSet();
        for (Long beId : backendsWorkingSlots.keySet()) {
            if (backends.containsKey(beId)) {
                List<Long> pathHashes = backends.get(beId).getDisks().values().stream()
                        .filter(v -> v.getState()==DiskState.ONLINE)
                        .map(v -> v.getPathHash()).collect(Collectors.toList());
                backendsWorkingSlots.get(beId).updatePaths(pathHashes);
            } else {
                deletedBeIds.add(beId);
            }
        }

        // delete non-exist backends
        for (Long beId : deletedBeIds) {
            backendsWorkingSlots.remove(beId);
            LOG.info("delete non exist backend: {}", beId);
        }

        // add new backends
        for (Backend be : backends.values()) {
            if (!backendsWorkingSlots.containsKey(be.getId())) {
                List<Long> pathHashes = be.getDisks().values().stream().map(v -> v.getPathHash()).collect(Collectors.toList());
                PathSlot slot = new PathSlot(pathHashes, Config.schedule_slot_num_per_path);
                backendsWorkingSlots.put(be.getId(), slot);
                LOG.info("add new backend {} with slots num: {}", be.getId(), be.getDisks().size());
            }
        }

        return true;
    }

    public Map<Long, PathSlot> getBackendsWorkingSlots() {
        return backendsWorkingSlots;
    }

    /*
     * add a ready-to-be-scheduled tablet to pendingTablets, if it has not being added before.
     * if force is true, do not check if tablet is already added before.
     */
    public synchronized AddResult addTablet(TabletSchedCtx tablet, boolean force) {
        if (!force && containsTablet(tablet.getTabletId())) {
            return AddResult.ALREADY_IN;
        }
        
        // if this is not a BALANCE task, and not a force add,
        // and number of scheduling tablets exceed the limit,
        // refuse to add.
        if (tablet.getType() != TabletSchedCtx.Type.BALANCE && !force
                && (pendingTablets.size() > MAX_SCHEDULING_TABLETS || runningTablets.size() > MAX_SCHEDULING_TABLETS)) {
            return AddResult.LIMIT_EXCEED;
        }

        allTabletIds.add(tablet.getTabletId());
        pendingTablets.offer(tablet);
        return AddResult.ADDED;
    }

    public synchronized boolean containsTablet(long tabletId) {
        return allTabletIds.contains(tabletId);
    }

    /*
     * Iterate current tablets, change their priority if necessary.
     */
    public synchronized void changePriorityOfTablets(long dbId, long tblId, List<Long> partitionIds) {
        PriorityQueue<TabletSchedCtx> newPendingTablets = new PriorityQueue<>();
        for (TabletSchedCtx tabletCtx : pendingTablets) {
            if (tabletCtx.getDbId() == dbId && tabletCtx.getTblId() == tblId
                    && partitionIds.contains(tabletCtx.getPartitionId())) {
                tabletCtx.setOrigPriority(Priority.VERY_HIGH);
            }
            newPendingTablets.add(tabletCtx);
        }
        pendingTablets = newPendingTablets;
    }

    /*
     * TabletScheduler will run as a daemon thread at a very short interval(default 5 sec)
     * Firstly, it will try to update cluster load statistic and check if priority need to be adjuested.
     * Than, it will schedule the tablets in pendingTablets.
     * Thirdly, it will check the current running tasks.
     * Finally, it try to balance the cluster if possible.
     * 
     * Schedule rules:
     * 1. tablet with higher priority will be scheduled first.
     * 2. high priority should be downgraded if it fails to be schedule too many times.
     * 3. priority may be upgraded if it is not being schedule for a long time.
     * 4. every pending task should has a max scheduled time, if schedule fails too many times, if should be removed.
     * 5. every running task should has a timeout, to avoid running forever.
     * 6. every running task should also has a max failure time, if clone task fails too many times, if should be removed.
     *
     */
    @Override
    protected void runOneCycle() {
        if (!updateWorkingSlots()) {
            return;
        }

        updateClusterLoadStatisticsAndPriorityIfNecessary();

        schedulePendingTablets();

        handleRunningTablets();

        selectTabletsForBalance();

        stat.counterTabletScheduleRound.incrementAndGet();
    }


    private void updateClusterLoadStatisticsAndPriorityIfNecessary() {
        if (System.currentTimeMillis() - lastStatUpdateTime < STAT_UPDATE_INTERVAL_MS) {
            return;
        }

        updateClusterLoadStatistic();
        adjustPriorities();

        lastStatUpdateTime = System.currentTimeMillis();
    }

    /*
     * Here is the only place we update the cluster load statistic info.
     * We will not update this info dynamically along with the clone job's running.
     * Although it will cause a little bit inaccurate, but is within a controllable range,
     * because we already limit the total number of running clone jobs in cluster by 'backend slots'
     */
    private void updateClusterLoadStatistic() {
        Map<String, ClusterLoadStatistic> newStatisticMap = Maps.newConcurrentMap();
        Set<String> clusterNames = infoService.getClusterNames();
        for (String clusterName : clusterNames) {
            ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(clusterName,
                    infoService, invertedIndex);
            clusterLoadStatistic.init();
            newStatisticMap.put(clusterName, clusterLoadStatistic);
            LOG.info("update cluster {} load statistic:\n{}", clusterName, clusterLoadStatistic.getBrief());
        }

        this.statisticMap = newStatisticMap;
    }

    public Map<String, ClusterLoadStatistic> getStatisticMap() {
        return statisticMap;
    }

    /*
     * adjust priorities of all tablet infos
     */
    private synchronized void adjustPriorities() {
        int size = pendingTablets.size();
        int changedNum = 0;
        TabletSchedCtx tabletCtx = null;
        for (int i = 0; i < size; i++) {
            tabletCtx = pendingTablets.poll();
            if (tabletCtx == null) {
                break;
            }

            if (tabletCtx.adjustPriority(stat)) {
                changedNum++;
            }
            pendingTablets.add(tabletCtx);
        }

        LOG.info("adjust priority for all tablets. changed: {}, total: {}", changedNum, size);
    }

    /*
     * get at most BATCH_NUM tablets from queue, and try to schedule them.
     * After handle, the tablet info should be
     * 1. in runningTablets with state RUNNING, if being scheduled success.
     * 2. or in schedHistory with state CANCELLING, if some unrecoverable error happens.
     * 3. or in pendingTablets with state PENDING, if failed to be scheduled.
     * 
     * if in schedHistory, it should be removed from allTabletIds.
     */
    private void schedulePendingTablets() {
        long start = System.currentTimeMillis();
        List<TabletSchedCtx> currentBatch = getNextTabletCtxBatch();
        LOG.debug("get {} tablets to schedule", currentBatch.size());

        AgentBatchTask batchTask = new AgentBatchTask();
        for (TabletSchedCtx tabletCtx : currentBatch) {
            try {
                scheduleTablet(tabletCtx, batchTask);
            } catch (SchedException e) {
                tabletCtx.increaseFailedSchedCounter();
                tabletCtx.setErrMsg(e.getMessage());

                if (e.getStatus() == Status.SCHEDULE_FAILED) {
                    if (tabletCtx.getType() == Type.BALANCE) {
                        // if balance is disabled, remove this tablet
                        if (Config.disable_balance) {
                            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED,
                                    "disable balance and " + e.getMessage());
                        } else {
                            // remove the balance task if it fails to be scheduled many times
                            if (tabletCtx.getFailedSchedCounter() > 10) {
                                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED,
                                        "schedule failed too many times and " + e.getMessage());
                            }
                        }
                    } else {
                        // we must release resource it current hold, and be scheduled again
                        tabletCtx.releaseResource(this);
                        // adjust priority to avoid some higher priority always be the first in pendingTablets
                        stat.counterTabletScheduledFailed.incrementAndGet();
                        dynamicAdjustPrioAndAddBackToPendingTablets(tabletCtx, e.getMessage());
                    }
                } else if (e.getStatus() == Status.FINISHED) {
                    // schedule redundant tablet will throw this exception
                    stat.counterTabletScheduledSucceeded.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, e.getMessage());
                } else {
                    Preconditions.checkState(e.getStatus() == Status.UNRECOVERABLE, e.getStatus());
                    // discard
                    stat.counterTabletScheduledDiscard.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getMessage());
                }
                continue;
            } catch (Exception e) {
                LOG.warn("got unexpected exception, discard this schedule. tablet: {}",
                        tabletCtx.getTabletId(), e);
                stat.counterTabletScheduledFailed.incrementAndGet();
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.UNEXPECTED, e.getMessage());
            }

            Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.RUNNING);
            stat.counterTabletScheduledSucceeded.incrementAndGet();
            addToRunningTablets(tabletCtx);
        }

        // must send task after adding tablet info to runningTablets.
        for (AgentTask task : batchTask.getAllTasks()) {
            if (AgentTaskQueue.addTask(task)) {
                stat.counterCloneTask.incrementAndGet();
            }
            LOG.info("add clone task to agent task queue: {}", task);
        }

        // send task immediately
        AgentTaskExecutor.submit(batchTask);

        long cost = System.currentTimeMillis() - start;
        stat.counterTabletScheduleCostMs.addAndGet(cost);
    }

    private synchronized void addToRunningTablets(TabletSchedCtx tabletCtx) {
        runningTablets.put(tabletCtx.getTabletId(), tabletCtx);
    }

    /*
     * we take the tablet out of the runningTablets and than handle it,
     * avoid other threads see it.
     * Whoever takes this tablet, make sure to put it to the schedHistory or back to runningTablets.
     */
    private synchronized TabletSchedCtx takeRunningTablets(long tabletId) {
        return runningTablets.remove(tabletId);
    }

    /*
     * Try to schedule a single tablet.
     */
    private void scheduleTablet(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        LOG.debug("schedule tablet: {}", tabletCtx.getTabletId());
        long currentTime = System.currentTimeMillis();
        tabletCtx.setLastSchedTime(currentTime);
        tabletCtx.setLastVisitedTime(currentTime);
        stat.counterTabletScheduled.incrementAndGet();

        // check this tablet again
        Database db = catalog.getDb(tabletCtx.getDbId());
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db does not exist");
        }

        Pair<TabletStatus, TabletSchedCtx.Priority> statusPair = null;
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tabletCtx.getTblId());
            if (tbl == null) {
                throw new SchedException(Status.UNRECOVERABLE, "tbl does not exist");
            }

            OlapTableState tableState = tbl.getState();

            Partition partition = tbl.getPartition(tabletCtx.getPartitionId());
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
            }

            MaterializedIndex idx = partition.getIndex(tabletCtx.getIndexId());
            if (idx == null) {
                throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
            }

            Tablet tablet = idx.getTablet(tabletCtx.getTabletId());
            Preconditions.checkNotNull(tablet);

            statusPair = tablet.getHealthStatusWithPriority(
                    infoService, tabletCtx.getCluster(),
                    partition.getVisibleVersion(),
                    partition.getVisibleVersionHash(),
                    tbl.getPartitionInfo().getReplicationNum(partition.getId()));

            if (tabletCtx.getType() == TabletSchedCtx.Type.BALANCE && tableState != OlapTableState.NORMAL) {
                // If table is under ALTER process, do not allow to do balance.
                throw new SchedException(Status.UNRECOVERABLE, "table's state is not NORMAL");
            }

            if (statusPair.first != TabletStatus.VERSION_INCOMPLETE && tableState != OlapTableState.NORMAL) {
                // If table is under ALTER process, do not allow to add or delete replica.
                // VERSION_INCOMPLETE will repair the replica in place, which is allowed.
                throw new SchedException(Status.UNRECOVERABLE,
                        "table's state is not NORMAL but tablet status is " + statusPair.first.name());
            }

            tabletCtx.setTabletStatus(statusPair.first);
            if (statusPair.first == TabletStatus.HEALTHY && tabletCtx.getType() == TabletSchedCtx.Type.REPAIR) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet is healthy");
            } else if (statusPair.first != TabletStatus.HEALTHY
                    && tabletCtx.getType() == TabletSchedCtx.Type.BALANCE) {
                tabletCtx.releaseResource(this);
                // we select an unhealthy tablet to do balance, which is not right.
                // so here we change it to a REPAIR task, and also reset its priority
                tabletCtx.setType(TabletSchedCtx.Type.REPAIR);
                tabletCtx.setOrigPriority(statusPair.second);
                tabletCtx.setLastSchedTime(currentTime);
                tabletCtx.setLastVisitedTime(currentTime);
            }

            // we do not concern priority here.
            // once we take the tablet out of priority queue, priority is meaningless.
            tabletCtx.setTablet(tablet);
            tabletCtx.setVersionInfo(partition.getVisibleVersion(), partition.getVisibleVersionHash(),
                    partition.getCommittedVersion(), partition.getCommittedVersionHash());
            tabletCtx.setSchemaHash(tbl.getSchemaHashByIndexId(idx.getId()));
            tabletCtx.setStorageMedium(tbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium());

            handleTabletByTypeAndStatus(statusPair.first, tabletCtx, batchTask);
        } finally {
            db.writeUnlock();
        }
    }

    private void handleTabletByTypeAndStatus(TabletStatus status, TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        if (tabletCtx.getType() == Type.REPAIR) {
            switch (status) {
                case REPLICA_MISSING:
                    handleReplicaMissing(tabletCtx, batchTask);
                    break;
                case VERSION_INCOMPLETE:
                    handleReplicaVersionIncomplete(tabletCtx, batchTask);
                    break;
                case REDUNDANT:
                    handleRedundantReplica(tabletCtx);
                    break;
                case REPLICA_MISSING_IN_CLUSTER:
                    handleReplicaClusterMigration(tabletCtx, batchTask);
                    break;
                default:
                    break;
            }
        } else {
            // balance
            doBalance(tabletCtx, batchTask);
        }
    }

    /*
     * Replica is missing, which means there is no enough alive replicas.
     * So we need to find a destination backend to clone a new replica as possible as we can.
     * 1. find an available path in a backend as destination:
     *      1. backend need to be alive.
     *      2. backend of existing replicas should be excluded.
     *      3. backend has available slot for clone.
     *      4. replica can fit in the path (consider the threshold of disk capacity and usage percent).
     *      5. try to find a path with lowest load score.
     * 2. find an appropriate source replica:
     *      1. source replica should be healthy
     *      2. backend of source replica has available slot for clone.
     *      
     * 3. send clone task to destination backend
     */
    private void handleReplicaMissing(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        stat.counterReplicaMissingErr.incrementAndGet();
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletCtx);
        Preconditions.checkNotNull(destPath);
        tabletCtx.setDest(destPath.getBeId(), destPath.getPathHash());

        // choose a source replica for cloning from
        tabletCtx.chooseSrcReplica(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }

    /*
     * Replica version is incomplete, which means this replica is missing some version,
     * and need to be cloned from a healthy replica, in-place.
     * 
     * 1. find the incomplete replica as destination replica
     * 2. find a healthy replica as source replica
     * 3. send clone task
     */
    private void handleReplicaVersionIncomplete(TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        stat.counterReplicaVersionMissingErr.incrementAndGet();
        ClusterLoadStatistic statistic = statisticMap.get(tabletCtx.getCluster());
        if (statistic == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }

        tabletCtx.chooseDestReplicaForVersionIncomplete(backendsWorkingSlots);
        tabletCtx.chooseSrcReplicaForVersionIncomplete(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }

    /*
     *  replica is redundant, which means there are more replicas than we expected, which need to be dropped.
     *  we just drop one redundant replica at a time, for safety reason.
     *  choosing a replica to drop base on following priority:
     *  1. backend has been dropped
     *  2. replica is bad
     *  3. backend is not available
     *  4. replica's state is CLONE
     *  5. replica's last failed version > 0
     *  6. replica with lower version
     *  7. replica not in right cluster
     *  8. replica in higher load backend
     */
    private void handleRedundantReplica(TabletSchedCtx tabletCtx) throws SchedException {
        stat.counterReplicaRedundantErr.incrementAndGet();
        if (deleteBackendDropped(tabletCtx)
                || deleteBadReplica(tabletCtx)
                || deleteBackendUnavailable(tabletCtx)
                || deleteCloneReplica(tabletCtx)
                || deleteReplicaWithFailedVersion(tabletCtx)
                || deleteReplicaWithLowerVersion(tabletCtx)
                || deleteReplicaNotInCluster(tabletCtx)
                || deleteReplicaOnHighLoadBackend(tabletCtx)) {
            // if we delete at least one redundant replica, we still throw a SchedException with status FINISHED
            // to remove this tablet from the pendingTablets(consider it as finished)
            throw new SchedException(Status.FINISHED, "redundant replica is deleted");
        }
        throw new SchedException(Status.SCHEDULE_FAILED, "unable to delete any redundant replicas");
    }

    private boolean deleteBackendDropped(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            long beId = replica.getBackendId();
            if (infoService.getBackend(beId) == null) {
                deleteReplicaInternal(tabletCtx, replica, "backend dropped");
                return true;
            }
        }
        return false;
    }

    private boolean deleteBadReplica(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.isBad()) {
                deleteReplicaInternal(tabletCtx, replica, "replica is bad");
                return true;
            }
        }
        return false;
    }

    private boolean deleteBackendUnavailable(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.isAvailable()) {
                deleteReplicaInternal(tabletCtx, replica, "backend unavailable");
                return true;
            }
        }
        return false;
    }

    private boolean deleteCloneReplica(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.getState() == ReplicaState.CLONE) {
                deleteReplicaInternal(tabletCtx, replica, "clone state");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithFailedVersion(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.getLastFailedVersion() > 0) {
                deleteReplicaInternal(tabletCtx, replica, "version incomplete");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithLowerVersion(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (!replica.checkVersionCatchUp(tabletCtx.getCommittedVersion(), tabletCtx.getCommittedVersionHash())) {
                deleteReplicaInternal(tabletCtx, replica, "lower version");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaNotInCluster(TabletSchedCtx tabletCtx) {
        for (Replica replica : tabletCtx.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.getOwnerClusterName().equals(tabletCtx.getCluster())) {
                deleteReplicaInternal(tabletCtx, replica, "not in cluster");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaOnHighLoadBackend(TabletSchedCtx tabletCtx) {
        ClusterLoadStatistic statistic = statisticMap.get(tabletCtx.getCluster());
        if (statistic == null) {
            return false;
        }
        
        Replica chosenReplica = null;
        double maxScore = 0;
        for (Replica replica : tabletCtx.getReplicas()) {
            BackendLoadStatistic beStatistic = statistic.getBackendLoadStatistic(replica.getBackendId());
            if (beStatistic == null) {
                continue;
            }
            if (beStatistic.getLoadScore(tabletCtx.getStorageMedium()) > maxScore) {
                maxScore = beStatistic.getLoadScore(tabletCtx.getStorageMedium());
                chosenReplica = replica;
            }
        }

        if (chosenReplica != null) {
            deleteReplicaInternal(tabletCtx, chosenReplica, "high load");
            return true;
        }
        return false;
    }

    private void deleteReplicaInternal(TabletSchedCtx tabletCtx, Replica replica, String reason) {
        // delete this replica from catalog.
        // it will also delete replica from tablet inverted index.
        tabletCtx.deleteReplica(replica);

        // write edit log
        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(tabletCtx.getDbId(),
                                                                     tabletCtx.getTblId(),
                                                                     tabletCtx.getPartitionId(),
                                                                     tabletCtx.getIndexId(),
                                                                     tabletCtx.getTabletId(),
                                                                     replica.getBackendId());

        Catalog.getInstance().getEditLog().logDeleteReplica(info);

        LOG.info("delete replica. tablet id: {}, backend id: {}. reason: {}",
                 tabletCtx.getTabletId(), replica.getBackendId(), reason);
    }

    /*
     * Cluster migration, which means the tablet has enough healthy replicas,
     * but some replicas are not in right cluster.
     * It is just same as 'replica missing'.
     * 
     * after clone finished, the replica in wrong cluster will be treated as redundant, and will be deleted soon.
     */
    private void handleReplicaClusterMigration(TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        stat.counterReplicaMissingInClusterErr.incrementAndGet();
        handleReplicaMissing(tabletCtx, batchTask);
    }

    /*
     * Try to select some alternative tablets for balance. Add them to pendingTablets with priority LOW,
     * and waiting to be scheduled.
     */
    private void selectTabletsForBalance() {
        if (Config.disable_balance) {
            LOG.info("balance is disabled. skip selecting tablets for balance");
            return;
        }
        
        long numOfBalancingTablets = getBalanceTabletsNumber();
        if (numOfBalancingTablets > MAX_BALANCING_TABLETS) {
            LOG.info("number of balancing tablets {} exceed limit: {}, skip selecting tablets for balance",
                    numOfBalancingTablets, MAX_BALANCING_TABLETS);
            return;
        }

        LoadBalancer loadBalancer = new LoadBalancer(statisticMap);
        List<TabletSchedCtx> alternativeTablets = loadBalancer.selectAlternativeTablets();
        for (TabletSchedCtx tabletCtx : alternativeTablets) {
            addTablet(tabletCtx, false);
        }
    }

    /*
     * Try to create a balance task for a tablet.
     */
    private void doBalance(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        stat.counterBalanceSchedule.incrementAndGet();
        LoadBalancer loadBalancer = new LoadBalancer(statisticMap);
        loadBalancer.createBalanceTask(tabletCtx, backendsWorkingSlots, batchTask);
    }

    // choose a path on a backend which is fit for the tablet
    private RootPathLoadStatistic chooseAvailableDestPath(TabletSchedCtx tabletCtx) throws SchedException {
        ClusterLoadStatistic statistic = statisticMap.get(tabletCtx.getCluster());
        if (statistic == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }
        List<BackendLoadStatistic> beStatistics = statistic.getSortedBeLoadStats(null /* sorted ignore medium */);

        // get all available paths which this tablet can fit in.
        // beStatistics is sorted by mix load score in ascend order, so select from first to last.
        List<RootPathLoadStatistic> allFitPaths = Lists.newArrayList();
        for (int i = 0; i < beStatistics.size(); i++) {
            BackendLoadStatistic bes = beStatistics.get(i);
            // exclude BE which already has replica of this tablet
            if (tabletCtx.containsBE(bes.getBeId())) {
                continue;
            }

            List<RootPathLoadStatistic> resultPaths = Lists.newArrayList();
            BalanceStatus st = bes.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(),
                    resultPaths, true /* is supplement */);
            if (!st.ok()) {
                LOG.debug("unable to find path for supplementing tablet: {}. {}", tabletCtx, st);
                continue;
            }

            Preconditions.checkState(resultPaths.size() == 1);
            allFitPaths.add(resultPaths.get(0));
        }

        if (allFitPaths.isEmpty()) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to find dest path for new replica");
        }

        // all fit paths has already been sorted by load score in 'allFitPaths' in ascend order.
        // just get first available path.
        // we try to find a path with specified media type, if not find, arbitrarily use one.
        for (RootPathLoadStatistic rootPathLoadStatistic : allFitPaths) {
            if (rootPathLoadStatistic.getStorageMedium() != tabletCtx.getStorageMedium()) {
                continue;
            }

            PathSlot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
            if (slot == null) {
                LOG.debug("backend {} does not found when getting slots", rootPathLoadStatistic.getBeId());
                continue;
            }

            if (slot.takeSlot(rootPathLoadStatistic.getPathHash()) != -1) {
                return rootPathLoadStatistic;
            }
        }

        // no root path with specified media type is found, get arbitrary one.
        for (RootPathLoadStatistic rootPathLoadStatistic : allFitPaths) {
            PathSlot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
            if (slot == null) {
                LOG.debug("backend {} does not found when getting slots", rootPathLoadStatistic.getBeId());
                continue;
            }

            if (slot.takeSlot(rootPathLoadStatistic.getPathHash()) != -1) {
                return rootPathLoadStatistic;
            }
        }
        
        throw new SchedException(Status.SCHEDULE_FAILED, "unable to find dest path which can be fit in");
    }

    /*
     * For some reason, a tablet info failed to be scheduled this time,
     * So we dynamically change its priority and add back to queue, waiting for next round.
     */
    private void dynamicAdjustPrioAndAddBackToPendingTablets(TabletSchedCtx tabletCtx, String message) {
        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.PENDING);
        tabletCtx.adjustPriority(stat);
        addTablet(tabletCtx, true /* force */);
    }

    private void finalizeTabletCtx(TabletSchedCtx tabletCtx, TabletSchedCtx.State state, String reason) {
        // use 2 steps to avoid nested database lock and synchronized.(releaseTabletCtx() may hold db lock)
        // remove the tablet ctx, so that no other process can see it
        removeTabletCtx(tabletCtx, reason);
        // release resources taken by tablet ctx
        releaseTabletCtx(tabletCtx, state);
    }

    private void releaseTabletCtx(TabletSchedCtx tabletCtx, TabletSchedCtx.State state) {
        tabletCtx.setState(state);
        tabletCtx.releaseResource(this);
        tabletCtx.setFinishedTime(System.currentTimeMillis());
    }

    private synchronized void removeTabletCtx(TabletSchedCtx tabletCtx, String reason) {
        runningTablets.remove(tabletCtx.getTabletId());
        allTabletIds.remove(tabletCtx.getTabletId());
        schedHistory.add(tabletCtx);
        LOG.info("remove the tablet {}. because: {}", tabletCtx.getTabletId(), reason);
    }


    // get next batch of tablets from queue.
    private synchronized List<TabletSchedCtx> getNextTabletCtxBatch() {
        List<TabletSchedCtx> list = Lists.newArrayList();
        int count = Math.max(MIN_BATCH_NUM, getCurrentAvailableSlotNum());
        while (count > 0) {
            TabletSchedCtx tablet = pendingTablets.poll();
            if (tablet == null) {
                // no more tablets
                break;
            }
            list.add(tablet);
            count--;
        }
        return list;
    }

    private int getCurrentAvailableSlotNum() {
        int total = 0;
        for (PathSlot pathSlot : backendsWorkingSlots.values()) {
            total += pathSlot.getTotalAvailSlotNum();
        }
        return total;
    }

    /*
     * return true if we want to remove the clone task from AgentTaskQueue
     */
    public boolean finishCloneTask(CloneTask cloneTask, TFinishTaskRequest request) {
        long tabletId = cloneTask.getTabletId();
        TabletSchedCtx tabletCtx = takeRunningTablets(tabletId);
        if (tabletCtx == null) {
            LOG.warn("tablet info does not exist: {}", tabletId);
            // tablet does not exist, no need to keep task.
            return true;
        }

        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.RUNNING, tabletCtx.getState());
        try {
            tabletCtx.finishCloneTask(cloneTask, request);
        } catch (SchedException e) {
            tabletCtx.increaseFailedRunningCounter();
            tabletCtx.setErrMsg(e.getMessage());
            if (e.getStatus() == Status.RUNNING_FAILED) {
                stat.counterCloneTaskFailed.incrementAndGet();
                addToRunningTablets(tabletCtx);
                return false;
            } else if (e.getStatus() == Status.UNRECOVERABLE) {
                // unrecoverable
                stat.counterTabletScheduledDiscard.incrementAndGet();
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getMessage());
                return true;
            } else if (e.getStatus() == Status.FINISHED) {
                // tablet is already healthy, just remove
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getMessage());
                return true;
            }
        } catch (Exception e) {
            LOG.warn("got unexpected exception when finish clone task. tablet: {}",
                    tabletCtx.getTabletId(), e);
            stat.counterTabletScheduledDiscard.incrementAndGet();
            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.UNEXPECTED, e.getMessage());
            return true;
        }

        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.FINISHED);
        stat.counterCloneTaskSucceeded.incrementAndGet();
        gatherStatistics(tabletCtx);
        finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, "finished");
        return true;
    }

    /*
     * Gather the running statistic of the task.
     * It will be evaluated for future strategy.  
     * This should only be called when the tablet is down with state FINISHED.
     */
    private void gatherStatistics(TabletSchedCtx tabletCtx) {
        if (tabletCtx.getCopySize() > 0 && tabletCtx.getCopyTimeMs() > 0) {
            if (tabletCtx.getSrcBackendId() != -1 && tabletCtx.getSrcPathHash() != -1) {
                PathSlot pathSlot = backendsWorkingSlots.get(tabletCtx.getSrcBackendId());
                if (pathSlot != null) {
                    pathSlot.updateStatistic(tabletCtx.getSrcPathHash(), tabletCtx.getCopySize(),
                            tabletCtx.getCopyTimeMs());
                }
            }

            if (tabletCtx.getDestBackendId() != -1 && tabletCtx.getDestPathHash() != -1) {
                PathSlot pathSlot = backendsWorkingSlots.get(tabletCtx.getDestBackendId());
                if (pathSlot != null) {
                    pathSlot.updateStatistic(tabletCtx.getDestPathHash(), tabletCtx.getCopySize(),
                            tabletCtx.getCopyTimeMs());
                }
            }
        }

        if (System.currentTimeMillis() - lastSlotAdjustTime < STAT_UPDATE_INTERVAL_MS) {
            return;
        }

        // TODO(cmy): update the slot num base on statistic.
        // need to find a better way to determine the slot number.

        lastSlotAdjustTime = System.currentTimeMillis();
    }

    /*
     * handle tablets which are running.
     * We should finished the task if
     * 1. Tablet is already healthy
     * 2. Task is timeout.
     * 
     * But here we just handle the timeout case here. Let the 'finishCloneTask()' check if tablet is healthy.
     * We guarantee that if tablet is in runningTablets, the 'finishCloneTask()' will finally be called,
     * so no need to worry that running tablets will never end.
     * This is also avoid nesting 'synchronized' and database lock.
     *
     * If task is timeout, remove the tablet.
     */
    public void handleRunningTablets() {
        // 1. remove the tablet ctx if timeout
        List<TabletSchedCtx> timeoutTablets = Lists.newArrayList();
        synchronized (this) {
            runningTablets.values().stream().filter(t -> t.isTimeout()).forEach(t -> {
                timeoutTablets.add(t);
            });

            for (TabletSchedCtx tabletSchedCtx : timeoutTablets) {
                removeTabletCtx(tabletSchedCtx, "timeout");
            }
        }
        
        // 2. release ctx
        timeoutTablets.stream().forEach(t -> {
            releaseTabletCtx(t, TabletSchedCtx.State.CANCELLED);
            stat.counterCloneTaskTimeout.incrementAndGet();
        });
    }

    public List<List<String>> getPendingTabletsInfo(int limit) {
        List<TabletSchedCtx> tabletCtxs = getCopiedTablets(pendingTablets, limit);
        return collectTabletCtx(tabletCtxs);
    }

    public List<List<String>> getRunningTabletsInfo(int limit) {
        List<TabletSchedCtx> tabletCtxs = getCopiedTablets(runningTablets.values(), limit);
        return collectTabletCtx(tabletCtxs);
    }

    public List<List<String>> getHistoryTabletsInfo(int limit) {
        List<TabletSchedCtx> tabletCtxs = getCopiedTablets(schedHistory, limit);
        return collectTabletCtx(tabletCtxs);
    }

    private List<List<String>> collectTabletCtx(List<TabletSchedCtx> tabletCtxs) {
        List<List<String>> result = Lists.newArrayList();
        tabletCtxs.stream().forEach(t -> {
            result.add(t.getBrief());
        });
        return result;
    }

    private synchronized List<TabletSchedCtx> getCopiedTablets(Collection<TabletSchedCtx> source, int limit) {
        List<TabletSchedCtx> tabletCtxs = Lists.newArrayList();
        source.stream().limit(limit).forEach(t -> {
            tabletCtxs.add(t);
        });
        return tabletCtxs;
    }

    public synchronized int getPendingNum() {
        return pendingTablets.size();
    }

    public synchronized int getRunningNum() {
        return runningTablets.size();
    }

    public synchronized int getHistoryNum() {
        return schedHistory.size();
    }

    public synchronized int getTotalNum() {
        return allTabletIds.size();
    }

    public synchronized long getBalanceTabletsNumber() {
        return pendingTablets.stream().filter(t -> t.getType() == Type.BALANCE).count()
                + runningTablets.values().stream().filter(t -> t.getType() == Type.BALANCE).count();
    }

    /*
     * PathSlot keeps track of slot num per path of a Backend.
     * Each path on a Backend has several slot.
     * If a path's available slot num become 0, no task should be assigned to this path.
     */
    public static class PathSlot {
        // path hash -> slot num
        private Map<Long, Slot> pathSlots = Maps.newConcurrentMap();

        public PathSlot(List<Long> paths, int initSlotNum) {
            for (Long pathHash : paths) {
                pathSlots.put(pathHash, new Slot(initSlotNum));
            }
        }

        // update the path
        public synchronized void updatePaths(List<Long> paths) {
            // delete non exist path
            Iterator<Map.Entry<Long, Slot>> iter = pathSlots.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Slot> entry = iter.next();
                if (!paths.contains(entry.getKey())) {
                    iter.remove();
                }
            }

            // add new path
            for (Long pathHash : paths) {
                if (!pathSlots.containsKey(pathHash)) {
                    pathSlots.put(pathHash, new Slot(Config.schedule_slot_num_per_path));
                }
            }
        }

        // Update the total slots num of specified paths, increase or decrease
        public synchronized void updateSlot(List<Long> pathHashs, int delta) {
            for (Long pathHash : pathHashs) {
                Slot slot = pathSlots.get(pathHash);
                if (slot == null) {
                    continue;
                }

                slot.total += delta;
                slot.rectify();
                LOG.debug("decrease path {} slots num to {}", pathHash, pathSlots.get(pathHash).total);
            }
        }

        /*
         * Update the statistic of specified path
         */
        public synchronized void updateStatistic(long pathHash, long copySize, long copyTimeMs) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            slot.totalCopySize += copySize;
            slot.totalCopyTimeMs += copyTimeMs;
        }

        /*
         * If the specified 'pathHash' has available slot, decrease the slot number and return this path hash
         */
        public synchronized long takeSlot(long pathHash) throws SchedException {
            if (pathHash == -1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("path hash is not set.", new Exception());
                }
                throw new SchedException(Status.SCHEDULE_FAILED, "path hash is not set");
            }

            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            slot.rectify();
            if (slot.available <= 0) {
                return -1;
            }
            slot.available--;
            return pathHash;
        }

        public synchronized void freeSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            slot.available++;
            slot.rectify();
        }

        public synchronized int peekSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            slot.rectify();
            return slot.available;
        }

        public synchronized int getTotalAvailSlotNum() {
            int total = 0;
            for (Slot slot : pathSlots.values()) {
                total += slot.available;
            }
            return total;
        }

        /*
         * get path whose balance slot num is larger than 0
         */
        public synchronized Set<Long> getAvailPathsForBalance() {
            Set<Long> pathHashs = Sets.newHashSet();
            for (Map.Entry<Long, Slot> entry : pathSlots.entrySet()) {
                if (entry.getValue().balanceSlot > 0) {
                    pathHashs.add(entry.getKey());
                }
            }
            return pathHashs;
        }

        public synchronized int getAvailBalanceSlotNum() {
            int num = 0;
            for (Map.Entry<Long, Slot> entry : pathSlots.entrySet()) {
                num += entry.getValue().balanceSlot;
            }
            return num;
        }

        public synchronized List<List<String>> getSlotInfo(long beId) {
            List<List<String>> results = Lists.newArrayList();
            pathSlots.entrySet().stream().forEach(t -> {
                t.getValue().rectify();
                List<String> result = Lists.newArrayList();
                result.add(String.valueOf(beId));
                result.add(String.valueOf(t.getKey()));
                result.add(String.valueOf(t.getValue().available));
                result.add(String.valueOf(t.getValue().total));
                result.add(String.valueOf(t.getValue().balanceSlot));
                result.add(String.valueOf(t.getValue().getAvgRate()));
                results.add(result);
            });
            return results;
        }

        public synchronized long takeBalanceSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            if (slot.balanceSlot > 0) {
                slot.balanceSlot--;
                return pathHash;
            }
            return -1;
        }

        public synchronized long takeAnAvailBalanceSlotFrom(Set<Long> pathHashs) {
            for (Long pathHash : pathHashs) {
                Slot slot = pathSlots.get(pathHash);
                if (slot == null) {
                    continue;
                }
                if (slot.balanceSlot > 0) {
                    slot.balanceSlot--;
                    return pathHash;
                }
            }
            return -1;
        }

        public void freeBalanceSlot(long destPathHash) {
            Slot slot = pathSlots.get(destPathHash);
            if (slot == null) {
                return;
            }
            slot.balanceSlot++;
            slot.rectify();
        }
    }

    public List<List<String>> getSlotsInfo() {
        List<List<String>> result = Lists.newArrayList();
        for (long beId : backendsWorkingSlots.keySet()) {
            PathSlot slot = backendsWorkingSlots.get(beId);
            result.addAll(slot.getSlotInfo(beId));
        }
        return result;
    }

    public static class Slot {
        public int total;
        public int available;
        // slot reserved for balance
        public int balanceSlot;

        public long totalCopySize = 0;
        public long totalCopyTimeMs = 0;

        public Slot(int total) {
            this.total = total;
            this.available = total;
            this.balanceSlot = BALANCE_SLOT_NUM_FOR_PATH;
        }

        public void rectify() {
            if (total <= 0) {
                total = 1;
            }
            if (available > total) {
                available = total;
            }

            if (balanceSlot > BALANCE_SLOT_NUM_FOR_PATH) {
                balanceSlot = BALANCE_SLOT_NUM_FOR_PATH;
            }
        }

        // return avg rate, Bytes/S
        public double getAvgRate() {
            if (totalCopyTimeMs / 1000 == 0) {
                return 0.0;
            }
            return totalCopySize / ((double) totalCopyTimeMs / 1000);
        }
    }
}
