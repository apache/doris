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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.TabletInfo.Priority;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    // this is a init #working slot per path in a backend.
    private static final int SLOT_NUM_PER_PATH = 1;

    // handle at most BATCH_NUM tablets in one loop
    private static final int BATCH_NUM = 10;

    // the minimum interval of updating cluster statistics and priority of tablet info
    private static final long STAT_UPDATE_INTERVAL_MS = 60 * 1000; // 1min

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
    private PriorityQueue<TabletInfo> pendingTablets = new PriorityQueue<>();
    private Set<Long> allTabletIds = Sets.newHashSet();
    // contains all tabletInfos which state are RUNNING
    private Map<Long, TabletInfo> runningTablets = Maps.newHashMap();
    // save the latest 1000 scheduled tablet info
    private Queue<TabletInfo> schedHistory = EvictingQueue.create(1000);

    // be id -> #working slots
    private Map<Long, Slot> backendsWorkingSlots = Maps.newConcurrentMap();
    // cluster name -> load statistic
    private Map<String, ClusterLoadStatistic> statisticMap = Maps.newConcurrentMap();
    private long lastStatUpdateTime = 0;
    
    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;


    public TabletScheduler(Catalog catalog, SystemInfoService infoService, TabletInvertedIndex invertedIndex) {
        super("tablet scheduler", 5000);
        this.catalog = catalog;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    /*
     * update working slots at the beginning of each round
     */
    private boolean updateWorkingSlots() {
        ImmutableMap<Long, Backend> backends = infoService.getBackendsInCluster(null);
        for (Backend backend : backends.values()) {
            if (!backend.hasPathHash()) {
                // when upgrading, backend may not get path info yet. so return false and wait for next round.
                LOG.info("not all backends have path info");
                return false;
            }
        }

        // update exist backends
        Set<Long> deletedBeIds = Sets.newHashSet();
        for (Long beId : backendsWorkingSlots.keySet()) {
            if (backends.containsKey(beId)) {
                List<Long> pathHashes = backends.get(beId).getDisks().values().stream().map(v -> v.getPathHash()).collect(Collectors.toList());
                backendsWorkingSlots.get(beId).updateSlots(pathHashes);
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
                Slot slot = new Slot(pathHashes, SLOT_NUM_PER_PATH);
                backendsWorkingSlots.put(be.getId(), slot);
                LOG.info("add new backend {} with slots num: {}", be.getId(), be.getDisks().size());
            }
        }

        return true;
    }

    public Map<Long, Slot> getBackendsWorkingSlots() {
        return backendsWorkingSlots;
    }

    /*
     * add a ready-to-be-scheduled tablet to pendingTablets, if it has not being added before.
     * if force is true, do not check if tablet is already added before.
     */
    public synchronized boolean addTablet(TabletInfo tablet, boolean force) {
        if (!force && allTabletIds.contains(tablet.getTabletId())) {
            return false;
        }
        allTabletIds.add(tablet.getTabletId());
        pendingTablets.offer(tablet);
        return true;
    }

    public synchronized boolean isEmpty() {
        return pendingTablets.isEmpty();
    }

    /*
     * Iterate current tablets, change their priority if necessary.
     */
    public synchronized void changePriorityOfTablets(long dbId, long tblId, List<Long> partitionIds) {
        PriorityQueue<TabletInfo> newPendingTablets = new PriorityQueue<>();
        for (TabletInfo tabletInfo : pendingTablets) {
            if (tabletInfo.getDbId() == dbId && tabletInfo.getTblId() == tblId
                    && partitionIds.contains(tabletInfo.getPartitionId())) {
                tabletInfo.setOrigPriority(Priority.VERY_HIGH);
            }
            newPendingTablets.add(tabletInfo);
        }
        pendingTablets = newPendingTablets;
    }

    /*
     * TabletScheduler will run as a daemon thread at a very short interval(default 5 sec)
     * It will try to schedule the tablet in queue.
     * and also try to balance the cluster if possible.
     * 
     * Schedule rules:
     * 1. tablet with higher priority will be scheduled first.
     * 2. high priority should be downgraded if it fails to be schedule too many times.
     * 3. priority may be upgraded if it is not being schedule for a long time.
     * 4. every pending task should has a max scheduled time, if schedule fails too many times, if should be removed.
     * 5. every running task should has a timeout, to avoid running forever.
     * 6. every running task should also has a max failure time, if clone task fails too many times, if should be removed.
     */
    @Override
    protected void runOneCycle() {
        if (!updateWorkingSlots()) {
            return;
        }

        updateClusterLoadStatisticsAndPriorityIfNecessary();

        schedulePendingTablets();

        handleRunningTablets();

        doBalance();
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
        statisticMap.clear();
        List<String> clusterNames = infoService.getClusterNames();
        for (String clusterName : clusterNames) {
            ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(catalog, infoService,
                    invertedIndex);
            clusterLoadStatistic.init(clusterName);
            statisticMap.put(clusterName, clusterLoadStatistic);
            LOG.info("update cluster {} load statistic:\n {}", clusterName, clusterLoadStatistic.getBrief());
        }
    }

    /*
     * adjust priorities of all tablet infos
     */
    private synchronized void adjustPriorities() {
        int size = pendingTablets.size();
        TabletInfo tabletInfo = null;
        for (int i = 0; i < size; i++) {
            tabletInfo = pendingTablets.poll();
            if (tabletInfo == null) {
                break;
            }

            tabletInfo.adjustPriority();
            pendingTablets.add(tabletInfo);
        }
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
        List<TabletInfo> currentBatch = getNextTabletInfoBatch();
        LOG.debug("get {} tablets info to schedule", currentBatch.size());

        AgentBatchTask batchTask = new AgentBatchTask();
        for (TabletInfo tabletInfo : currentBatch) {
            try {
                scheduleTablet(tabletInfo, batchTask);
            } catch (SchedException e) {
                tabletInfo.increaseFailedSchedCounter();
                if (e.getStatus() == Status.SCHEDULE_FAILED) {
                    // adjust priority to avoid some higher priority always be the first in pendingTablets
                    dynamicAdjustPrioAndAddBackToPendingTablets(tabletInfo, e.getMessage());
                } else {
                    // discard
                    removeTabletInfo(tabletInfo, e.getMessage());
                }
            }

            Preconditions.checkState(tabletInfo.getState() == TabletInfo.State.RUNNING);
            addToRunningTablets(tabletInfo);
        }

        // must send task after adding tablet info to runningTablets.
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
            LOG.info("add clone task to agent task queue: {}", task);
        }
    }

    private synchronized void addToRunningTablets(TabletInfo tabletInfo) {
        runningTablets.put(tabletInfo.getTabletId(), tabletInfo);
    }

    /*
     * Try to schedule a single tablet.
     */
    private void scheduleTablet(TabletInfo tabletInfo, AgentBatchTask batchTask) throws SchedException {
        tabletInfo.setLastSchedTime(System.currentTimeMillis());

        Database db = catalog.getDb(tabletInfo.getDbId());
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db does not exist");
        }

        Pair<TabletStatus, TabletInfo.Priority> statusPair = null;
        db.writeLock();
        try {
            // check this tablet again
            OlapTable tbl = (OlapTable) db.getTable(tabletInfo.getTblId());
            if (tbl == null) {
                throw new SchedException(Status.UNRECOVERABLE, "tbl does not exist");
            }

            Partition partition = tbl.getPartition(tabletInfo.getPartitionId());
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
            }

            MaterializedIndex idx = partition.getIndex(tabletInfo.getIndexId());
            if (idx == null) {
                throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
            }

            Tablet tablet = idx.getTablet(tabletInfo.getTabletId());
            Preconditions.checkNotNull(tablet);

            statusPair = tablet.getHealthStatusWithPriority(
                                               infoService, tabletInfo.getCluster(),
                                               partition.getCommittedVersion(),
                                               partition.getCommittedVersionHash(),
                                               tbl.getPartitionInfo().getReplicationNum(partition.getId()));

            if (statusPair.first == TabletStatus.HEALTHY) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet is healthy");
            }

            // we do not concern priority here. once we take the tablet out of priority queue,
            // priority is meaningless.

            tabletInfo.setTablet(tablet);
            tabletInfo.setVersionInfo(partition.getVisibleVersion(), partition.getVisibleVersionHash(),
                                      partition.getCommittedVersion(), partition.getCommittedVersionHash());

            handleTabletByStatus(statusPair.first, tabletInfo, batchTask);
        } finally {
            db.writeUnlock();
        }
    }

    private void handleTabletByStatus(TabletStatus status, TabletInfo tabletInfo, AgentBatchTask batchTask)
            throws SchedException {
        switch (status) {
            case REPLICA_MISSING:
                handleReplicaMissing(tabletInfo, batchTask);
                break;
            case VERSION_INCOMPLETE:
                handleReplicaVersionIncomplete(tabletInfo, batchTask);
                break;
            case REDUNDANT:
                handleRedundantReplica(tabletInfo);
                break;
            case REPLICA_MISSING_IN_CLUSTER:
                handleReplicaClusterMigration(tabletInfo, batchTask);
                break;
            default:
                break;
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
    private void handleReplicaMissing(TabletInfo tabletInfo, AgentBatchTask batchTask) throws SchedException {
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletInfo);
        Preconditions.checkNotNull(destPath);
        tabletInfo.setDestination(destPath.getBeId(), destPath.getPathHash());

        // choose a source replica for cloning from
        tabletInfo.chooseSrcReplica(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletInfo.createCloneReplicaAndTask());
    }

    /*
     * Replica version is incomplete, which means this replica is missing some version,
     * and need to be cloned from a healthy replica, in-place.
     * 
     * 1. find the incomplete replica as destination replica
     * 2. find a healthy replica as source replica
     * 3. send clone task
     */
    private void handleReplicaVersionIncomplete(TabletInfo tabletInfo, AgentBatchTask batchTask)
            throws SchedException {
        ClusterLoadStatistic statistic = statisticMap.get(tabletInfo.getCluster());
        if (statistic == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }

        tabletInfo.chooseDestReplicaForVersionIncomplete(backendsWorkingSlots);
        tabletInfo.chooseSrcReplicaForVersionIncomplete(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletInfo.createCloneReplicaAndTask());
    }

    /*
     *  replica is redundant, which means there are more replicas than we expected, which need to be dropped.
     *  we just drop one redundant replica at a time, for safety reason.
     *  choosing a replica to drop base on following priority:
     *  1. backend has been dropped
     *  2. backend is not available
     *  3. replica's state is CLONE
     *  4. replica's last failed version > 0
     *  5. replica with lower version
     *  6. replica not in right cluster
     *  7. replica in higher load backend
     */
    private void handleRedundantReplica(TabletInfo tabletInfo) throws SchedException {
        if (deleteBackendDropped(tabletInfo)
                || deleteBackendUnavailable(tabletInfo)
                || deleteCloneReplica(tabletInfo)
                || deleteReplicaWithFailedVersion(tabletInfo)
                || deleteReplicaWithLowerVersion(tabletInfo)
                || deleteReplicaNotInCluster(tabletInfo)
                || deleteReplicaOnHighLoadBackend(tabletInfo)) {
            return;
        }
        throw new SchedException(Status.SCHEDULE_FAILED, "unable to delete any redundant replicas");
    }

    private boolean deleteBackendDropped(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            long beId = replica.getBackendId();
            if (infoService.getBackend(beId) == null) {
                deleteReplicaInternal(tabletInfo, replica, "backend dropped");
                return true;
            }
        }
        return false;
    }

    private boolean deleteBackendUnavailable(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.isAvailable()) {
                deleteReplicaInternal(tabletInfo, replica, "backend unavailable");
                return true;
            }
        }
        return false;
    }

    private boolean deleteCloneReplica(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            if (replica.getState() == ReplicaState.CLONE) {
                deleteReplicaInternal(tabletInfo, replica, "clone state");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithFailedVersion(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            if (replica.getLastFailedVersion() > 0) {
                deleteReplicaInternal(tabletInfo, replica, "version incomplete");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithLowerVersion(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            if (!replica.checkVersionCatchUp(tabletInfo.getCommittedVersion(), tabletInfo.getCommittedVersionHash())) {
                deleteReplicaInternal(tabletInfo, replica, "lower version");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaNotInCluster(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.getOwnerClusterName().equals(tabletInfo.getCluster())) {
                deleteReplicaInternal(tabletInfo, replica, "not in cluster");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaOnHighLoadBackend(TabletInfo tabletInfo) {
        ClusterLoadStatistic statistic = statisticMap.get(tabletInfo.getCluster());
        if (statistic == null) {
            return false;
        }
        
        Replica chosenReplica = null;
        double maxScore = 0;
        for (Replica replica : tabletInfo.getReplicas()) {
            BackendLoadStatistic beStatistic = statistic.getBackendLoadStatistic(replica.getBackendId());
            if (beStatistic == null) {
                continue;
            }
            if (beStatistic.getLoadScore() > maxScore) {
                maxScore = beStatistic.getLoadScore();
                chosenReplica = replica;
            }
        }

        if (chosenReplica != null) {
            deleteReplicaInternal(tabletInfo, chosenReplica, "high load");
            return true;
        }
        return false;
    }

    private void deleteReplicaInternal(TabletInfo tabletInfo, Replica replica, String reason) {
        // delete this replica from catalog.
        // it will also delete replica from tablet inverted index.
        tabletInfo.deleteReplica(replica);

        // TODO(cmy): this should be removed after I finish modifying alter job logic
        // Catalog.getInstance().handleJobsWhenDeleteReplica(tabletInfo.getTblId(), tabletInfo.getPartitionId(),
        //                                                   tabletInfo.getIndexId(), tabletInfo.getTabletId(),
        //                                                   replica.getId(), replica.getBackendId());

        // write edit log
        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(tabletInfo.getDbId(),
                                                                     tabletInfo.getTblId(),
                                                                     tabletInfo.getPartitionId(),
                                                                     tabletInfo.getIndexId(),
                                                                     tabletInfo.getTabletId(),
                                                                     replica.getBackendId());

        Catalog.getInstance().getEditLog().logDeleteReplica(info);

        LOG.info("delete replica. tablet id: {}, backend id: {}. reason: {}",
                 tabletInfo.getTabletId(), replica.getBackendId(), reason);
    }

    /*
     * Cluster migration, which means the tablet has enough healthy replicas,
     * but some replicas are not in right cluster.
     * It is just same as 'replica missing'.
     * 
     * after clone finished, the replica in wrong cluster will be treated as redundant, and will be deleted soon.
     */
    private void handleReplicaClusterMigration(TabletInfo tabletInfo, AgentBatchTask batchTask)
            throws SchedException {
        handleReplicaMissing(tabletInfo, batchTask);
    }

    private void doBalance() {
        // TODO(cmy)
    }

    // choose a path on a backend which is fit for the tablet
    private RootPathLoadStatistic chooseAvailableDestPath(TabletInfo tabletInfo) throws SchedException {
        ClusterLoadStatistic statistic = statisticMap.get(tabletInfo.getCluster());
        if (statistic == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }
        List<BackendLoadStatistic> beStatistics = statistic.getBeLoadStatistics();

        // get all available paths which this tablet can fit in.
        List<RootPathLoadStatistic> allFitPaths = Lists.newArrayList();
        for (int i = 0; i < beStatistics.size(); i++) {
            BackendLoadStatistic bes = beStatistics.get(i);
            // exclude BE which already has replica of this tablet
            if (tabletInfo.containsBE(bes.getBeId())) {
                continue;
            }

            List<RootPathLoadStatistic> resultPaths = Lists.newArrayList();
            BalanceStatus st = bes.isFit(tabletInfo.getTabletSize(), resultPaths, true /* is supplement */);
            if (!st.ok()) {
                LOG.debug("unable to find path for supplementing tablet: {}. {}", tabletInfo, st);
                continue;
            }

            Preconditions.checkState(resultPaths.size() == 1);
            allFitPaths.add(resultPaths.get(0));
        }

        if (allFitPaths.isEmpty()) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to find dest path for new replica");
        }

        // all fit paths has already been sorted by load score in 'allFitPaths'.
        // just get first available path.
        for (RootPathLoadStatistic rootPathLoadStatistic : allFitPaths) {
            Slot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
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
    private void dynamicAdjustPrioAndAddBackToPendingTablets(TabletInfo tabletInfo, String message) {
        Preconditions.checkState(tabletInfo.getState() == TabletInfo.State.PENDING);
        tabletInfo.adjustPriority();
        addTablet(tabletInfo, true /* force */);
    }

    private synchronized void removeTabletInfo(TabletInfo tabletInfo, String reason) {
        tabletInfo.setState(TabletInfo.State.CANCELLED, reason);
        tabletInfo.releaseResource(this);
        allTabletIds.remove(tabletInfo.getTabletId());
        schedHistory.add(tabletInfo);
        LOG.info("remove the tablet info {} because: {}", tabletInfo.getTabletId(), reason);
    }

    // get at most BATCH_NUM tablets from queue.
    private synchronized List<TabletInfo> getNextTabletInfoBatch() {
        List<TabletInfo> list = Lists.newArrayList();
        int count = BATCH_NUM;
        while (count > 0) {
            TabletInfo tablet = pendingTablets.poll();
            if (tablet == null) {
                // no more tablets
                break;
            }
            list.add(tablet);
            count--;
        }
        return list;
    }

    public void finishCloneTask(CloneTask cloneTask, TTabletInfo reportedTablet) {
        long tabletId = cloneTask.getTabletId();
        TabletInfo tabletInfo = takeRunningTablets(tabletId);
        if (tabletInfo == null) {
            LOG.warn("tablet info does not exist: {}", tabletId);
            return;
        }
        Preconditions.checkState(tabletInfo.getState() == TabletInfo.State.RUNNING);
        try {
            tabletInfo.finishCloneTask(cloneTask, reportedTablet);
        } catch (SchedException e) {
            tabletInfo.increaseFailedRunningCounter();
            if (e.getStatus() == Status.SCHEDULE_FAILED) {
                addToRunningTablets(tabletInfo);
            } else {
                // discard
                removeTabletInfo(tabletInfo, e.getMessage());
            }
            return;
        }

        Preconditions.checkState(tabletInfo.getState() == TabletInfo.State.FINISHED);
        removeTabletInfo(tabletInfo, "finished");
    }

    /*
     * we take the tablet out of the runningTablets and than handle it,
     * avoid other threads see it.
     * Whoever takes this tablet, make sure to put it to the schedHistory or back to runningTablets.
     */
    private synchronized TabletInfo takeRunningTablets(long tabletId) {
        return runningTablets.remove(tabletId);
    }

    /*
     * handle tablets which are running.
     * If task is timeout, remove the tablet
     */
    public synchronized void handleRunningTablets() {
        Iterator<Map.Entry<Long, TabletInfo>> iter = runningTablets.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, TabletInfo> entry = iter.next();
            TabletInfo tabletInfo = entry.getValue();
            if (tabletInfo.isTimeout()) {
                tabletInfo.setState(TabletInfo.State.CANCELLED, "timeout");
            }
            iter.remove();
            tabletInfo.releaseResource(this);
            schedHistory.add(tabletInfo);
            LOG.info("tablet info is timeout: {}", tabletInfo.getTabletId());
        }
    }

    public class Slot {
        // path hash -> slot num
        private Map<Long, Integer> pathSlots = Maps.newConcurrentMap();

        public Slot(List<Long> paths, int initSlotNum) {
            for (Long pathHash : paths) {
                pathSlots.put(pathHash, initSlotNum);
            }
        }

        public synchronized void updateSlots(List<Long> paths) {
            // delete non exist path
            Iterator<Map.Entry<Long, Integer>> iter = pathSlots.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Integer> entry = iter.next();
                if (!paths.contains(entry.getKey())) {
                    iter.remove();
                }
            }

            // add new path
            for (Long pathHash : paths) {
                if (!pathSlots.containsKey(pathHash)) {
                    pathSlots.put(pathHash, SLOT_NUM_PER_PATH);
                }
            }
        }

        /*
         * If the specified 'pathHash' has available slot, decrease the slot number and return this path hash
         */
        public synchronized long takeSlot(long pathHash) {
            Preconditions.checkArgument(pathHash != -1);
            Integer slot = pathSlots.get(pathHash);
            if (slot == null || slot <= 0) {
                return -1;
            }
            
            pathSlots.put(pathHash, slot - 1);
            return pathHash;
        }

        public synchronized void freeSlot(long pathHash) {
            Integer slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            
            pathSlots.put(pathHash, slot + 1);
        }

        public synchronized int peekSlot(long pathHash) {
            Integer slot = pathSlots.get(pathHash);
            return slot == null ? -1 : slot;
        }
    }
}
