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

import org.apache.doris.analysis.AdminCancelRebalanceDiskStmt;
import org.apache.doris.analysis.AdminRebalanceDiskStmt;
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.BackendLoadStatistic.BePathLoadStatPair;
import org.apache.doris.clone.BackendLoadStatistic.BePathLoadStatPairComparator;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.SchedException.SubCode;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletSchedCtx.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
import org.apache.doris.task.DropReplicaTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.transaction.DatabaseTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/**
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
public class TabletScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletScheduler.class);

    // handle at most BATCH_NUM tablets in one loop
    private static final int MIN_BATCH_NUM = 50;

    // the minimum interval of updating cluster statistics and priority of tablet info
    private static final long STAT_UPDATE_INTERVAL_MS = 20 * 1000; // 20s

    /*
     * Tablet is added to pendingTablets as well it's id in allTabletTypes.
     * TabletScheduler will take tablet from pendingTablets but will not remove it's id from allTabletTypes when
     * handling a tablet.
     * Tablet' id can only be removed after the clone task or migration task is done(timeout, cancelled or finished).
     * So if a tablet's id is still in allTabletTypes, TabletChecker can not add tablet to TabletScheduler.
     *
     * pendingTablets + runningTablets = allTabletTypes
     *
     * pendingTablets, allTabletTypes, runningTablets and schedHistory are protected by 'synchronized'
     */
    private PriorityQueue<TabletSchedCtx> pendingTablets = new PriorityQueue<>();
    private Map<Long, TabletSchedCtx.Type> allTabletTypes = Maps.newHashMap();
    // contains all tabletCtxs which state are RUNNING
    private Map<Long, TabletSchedCtx> runningTablets = Maps.newHashMap();
    // save the latest 1000 scheduled tablet info
    private Queue<TabletSchedCtx> schedHistory = EvictingQueue.create(1000);

    // be id -> #working slots
    private Map<Long, PathSlot> backendsWorkingSlots = Maps.newConcurrentMap();
    // Tag -> load statistic
    private Map<Tag, LoadStatisticForTag> statisticMap = Maps.newHashMap();

    private long lastStatUpdateTime = 0;

    private long lastSlotAdjustTime = 0;

    private Env env;
    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;
    private ColocateTableIndex colocateTableIndex;
    private TabletSchedulerStat stat;
    private Rebalancer rebalancer;
    private Rebalancer diskRebalancer;

    // result of adding a tablet to pendingTablets
    public enum AddResult {
        ADDED, // success to add
        ALREADY_IN, // already added, skip
        LIMIT_EXCEED, // number of pending tablets exceed the limit
        DISABLED // scheduler has been disabled.
    }

    public TabletScheduler(Env env, SystemInfoService infoService, TabletInvertedIndex invertedIndex,
                           TabletSchedulerStat stat, String rebalancerType) {
        super("tablet scheduler", Config.tablet_schedule_interval_ms);
        this.env = env;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
        this.colocateTableIndex = env.getColocateTableIndex();
        this.stat = stat;
        if (rebalancerType.equalsIgnoreCase("partition")) {
            this.rebalancer = new PartitionRebalancer(infoService, invertedIndex, backendsWorkingSlots);
        } else {
            this.rebalancer = new BeLoadRebalancer(infoService, invertedIndex, backendsWorkingSlots);
        }
        // if rebalancer can not get new task, then use diskRebalancer to get task
        this.diskRebalancer = new DiskRebalancer(infoService, invertedIndex, backendsWorkingSlots);
    }

    public TabletSchedulerStat getStat() {
        return stat;
    }

    public Rebalancer getRebalancer() {
        return rebalancer;
    }

    /*
     * update working slots at the beginning of each round
     */
    private boolean updateWorkingSlots() {
        ImmutableMap<Long, Backend> backends = infoService.getAllBackendsMap();
        for (Backend backend : backends.values()) {
            if (!backend.hasPathHash() && backend.isAlive()) {
                // when upgrading, backend may not get path info yet. so return false and wait for next round.
                // and we should check if backend is alive. If backend is dead when upgrading, this backend
                // will never report its path hash, and tablet scheduler is blocked.
                LOG.info("backend {}:{} with id {} doesn't have path info.", backend.getHost(),
                        backend.getBePort(), backend.getId());
                return false;
            }
        }

        // update exist backends
        Set<Long> deletedBeIds = Sets.newHashSet();
        for (Long beId : backendsWorkingSlots.keySet()) {
            if (backends.containsKey(beId)) {
                Map<Long, TStorageMedium> paths = Maps.newHashMap();
                backends.get(beId).getDisks().values().stream()
                        .filter(v -> v.getState() == DiskState.ONLINE)
                        .forEach(v -> paths.put(v.getPathHash(), v.getStorageMedium()));
                backendsWorkingSlots.get(beId).updatePaths(paths);
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
                Map<Long, TStorageMedium> paths = Maps.newHashMap();
                be.getDisks().values().stream()
                        .filter(v -> v.getState() == DiskState.ONLINE)
                        .forEach(v -> paths.put(v.getPathHash(), v.getStorageMedium()));
                PathSlot slot = new PathSlot(paths, be.getId());
                backendsWorkingSlots.put(be.getId(), slot);
                LOG.info("add new backend {} with slots num: {}", be.getId(), be.getDisks().size());
            }
        }

        return true;
    }

    public Map<Long, PathSlot> getBackendsWorkingSlots() {
        return backendsWorkingSlots;
    }

    /**
     * add a ready-to-be-scheduled tablet to pendingTablets, if it has not being added before.
     * if force is true, do not check if tablet is already added before.
     */
    public synchronized AddResult addTablet(TabletSchedCtx tablet, boolean force) {
        if (!force && Config.disable_tablet_scheduler) {
            return AddResult.DISABLED;
        }

        // REPAIR has higher priority than BALANCE.
        // Suppose adding a BALANCE tablet successfully, then adding this tablet's REPAIR ctx will fail.
        // But we set allTabletTypes[tabletId] to REPAIR. Later at the beginning of scheduling this tablet,
        // it will reset its type as allTabletTypes[tabletId], so its type will convert to REPAIR.

        long tabletId = tablet.getTabletId();
        boolean contains = allTabletTypes.containsKey(tabletId);
        if (contains && !force) {
            if (tablet.getType() == TabletSchedCtx.Type.REPAIR) {
                allTabletTypes.put(tabletId, TabletSchedCtx.Type.REPAIR);
            }
            return AddResult.ALREADY_IN;
        }

        // if this is not a force add,
        // and number of scheduling tablets exceed the limit,
        // refuse to add.
        if (!force && (pendingTablets.size() > Config.max_scheduling_tablets
                || runningTablets.size() > Config.max_scheduling_tablets)) {
            return AddResult.LIMIT_EXCEED;
        }

        if (!contains || tablet.getType() == TabletSchedCtx.Type.REPAIR) {
            allTabletTypes.put(tabletId, tablet.getType());
        }

        pendingTablets.offer(tablet);
        if (!contains) {
            LOG.info("Add tablet to pending queue, {}", tablet);
        }

        return AddResult.ADDED;
    }

    public synchronized boolean containsTablet(long tabletId) {
        return allTabletTypes.containsKey(tabletId);
    }

    public synchronized void rebalanceDisk(AdminRebalanceDiskStmt stmt) {
        diskRebalancer.addPrioBackends(stmt.getBackends(), stmt.getTimeoutS());
    }

    public synchronized void cancelRebalanceDisk(AdminCancelRebalanceDiskStmt stmt) {
        diskRebalancer.removePrioBackends(stmt.getBackends());
    }

    /**
     * Iterate current tablets, change their priority to VERY_HIGH if necessary.
     */
    public synchronized void changeTabletsPriorityToVeryHigh(long dbId, long tblId, List<Long> partitionIds) {
        PriorityQueue<TabletSchedCtx> newPendingTablets = new PriorityQueue<>();
        for (TabletSchedCtx tabletCtx : pendingTablets) {
            if (tabletCtx.getDbId() == dbId && tabletCtx.getTblId() == tblId
                    && partitionIds.contains(tabletCtx.getPartitionId())) {
                tabletCtx.setPriority(Priority.VERY_HIGH);
            }
            newPendingTablets.add(tabletCtx);
        }
        pendingTablets = newPendingTablets;
    }

    /**
     * TabletScheduler will run as a daemon thread at a very short interval(default 5 sec)
     * Firstly, it will try to update cluster load statistic and check if priority need to be adjusted.
     * Then, it will schedule the tablets in pendingTablets.
     * Thirdly, it will check the current running tasks.
     * Finally, it try to balance the cluster if possible.
     *
     * Schedule rules:
     * 1. tablet with higher priority will be scheduled first.
     * 2. high priority should be downgraded if it fails to be schedule too many times.
     * 3. priority may be upgraded if it is not being schedule for a long time.
     * 4. every pending task should has a max scheduled time, if schedule fails too many times, if should be removed.
     * 5. every running task should has a timeout, to avoid running forever.
     * 6. every running task should also has a max failure time,
     *    if clone task fails too many times, if should be removed.
     *
     */
    @Override
    protected void runAfterCatalogReady() {
        if (!updateWorkingSlots()) {
            return;
        }

        updateLoadStatistics();
        handleRunningTablets();
        selectTabletsForBalance();
        schedulePendingTablets();

        stat.counterTabletScheduleRound.incrementAndGet();
    }


    private void updateLoadStatistics() {
        updateLoadStatistic();
        rebalancer.updateLoadStatistic(statisticMap);
        diskRebalancer.updateLoadStatistic(statisticMap);

        lastStatUpdateTime = System.currentTimeMillis();
    }

    /**
     * Here is the only place we update the load statistic info.
     * We will not update this info dynamically along with the clone job's running.
     * Although it will cause a little bit inaccurate, but is within a controllable range,
     * because we already limit the total number of running clone jobs in cluster by 'backend slots'
     */
    private void updateLoadStatistic() {
        Map<Tag, LoadStatisticForTag> newStatisticMap = Maps.newHashMap();
        Set<Tag> tags = infoService.getTags();
        for (Tag tag : tags) {
            LoadStatisticForTag loadStatistic = new LoadStatisticForTag(tag, infoService, invertedIndex);
            loadStatistic.init();
            newStatisticMap.put(tag, loadStatistic);
            LOG.debug("update load statistic for tag {}:\n{}", tag, loadStatistic.getBrief());
        }
        Map<Long, Long> pathsCopingSize = getPathsCopingSize();
        for (LoadStatisticForTag loadStatistic : newStatisticMap.values()) {
            for (BackendLoadStatistic beLoadStatistic : loadStatistic.getBackendLoadStatistics()) {
                beLoadStatistic.incrPathsCopingSize(pathsCopingSize);
            }
        }

        this.statisticMap = newStatisticMap;
    }

    public Map<Tag, LoadStatisticForTag> getStatisticMap() {
        return statisticMap;
    }

    /**
     * get at most BATCH_NUM tablets from queue, and try to schedule them.
     * After handle, the tablet info should be
     * 1. in runningTablets with state RUNNING, if being scheduled success.
     * 2. or in schedHistory with state CANCELLING, if some unrecoverable error happens.
     * 3. or in pendingTablets with state PENDING, if failed to be scheduled.
     *
     * if in schedHistory, it should be removed from allTabletTypes.
     */
    private void schedulePendingTablets() {
        long start = System.currentTimeMillis();
        List<TabletSchedCtx> currentBatch = getNextTabletCtxBatch();
        LOG.debug("get {} tablets to schedule", currentBatch.size());

        AgentBatchTask batchTask = new AgentBatchTask();
        for (TabletSchedCtx tabletCtx : currentBatch) {
            try {
                if (Config.disable_tablet_scheduler) {
                    // do not schedule more tablet is tablet scheduler is disabled.
                    throw new SchedException(Status.FINISHED, "tablet scheduler is disabled");
                }
                if (Config.disable_balance && tabletCtx.getType() == Type.BALANCE) {
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, Status.UNRECOVERABLE,
                            "config disable balance");
                    continue;
                }
                scheduleTablet(tabletCtx, batchTask);
            } catch (SchedException e) {
                tabletCtx.setErrMsg(e.getMessage());
                if (e.getStatus() == Status.SCHEDULE_FAILED) {
                    boolean isExceedLimit = tabletCtx.onSchedFailedAndCheckExceedLimit(e.getSubCode());
                    if (isExceedLimit) {
                        finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getStatus(),
                                "schedule failed too many times and " + e.getMessage());
                    } else {
                        // we must release resource it current hold, and be scheduled again
                        tabletCtx.releaseResource(this);
                        // adjust priority to avoid some higher priority always be the first in pendingTablets
                        stat.counterTabletScheduledFailed.incrementAndGet();
                        addBackToPendingTablets(tabletCtx);
                    }
                } else if (e.getStatus() == Status.FINISHED) {
                    // schedule redundant tablet or scheduler disabled will throw this exception
                    stat.counterTabletScheduledSucceeded.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, e.getStatus(), e.getMessage());
                } else {
                    Preconditions.checkState(e.getStatus() == Status.UNRECOVERABLE, e.getStatus());
                    // discard
                    stat.counterTabletScheduledDiscard.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getStatus(), e.getMessage());
                }
                continue;
            } catch (Exception e) {
                LOG.warn("got unexpected exception, discard this schedule. tablet: {}",
                        tabletCtx.getTabletId(), e);
                stat.counterTabletScheduledFailed.incrementAndGet();
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.UNEXPECTED, Status.UNRECOVERABLE, e.getMessage());
                continue;
            }

            Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.RUNNING, tabletCtx.getState());
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

    /**
     * we take the tablet out of the runningTablets and than handle it,
     * avoid other threads see it.
     * Whoever takes this tablet, make sure to put it to the schedHistory or back to runningTablets.
     */
    private synchronized TabletSchedCtx takeRunningTablets(long tabletId) {
        return runningTablets.remove(tabletId);
    }

    /**
     * Try to schedule a single tablet.
     */
    private void scheduleTablet(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        long currentTime = System.currentTimeMillis();
        tabletCtx.setLastSchedTime(currentTime);
        tabletCtx.setLastVisitedTime(currentTime);
        stat.counterTabletScheduled.incrementAndGet();

        Pair<TabletStatus, TabletSchedCtx.Priority> statusPair;
        Database db = Env.getCurrentInternalCatalog().getDbOrException(tabletCtx.getDbId(),
                s -> new SchedException(Status.UNRECOVERABLE, "db " + tabletCtx.getDbId() + " does not exist"));
        OlapTable tbl = (OlapTable) db.getTableOrException(tabletCtx.getTblId(),
                s -> new SchedException(Status.UNRECOVERABLE, "tbl " + tabletCtx.getTblId() + " does not exist"));
        tbl.writeLockOrException(new SchedException(Status.UNRECOVERABLE, "table "
                + tbl.getName() + " does not exist"));
        try {
            long tabletId = tabletCtx.getTabletId();

            boolean isColocateTable = colocateTableIndex.isColocateTable(tbl.getId());

            OlapTableState tableState = tbl.getState();

            Partition partition = tbl.getPartition(tabletCtx.getPartitionId());
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
            }

            MaterializedIndex idx = partition.getIndex(tabletCtx.getIndexId());
            if (idx == null) {
                throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
            }

            ReplicaAllocation replicaAlloc = null;
            Tablet tablet = idx.getTablet(tabletId);
            Preconditions.checkNotNull(tablet);
            if (isColocateTable) {
                GroupId groupId = colocateTableIndex.getGroup(tbl.getId());
                if (groupId == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "colocate group does not exist");
                }
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(groupId);
                if (groupSchema == null) {
                    throw new SchedException(Status.UNRECOVERABLE,
                            "colocate group schema " + groupId + " does not exist");
                }
                replicaAlloc = groupSchema.getReplicaAlloc();

                int tabletOrderIdx = tabletCtx.getTabletOrderIdx();
                if (tabletOrderIdx == -1) {
                    tabletOrderIdx = idx.getTabletOrderIdx(tablet.getId());
                }
                Preconditions.checkState(tabletOrderIdx != -1);

                Set<Long> backendsSet = colocateTableIndex.getTabletBackendsByGroup(groupId, tabletOrderIdx);
                TabletStatus st = tablet.getColocateHealthStatus(
                        partition.getVisibleVersion(), replicaAlloc, backendsSet);
                statusPair = Pair.of(st, Priority.HIGH);
                tabletCtx.setColocateGroupBackendIds(backendsSet);
            } else {
                replicaAlloc = tbl.getPartitionInfo().getReplicaAllocation(partition.getId());
                List<Long> aliveBeIds = infoService.getAllBackendIds(true);
                statusPair = tablet.getHealthStatusWithPriority(
                        infoService, partition.getVisibleVersion(), replicaAlloc, aliveBeIds);
            }

            if (tabletCtx.getType() != allTabletTypes.get(tabletId)) {
                TabletSchedCtx.Type curType = tabletCtx.getType();
                TabletSchedCtx.Type newType = allTabletTypes.get(tabletId);
                if (curType == TabletSchedCtx.Type.BALANCE && newType == TabletSchedCtx.Type.REPAIR) {
                    tabletCtx.setType(newType);
                    tabletCtx.setReplicaAlloc(replicaAlloc);
                    tabletCtx.setTag(null);
                } else {
                    throw new SchedException(Status.UNRECOVERABLE, "can not convert type of tablet "
                            + tabletId + " from " + curType.name() + " to " + newType.name());
                }
            }

            if (tabletCtx.getType() == TabletSchedCtx.Type.BALANCE && tableState != OlapTableState.NORMAL) {
                // If table is under ALTER process, do not allow to do balance.
                throw new SchedException(Status.UNRECOVERABLE, "table's state is not NORMAL");
            }

            if (tabletCtx.getType() == TabletSchedCtx.Type.BALANCE) {
                try {
                    DatabaseTransactionMgr dbTransactionMgr
                            = Env.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(db.getId());
                    for (TransactionState transactionState : dbTransactionMgr.getPreCommittedTxnList()) {
                        if (transactionState.getTableIdList().contains(tbl.getId())) {
                            // If table releate to transaction with precommitted status, do not allow to do balance.
                            throw new SchedException(Status.UNRECOVERABLE,
                                    "There exists PRECOMMITTED transaction related to table");
                        }
                    }
                } catch (AnalysisException e) {
                    // CHECKSTYLE IGNORE THIS LINE
                }
            }

            if (statusPair.first != TabletStatus.VERSION_INCOMPLETE
                    && (partition.getState() != PartitionState.NORMAL || tableState != OlapTableState.NORMAL)
                    && tableState != OlapTableState.WAITING_STABLE) {
                // If table is under ALTER process(before FINISHING), do not allow to add or delete replica.
                // VERSION_INCOMPLETE will repair the replica in place, which is allowed.
                // The WAITING_STABLE state is an exception. This state indicates that the table is
                // executing an alter job, but the alter job is in a PENDING state and is waiting for
                // the table to become stable. In this case, we allow the tablet repair to proceed.
                throw new SchedException(Status.UNRECOVERABLE,
                        "table is in alter process, but tablet status is " + statusPair.first.name());
            }

            tabletCtx.setTabletStatus(statusPair.first);
            if (statusPair.first == TabletStatus.HEALTHY && tabletCtx.getType() == TabletSchedCtx.Type.REPAIR) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet is healthy");
            } else if (statusPair.first != TabletStatus.HEALTHY
                    && tabletCtx.getType() == TabletSchedCtx.Type.BALANCE) {
                // we select an unhealthy tablet to do balance, which is not right.
                // so here we stop this task.
                throw new SchedException(Status.UNRECOVERABLE, "tablet is unhealthy when doing balance");
            }

            // for disk balance more accurately, we only schedule tablet when has lastly stat info about disk
            if (tabletCtx.getType() == TabletSchedCtx.Type.BALANCE
                    && tabletCtx.getBalanceType() == TabletSchedCtx.BalanceType.DISK_BALANCE) {
                checkDiskBalanceLastSuccTime(tabletCtx.getTempSrcBackendId(), tabletCtx.getTempSrcPathHash());
            }
            // we do not concern priority here.
            // once we take the tablet out of priority queue, priority is meaningless.
            tabletCtx.setTablet(tablet);
            tabletCtx.updateTabletSize();
            tabletCtx.setVersionInfo(partition.getVisibleVersion(), partition.getCommittedVersion());
            tabletCtx.setSchemaHash(tbl.getSchemaHashByIndexId(idx.getId()));
            tabletCtx.setStorageMedium(tbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium());

            handleTabletByTypeAndStatus(statusPair.first, tabletCtx, batchTask);
        } finally {
            tbl.writeUnlock();
        }
    }

    private void checkDiskBalanceLastSuccTime(long beId, long pathHash) throws SchedException {
        PathSlot pathSlot = backendsWorkingSlots.get(beId);
        if (pathSlot == null) {
            throw new SchedException(Status.UNRECOVERABLE, "path slot does not exist");
        }
        long succTime = pathSlot.getDiskBalanceLastSuccTime(pathHash);
        if (succTime > lastStatUpdateTime) {
            throw new SchedException(Status.UNRECOVERABLE, "stat info is outdated");
        }
    }

    public void updateDiskBalanceLastSuccTime(long beId, long pathHash) {
        PathSlot pathSlot = backendsWorkingSlots.get(beId);
        if (pathSlot == null) {
            return;
        }
        pathSlot.updateDiskBalanceLastSuccTime(pathHash);
    }

    private void handleTabletByTypeAndStatus(TabletStatus status, TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        if (tabletCtx.getType() == Type.REPAIR) {
            switch (status) {
                case REPLICA_MISSING:
                    handleReplicaMissing(tabletCtx, batchTask);
                    break;
                case VERSION_INCOMPLETE:
                case NEED_FURTHER_REPAIR:
                    // same as version incomplete, it prefers to the dest replica which need further repair
                    handleReplicaVersionIncomplete(tabletCtx, batchTask);
                    break;
                case REPLICA_RELOCATING:
                    handleReplicaRelocating(tabletCtx, batchTask);
                    break;
                case REDUNDANT:
                    handleRedundantReplica(tabletCtx, false);
                    break;
                case FORCE_REDUNDANT:
                    handleRedundantReplica(tabletCtx, true);
                    break;
                case REPLICA_MISSING_FOR_TAG:
                    handleReplicaMissingForTag(tabletCtx, batchTask);
                    break;
                case COLOCATE_MISMATCH:
                    handleColocateMismatch(tabletCtx, batchTask);
                    break;
                case COLOCATE_REDUNDANT:
                    handleColocateRedundant(tabletCtx);
                    break;
                case REPLICA_COMPACTION_TOO_SLOW:
                    handleReplicaTooSlow(tabletCtx);
                    break;
                case UNRECOVERABLE:
                    throw new SchedException(Status.UNRECOVERABLE, "tablet is unrecoverable");
                default:
                    break;
            }
        } else {
            // balance
            doBalance(tabletCtx, batchTask);
        }
    }

    /**
     * Replica is missing, which means there is no enough alive replicas.
     * So we need to find a destination backend to clone a new replica as possible as we can.
     * 1. find an available path in a backend as destination:
     *      1. backend need to be alive.
     *      2. backend of existing replicas should be excluded. (should not be on same host either)
     *      3. backend with proper tag.
     *      4. backend has available slot for clone.
     *      5. replica can fit in the path (consider the threshold of disk capacity and usage percent).
     *      6. try to find a path with lowest load score.
     * 2. find an appropriate source replica:
     *      1. source replica should be healthy
     *      2. backend of source replica has available slot for clone.
     *
     * 3. send clone task to destination backend
     */
    private void handleReplicaMissing(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        stat.counterReplicaMissingErr.incrementAndGet();
        // check compaction too slow file is recovered
        if (tabletCtx.compactionRecovered()) {
            return;
        }

        // find proper tag
        Tag tag = chooseProperTag(tabletCtx, true);
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletCtx, tag, false /* not for colocate */);
        Preconditions.checkNotNull(destPath);
        tabletCtx.setDest(destPath.getBeId(), destPath.getPathHash());
        // choose a source replica for cloning from
        tabletCtx.chooseSrcReplica(backendsWorkingSlots, -1);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
        incrDestPathCopingSize(tabletCtx);
    }

    // In dealing with the case of missing replicas, we need to select a tag with missing replicas
    // according to the distribution of replicas.
    // If no replica of the tag is missing, an exception is thrown.
    // And for deleting redundant replica, also find out a tag which has redundant replica.
    private Tag chooseProperTag(TabletSchedCtx tabletCtx, boolean forMissingReplica) throws SchedException {
        Tablet tablet = tabletCtx.getTablet();
        List<Replica> replicas = tablet.getReplicas();
        Map<Tag, Short> allocMap = tabletCtx.getReplicaAlloc().getAllocMap();
        Map<Tag, Short> currentAllocMap = Maps.newHashMap();
        for (Replica replica : replicas) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be != null && be.isScheduleAvailable() && replica.isAlive() && !replica.tooSlow()
                    && be.isMixNode()) {
                Short num = currentAllocMap.getOrDefault(be.getLocationTag(), (short) 0);
                currentAllocMap.put(be.getLocationTag(), (short) (num + 1));
            }
        }

        for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
            short curNum = currentAllocMap.getOrDefault(entry.getKey(), (short) 0);
            if (forMissingReplica && curNum < entry.getValue()) {
                return entry.getKey();
            }
            if (!forMissingReplica && curNum > entry.getValue()) {
                return entry.getKey();
            }
        }

        throw new SchedException(Status.UNRECOVERABLE, "no proper tag is chose for tablet " + tablet.getId());
    }

    /**
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
        try {
            tabletCtx.chooseDestReplicaForVersionIncomplete(backendsWorkingSlots);
        } catch (SchedException e) {
            // could not find dest, try add a missing.
            if (e.getStatus() == Status.UNRECOVERABLE) {
                // This situation may occur when the BE nodes
                // where all replicas of a tablet are located are decommission,
                // and this task is a VERSION_INCOMPLETE task.
                // This will lead to failure to select a suitable dest replica.
                // At this time, we try to convert this task to a REPLICA_MISSING task, and schedule it again.
                LOG.debug("failed to find version incomplete replica for VERSION_INCOMPLETE task. tablet id: {}, "
                        + "try to find a new backend", tabletCtx.getTabletId());
                tabletCtx.releaseResource(this, true);
                tabletCtx.setTabletStatus(TabletStatus.REPLICA_MISSING);
                handleReplicaMissing(tabletCtx, batchTask);
                LOG.debug("succeed to find new backend for VERSION_INCOMPLETE task. tablet id: {}",
                        tabletCtx.getTabletId());
                return;
            } else {
                throw e;
            }
        }
        tabletCtx.chooseSrcReplicaForVersionIncomplete(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }

    /*
     * There are enough alive replicas with complete version in this tablet, but some of backends may
     * under decommission.
     * First, we try to find a version incomplete replica on available BE.
     * If failed to find, then try to find a new BE to clone the replicas.
     *
     * Give examples of why:
     * Tablet X has 3 replicas on A, B, C 3 BEs.
     * C is decommission, so we choose the BE D to relocating the new replica,
     * After relocating, Tablet X has 4 replicas: A, B, C(decommission), D(may be version incomplete)
     * But D may be version incomplete because the clone task ran a long time, the new version
     * has been published.
     * At the next time of tablet checking, Tablet X's status is still REPLICA_RELOCATING,
     * If we don't choose D as dest BE to do the new relocating, it will choose new backend E to
     * store the new replicas. So back and forth, the number of replicas will increase forever.
     */
    private void handleReplicaRelocating(TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        stat.counterReplicaUnavailableErr.incrementAndGet();
        tabletCtx.setTabletStatus(TabletStatus.VERSION_INCOMPLETE);
        handleReplicaVersionIncomplete(tabletCtx, batchTask);
    }

    /**
     *  replica is redundant, which means there are more replicas than we expected, which need to be dropped.
     *  we just drop one redundant replica at a time, for safety reason.
     *  choosing a replica to drop base on following priority:
     *  1. backend has been dropped
     *  2. replica is bad
     *  3. backend is not available
     *  4. replica's state is CLONE or DECOMMISSION
     *  5. replica's last failed version > 0
     *  6. replica with lower version
     *  7. replica is the src replica of a rebalance task, we can try to get it from rebalancer
     *  8. replica on higher load backend
     */
    private void handleRedundantReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        stat.counterReplicaRedundantErr.incrementAndGet();

        if (deleteBackendDropped(tabletCtx, force)
                || deleteBadReplica(tabletCtx, force)
                || deleteBackendUnavailable(tabletCtx, force)
                || deleteTooSlowReplica(tabletCtx, force)
                || deleteCloneOrDecommissionReplica(tabletCtx, force)
                || deleteReplicaWithFailedVersion(tabletCtx, force)
                || deleteReplicaWithLowerVersion(tabletCtx, force)
                || deleteReplicaOnSameHost(tabletCtx, force)
                || deleteReplicaNotInValidTag(tabletCtx, force)
                || deleteReplicaChosenByRebalancer(tabletCtx, force)
                || deleteReplicaOnHighLoadBackend(tabletCtx, force)) {
            // if we delete at least one redundant replica, we still throw a SchedException with status FINISHED
            // to remove this tablet from the pendingTablets(consider it as finished)
            throw new SchedException(Status.FINISHED, "redundant replica is deleted");
        }
        throw new SchedException(Status.UNRECOVERABLE, "unable to delete any redundant replicas");
    }

    private boolean deleteBackendDropped(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            long beId = replica.getBackendId();
            if (infoService.getBackend(beId) == null) {
                deleteReplicaInternal(tabletCtx, replica, "backend dropped", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteBadReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.isBad()) {
                deleteReplicaInternal(tabletCtx, replica, "replica is bad", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteTooSlowReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.tooSlow()) {
                deleteReplicaInternal(tabletCtx, replica, "replica is too slow", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteBackendUnavailable(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.isScheduleAvailable()) {
                deleteReplicaInternal(tabletCtx, replica, "backend unavailable", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteCloneOrDecommissionReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.getState() == ReplicaState.CLONE || replica.getState() == ReplicaState.DECOMMISSION) {
                deleteReplicaInternal(tabletCtx, replica, replica.getState() + " state", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithFailedVersion(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.getLastFailedVersion() > 0) {
                deleteReplicaInternal(tabletCtx, replica, "version incomplete", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithLowerVersion(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (!replica.checkVersionCatchUp(tabletCtx.getCommittedVersion(), false)) {
                deleteReplicaInternal(tabletCtx, replica, "lower version", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaOnSameHost(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        // collect replicas of this tablet.
        // host -> (replicas on same host)
        Map<String, List<Replica>> hostToReplicas = Maps.newHashMap();
        for (Replica replica : tabletCtx.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                return false;
            }
            List<Replica> replicas = hostToReplicas.get(be.getHost());
            if (replicas == null) {
                replicas = Lists.newArrayList();
                hostToReplicas.put(be.getHost(), replicas);
            }
            replicas.add(replica);
        }

        // find if there are replicas on same host, if yes, delete one.
        for (List<Replica> replicas : hostToReplicas.values()) {
            if (replicas.size() > 1) {
                // delete one replica from replicas on same host.
                // better to choose high load backend
                Tag tag = chooseProperTag(tabletCtx, false);
                LoadStatisticForTag statistic = statisticMap.get(tag);
                if (statistic == null) {
                    return false;
                }
                return deleteFromHighLoadBackend(tabletCtx, replicas, force, statistic);
            }
        }

        return false;
    }

    private boolean deleteReplicaNotInValidTag(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        Tablet tablet = tabletCtx.getTablet();
        List<Replica> replicas = tablet.getReplicas();
        Map<Tag, Short> allocMap = tabletCtx.getReplicaAlloc().getAllocMap();
        for (Replica replica : replicas) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be.isMixNode() && !allocMap.containsKey(be.getLocationTag())) {
                deleteReplicaInternal(tabletCtx, replica, "not in valid tag", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaChosenByRebalancer(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        Long id = rebalancer.getToDeleteReplicaId(tabletCtx);
        if (id == -1L) {
            return false;
        }
        Replica chosenReplica = tabletCtx.getTablet().getReplicaById(id);
        if (chosenReplica == null) {
            return false;
        }
        deleteReplicaInternal(tabletCtx, chosenReplica, "src replica of rebalance", force);

        return true;
    }

    private boolean deleteReplicaOnHighLoadBackend(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        Tag tag = chooseProperTag(tabletCtx, false);
        LoadStatisticForTag statistic = statisticMap.get(tag);
        if (statistic == null) {
            return false;
        }

        return deleteFromHighLoadBackend(tabletCtx, tabletCtx.getReplicas(), force, statistic);
    }

    private boolean deleteFromHighLoadBackend(TabletSchedCtx tabletCtx, List<Replica> replicas,
            boolean force, LoadStatisticForTag statistic) throws SchedException {
        Replica chosenReplica = null;
        double maxScore = 0;
        for (Replica replica : replicas) {
            BackendLoadStatistic beStatistic = statistic.getBackendLoadStatistic(replica.getBackendId());
            if (beStatistic == null) {
                continue;
            }

            /*
             * If the backend does not have the specified storage medium, we use mix load score to make
             * sure that at least one replica can be chosen.
             * This can happen if the Doris cluster is deployed with all, for example, SSD medium,
             * but create all tables with HDD storage medium property. Then getLoadScore(SSD) will
             * always return 0.0, so that no replica will be chosen.
             */
            double loadScore = 0.0;
            if (beStatistic.hasMedium(tabletCtx.getStorageMedium())) {
                loadScore = beStatistic.getLoadScore(tabletCtx.getStorageMedium());
            } else {
                loadScore = beStatistic.getMixLoadScore();
            }

            if (loadScore > maxScore) {
                maxScore = loadScore;
                chosenReplica = replica;
            }
        }

        if (chosenReplica != null) {
            deleteReplicaInternal(tabletCtx, chosenReplica, "high load", force);
            return true;
        }
        return false;
    }

    /**
     * Just delete replica which does not locate in colocate backends set.
     * return true if delete one replica, otherwise, return false.
     */
    private boolean handleColocateRedundant(TabletSchedCtx tabletCtx) throws SchedException {
        Preconditions.checkNotNull(tabletCtx.getColocateBackendsSet());
        for (Replica replica : tabletCtx.getReplicas()) {
            if (tabletCtx.getColocateBackendsSet().contains(replica.getBackendId()) && !replica.isBad()) {
                continue;
            }

            // If the replica is not in ColocateBackendsSet or is bad, delete it.
            deleteReplicaInternal(tabletCtx, replica, "colocate redundant", false);
            throw new SchedException(Status.FINISHED, "colocate redundant replica is deleted");
        }
        throw new SchedException(Status.UNRECOVERABLE, "unable to delete any colocate redundant replicas");
    }

    /**
     * remove the replica which has the most version count, and much more than others
     * return true if delete one replica, otherwise, return false.
     */
    private void handleReplicaTooSlow(TabletSchedCtx tabletCtx) throws SchedException {
        Replica chosenReplica = null;
        long maxVersionCount = -1;
        int normalReplicaCount = 0;
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.isAlive() && !replica.tooSlow()) {
                normalReplicaCount++;
            }
            if (replica.getVersionCount() > maxVersionCount) {
                maxVersionCount = replica.getVersionCount();
                chosenReplica = replica;
            }
        }
        if (chosenReplica != null && chosenReplica.isAlive() && !chosenReplica.tooSlow()
                && chosenReplica.getVersionCount() > Config.min_version_count_indicate_replica_compaction_too_slow
                && normalReplicaCount - 1 >= tabletCtx.getReplicas().size() / 2 + 1) {
            chosenReplica.setState(ReplicaState.COMPACTION_TOO_SLOW);
            LOG.info("set replica id :{} tablet id: {}, backend id: {} to COMPACTION_TOO_SLOW",
                    chosenReplica.getId(), tabletCtx.getTablet().getId(), chosenReplica.getBackendId());
            throw new SchedException(Status.FINISHED, "set replica to COMPACTION_TOO_SLOW");
        }
        throw new SchedException(Status.FINISHED, "No replica set to COMPACTION_TOO_SLOW");
    }

    private void deleteReplicaInternal(TabletSchedCtx tabletCtx,
            Replica replica, String reason, boolean force) throws SchedException {

        List<Replica> replicas = tabletCtx.getTablet().getReplicas();
        boolean otherCatchup = replicas.stream().anyMatch(
                r -> r.getId() != replica.getId()
                && (r.getVersion() > replica.getVersion()
                        || (r.getVersion() == replica.getVersion() && r.getLastFailedVersion() < 0)));
        if (!otherCatchup) {
            LOG.info("can not delete only one replica, tabletId = {} replicaId = {}", tabletCtx.getTabletId(),
                     replica.getId());
            throw new SchedException(Status.UNRECOVERABLE, "the only one latest replia can not be dropped, tabletId = "
                                        + tabletCtx.getTabletId() + ", replicaId = " + replica.getId());
        }

        /*
         * Before deleting a replica, we should make sure that
         * there is no running txn on it and no more txns will be on it.
         * So we do followings:
         * 1. If replica is loadable, set a watermark txn id on it and set it state as DECOMMISSION,
         *      but not deleting it this time.
         *      The DECOMMISSION state will ensure that no more txns will be on this replicas.
         * 2. Wait for any txns before the watermark txn id to be finished.
         *      If all are finished, which means this replica is
         *      safe to be deleted.
         */
        if (!force && !Config.enable_force_drop_redundant_replica
                && !FeConstants.runningUnitTest
                && (replica.getState().canLoad() || replica.getState() == ReplicaState.DECOMMISSION)) {
            if (replica.getState() != ReplicaState.DECOMMISSION) {
                replica.setState(ReplicaState.DECOMMISSION);
                // set priority to normal because it may wait for a long time.
                // Remain it as VERY_HIGH may block other task.
                tabletCtx.setPriority(Priority.NORMAL);
                LOG.info("set replica {} on backend {} of tablet {} state to DECOMMISSION due to reason {}",
                        replica.getId(), replica.getBackendId(), tabletCtx.getTabletId(), reason);
            }

            long preWatermarkTxnId = replica.getPreWatermarkTxnId();
            if (preWatermarkTxnId == -1) {
                preWatermarkTxnId = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionIDGenerator().getNextTransactionId();
                replica.setPreWatermarkTxnId(preWatermarkTxnId);
                LOG.info("set decommission replica {} on backend {} of tablet {} pre watermark txn id {}",
                        replica.getId(), replica.getBackendId(), tabletCtx.getTabletId(), preWatermarkTxnId);
            }

            long postWatermarkTxnId = replica.getPostWatermarkTxnId();
            if (postWatermarkTxnId == -1) {
                try {
                    if (!Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(preWatermarkTxnId,
                            tabletCtx.getDbId(), tabletCtx.getTblId(), tabletCtx.getPartitionId())) {
                        throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_DECOMMISSION,
                                "wait txn before pre watermark txn " + preWatermarkTxnId + " to be finished");
                    }
                } catch (AnalysisException e) {
                    throw new SchedException(Status.UNRECOVERABLE, e.getMessage());
                }
                postWatermarkTxnId = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionIDGenerator().getNextTransactionId();
                replica.setPostWatermarkTxnId(postWatermarkTxnId);
                LOG.info("set decommission replica {} on backend {} of tablet {} post watermark txn id {}",
                        replica.getId(), replica.getBackendId(), tabletCtx.getTabletId(), postWatermarkTxnId);
            }

            try {
                if (!Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(postWatermarkTxnId,
                        tabletCtx.getDbId(), tabletCtx.getTblId(), tabletCtx.getPartitionId())) {
                    throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_DECOMMISSION,
                            "wait txn before post watermark txn  " + postWatermarkTxnId + " to be finished");
                }
            } catch (AnalysisException e) {
                throw new SchedException(Status.UNRECOVERABLE, e.getMessage());
            }
        }

        // delete this replica from catalog.
        // it will also delete replica from tablet inverted index.
        tabletCtx.deleteReplica(replica);

        if (force || FeConstants.runningUnitTest) {
            // send the delete replica task.
            // also, this may not be necessary, but delete it will make things simpler.
            // NOTICE: only delete the replica from meta may not work. sometimes we can depend on tablet report
            // deleting these replicas, but in FORCE_REDUNDANT case, replica may be added to meta again in report
            // process.
            sendDeleteReplicaTask(replica.getBackendId(), tabletCtx.getTabletId(), replica.getId(),
                    tabletCtx.getSchemaHash());
        }

        // write edit log
        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(tabletCtx.getDbId(),
                tabletCtx.getTblId(),
                tabletCtx.getPartitionId(),
                tabletCtx.getIndexId(),
                tabletCtx.getTabletId(),
                replica.getBackendId());

        Env.getCurrentEnv().getEditLog().logDeleteReplica(info);

        LOG.info("delete replica. tablet id: {}, backend id: {}. reason: {}, force: {}",
                tabletCtx.getTabletId(), replica.getBackendId(), reason, force);
    }

    private void sendDeleteReplicaTask(long backendId, long tabletId, long replicaId, int schemaHash) {
        DropReplicaTask task = new DropReplicaTask(backendId, tabletId, replicaId, schemaHash, false);
        AgentBatchTask batchTask = new AgentBatchTask();
        batchTask.addTask(task);
        AgentTaskExecutor.submit(batchTask);
        LOG.info("send delete replica task for tablet {} in backend {}", tabletId, backendId);
    }

    /**
     * Missing for tag, which means some of replicas of this tablet are allocated in wrong backend with specified tag.
     * Treat it as replica missing, and in handleReplicaMissing(),
     * it will find a property backend to create new replica.
     */
    private void handleReplicaMissingForTag(TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        stat.counterReplicaMissingForTagErr.incrementAndGet();
        handleReplicaMissing(tabletCtx, batchTask);
    }

    /**
     * Replicas of colocate table's tablet does not locate on right backends set.
     *      backends set:       1,2,3
     *      tablet replicas:    1,2,5
     *
     *      backends set:       1,2,3
     *      tablet replicas:    1,2
     *
     *      backends set:       1,2,3
     *      tablet replicas:    1,2,4,5
     */
    private void handleColocateMismatch(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        Preconditions.checkNotNull(tabletCtx.getColocateBackendsSet());

        stat.counterReplicaColocateMismatch.incrementAndGet();
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletCtx, null, true /* for colocate */);
        Preconditions.checkNotNull(destPath);
        tabletCtx.setDest(destPath.getBeId(), destPath.getPathHash());

        // choose a source replica for cloning from
        tabletCtx.chooseSrcReplica(backendsWorkingSlots, -1);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
        incrDestPathCopingSize(tabletCtx);
    }

    /**
     * Try to select some alternative tablets for balance. Add them to pendingTablets with priority LOW,
     * and waiting to be scheduled.
     */
    private void selectTabletsForBalance() {
        if (Config.disable_balance || Config.disable_tablet_scheduler) {
            LOG.info("balance or tablet scheduler is disabled. skip selecting tablets for balance");
            return;
        }

        // No need to prefetch too many balance task to pending queue.
        // Because for every sched, it will re select the balance task.
        int needAddBalanceNum = Math.min(Config.schedule_batch_size - getPendingNum(),
                    Config.max_balancing_tablets - getBalanceTabletsNumber());
        if (needAddBalanceNum <= 0) {
            return;
        }

        List<TabletSchedCtx> alternativeTablets = rebalancer.selectAlternativeTablets();
        Collections.shuffle(alternativeTablets);
        for (TabletSchedCtx tabletCtx : alternativeTablets) {
            if (needAddBalanceNum > 0 && addTablet(tabletCtx, false) == AddResult.ADDED) {
                needAddBalanceNum--;
            } else {
                rebalancer.onTabletFailed(tabletCtx);
            }
        }
        if (needAddBalanceNum <= 0) {
            return;
        }
        if (Config.disable_disk_balance) {
            LOG.info("disk balance is disabled. skip selecting tablets for disk balance");
            return;
        }
        List<TabletSchedCtx> diskBalanceTablets = Lists.newArrayList();
        // if default rebalancer can not get new task or user given prio BEs, then use disk rebalancer to get task
        if (diskRebalancer.hasPrioBackends() || alternativeTablets.isEmpty()) {
            diskBalanceTablets = diskRebalancer.selectAlternativeTablets();
        }
        for (TabletSchedCtx tabletCtx : diskBalanceTablets) {
            // add if task from prio backend or cluster is balanced
            if (alternativeTablets.isEmpty() || tabletCtx.getPriority() == TabletSchedCtx.Priority.NORMAL) {
                if (addTablet(tabletCtx, false) == AddResult.ADDED) {
                    needAddBalanceNum--;
                    if (needAddBalanceNum <= 0) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Try to create a balance task for a tablet.
     */
    private void doBalance(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        stat.counterBalanceSchedule.incrementAndGet();
        AgentTask task = null;
        if (tabletCtx.getBalanceType() == TabletSchedCtx.BalanceType.DISK_BALANCE) {
            task = diskRebalancer.createBalanceTask(tabletCtx);
            checkDiskBalanceLastSuccTime(tabletCtx.getSrcBackendId(), tabletCtx.getSrcPathHash());
            checkDiskBalanceLastSuccTime(tabletCtx.getDestBackendId(), tabletCtx.getDestPathHash());
        } else if (tabletCtx.getBalanceType() == TabletSchedCtx.BalanceType.BE_BALANCE) {
            task = rebalancer.createBalanceTask(tabletCtx);
        } else {
            throw new SchedException(Status.UNRECOVERABLE,
                "unknown balance type: " + tabletCtx.getBalanceType().toString());
        }
        batchTask.addTask(task);
        incrDestPathCopingSize(tabletCtx);
    }

    // choose a path on a backend which is fit for the tablet
    // if forColocate is false, the tag must be set.
    private RootPathLoadStatistic chooseAvailableDestPath(TabletSchedCtx tabletCtx, Tag tag, boolean forColocate)
            throws SchedException {
        List<BackendLoadStatistic> beStatistics;
        if (tag != null) {
            Preconditions.checkState(!forColocate);
            LoadStatisticForTag statistic = statisticMap.get(tag);
            if (statistic == null) {
                throw new SchedException(Status.UNRECOVERABLE,
                        String.format("tag %s does not exist. available tags: %s", tag,
                            Joiner.on(",").join(statisticMap.keySet().stream().limit(5).toArray())));
            }
            beStatistics = statistic.getSortedBeLoadStats(null /* sorted ignore medium */);
        } else {
            // for colocate task, get BackendLoadStatistic by colocateBackendIds
            Preconditions.checkState(forColocate);
            Preconditions.checkState(tabletCtx.getColocateBackendsSet() != null);
            Set<Long> colocateBackendIds = tabletCtx.getColocateBackendsSet();

            beStatistics = Lists.newArrayList();
            for (LoadStatisticForTag loadStatisticForTag : statisticMap.values()) {
                for (long beId : colocateBackendIds) {
                    BackendLoadStatistic backendLoadStatistic = loadStatisticForTag.getBackendLoadStatistic(beId);
                    if (backendLoadStatistic != null) {
                        beStatistics.add(backendLoadStatistic);
                    }
                }
            }
        }

        // get all available paths which this tablet can fit in.
        // beStatistics is sorted by mix load score in ascend order, so select from first to last.
        List<BePathLoadStatPair> allFitPathsSameMedium = Lists.newArrayList();
        List<BePathLoadStatPair> allFitPathsDiffMedium = Lists.newArrayList();
        for (BackendLoadStatistic bes : beStatistics) {
            if (!bes.isAvailable()) {
                LOG.debug("backend {} is not available, skip. tablet: {}", bes.getBeId(), tabletCtx.getTabletId());
                continue;
            }

            // exclude BE which already has replica of this tablet or another BE at same host has this replica
            if (tabletCtx.filterDestBE(bes.getBeId())) {
                LOG.debug("backend {} already has replica of this tablet or another BE "
                                + "at same host has this replica, skip. tablet: {}",
                        bes.getBeId(), tabletCtx.getTabletId());
                continue;
            }

            // If this for colocate table, only choose backend in colocate backend set.
            // Else, check the tag.
            if (forColocate) {
                if (!tabletCtx.getColocateBackendsSet().contains(bes.getBeId())) {
                    LOG.debug("backend {} is not in colocate backend set, skip. tablet: {}",
                            bes.getBeId(), tabletCtx.getTabletId());
                    continue;
                }
            } else if (!bes.getTag().equals(tag)) {
                LOG.debug("backend {}'s tag {} is not equal to tablet's tag {}, skip. tablet: {}",
                        bes.getBeId(), bes.getTag(), tag, tabletCtx.getTabletId());
                continue;
            }

            List<RootPathLoadStatistic> resultPaths = Lists.newArrayList();
            BalanceStatus st = bes.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(),
                    resultPaths, false);
            if (st.ok()) {
                resultPaths.stream().forEach(path -> allFitPathsSameMedium.add(new BePathLoadStatPair(bes, path)));
            } else {
                resultPaths.clear();
                st = bes.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(), resultPaths, true);
                if (st.ok()) {
                    resultPaths.stream().forEach(path -> allFitPathsDiffMedium.add(new BePathLoadStatPair(bes, path)));
                }
            }
        }

        // all fit paths has already been sorted by load score in 'allFitPaths' in ascend order.
        // just get first available path.
        // we try to find a path with specified media type, if not find, arbitrarily use one.
        List<BePathLoadStatPair> allFitPaths =
                !allFitPathsSameMedium.isEmpty() ? allFitPathsSameMedium : allFitPathsDiffMedium;
        if (allFitPaths.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to find dest path for new replica");
        }

        BePathLoadStatPairComparator comparator = new BePathLoadStatPairComparator(allFitPaths);
        Collections.sort(allFitPaths, comparator);

        for (BePathLoadStatPair bePathLoadStat : allFitPaths) {
            RootPathLoadStatistic rootPathLoadStatistic = bePathLoadStat.getPathLoadStatistic();
            if (rootPathLoadStatistic.getStorageMedium() != tabletCtx.getStorageMedium()) {
                LOG.debug("backend {}'s path {}'s storage medium {} "
                        + "is not equal to tablet's storage medium {}, skip. tablet: {}",
                        rootPathLoadStatistic.getBeId(), rootPathLoadStatistic.getPathHash(),
                        rootPathLoadStatistic.getStorageMedium(), tabletCtx.getStorageMedium(),
                        tabletCtx.getTabletId());
                continue;
            }

            PathSlot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
            if (slot == null) {
                LOG.debug("backend {}'s path {}'s slot is null, skip. tablet: {}",
                        rootPathLoadStatistic.getBeId(), rootPathLoadStatistic.getPathHash(),
                        tabletCtx.getTabletId());
                continue;
            }

            long pathHash = slot.takeSlot(rootPathLoadStatistic.getPathHash());
            if (pathHash == -1) {
                LOG.debug("backend {}'s path {}'s slot is full, skip. tablet: {}",
                        rootPathLoadStatistic.getBeId(), rootPathLoadStatistic.getPathHash(),
                        tabletCtx.getTabletId());
                continue;
            }
            return rootPathLoadStatistic;
        }

        boolean hasBePath = false;

        // no root path with specified media type is found, get arbitrary one.
        for (BePathLoadStatPair bePathLoadStat : allFitPaths) {
            RootPathLoadStatistic rootPathLoadStatistic = bePathLoadStat.getPathLoadStatistic();
            PathSlot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
            if (slot == null) {
                LOG.debug("backend {}'s path {}'s slot is null, skip. tablet: {}",
                        rootPathLoadStatistic.getBeId(), rootPathLoadStatistic.getPathHash(),
                        tabletCtx.getTabletId());
                continue;
            }

            hasBePath = true;
            long pathHash = slot.takeSlot(rootPathLoadStatistic.getPathHash());
            if (pathHash == -1) {
                LOG.debug("backend {}'s path {}'s slot is full, skip. tablet: {}",
                        rootPathLoadStatistic.getBeId(), rootPathLoadStatistic.getPathHash(),
                        tabletCtx.getTabletId());
                continue;
            }
            return rootPathLoadStatistic;
        }

        if (hasBePath) {
            throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                    "unable to find dest path which can be fit in");
        } else {
            throw new SchedException(Status.UNRECOVERABLE,
                    "unable to find dest path which can be fit in");
        }
    }


    private void addBackToPendingTablets(TabletSchedCtx tabletCtx) {
        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.PENDING);
        addTablet(tabletCtx, true /* force */);
    }


    private void finalizeTabletCtx(TabletSchedCtx tabletCtx, TabletSchedCtx.State state, Status status, String reason) {
        if (state == TabletSchedCtx.State.CANCELLED || state == TabletSchedCtx.State.UNEXPECTED) {
            if (tabletCtx.getType() == TabletSchedCtx.Type.BALANCE
                    && tabletCtx.getBalanceType() == TabletSchedCtx.BalanceType.BE_BALANCE) {
                rebalancer.onTabletFailed(tabletCtx);
            }
        }

        // use 2 steps to avoid nested database lock and synchronized.(releaseTabletCtx() may hold db lock)
        // remove the tablet ctx, so that no other process can see it
        removeTabletCtx(tabletCtx, reason);
        // release resources taken by tablet ctx
        releaseTabletCtx(tabletCtx, state, status == Status.UNRECOVERABLE);

        // if check immediately, then no need to wait TabletChecker's 20s
        if (state == TabletSchedCtx.State.FINISHED) {
            tryAddAfterFinished(tabletCtx);
        }
    }

    private void tryAddAfterFinished(TabletSchedCtx tabletCtx) {
        int finishedCounter = tabletCtx.getFinishedCounter();
        finishedCounter++;
        tabletCtx.setFinishedCounter(finishedCounter);
        if (finishedCounter >= TabletSchedCtx.FINISHED_COUNTER_THRESHOLD) {
            return;
        }

        Database db = Env.getCurrentInternalCatalog().getDbNullable(tabletCtx.getDbId());
        if (db == null) {
            return;
        }
        OlapTable tbl = (OlapTable) db.getTableNullable(tabletCtx.getTblId());
        if (tbl == null) {
            return;
        }
        Pair<TabletStatus, TabletSchedCtx.Priority> statusPair;
        ReplicaAllocation replicaAlloc = null;
        tbl.readLock();
        try {
            Partition partition = tbl.getPartition(tabletCtx.getPartitionId());
            if (partition == null) {
                return;
            }

            MaterializedIndex idx = partition.getIndex(tabletCtx.getIndexId());
            if (idx == null) {
                return;
            }

            Tablet tablet = idx.getTablet(tabletCtx.getTabletId());
            if (tablet == null) {
                return;
            }

            boolean isColocateTable = colocateTableIndex.isColocateTable(tbl.getId());
            if (isColocateTable) {
                GroupId groupId = colocateTableIndex.getGroup(tbl.getId());
                if (groupId == null) {
                    return;
                }
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(groupId);
                if (groupSchema == null) {
                    return;
                }

                replicaAlloc = groupSchema.getReplicaAlloc();
                int tabletOrderIdx = tabletCtx.getTabletOrderIdx();
                if (tabletOrderIdx == -1) {
                    tabletOrderIdx = idx.getTabletOrderIdx(tablet.getId());
                }
                Preconditions.checkState(tabletOrderIdx != -1);

                Set<Long> backendsSet = colocateTableIndex.getTabletBackendsByGroup(groupId, tabletOrderIdx);
                TabletStatus st = tablet.getColocateHealthStatus(
                        partition.getVisibleVersion(), replicaAlloc, backendsSet);
                statusPair = Pair.of(st, Priority.HIGH);
                tabletCtx.setColocateGroupBackendIds(backendsSet);
            } else {
                replicaAlloc = tbl.getPartitionInfo().getReplicaAllocation(partition.getId());
                List<Long> aliveBeIds = infoService.getAllBackendIds(true);
                statusPair = tablet.getHealthStatusWithPriority(
                        infoService, partition.getVisibleVersion(), replicaAlloc, aliveBeIds);

                if (statusPair.second.ordinal() < tabletCtx.getPriority().ordinal()) {
                    statusPair.second = tabletCtx.getPriority();
                }
            }
        } finally {
            tbl.readUnlock();
        }

        if (statusPair.first == TabletStatus.HEALTHY) {
            return;
        }

        TabletSchedCtx newTabletCtx = new TabletSchedCtx(
                TabletSchedCtx.Type.REPAIR, tabletCtx.getDbId(), tabletCtx.getTblId(),
                tabletCtx.getPartitionId(), tabletCtx.getIndexId(), tabletCtx.getTabletId(),
                replicaAlloc, System.currentTimeMillis());

        newTabletCtx.setTabletStatus(statusPair.first);
        newTabletCtx.setPriority(statusPair.second);
        newTabletCtx.setFinishedCounter(finishedCounter);

        addTablet(newTabletCtx, false);
    }

    private void releaseTabletCtx(TabletSchedCtx tabletCtx, TabletSchedCtx.State state, boolean resetReplicaState) {
        tabletCtx.setState(state);
        tabletCtx.releaseResource(this);
        if (resetReplicaState) {
            tabletCtx.resetReplicaState();
        }
        tabletCtx.setFinishedTime(System.currentTimeMillis());
    }

    private synchronized void removeTabletCtx(TabletSchedCtx tabletCtx, String reason) {
        runningTablets.remove(tabletCtx.getTabletId());
        allTabletTypes.remove(tabletCtx.getTabletId());
        schedHistory.add(tabletCtx);
        LOG.info("remove the tablet {}. because: {}", tabletCtx, reason);
    }

    // get next batch of tablets from queue.
    private synchronized List<TabletSchedCtx> getNextTabletCtxBatch() {
        List<TabletSchedCtx> list = Lists.newArrayList();
        int slotNum = getCurrentAvailableSlotNum();
        // Make slotNum >= 1 to ensure that it could return at least 1 ctx
        // when the pending list is not empty.
        if (slotNum < 1) {
            slotNum = 1;
        }
        while (list.size() < Config.schedule_batch_size && slotNum > 0) {
            TabletSchedCtx tablet = pendingTablets.poll();
            if (tablet == null) {
                // no more tablets
                break;
            }
            list.add(tablet);
            TabletStatus status = tablet.getTabletStatus();
            // for a clone, it will take 2 slots: src slot and dst slot.
            if (!(status == TabletStatus.REDUNDANT
                    || status == TabletStatus.FORCE_REDUNDANT
                    || status == TabletStatus.COLOCATE_REDUNDANT
                    || status == TabletStatus.REPLICA_COMPACTION_TOO_SLOW)) {
                slotNum -= 2;
            }
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

    public boolean finishStorageMediaMigrationTask(StorageMediaMigrationTask migrationTask,
                        TFinishTaskRequest request) {
        long tabletId = migrationTask.getTabletId();
        TabletSchedCtx tabletCtx = takeRunningTablets(tabletId);
        if (tabletCtx == null) {
            // tablet does not exist, the task may be created by ReportHandler.tabletReport(ssd => hdd)
            LOG.warn("tablet info does not exist: {}", tabletId);
            return true;
        }
        if (tabletCtx.getBalanceType() != TabletSchedCtx.BalanceType.DISK_BALANCE) {
            // this should not happen
            LOG.warn("task type is not as excepted. tablet {}", tabletId);
            return true;
        }
        if (request.getTaskStatus().getStatusCode() == TStatusCode.OK) {
            // if we have a success task, then stat must be refreshed before schedule a new task
            updateDiskBalanceLastSuccTime(tabletCtx.getSrcBackendId(), tabletCtx.getSrcPathHash());
            updateDiskBalanceLastSuccTime(tabletCtx.getDestBackendId(), tabletCtx.getDestPathHash());
            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, Status.FINISHED, "finished");
        } else {
            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, Status.UNRECOVERABLE,
                    request.getTaskStatus().getErrorMsgs().get(0));
        }

        return true;
    }

    /**
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
        if (tabletCtx.getBalanceType() == TabletSchedCtx.BalanceType.DISK_BALANCE) {
            // this should not happen
            LOG.warn("task type is not as excepted. tablet {}", tabletId);
            return true;
        }

        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.RUNNING, tabletCtx.getState());
        try {
            tabletCtx.finishCloneTask(cloneTask, request);
        } catch (SchedException e) {
            tabletCtx.setErrMsg(e.getMessage());
            if (e.getStatus() == Status.RUNNING_FAILED) {
                tabletCtx.increaseFailedRunningCounter();
                if (!tabletCtx.isExceedFailedRunningLimit()) {
                    stat.counterCloneTaskFailed.incrementAndGet();
                    tabletCtx.releaseResource(this);
                    tabletCtx.resetFailedSchedCounter();
                    tabletCtx.setState(TabletSchedCtx.State.PENDING);
                    addBackToPendingTablets(tabletCtx);
                    return false;
                } else {
                    // unrecoverable
                    stat.counterTabletScheduledDiscard.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, Status.UNRECOVERABLE,
                            e.getMessage());
                    return true;
                }
            } else if (e.getStatus() == Status.UNRECOVERABLE) {
                // unrecoverable
                stat.counterTabletScheduledDiscard.incrementAndGet();
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getStatus(), e.getMessage());
                return true;
            } else if (e.getStatus() == Status.FINISHED) {
                // tablet is already healthy, just remove
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getStatus(), e.getMessage());
                return true;
            }
        } catch (Exception e) {
            LOG.warn("got unexpected exception when finish clone task. tablet: {}",
                    tabletCtx.getTabletId(), e);
            stat.counterTabletScheduledDiscard.incrementAndGet();
            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.UNEXPECTED, Status.UNRECOVERABLE, e.getMessage());
            return true;
        }

        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.FINISHED);
        stat.counterCloneTaskSucceeded.incrementAndGet();
        gatherStatistics(tabletCtx);
        finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, Status.FINISHED, "finished");
        return true;
    }

    /**
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

    /**
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
            runningTablets.values().stream().filter(TabletSchedCtx::isTimeout).forEach(timeoutTablets::add);
        }

        // 2. release ctx
        timeoutTablets.forEach(t -> {
            // Set "resetReplicaState" to true because
            // the timeout task should also be considered as UNRECOVERABLE,
            // so need to reset replica state.
            finalizeTabletCtx(t, TabletSchedCtx.State.CANCELLED, Status.UNRECOVERABLE, "timeout");
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
        tabletCtxs.forEach(t -> {
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
        return allTabletTypes.size();
    }

    public synchronized int getBalanceTabletsNumber() {
        return (int) (pendingTablets.stream().filter(t -> t.getType() == Type.BALANCE).count()
                + runningTablets.values().stream().filter(t -> t.getType() == Type.BALANCE).count());
    }

    private synchronized Map<Long, Long> getPathsCopingSize() {
        Map<Long, Long> pathsCopingSize = Maps.newHashMap();
        for (TabletSchedCtx tablet : runningTablets.values()) {
            long pathHash = tablet.getDestPathHash();
            if (pathHash == 0 || pathHash == -1) {
                continue;
            }

            long copingSize = tablet.getDestEstimatedCopingSize();
            if (copingSize > 0) {
                Long size = pathsCopingSize.getOrDefault(pathHash, 0L);
                pathsCopingSize.put(pathHash, size + copingSize);
            }
        }
        return pathsCopingSize;
    }

    private void incrDestPathCopingSize(TabletSchedCtx tablet) {
        long destPathHash = tablet.getDestPathHash();
        if (destPathHash == -1 || destPathHash == 0) {
            return;
        }

        for (LoadStatisticForTag loadStatistic : statisticMap.values()) {
            BackendLoadStatistic beLoadStatistic = loadStatistic.getBackendLoadStatistics().stream()
                    .filter(v -> v.getBeId() == tablet.getDestBackendId()).findFirst().orElse(null);
            if (beLoadStatistic != null) {
                beLoadStatistic.incrPathCopingSize(destPathHash, tablet.getDestEstimatedCopingSize());
                break;
            }
        }
    }

    /**
     * PathSlot keeps track of slot num per path of a Backend.
     * Each path on a Backend has several slot.
     * If a path's available slot num become 0, no task should be assigned to this path.
     */
    public static class PathSlot {
        // path hash -> slot num
        private Map<Long, Slot> pathSlots = Maps.newConcurrentMap();
        private long beId;

        public PathSlot(Map<Long, TStorageMedium> paths, long beId) {
            this.beId = beId;
            for (Map.Entry<Long, TStorageMedium> entry : paths.entrySet()) {
                pathSlots.put(entry.getKey(), new Slot(entry.getValue()));
            }
        }

        // update the path
        public synchronized void updatePaths(Map<Long, TStorageMedium> paths) {
            // delete non exist path
            pathSlots.entrySet().removeIf(entry -> !paths.containsKey(entry.getKey()));

            // add new path
            for (Map.Entry<Long, TStorageMedium> entry : paths.entrySet()) {
                if (!pathSlots.containsKey(entry.getKey())) {
                    pathSlots.put(entry.getKey(), new Slot(entry.getValue()));
                }
            }
        }

        /**
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

        public synchronized boolean hasAvailableSlot(long pathHash) {
            if (pathHash == -1) {
                return false;
            }

            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return false;
            }
            if (slot.getAvailable() == 0) {
                return false;
            }
            return true;
        }

        public synchronized boolean hasAvailableBalanceSlot(long pathHash) {
            if (pathHash == -1) {
                return false;
            }
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return false;
            }
            if (slot.getAvailableBalance() == 0) {
                return false;
            }
            return true;
        }

        /**
         * If the specified 'pathHash' has available slot, decrease the slot number and return this path hash
         */
        public synchronized long takeSlot(long pathHash) throws SchedException {
            if (pathHash == -1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("path hash is not set.", new Exception());
                }
                throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                        "path hash is not set");
            }

            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                LOG.debug("path {} is not exist", pathHash);
                return -1;
            }
            if (slot.used >= slot.getTotal()) {
                LOG.debug("path {} has no available slot", pathHash);
                return -1;
            }
            slot.used++;
            return pathHash;
        }

        public synchronized void freeSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            if (slot.used > 0) {
                slot.used--;
            }
        }

        public synchronized int getTotalAvailSlotNum() {
            int total = 0;
            for (Slot slot : pathSlots.values()) {
                total += slot.getAvailable();
            }
            return total;
        }

        public synchronized int getTotalAvailBalanceSlotNum() {
            int num = 0;
            for (Slot slot : pathSlots.values()) {
                num += slot.getAvailableBalance();
            }
            return num;
        }

        /**
         * get path whose balance slot num is larger than 0
         */
        public synchronized Set<Long> getAvailPathsForBalance() {
            Set<Long> pathHashs = Sets.newHashSet();
            for (Map.Entry<Long, Slot> entry : pathSlots.entrySet()) {
                if (entry.getValue().getAvailableBalance() > 0) {
                    pathHashs.add(entry.getKey());
                }
            }
            return pathHashs;
        }

        public synchronized List<List<String>> getSlotInfo(long beId) {
            List<List<String>> results = Lists.newArrayList();
            pathSlots.forEach((key, value) -> {
                List<String> result = Lists.newArrayList();
                result.add(String.valueOf(beId));
                result.add(String.valueOf(key));
                result.add(String.valueOf(value.getAvailable()));
                result.add(String.valueOf(value.getTotal()));
                result.add(String.valueOf(value.getAvailableBalance()));
                result.add(String.valueOf(value.getAvgRate()));
                results.add(result);
            });
            return results;
        }

        public synchronized int getAvailableBalanceNum(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            return slot != null ? slot.getAvailableBalance() : 0;
        }

        public synchronized long takeBalanceSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            if (slot.balanceUsed < slot.getBalanceTotal()) {
                slot.balanceUsed++;
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
                if (slot.balanceUsed < slot.getBalanceTotal()) {
                    slot.balanceUsed++;
                    return pathHash;
                }
            }
            return -1;
        }

        public synchronized void freeBalanceSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            if (slot.balanceUsed > 0) {
                slot.balanceUsed--;
            }
        }

        public synchronized void updateDiskBalanceLastSuccTime(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            slot.diskBalanceLastSuccTime = System.currentTimeMillis();
        }

        public synchronized long getDiskBalanceLastSuccTime(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return 0L;
            }
            return slot.diskBalanceLastSuccTime;
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
        public int used;
        public int balanceUsed;

        public long totalCopySize = 0;
        public long totalCopyTimeMs = 0;

        // for disk balance
        public long diskBalanceLastSuccTime = 0;

        private TStorageMedium storageMedium;

        public Slot(TStorageMedium storageMedium) {
            this.storageMedium = storageMedium;
            this.used = 0;
            this.balanceUsed = 0;
        }

        public int getAvailable() {
            return Math.max(0, getTotal() - used);
        }

        public int getTotal() {
            if (storageMedium == TStorageMedium.SSD) {
                return Config.schedule_slot_num_per_ssd_path;
            } else {
                return Config.schedule_slot_num_per_hdd_path;
            }
        }

        public int getAvailableBalance() {
            int leftBalance = Math.max(0, getBalanceTotal() - balanceUsed);
            return Math.min(leftBalance, getAvailable());
        }

        public int getBalanceTotal() {
            return Math.max(1, Config.balance_slot_num_per_path);
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
