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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletSlidingWindowAccessStats;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TCheckWarmUpCacheAsyncRequest;
import org.apache.doris.thrift.TCheckWarmUpCacheAsyncResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWarmUpCacheAsyncRequest;
import org.apache.doris.thrift.TWarmUpCacheAsyncResponse;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    private volatile ConcurrentHashMap<Long, Set<Tablet>> beToTabletsGlobal =
            new ConcurrentHashMap<Long, Set<Tablet>>();

    private volatile ConcurrentHashMap<Long, Set<Tablet>> beToColocateTabletsGlobal =
            new ConcurrentHashMap<Long, Set<Tablet>>();

    // used for cloud tablet report
    private volatile ConcurrentHashMap<Long, Set<Tablet>> beToTabletsGlobalInSecondary =
            new ConcurrentHashMap<Long, Set<Tablet>>();

    private volatile ConcurrentHashMap<Long, Set<Tablet>> futureBeToTabletsGlobal;

    private Map<String, List<Long>> clusterToBes;

    private Set<Long> allBes;

    // partitionId -> indexId -> be -> tablet
    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>> partitionToTablets;

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>
            futurePartitionToTablets;

    // tableId -> be -> tablet
    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> beToTabletsInTable;

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> futureBeToTabletsInTable;

    private Map<Long, Long> beToDecommissionedTime = new HashMap<Long, Long>();

    private Random rand = new Random();

    private boolean indexBalanced = true;

    private boolean tableBalanced = true;

    // Scheduling phase for active-tablet priority scheduling.
    // ACTIVE_ONLY: only schedule objects (partition/table) that have activeCnt > 0 (non-internal).
    // INACTIVE_ONLY: schedule objects that are not in ACTIVE_ONLY set, with internal db objects always last.
    // ALL: schedule all objects (keeps internal db last when priority scheduling enabled).
    private enum ActiveSchedulePhase {
        ACTIVE_ONLY,
        INACTIVE_ONLY,
        ALL
    }

    private volatile boolean inited = false;

    private LinkedBlockingQueue<Pair<Long, Long>> tabletsMigrateTasks = new LinkedBlockingQueue<Pair<Long, Long>>();

    private Map<InfightTablet, InfightTask> tabletToInfightTask = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<WarmupBatchKey, WarmupBatch> warmupBatches = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService warmupBatchScheduler;

    private volatile ScheduledExecutorService warmupCheckScheduler;

    private volatile ExecutorService warmupRpcExecutor;

    private final ConcurrentLinkedQueue<WarmupTabletTask> failedWarmupTasks = new ConcurrentLinkedQueue<>();

    private CloudSystemInfoService cloudSystemInfoService;

    private final Object warmupExecutorInitLock = new Object();

    private BalanceTypeEnum globalBalanceTypeEnum = BalanceTypeEnum.getCloudWarmUpForRebalanceTypeEnum();

    private Set<Long> activeTabletIds = new HashSet<>();

    // cache for scheduling order in one daemon run (rebuilt in statRouteInfo)
    // table/partition active count is computed from activeTabletIds
    private volatile Map<Long, Long> tableIdToActiveCount = new ConcurrentHashMap<>();
    private volatile Map<Long, Long> partitionIdToActiveCount = new ConcurrentHashMap<>();
    private volatile Map<Long, Long> dbIdToActiveCount = new ConcurrentHashMap<>();
    private volatile Map<Long, Long> tableIdToDbId = new ConcurrentHashMap<>();
    private volatile Map<Long, Long> partitionIdToDbId = new ConcurrentHashMap<>();
    // run-level cache: dbId -> isInternalDb (rebuilt in statRouteInfo)
    private volatile Map<Long, Boolean> dbIdToInternal = new ConcurrentHashMap<>();
    private static final Set<String> INTERNAL_DB_NAMES = Sets.newHashSet("__internal_schema", "information_schema");

    private static final class LocationKey {
        private final long dbId;
        private final long tableId;
        private final long partitionId;
        private final long indexId;

        private LocationKey(long dbId, long tableId, long partitionId, long indexId) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.indexId = indexId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocationKey)) {
                return false;
            }
            LocationKey that = (LocationKey) o;
            return dbId == that.dbId
                    && tableId == that.tableId
                    && partitionId == that.partitionId
                    && indexId == that.indexId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbId, tableId, partitionId, indexId);
        }
    }

    /**
     * Get the current balance type for a compute group, falling back to global balance type if not found
     */
    private BalanceTypeEnum getCurrentBalanceType(String clusterId) {
        ComputeGroup cg = cloudSystemInfoService.getComputeGroupById(clusterId);
        if (cg == null) {
            LOG.debug("compute group not found, use global balance type, id {}", clusterId);
            return globalBalanceTypeEnum;
        }

        BalanceTypeEnum computeGroupBalanceType = cg.getBalanceType();
        if (isComputeGroupBalanceChanged(clusterId)) {
            return computeGroupBalanceType;
        }
        return globalBalanceTypeEnum;
    }

    /**
     * Get the current task timeout for a compute group, falling back to global timeout if not found
     */
    private int getCurrentTaskTimeout(String clusterId) {
        ComputeGroup cg = cloudSystemInfoService.getComputeGroupById(clusterId);
        if (cg == null) {
            return Config.cloud_pre_heating_time_limit_sec;
        }

        int computeGroupTimeout = cg.getBalanceWarmUpTaskTimeout();
        if (isComputeGroupBalanceChanged(clusterId)) {
            return computeGroupTimeout;
        }

        return Config.cloud_pre_heating_time_limit_sec;
    }

    private boolean isComputeGroupBalanceChanged(String clusterId) {
        ComputeGroup cg = cloudSystemInfoService.getComputeGroupById(clusterId);
        if (cg == null) {
            return false;
        }

        BalanceTypeEnum computeGroupBalanceType = cg.getBalanceType();
        int computeGroupTimeout = cg.getBalanceWarmUpTaskTimeout();
        return computeGroupBalanceType != ComputeGroup.DEFAULT_COMPUTE_GROUP_BALANCE_ENUM
               || computeGroupTimeout != ComputeGroup.DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT;
    }

    public CloudTabletRebalancer(CloudSystemInfoService cloudSystemInfoService) {
        super("cloud tablet rebalancer", Config.cloud_tablet_rebalancer_interval_second * 1000);
        this.cloudSystemInfoService = cloudSystemInfoService;
    }

    private void initializeWarmupExecutorsIfNeeded() {
        if (warmupRpcExecutor != null) {
            return; // Already initialized
        }
        synchronized (warmupExecutorInitLock) {
            if (warmupRpcExecutor != null) {
                return; // Double check
            }
            Env env = Env.getCurrentEnv();
            if (env == null || !env.isMaster()) {
                LOG.info("Env not initialized or not master, skip start warmup batch scheduler");
                return;
            }
            warmupRpcExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                Math.max(1, Config.cloud_warm_up_rpc_async_pool_size), 1000,
                "cloud-warmup-rpc-dispatch", true);
            warmupBatchScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cloud-warmup-batch-flusher");
                t.setDaemon(true);
                return t;
            });
            long flushInterval = Math.max(1L, Config.cloud_warm_up_batch_flush_interval_ms);
            warmupBatchScheduler.scheduleAtFixedRate(this::flushExpiredWarmupBatches,
                    flushInterval, flushInterval, TimeUnit.MILLISECONDS);

            warmupCheckScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cloud-warmup-checker");
                t.setDaemon(true);
                return t;
            });
            long warmupCheckInterval = 10L;
            warmupCheckScheduler.scheduleAtFixedRate(() -> {
                try {
                    // send check rpc to be, 10s check once
                    checkInflightWarmUpCacheAsync();
                } catch (Throwable t) {
                    LOG.warn("unexpected error when checking inflight warm up cache async", t);
                }
            }, warmupCheckInterval, warmupCheckInterval, TimeUnit.SECONDS);
            LOG.info("Warmup executors initialized successfully");
        }
    }

    private interface Operator {
        void op(Database db, Table table, Partition partition, MaterializedIndex index, String cluster);
    }

    public enum BalanceType {
        GLOBAL,
        TABLE,
        PARTITION
    }

    public enum StatType {
        GLOBAL,
        TABLE,
        PARTITION,
        SMOOTH_UPGRADE,
        WARM_UP_CACHE
    }

    @Getter
    private class InfightTablet {
        private final Long tabletId;
        private final String clusterId;

        public InfightTablet(Long tabletId, String clusterId) {
            this.tabletId = tabletId;
            this.clusterId = clusterId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InfightTablet that = (InfightTablet) o;
            return tabletId.equals(that.tabletId) && clusterId.equals(that.clusterId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tabletId, clusterId);
        }
    }

    private class InfightTask {
        public Tablet pickedTablet;
        public long srcBe;
        public long destBe;
        public Map<Long, Set<Tablet>> beToTablets;
        public long startTimestamp;
        BalanceType balanceType;
    }

    @Getter
    private static class WarmupBatchKey {
        private final long srcBe;
        private final long destBe;

        WarmupBatchKey(long srcBe, long destBe) {
            this.srcBe = srcBe;
            this.destBe = destBe;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WarmupBatchKey)) {
                return false;
            }
            WarmupBatchKey that = (WarmupBatchKey) o;
            return srcBe == that.srcBe && destBe == that.destBe;
        }

        @Override
        public int hashCode() {
            return Objects.hash(srcBe, destBe);
        }
    }

    private static class WarmupTabletTask {
        private final Tablet pickedTablet;
        private final long srcBe;
        private final long destBe;
        private final String clusterId;

        WarmupTabletTask(Tablet pickedTablet, long srcBe, long destBe, String clusterId) {
            this.pickedTablet = pickedTablet;
            this.srcBe = srcBe;
            this.destBe = destBe;
            this.clusterId = clusterId;
        }
    }

    private static class WarmupBatch {
        private final WarmupBatchKey key;
        private final List<WarmupTabletTask> tasks = new ArrayList<>();
        private long lastUpdateMs = System.currentTimeMillis();

        WarmupBatch(WarmupBatchKey key) {
            this.key = key;
        }

        synchronized List<WarmupTabletTask> addTask(WarmupTabletTask task, int batchSize) {
            tasks.add(task);
            lastUpdateMs = System.currentTimeMillis();
            if (tasks.size() >= batchSize) {
                return drain();
            }
            return Collections.emptyList();
        }

        synchronized List<WarmupTabletTask> drainIfExpired(long flushIntervalMs) {
            if (tasks.isEmpty()) {
                return Collections.emptyList();
            }
            if (System.currentTimeMillis() - lastUpdateMs >= flushIntervalMs) {
                return drain();
            }
            return Collections.emptyList();
        }

        synchronized boolean isEmpty() {
            return tasks.isEmpty();
        }

        private List<WarmupTabletTask> drain() {
            List<WarmupTabletTask> copy = new ArrayList<>(tasks);
            tasks.clear();
            lastUpdateMs = System.currentTimeMillis();
            return copy;
        }
    }

    private class TransferPairInfo {
        public long srcBe;
        public long destBe;
        public long minTabletsNum;
        public long maxTabletsNum;
    }

    public Set<Long> getSnapshotTabletsInPrimaryByBeId(Long beId) {
        Set<Long> tabletIds = Sets.newHashSet();
        Set<Tablet> tablets = beToTabletsGlobal.get(beId);
        if (tablets != null) {
            //  Create a copy
            for (Tablet tablet : new HashSet<>(tablets)) {
                tabletIds.add(tablet.getId());
            }
        }

        Set<Tablet> colocateTablets = beToColocateTabletsGlobal.get(beId);
        if (colocateTablets != null) {
            //  Create a copy
            for (Tablet tablet : new HashSet<>(colocateTablets)) {
                tabletIds.add(tablet.getId());
            }
        }

        return tabletIds;
    }

    public Set<Long> getSnapshotTabletsInSecondaryByBeId(Long beId) {
        Set<Long> tabletIds = Sets.newHashSet();
        Set<Tablet> tablets = beToTabletsGlobalInSecondary.get(beId);
        if (tablets != null) {
            //  Create a copy
            for (Tablet tablet : new HashSet<>(tablets)) {
                tabletIds.add(tablet.getId());
            }
        }
        return tabletIds;
    }

    public Set<Long> getSnapshotTabletsInPrimaryAndSecondaryByBeId(Long beId) {
        Set<Long> tabletIds = Sets.newHashSet();
        tabletIds.addAll(getSnapshotTabletsInPrimaryByBeId(beId));
        tabletIds.addAll(getSnapshotTabletsInSecondaryByBeId(beId));
        return tabletIds;
    }

    public int getTabletNumByBackendId(long beId) {
        Map<Long, Set<Tablet>> sourceMap = beToTabletsGlobal;
        ConcurrentHashMap<Long, Set<Tablet>> futureMap = futureBeToTabletsGlobal;
        if (futureMap != null && !futureMap.isEmpty()) {
            sourceMap = futureMap;
        }

        Set<Tablet> tablets = sourceMap.get(beId);
        Set<Tablet> colocateTablets = beToColocateTabletsGlobal.get(beId);

        int tabletsSize = (tablets == null) ? 0 : tablets.size();
        int colocateTabletsSize = (colocateTablets == null) ? 0 : colocateTablets.size();

        return tabletsSize + colocateTabletsSize;
    }

    // 1 build cluster to backends info
    // 2 complete route info
    // 3 check whether the inflight preheating task has been completed
    // 4 migrate tablet for smooth upgrade
    // 5 statistics be to tablets mapping information
    // 6 partition-level balance
    // 7 if tablets in partition-level already balanced, perform table balance
    // 8 if tablets in partition-level and table-level already balanced, perform global balance
    // 9 check whether all tablets of decomission node have been migrated
    @Override
    protected void runAfterCatalogReady() {
        // Initialize warmup executors when catalog is ready
        initializeWarmupExecutorsIfNeeded();

        if (Config.enable_cloud_multi_replica) {
            LOG.info("Tablet balance is temporarily not supported when multi replica enabled");
            return;
        }

        LOG.info("cloud tablet rebalance begin");
        long start = System.currentTimeMillis();
        activeTabletIds = getActiveTabletIds();
        globalBalanceTypeEnum = BalanceTypeEnum.getCloudWarmUpForRebalanceTypeEnum();

        buildClusterToBackendMap();
        if (!completeRouteInfo()) {
            return;
        }

        statRouteInfo();
        migrateTabletsForSmoothUpgrade();
        statRouteInfo();

        indexBalanced = true;
        tableBalanced = true;

        performBalancing();

        checkDecommissionState(clusterToBes);
        inited = true;
        long sleepSeconds = Config.cloud_tablet_rebalancer_interval_second;
        if (sleepSeconds < 0L) {
            LOG.warn("cloud tablet rebalance interval second is negative, change it to default 1s");
            sleepSeconds = 1L;
        }
        long balanceEnd = System.currentTimeMillis();
        if (DebugPointUtil.isEnable("CloudTabletRebalancer.balanceEnd.tooLong")) {
            LOG.info("debug pointCloudTabletRebalancer.balanceEnd.tooLong");
            // slower the balance end time to trigger next balance immediately
            balanceEnd += (Config.cloud_tablet_rebalancer_interval_second + 10L) * 1000L;
        }
        if (balanceEnd - start > Config.cloud_tablet_rebalancer_interval_second * 1000L) {
            sleepSeconds = 1L;
        }
        setInterval(sleepSeconds * 1000L);
        LOG.info("finished to rebalancer. cost: {} ms, rebalancer sche interval {} s",
                (System.currentTimeMillis() - start), sleepSeconds);
    }

    private void buildClusterToBackendMap() {
        clusterToBes = new HashMap<>();
        allBes = new HashSet<>();
        for (Long beId : cloudSystemInfoService.getAllBackendIds()) {
            Backend be = cloudSystemInfoService.getBackend(beId);
            if (be == null) {
                LOG.info("backend {} not found", beId);
                continue;
            }
            clusterToBes.putIfAbsent(be.getCloudClusterId(), new ArrayList<>());
            clusterToBes.get(be.getCloudClusterId()).add(beId);
            allBes.add(beId);
        }
        LOG.info("cluster to backends {}", clusterToBes);
    }

    private void migrateTabletsForSmoothUpgrade() {
        Pair<Long, Long> pair;
        while (!tabletsMigrateTasks.isEmpty()) {
            try {
                pair = tabletsMigrateTasks.take();
                LOG.debug("begin tablets migration from be {} to be {}", pair.first, pair.second);
                migrateTablets(pair.first, pair.second);
            } catch (InterruptedException e) {
                LOG.warn("migrate tablets failed", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void performBalancing() {
        // ATTN: In general, the order of `balance` should follow `partition`, `table`, and `global`.
        // This is because performing `global` scheduling first and then `partition` scheduling may
        // lead to ineffective scheduling. Specifically, `global` scheduling might place multiple tablets belonging
        // to the same table or partition onto the same BE, while `partition` scheduling later requires these tablets
        // to be dispersed across different BEs, resulting in unnecessary scheduling.
        if (!Config.enable_cloud_active_tablet_priority_scheduling) {
            // Legacy scheduling: schedule the full set.
            if (Config.enable_cloud_partition_balance) {
                balanceAllPartitionsByPhase(ActiveSchedulePhase.ALL);
            }
            if (Config.enable_cloud_table_balance && indexBalanced) {
                balanceAllTablesByPhase(ActiveSchedulePhase.ALL);
            }
            if (Config.enable_cloud_global_balance && indexBalanced && tableBalanced) {
                globalBalance();
            }
        } else {
            // When enabled, do a real two-phase scheduling:
            // Phase 1: schedule only active partitions/tables first.
            // If all active objects are balanced in this run, enter Phase 2:
            // schedule remaining (all - active) objects.
            boolean activeBalanced = true;

            // Phase 1: active-only
            boolean activeIndexBalanced = true;
            boolean activeTableBalanced = true;
            if (Config.enable_cloud_partition_balance) {
                activeIndexBalanced = balanceAllPartitionsByPhase(ActiveSchedulePhase.ACTIVE_ONLY);
            }
            if (Config.enable_cloud_table_balance && activeIndexBalanced) {
                activeTableBalanced = balanceAllTablesByPhase(ActiveSchedulePhase.ACTIVE_ONLY);
            }

            activeBalanced = (!Config.enable_cloud_partition_balance || activeIndexBalanced)
                    && (!Config.enable_cloud_table_balance || activeTableBalanced);

            if (LOG.isDebugEnabled()) {
                LOG.debug("active scheduling phase done: activeIndexBalanced={}, activeTableBalanced={}, "
                                + "activeBalanced={}, clusterNum={}",
                        activeIndexBalanced, activeTableBalanced, activeBalanced, clusterToBes.size());
            }

            if (!activeBalanced) {
                // Active objects are not balanced yet; skip phase2 to avoid diluting scheduling budget.
                return;
            }

            // Phase 2: inactive (all - active), then global if enabled and ready.
            boolean phase2IndexBalanced = true;
            boolean phase2TableBalanced = true;
            if (Config.enable_cloud_partition_balance) {
                phase2IndexBalanced = balanceAllPartitionsByPhase(ActiveSchedulePhase.INACTIVE_ONLY);
            }
            if (Config.enable_cloud_table_balance && phase2IndexBalanced) {
                phase2TableBalanced = balanceAllTablesByPhase(ActiveSchedulePhase.INACTIVE_ONLY);
            }
            if (Config.enable_cloud_global_balance && phase2IndexBalanced && phase2TableBalanced) {
                globalBalance();
            }
        }
    }

    private boolean balanceAllPartitionsByPhase(ActiveSchedulePhase phase) {
        // Reuse existing "balanced" flags as a per-phase signal.
        indexBalanced = true;

        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.debug("before partition balance({}) be {} tablet num {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
            for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.debug("before partition balance({}) be {} tablet num(current + pre heating inflight) {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInPartition(entry.getValue(), entry.getKey(), infos, phase);
        }
        // In warmup mode (ASYNC_WARMUP / SYNC_WARMUP), balanceImpl goes through preheatAndUpdateTablet
        // which only updates future maps and enqueues warmup tasks without adding to infos.
        // So infos can be empty even when balance work was done. Use indexBalanced (set to false by
        // updateBalanceStatus inside balanceImpl when warmup moves succeed) to reflect the real state.
        if (infos.isEmpty()) {
            LOG.info("partition balance({}) done, infos empty (warmup or already balanced), indexBalanced={}",
                    phase, indexBalanced);
            return indexBalanced;
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos, StatType.PARTITION);
        LOG.info("partition balance({}) collect to editlog before size={} after size={} infos, indexBalanced={}",
                phase, oldSize, infos.size(), indexBalanced);
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            return false;
        }

        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.debug("after partition balance({}) be {} tablet num {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
            for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.debug("after partition balance({}) be {} tablet num(current + pre heating inflight) {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
        }
        return indexBalanced;
    }

    private boolean balanceAllTablesByPhase(ActiveSchedulePhase phase) {
        tableBalanced = true;

        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.debug("before table balance({}) be {} tablet num {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
            for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.debug("before table balance({}) be {} tablet num(current + pre heating inflight) {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInTable(entry.getValue(), entry.getKey(), infos, phase);
        }
        // Same as balanceAllPartitionsByPhase: in warmup mode infos stays empty even when
        // warmup tasks were scheduled. Use tableBalanced to reflect the real state.
        if (infos.isEmpty()) {
            LOG.info("table balance({}) done, infos empty (warmup or already balanced), tableBalanced={}",
                    phase, tableBalanced);
            return tableBalanced;
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos, StatType.TABLE);
        LOG.info("table balance({}) collect to editlog before size={} after size={} infos, tableBalanced={}",
                phase, oldSize, infos.size(), tableBalanced);
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            return false;
        }

        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.debug("after table balance({}) be {} tablet num {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
            for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.debug("after table balance({}) be {} tablet num(current + pre heating inflight) {}",
                        phase, entry.getKey(), entry.getValue().size());
            }
        }
        return tableBalanced;
    }

    public void globalBalance() {
        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.debug("before global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }
            for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.debug("before global balance be {} tablet num(current + pre heating inflight) {}",
                        entry.getKey(), entry.getValue().size());
            }
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceImpl(entry.getValue(), entry.getKey(), futureBeToTabletsGlobal, BalanceType.GLOBAL, infos);
        }
        if (infos.isEmpty()) {
            return;
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos, StatType.GLOBAL);
        LOG.info("collect to editlog global before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.debug("after global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }
            for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.debug("after global balance be {} tablet num(current + pre heating inflight) {}",
                        entry.getKey(), entry.getValue().size());
            }
        }
    }

    public void checkInflightWarmUpCacheAsync() {
        Map<Long, List<InfightTask>> beToInfightTasks = new HashMap<Long, List<InfightTask>>();

        Set<InfightTablet> invalidTasks = new HashSet<>();
        for (Map.Entry<InfightTablet, InfightTask> entry : tabletToInfightTask.entrySet()) {
            String clusterId = entry.getKey().getClusterId();
            BalanceTypeEnum balanceTypeEnum = getCurrentBalanceType(clusterId);
            if (balanceTypeEnum == BalanceTypeEnum.WITHOUT_WARMUP) {
                // no need check warmup cache async
                invalidTasks.add(entry.getKey());
                continue;
            }
            beToInfightTasks.putIfAbsent(entry.getValue().destBe, new ArrayList<>());
            beToInfightTasks.get(entry.getValue().destBe).add(entry.getValue());
        }
        invalidTasks.forEach(key -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("remove inflight warmup task tablet {} cluster {} no need warmup",
                        key.getTabletId(), key.getClusterId());
            }
            tabletToInfightTask.remove(key);
        });

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        long needRehashDeadTime = System.currentTimeMillis() - Config.rehash_tablet_after_be_dead_seconds * 1000L;
        for (Map.Entry<Long, List<InfightTask>> entry : beToInfightTasks.entrySet()) {
            LOG.info("before pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
            Backend destBackend = cloudSystemInfoService.getBackend(entry.getKey());
            if (DebugPointUtil.isEnable("CloudTabletRebalancer.checkInflghtWarmUpCacheAsync.beNull")) {
                LOG.info("debug point CloudTabletRebalancer.checkInflghtWarmUpCacheAsync.beNull, be {}", destBackend);
                destBackend = null;
            }
            if (destBackend == null || (!destBackend.isAlive() && destBackend.getLastUpdateMs() < needRehashDeadTime)) {
                // dest backend not exist or dead too long, need remove all inflight tasks in this dest backend
                List<InfightTablet> toRemove = new LinkedList<>();
                for (InfightTask task : entry.getValue()) {
                    for (InfightTablet key : tabletToInfightTask.keySet()) {
                        toRemove.add(new InfightTablet(task.pickedTablet.getId(), key.clusterId));
                    }
                }
                for (InfightTablet key : toRemove) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("remove tablet {}-{}", key.getClusterId(), key.getTabletId());
                    }
                    tabletToInfightTask.remove(key);
                }
                continue;
            }
            if (!destBackend.isAlive()) {
                // dest backend dead, dead time smaller than rehash_tablet_after_be_dead_seconds, wait next time
                continue;
            }
            List<Long> tablets = entry.getValue().stream()
                    .map(task -> task.pickedTablet.getId()).collect(Collectors.toList());
            // check dest backend whether warmup cache done
            Map<Long, Boolean> taskDone = sendCheckWarmUpCacheAsyncRpc(tablets, entry.getKey());
            if (taskDone == null) {
                LOG.warn("sendCheckWarmUpCacheAsyncRpc return null be {}, inFight tasks {}",
                        entry.getKey(), entry.getValue());
                continue;
            }
            String clusterId = cloudSystemInfoService.getBackend(entry.getKey()).getCloudClusterId();
            for (Map.Entry<Long, Boolean> result : taskDone.entrySet()) {
                InfightTask task = tabletToInfightTask
                        .getOrDefault(new InfightTablet(result.getKey(), clusterId), null);
                handleWarmupCompletion(task, clusterId, result.getValue(), result.getKey(), infos);
            }
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos, StatType.WARM_UP_CACHE);
        LOG.info("collect to editlog warmup before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        // recalculate inflight beToTablets, just for print the log
        beToInfightTasks.clear();
        for (Map.Entry<InfightTablet, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToInfightTasks.putIfAbsent(entry.getValue().destBe, new ArrayList<>());
            beToInfightTasks.get(entry.getValue().destBe).add(entry.getValue());
        }

        for (Map.Entry<Long, List<InfightTask>> entry : beToInfightTasks.entrySet()) {
            LOG.info("after pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
        }
    }

    public void checkDecommissionState(Map<String, List<Long>> clusterToBes) {
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            List<Long> beList = entry.getValue();
            for (long beId : beList) {
                Set<Tablet> tablets = beToTabletsGlobal.get(beId);
                int tabletNum = tablets == null ? 0 : tablets.size();
                Backend backend = cloudSystemInfoService.getBackend(beId);
                if (backend == null) {
                    LOG.info("backend {} not found", beId);
                    continue;
                }
                if (!backend.isDecommissioning()) {
                    continue;
                }
                // here check wal
                long walNum = Env.getCurrentEnv().getGroupCommitManager().getAllWalQueueSize(backend);
                LOG.info("check decommissioning be {} state {} tabletNum {} isActive {} beList {}, wal num {}",
                        backend.getId(), backend.isDecommissioning(), tabletNum, backend.isActive(), beList, walNum);
                if ((tabletNum != 0 || backend.isActive() || walNum != 0) && beList.size() != 1) {
                    continue;
                }
                if (beToDecommissionedTime.containsKey(beId)) {
                    continue;
                }
                LOG.info("prepare to notify meta service be {} decommissioned", backend.getAddress());
                Cloud.AlterClusterRequest.Builder builder =
                        Cloud.AlterClusterRequest.newBuilder()
                                .setRequestIp(FrontendOptions.getLocalHostAddressCached());
                builder.setCloudUniqueId(Config.cloud_unique_id);
                builder.setOp(Cloud.AlterClusterRequest.Operation.NOTIFY_DECOMMISSIONED);

                Cloud.ClusterPB.Builder clusterBuilder =
                        Cloud.ClusterPB.newBuilder();
                clusterBuilder.setClusterName(backend.getCloudClusterName());
                clusterBuilder.setClusterId(backend.getCloudClusterId());
                clusterBuilder.setType(Cloud.ClusterPB.Type.COMPUTE);

                Cloud.NodeInfoPB.Builder nodeBuilder = Cloud.NodeInfoPB.newBuilder();
                nodeBuilder.setIp(backend.getHost());
                nodeBuilder.setHeartbeatPort(backend.getHeartbeatPort());
                nodeBuilder.setCloudUniqueId(backend.getCloudUniqueId());
                nodeBuilder.setStatus(Cloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONED);

                clusterBuilder.addNodes(nodeBuilder);
                builder.setCluster(clusterBuilder);

                Cloud.AlterClusterResponse response;
                try {
                    response = MetaServiceProxy.getInstance().alterCluster(builder.build());
                    if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                        LOG.warn("notify decommission response: {}", response);
                        continue;
                    }
                    LOG.info("notify decommission response: {} ", response);
                } catch (RpcException e) {
                    LOG.warn("failed to notify decommission", e);
                    continue;
                }
                beToDecommissionedTime.put(beId, System.currentTimeMillis() / 1000);
            }
        }
    }

    private boolean completeRouteInfo() {
        List<UpdateCloudReplicaInfo> updateReplicaInfos = new ArrayList<UpdateCloudReplicaInfo>();
        long[] assignedErrNum = {0L};
        long needRehashDeadTime = System.currentTimeMillis() - Config.rehash_tablet_after_be_dead_seconds * 1000L;
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean assigned = false;
            List<Long> beIds = new ArrayList<Long>();
            List<Long> tabletIds = new ArrayList<Long>();
            boolean isColocated = Env.getCurrentColocateIndex().isColocateTable(table.getId());
            for (Tablet tablet : index.getTablets()) {
                for (Replica r : tablet.getReplicas()) {
                    CloudReplica replica = (CloudReplica) r;
                    // clean secondary map
                    replica.checkAndClearSecondaryClusterToBe(cluster, needRehashDeadTime);
                    InfightTablet taskKey = new InfightTablet(tablet.getId(), cluster);
                    // colocate table no need to update primary backends
                    if (isColocated) {
                        replica.clearClusterToBe(cluster);
                        tabletToInfightTask.remove(taskKey);
                        continue;
                    }

                    // primary backend is alive or dead not long
                    Backend be = replica.getPrimaryBackend(cluster, false);
                    if (be != null && (be.isQueryAvailable()
                            || (!be.isQueryDisabled()
                            // Compatible with older version upgrades, see https://github.com/apache/doris/pull/42986
                            && (be.getLastUpdateMs() <= 0 || be.getLastUpdateMs() > needRehashDeadTime)))) {
                        beIds.add(be.getId());
                        tabletIds.add(tablet.getId());
                        continue;
                    }

                    // primary backend not available too long, change one
                    long beId = -1L;
                    be = replica.getSecondaryBackend(cluster);
                    if (be != null && be.isQueryAvailable()) {
                        beId = be.getId();
                    } else {
                        InfightTask task = tabletToInfightTask.get(taskKey);
                        be = task == null ? null : Env.getCurrentSystemInfo().getBackend(task.destBe);
                        if (be != null && be.isQueryAvailable()) {
                            beId = be.getId();
                        } else {
                            try {
                                beId = ((CloudReplica) replica).hashReplicaToBe(cluster, true);
                            } catch (ComputeGroupException e) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("failed to hash replica to be {}", cluster, e);
                                }
                                beId = -1;
                            }
                        }
                    }

                    if (beId <= 0) {
                        assignedErrNum[0]++;
                        continue;
                    }

                    LOG.debug("clusterId {} tablet {} change primary backend from {} to {}",
                            cluster, tablet.getId(), be == null ? -1 : be.getId(), beId);
                    tabletToInfightTask.remove(taskKey);

                    ((CloudReplica) replica).updateClusterToPrimaryBe(cluster, beId);
                    beIds.add(beId);
                    tabletIds.add(tablet.getId());
                    assigned = true;
                }
            }

            if (assigned) {
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(db.getId(), table.getId(),
                        partition.getId(), index.getId(), cluster, beIds, tabletIds);
                updateReplicaInfos.add(info);
            }
        });

        LOG.info("collect to editlog route {} infos, error num {}", updateReplicaInfos.size(), assignedErrNum[0]);

        if (updateReplicaInfos.isEmpty()) {
            return true;
        }

        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(updateReplicaInfos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return false;
        }

        return true;
    }

    public void fillBeToTablets(long be, long tableId, long partId, long indexId, Tablet tablet,
                                ConcurrentHashMap<Long, Set<Tablet>> globalBeToTablets,
                                ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> beToTabletsInTable,
                                ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>
                                    partToTablets) {
        // global
        globalBeToTablets.putIfAbsent(be, ConcurrentHashMap.newKeySet());
        globalBeToTablets.get(be).add(tablet);

        // table
        beToTabletsInTable.putIfAbsent(tableId, new ConcurrentHashMap<Long, Set<Tablet>>());
        ConcurrentHashMap<Long, Set<Tablet>> beToTabletsOfTable = beToTabletsInTable.get(tableId);
        beToTabletsOfTable.putIfAbsent(be, ConcurrentHashMap.newKeySet());
        beToTabletsOfTable.get(be).add(tablet);

        // partition
        partToTablets.putIfAbsent(partId, new ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>());
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> indexToTablets = partToTablets.get(partId);
        indexToTablets.putIfAbsent(indexId, new ConcurrentHashMap<Long, Set<Tablet>>());
        ConcurrentHashMap<Long, Set<Tablet>> beToTabletsOfIndex = indexToTablets.get(indexId);
        beToTabletsOfIndex.putIfAbsent(be, ConcurrentHashMap.newKeySet());
        beToTabletsOfIndex.get(be).add(tablet);
    }

    private void enqueueWarmupTask(WarmupTabletTask task) {
        WarmupBatchKey key = new WarmupBatchKey(task.srcBe, task.destBe);
        WarmupBatch batch = warmupBatches.computeIfAbsent(key, WarmupBatch::new);
        List<WarmupTabletTask> readyTasks = batch.addTask(task, Math.max(1, Config.cloud_warm_up_batch_size));
        if (!readyTasks.isEmpty()) {
            dispatchWarmupBatch(key, readyTasks);
        }
    }

    private void dispatchWarmupBatch(WarmupBatchKey key, List<WarmupTabletTask> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        initializeWarmupExecutorsIfNeeded();
        if (warmupRpcExecutor != null) {
            warmupRpcExecutor.submit(() -> sendWarmupBatch(key, tasks));
        } else {
            LOG.warn("warmupRpcExecutor is not initialized, skip dispatching warmup batch");
        }
    }

    private void sendWarmupBatch(WarmupBatchKey key, List<WarmupTabletTask> tasks) {
        Backend srcBackend = cloudSystemInfoService.getBackend(key.getSrcBe());
        Backend destBackend = cloudSystemInfoService.getBackend(key.getDestBe());
        if (srcBackend == null || destBackend == null || !destBackend.isAlive()) {
            handleWarmupBatchFailure(tasks, new IllegalStateException(
                    String.format("backend missing or dead, src %s dest %s", srcBackend, destBackend)));
            return;
        }
        List<Long> tabletIds = tasks.stream().map(task -> task.pickedTablet.getId()).collect(Collectors.toList());
        try {
            sendPreHeatingRpc(tabletIds, key.getSrcBe(), key.getDestBe());
        } catch (Exception e) {
            handleWarmupBatchFailure(tasks, e);
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("dispatch preheat batch {} from {} to {}, tablet num {}",
                    tabletIds, key.getSrcBe(), key.getDestBe(), tabletIds.size());
        }
    }

    private void handleWarmupBatchFailure(List<WarmupTabletTask> tasks, Exception e) {
        if (e != null) {
            LOG.warn("preheat batch failed, size {}", tasks.size(), e);
        }
        for (WarmupTabletTask task : tasks) {
            failedWarmupTasks.offer(task);
        }
    }

    private void revertWarmupState(WarmupTabletTask task) {
        updateBeToTablets(task.pickedTablet, task.destBe, task.srcBe,
                futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
        tabletToInfightTask.remove(new InfightTablet(task.pickedTablet.getId(), task.clusterId));
    }

    private void processFailedWarmupTasks() {
        WarmupTabletTask task;
        while ((task = failedWarmupTasks.poll()) != null) {
            revertWarmupState(task);
        }
    }

    private void flushExpiredWarmupBatches() {
        long flushInterval = Math.max(1L, Config.cloud_warm_up_batch_flush_interval_ms);
        for (Map.Entry<WarmupBatchKey, WarmupBatch> entry : warmupBatches.entrySet()) {
            List<WarmupTabletTask> readyTasks = entry.getValue().drainIfExpired(flushInterval);
            if (!readyTasks.isEmpty()) {
                dispatchWarmupBatch(entry.getKey(), readyTasks);
            }
        }
    }

    public void statRouteInfo() {
        ConcurrentHashMap<Long, Set<Tablet>> tmpBeToTabletsGlobal = new ConcurrentHashMap<Long, Set<Tablet>>();
        ConcurrentHashMap<Long, Set<Tablet>> tmpFutureBeToTabletsGlobal = new ConcurrentHashMap<Long, Set<Tablet>>();
        ConcurrentHashMap<Long, Set<Tablet>> tmpBeToTabletsGlobalInSecondary
                = new ConcurrentHashMap<Long, Set<Tablet>>();
        ConcurrentHashMap<Long, Set<Tablet>> tmpBeToColocateTabletsGlobal
                = new ConcurrentHashMap<Long, Set<Tablet>>();

        partitionToTablets = new ConcurrentHashMap<Long,
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>();
        futurePartitionToTablets =
                new ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>();

        beToTabletsInTable = new ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>();
        futureBeToTabletsInTable = new ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>();

        // rebuild scheduling caches for this run
        Map<Long, Long> tmpTableActive = new HashMap<>();
        Map<Long, Long> tmpPartitionActive = new HashMap<>();
        Map<Long, Long> tmpDbActive = new HashMap<>();
        Map<Long, Long> tmpTableToDb = new HashMap<>();
        Map<Long, Long> tmpPartitionToDb = new HashMap<>();
        Map<Long, Boolean> tmpDbInternal = new HashMap<>();

        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean isColocated = Env.getCurrentColocateIndex().isColocateTable(table.getId());
            tmpTableToDb.put(table.getId(), db.getId());
            tmpPartitionToDb.put(partition.getId(), db.getId());
            tmpDbInternal.computeIfAbsent(db.getId(), k -> {
                String name = db.getFullName();
                return name != null && INTERNAL_DB_NAMES.contains(name);
            });
            for (Tablet tablet : index.getTablets()) {
                // active tablet scoring (used for scheduling order)
                if (activeTabletIds != null && !activeTabletIds.isEmpty() && activeTabletIds.contains(tablet.getId())) {
                    tmpTableActive.merge(table.getId(), 1L, Long::sum);
                    tmpPartitionActive.merge(partition.getId(), 1L, Long::sum);
                    tmpDbActive.merge(db.getId(), 1L, Long::sum);
                }
                for (Replica r : tablet.getReplicas()) {
                    CloudReplica replica = (CloudReplica) r;
                    if (isColocated) {
                        long beId = -1L;
                        try {
                            beId = replica.getColocatedBeId(cluster);
                        } catch (ComputeGroupException e) {
                            continue;
                        }
                        if (allBes.contains(beId)) {
                            Set<Tablet> colocateTablets =
                                    tmpBeToColocateTabletsGlobal.computeIfAbsent(beId, k -> new HashSet<>());
                            colocateTablets.add(tablet);
                        }
                        continue;
                    }

                    Backend be = replica.getPrimaryBackend(cluster, false);
                    long beId = be == null ? -1L : be.getId();
                    if (!allBes.contains(beId)) {
                        continue;
                    }

                    Backend secondaryBe = replica.getSecondaryBackend(cluster);
                    long secondaryBeId = secondaryBe == null ? -1L : secondaryBe.getId();
                    if (allBes.contains(secondaryBeId)) {
                        Set<Tablet> tablets = tmpBeToTabletsGlobalInSecondary
                                .computeIfAbsent(secondaryBeId, k -> new HashSet<>());
                        tablets.add(tablet);
                    }

                    InfightTablet taskKey = new InfightTablet(tablet.getId(), cluster);
                    InfightTask task = tabletToInfightTask.get(taskKey);
                    long futureBeId = task == null ? beId : task.destBe;
                    fillBeToTablets(beId, table.getId(), partition.getId(), index.getId(), tablet,
                            tmpBeToTabletsGlobal, beToTabletsInTable, this.partitionToTablets);

                    fillBeToTablets(futureBeId, table.getId(), partition.getId(), index.getId(), tablet,
                            tmpFutureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
                }
            }
        });

        beToTabletsGlobal = tmpBeToTabletsGlobal;
        futureBeToTabletsGlobal = tmpFutureBeToTabletsGlobal;
        beToTabletsGlobalInSecondary = tmpBeToTabletsGlobalInSecondary;
        beToColocateTabletsGlobal = tmpBeToColocateTabletsGlobal;

        tableIdToActiveCount = new ConcurrentHashMap<>(tmpTableActive);
        partitionIdToActiveCount = new ConcurrentHashMap<>(tmpPartitionActive);
        dbIdToActiveCount = new ConcurrentHashMap<>(tmpDbActive);
        tableIdToDbId = new ConcurrentHashMap<>(tmpTableToDb);
        partitionIdToDbId = new ConcurrentHashMap<>(tmpPartitionToDb);
        dbIdToInternal = new ConcurrentHashMap<>(tmpDbInternal);
    }

    public void loopCloudReplica(Operator operator) {
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                table.readLock();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                                String cluster = entry.getKey();
                                operator.op(db, table, partition, index, cluster);
                            }
                        } // end for indices
                    } // end for partitions
                } finally {
                    table.readUnlock();
                }
            }
        }
    }


    private void balanceInPartition(List<Long> bes, String clusterId, List<UpdateCloudReplicaInfo> infos,
                                    ActiveSchedulePhase phase) {
        // balance all partition (prefer active partitions/tables, put internal db at tail)
        Iterable<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> partitions;
        if (Config.enable_cloud_active_tablet_priority_scheduling) {
            final Comparator<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> cmp =
                    partitionEntryComparator();
            // Phase-aware filtering and ordering.
            // - ACTIVE_ONLY: only non-internal partitions with activeCnt > 0
            // - INACTIVE_ONLY: all remaining partitions (non-internal inactive first, internal last)
            // - ALL: active (TopN first if configured) -> inactive -> internal
            List<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> nonInternalActive =
                    new ArrayList<>();
            List<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> nonInternalInactive =
                    new ArrayList<>();
            List<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> internalPartitions =
                    new ArrayList<>();

            for (Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>> e
                    : futurePartitionToTablets.entrySet()) {
                long partId = e.getKey();
                boolean internal = isInternalDbId(partitionIdToDbId.get(partId));
                long activeCnt = partitionIdToActiveCount.getOrDefault(partId, 0L);

                if (internal) {
                    // internal partitions are always handled at the end (not in ACTIVE_ONLY).
                    internalPartitions.add(e);
                    continue;
                }

                if (activeCnt > 0) {
                    nonInternalActive.add(e);
                } else {
                    nonInternalInactive.add(e);
                }
            }

            nonInternalActive.sort(cmp);
            nonInternalInactive.sort(cmp);
            internalPartitions.sort(cmp);

            List<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> ordered =
                    new ArrayList<>(futurePartitionToTablets.size());
            if (phase == ActiveSchedulePhase.ACTIVE_ONLY) {
                // In ACTIVE_ONLY phase, schedule all active partitions (already sorted by cmp, most active first)
                ordered.addAll(nonInternalActive);
            } else if (phase == ActiveSchedulePhase.INACTIVE_ONLY) {
                ordered.addAll(nonInternalInactive);
                ordered.addAll(internalPartitions);
            } else { // ALL
                // All active (already sorted by cmp, most active first), then inactive, then internal
                ordered.addAll(nonInternalActive);
                ordered.addAll(nonInternalInactive);
                ordered.addAll(internalPartitions);
            }

            partitions = ordered;
        } else {
            partitions = futurePartitionToTablets.entrySet();
        }

        for (Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>> partitionEntry
                : partitions) {
            Map<Long, ConcurrentHashMap<Long, Set<Tablet>>> indexToTablets = partitionEntry.getValue();
            // balance all index of a partition
            List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> indexes =
                    new ArrayList<>(indexToTablets.entrySet());
            // index-level ordering is not critical; keep stable by id
            indexes.sort(Comparator.comparingLong(Map.Entry::getKey));
            for (Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>> entry : indexes) {
                // balance a index
                // Fast path: this index has no tablets in this cluster, skip to avoid useless balanceImpl work.
                if (calculateTotalTablets(bes, entry.getValue()) == 0) {
                    continue;
                }
                balanceImpl(bes, clusterId, entry.getValue(), BalanceType.PARTITION, infos);
            }
        }
    }

    private void balanceInTable(List<Long> bes, String clusterId, List<UpdateCloudReplicaInfo> infos,
                                ActiveSchedulePhase phase) {
        // balance all tables (prefer active tables/dbs, put internal db at tail)
        Iterable<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> tables;
        if (Config.enable_cloud_active_tablet_priority_scheduling) {
            final Comparator<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> cmp = tableEntryComparator();
            List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> nonInternalActive = new ArrayList<>();
            List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> nonInternalInactive = new ArrayList<>();
            List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> internalTables = new ArrayList<>();

            for (Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>> e : futureBeToTabletsInTable.entrySet()) {
                long tableId = e.getKey();
                boolean internal = isInternalDbId(tableIdToDbId.get(tableId));
                long activeCnt = tableIdToActiveCount.getOrDefault(tableId, 0L);
                if (internal) {
                    internalTables.add(e);
                    continue;
                }
                if (activeCnt > 0) {
                    nonInternalActive.add(e);
                } else {
                    nonInternalInactive.add(e);
                }
            }

            nonInternalActive.sort(cmp);
            nonInternalInactive.sort(cmp);
            internalTables.sort(cmp);

            List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> ordered =
                    new ArrayList<>(futureBeToTabletsInTable.size());
            if (phase == ActiveSchedulePhase.ACTIVE_ONLY) {
                ordered.addAll(nonInternalActive);
            } else if (phase == ActiveSchedulePhase.INACTIVE_ONLY) {
                ordered.addAll(nonInternalInactive);
                ordered.addAll(internalTables);
            } else { // ALL
                ordered.addAll(nonInternalActive);
                ordered.addAll(nonInternalInactive);
                ordered.addAll(internalTables);
            }

            tables = ordered;
        } else {
            tables = futureBeToTabletsInTable.entrySet();
        }

        for (Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>> entry : tables) {
            // Fast path: this table has no tablets in this cluster, skip.
            if (calculateTotalTablets(bes, entry.getValue()) == 0) {
                continue;
            }
            balanceImpl(bes, clusterId, entry.getValue(), BalanceType.TABLE, infos);
        }
    }

    // For unit test: override this method to avoid dependency on Env/internal catalog.
    protected boolean isInternalDbId(Long dbId) {
        if (dbId == null || dbId <= 0) {
            return false;
        }
        Boolean cached = dbIdToInternal.get(dbId);
        if (cached != null) {
            return cached;
        }
        // Fallback (should be rare): consult catalog and populate cache.
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        boolean internal = false;
        if (db != null) {
            String name = db.getFullName();
            internal = name != null && INTERNAL_DB_NAMES.contains(name);
        }
        dbIdToInternal.put(dbId, internal);
        return internal;
    }

    private Comparator<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> tableEntryComparator() {
        return (a, b) -> {
            Long tableIdA = a.getKey();
            Long tableIdB = b.getKey();
            boolean internalA = isInternalDbId(tableIdToDbId.get(tableIdA));
            boolean internalB = isInternalDbId(tableIdToDbId.get(tableIdB));
            if (internalA != internalB) {
                return internalA ? 1 : -1; // internal goes last
            }
            long dbActiveA = dbIdToActiveCount.getOrDefault(tableIdToDbId.get(tableIdA), 0L);
            long dbActiveB = dbIdToActiveCount.getOrDefault(tableIdToDbId.get(tableIdB), 0L);
            int cmpDb = Long.compare(dbActiveB, dbActiveA);
            if (cmpDb != 0) {
                return cmpDb;
            }
            long activeA = tableIdToActiveCount.getOrDefault(tableIdA, 0L);
            long activeB = tableIdToActiveCount.getOrDefault(tableIdB, 0L);
            int cmp = Long.compare(activeB, activeA); // more active first
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(tableIdB, tableIdA); // tabletId bigger, newer first
        };
    }

    private Comparator<Map.Entry<Long,
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> partitionEntryComparator() {
        return (a, b) -> {
            Long partIdA = a.getKey();
            Long partIdB = b.getKey();
            boolean internalA = isInternalDbId(partitionIdToDbId.get(partIdA));
            boolean internalB = isInternalDbId(partitionIdToDbId.get(partIdB));
            if (internalA != internalB) {
                return internalA ? 1 : -1; // internal goes last
            }
            long dbActiveA = dbIdToActiveCount.getOrDefault(partitionIdToDbId.get(partIdA), 0L);
            long dbActiveB = dbIdToActiveCount.getOrDefault(partitionIdToDbId.get(partIdB), 0L);
            int cmpDb = Long.compare(dbActiveB, dbActiveA);
            if (cmpDb != 0) {
                return cmpDb;
            }
            long activeA = partitionIdToActiveCount.getOrDefault(partIdA, 0L);
            long activeB = partitionIdToActiveCount.getOrDefault(partIdB, 0L);
            int cmp = Long.compare(activeB, activeA); // more active first
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(partIdB, partIdA); // partId bigger, newer first
        };
    }

    private void sendPreHeatingRpc(Tablet pickedTablet, long srcBe, long destBe) throws Exception {
        sendPreHeatingRpc(Collections.singletonList(pickedTablet.getId()), srcBe, destBe);
    }

    private void sendPreHeatingRpc(List<Long> tabletIds, long srcBe, long destBe) throws Exception {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend srcBackend = cloudSystemInfoService.getBackend(srcBe);
        Backend destBackend = cloudSystemInfoService.getBackend(destBe);
        boolean ok = true;
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TWarmUpCacheAsyncRequest req = new TWarmUpCacheAsyncRequest();
            req.setHost(srcBackend.getHost());
            req.setBrpcPort(srcBackend.getBrpcPort());
            req.setTabletIds(new ArrayList<>(tabletIds));
            TWarmUpCacheAsyncResponse result = client.warmUpCacheAsync(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("pre cache failed status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
        } catch (Exception e) {
            LOG.warn("send pre heating rpc error. backend[{}]", destBackend.getId(), e);
            ok = false;
            throw e;
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    private Map<Long, Boolean> sendCheckWarmUpCacheAsyncRpc(List<Long> tabletIds, long be) {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend destBackend = cloudSystemInfoService.getBackend(be);
        boolean ok = true;
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TCheckWarmUpCacheAsyncRequest req = new TCheckWarmUpCacheAsyncRequest();
            req.setTablets(tabletIds);
            TCheckWarmUpCacheAsyncResponse result = client.checkWarmUpCacheAsync(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("check pre tablets {} cache status {} {}", tabletIds, result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            } else {
                LOG.debug("check pre tablets {} cache succ status {} {}", tabletIds, result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
            return result.getTaskDone();
        } catch (Exception e) {
            LOG.warn("send check pre cache rpc error. tablets{} backend[{}]", tabletIds, destBackend.getId(), e);
            ok = false;
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
        return null;
    }

    private void handleWarmupCompletion(InfightTask task, String clusterId, boolean isDone, long tabletId,
                                           List<UpdateCloudReplicaInfo> infos) {
        if (task == null) {
            LOG.warn("cannot find inflight task for tablet {}-{}", clusterId, tabletId);
            return;
        }
        boolean shouldUpdateMapping = false;
        BalanceTypeEnum currentBalanceType = getCurrentBalanceType(clusterId);
        LOG.debug("cluster id {}, balance type {}, tabletId {}, ", clusterId, currentBalanceType, tabletId);

        switch (currentBalanceType) {
            case ASYNC_WARMUP: {
                int currentTaskTimeout = getCurrentTaskTimeout(clusterId);
                boolean timeExceeded = System.currentTimeMillis() / 1000 - task.startTimestamp > currentTaskTimeout;
                LOG.debug("tablet {}-{} warmup cache isDone {} timeExceeded {}",
                        clusterId, tabletId, isDone, timeExceeded);
                if (isDone || timeExceeded) {
                    if (!isDone) {
                        // timeout but not done, not normal, info log
                        LOG.info("{}-{} warmup cache timeout {}, forced to change the mapping",
                                clusterId, tabletId, currentTaskTimeout);
                    } else {
                        // done, normal
                        LOG.debug("{}-{} warmup cache done, change the mapping", clusterId, tabletId);
                    }
                    shouldUpdateMapping = true;
                }
                break;
            }
            case SYNC_WARMUP: {
                if (isDone) {
                    // done, normal
                    LOG.debug("{} sync cache done, change the mapping", tabletId);
                    shouldUpdateMapping = true;
                }
                break;
            }
            default:
                break;
        }

        if (!shouldUpdateMapping) {
            return;
        }

        updateClusterToBeMap(task.pickedTablet, task.destBe, clusterId, infos);

        if (LOG.isDebugEnabled()) {
            LOG.debug("remove tablet {}-{}", clusterId, task.pickedTablet.getId());
        }
        tabletToInfightTask.remove(new InfightTablet(task.pickedTablet.getId(), clusterId));

        if (BalanceTypeEnum.SYNC_WARMUP.equals(currentBalanceType)) {
            try {
                // send sync cache rpc again, ignore the result, the best effort to sync some new data
                sendPreHeatingRpc(task.pickedTablet, task.srcBe, task.destBe);
            } catch (Exception e) {
                LOG.warn("Failed to preheat tablet {} from {} to {}, "
                                + "help msg change fe config cloud_warm_up_for_rebalance_type to without_warmup, ",
                        task.pickedTablet.getId(), task.srcBe, task.destBe, e);
            }
        }
    }

    private void updateBeToTablets(Tablet pickedTablet, long srcBe, long destBe,
                                   ConcurrentHashMap<Long, Set<Tablet>> globalBeToTablets,
                                   ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> beToTabletsInTable,
                                   ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long,
                                       Set<Tablet>>>> partToTablets) {
        CloudReplica replica = ((CloudTablet) pickedTablet).getCloudReplica();
        long tableId = replica.getTableId();
        long partId = replica.getPartitionId();
        long indexId = replica.getIndexId();

        globalBeToTablets.get(srcBe).remove(pickedTablet);
        beToTabletsInTable.get(tableId).get(srcBe).remove(pickedTablet);
        partToTablets.get(partId).get(indexId).get(srcBe).remove(pickedTablet);

        fillBeToTablets(destBe, tableId, partId, indexId, pickedTablet, globalBeToTablets, beToTabletsInTable,
                        partToTablets);
    }

    private void updateClusterToBeMap(Tablet pickedTablet, long destBe, String clusterId,
                                      List<UpdateCloudReplicaInfo> infos) {
        CloudReplica cloudReplica = ((CloudTablet) pickedTablet).getCloudReplica();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
        if (db == null) {
            return;
        }
        OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
        if (table == null) {
            return;
        }

        table.readLock();

        try {
            if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                return;
            }

            cloudReplica.updateClusterToPrimaryBe(clusterId, destBe);
            UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                    cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                    pickedTablet.getId(), cloudReplica.getId(), clusterId, destBe);
            infos.add(info);
        } finally {
            table.readUnlock();
        }
    }

    private boolean getTransferPair(List<Long> bes, Map<Long, Set<Tablet>> beToTablets, long avgNum,
                                    TransferPairInfo pairInfo) {
        long srcBe = findSourceBackend(bes, beToTablets);
        long destBe = findDestinationBackend(bes, beToTablets, srcBe);

        if (srcBe == -1 || destBe == -1) {
            return false; // No valid backend found
        }

        long minTabletsNum = beToTablets.get(destBe) == null ? 0 : beToTablets.get(destBe).size();
        long maxTabletsNum = beToTablets.get(srcBe) == null ? 0 : beToTablets.get(srcBe).size();

        if (!isTransferValid(srcBe, minTabletsNum, maxTabletsNum, avgNum)) {
            return false; // Transfer conditions not met
        }

        pairInfo.srcBe = srcBe;
        pairInfo.destBe = destBe;
        pairInfo.minTabletsNum = minTabletsNum;
        pairInfo.maxTabletsNum = maxTabletsNum;
        return true;
    }

    private long findSourceBackend(List<Long> bes, Map<Long, Set<Tablet>> beToTablets) {
        long srcBe = -1;
        long maxTabletsNum = 0;

        for (Long be : bes) {
            long tabletNum = beToTablets.getOrDefault(be, Collections.emptySet()).size();
            Backend backend = cloudSystemInfoService.getBackend(be);

            // Check if the backend is decommissioned
            if (backend != null) {
                if ((backend.isDecommissioning() || backend.isDecommissioned()) && tabletNum > 0) {
                    srcBe = be; // Mark as source if decommissioned and has tablets
                    break; // Exit early if we found a decommissioned backend
                }
                if (!backend.isDecommissioning() && !backend.isDecommissioned() && tabletNum > maxTabletsNum) {
                    srcBe = be;
                    maxTabletsNum = tabletNum;
                }
            } else {
                LOG.info("backend {} not found", be);
            }
        }
        return srcBe;
    }

    private long findDestinationBackend(List<Long> bes, Map<Long, Set<Tablet>> beToTablets, long srcBe) {
        long minTabletsNum = Long.MAX_VALUE;
        List<Long> candidateBes = new ArrayList<>();

        for (Long be : bes) {
            long tabletNum = beToTablets.getOrDefault(be, Collections.emptySet()).size();
            Backend backend = cloudSystemInfoService.getBackend(be);
            if (backend != null && backend.isAlive() && !backend.isDecommissioning()
                    && !backend.isDecommissioned() && !backend.isSmoothUpgradeSrc()) {
                if (tabletNum < minTabletsNum) {
                    // Found a BE with fewer tablets, reset candidates
                    minTabletsNum = tabletNum;
                    candidateBes.clear();
                    candidateBes.add(be);
                } else if (tabletNum == minTabletsNum) {
                    // Found a BE with the same minimum tablet count, add to candidates
                    candidateBes.add(be);
                }
            }
        }

        if (candidateBes.isEmpty()) {
            return -1;
        }

        // Shuffle candidates with the same tablet count for better load balancing
        Collections.shuffle(candidateBes, rand);
        return candidateBes.get(0);
    }

    private boolean isTransferValid(long srcBe, long minTabletsNum, long maxTabletsNum, long avgNum) {
        boolean srcDecommissioned = cloudSystemInfoService.getBackend(srcBe).isDecommissioning()
                || cloudSystemInfoService.getBackend(srcBe).isDecommissioned();

        if (!srcDecommissioned) {
            if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                    && minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold))
                    || minTabletsNum > maxTabletsNum - Config.cloud_rebalance_number_threshold) {
                return false;
            }
        }
        return true;
    }

    private boolean isConflict(long srcBe, long destBe, CloudReplica cloudReplica, BalanceType balanceType,
                               ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>
                                   beToTabletsInParts,
                               ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> beToTabletsInTables) {
        if (cloudSystemInfoService.getBackend(srcBe).isDecommissioning()
                || cloudSystemInfoService.getBackend(srcBe).isDecommissioned()) {
            return false; // If source BE is decommissioned, no conflict
        }

        if (balanceType == BalanceType.GLOBAL) {
            return checkGlobalBalanceConflict(srcBe, destBe, cloudReplica, beToTabletsInParts, beToTabletsInTables);
        } else if (balanceType == BalanceType.TABLE) {
            return checkTableBalanceConflict(srcBe, destBe, cloudReplica, beToTabletsInParts);
        }

        return false;
    }

    private boolean checkGlobalBalanceConflict(long srcBe, long destBe, CloudReplica cloudReplica,
                                               ConcurrentHashMap<Long,
                                                   ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>
                                                   beToTabletsInParts,
                                               ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>
                                                   beToTabletsInTables) {
        long maxBeSize = getTabletSizeInParts(srcBe, cloudReplica, beToTabletsInParts);
        long minBeSize = getTabletSizeInParts(destBe, cloudReplica, beToTabletsInParts);

        if (minBeSize >= maxBeSize) {
            return true; // Conflict detected
        }

        maxBeSize = getTabletSizeInBes(srcBe, cloudReplica, beToTabletsInTables);
        minBeSize = getTabletSizeInBes(destBe, cloudReplica, beToTabletsInTables);

        return minBeSize >= maxBeSize; // Conflict detected
    }

    private boolean checkTableBalanceConflict(long srcBe, long destBe, CloudReplica cloudReplica,
                                              ConcurrentHashMap<Long,
                                                  ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>
                                                  beToTabletsInParts) {
        long maxBeSize = getTabletSizeInParts(srcBe, cloudReplica, beToTabletsInParts);
        long minBeSize = getTabletSizeInParts(destBe, cloudReplica, beToTabletsInParts);

        return minBeSize >= maxBeSize; // Conflict detected
    }

    private long getTabletSizeInParts(long beId, CloudReplica cloudReplica,
                                      ConcurrentHashMap<Long,
                                          ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>
                                          beToTabletsInParts) {
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> indexToTablets
                = beToTabletsInParts.get(cloudReplica.getPartitionId());
        if (indexToTablets == null) {
            return 0;
        }
        ConcurrentHashMap<Long, Set<Tablet>> beToTablets = indexToTablets.get(cloudReplica.getIndexId());
        if (beToTablets == null) {
            return 0;
        }
        Set<Tablet> tablets = beToTablets.get(beId);
        return tablets == null ? 0 : tablets.size();
    }

    private long getTabletSizeInBes(long beId, CloudReplica cloudReplica,
                                    ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>> beToTabletsInTables) {
        ConcurrentHashMap<Long, Set<Tablet>> beToTablets = beToTabletsInTables.get(cloudReplica.getTableId());
        if (beToTablets == null) {
            return 0;
        }
        Set<Tablet> tablets = beToTablets.get(beId);
        return tablets == null ? 0 : tablets.size();
    }


    private void balanceImpl(List<Long> bes, String clusterId, Map<Long, Set<Tablet>> beToTablets,
            BalanceType balanceType, List<UpdateCloudReplicaInfo> infos) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        processFailedWarmupTasks();

        long totalTabletsNum = calculateTotalTablets(bes, beToTablets);
        long beNum = countActiveBackends(bes);

        if (beNum == 0) {
            LOG.warn("zero be, but want balance, skip");
            return;
        }

        long avgNum = totalTabletsNum / beNum;
        long transferNum = calculateTransferNum(avgNum, beNum);

        BalanceTypeEnum currentBalanceType = getCurrentBalanceType(clusterId);
        LOG.debug("balance type {}, be num {}, total tablets num {}, avg num {}, transfer num {}",
                currentBalanceType, beNum, totalTabletsNum, avgNum, transferNum);

        final Set<Long> pickedTabletIds = new HashSet<>();

        for (int i = 0; i < transferNum; i++) {
            TransferPairInfo pairInfo = new TransferPairInfo();
            if (!getTransferPair(bes, beToTablets, avgNum, pairInfo)) {
                break; // no need balance
            }

            long srcBe = pairInfo.srcBe;
            long destBe = pairInfo.destBe;

            Tablet pickedTablet = pickTabletPreferCold(srcBe, beToTablets.get(srcBe),
                    this.activeTabletIds, pickedTabletIds);
            if (pickedTablet == null) {
                continue; // No tablet to pick
            }

            pickedTabletIds.add(pickedTablet.getId());
            CloudReplica cloudReplica = ((CloudTablet) pickedTablet).getCloudReplica();
            Backend srcBackend = Env.getCurrentSystemInfo().getBackend(srcBe);

            if ((BalanceTypeEnum.WITHOUT_WARMUP.equals(currentBalanceType)
                    || BalanceTypeEnum.PEER_READ_ASYNC_WARMUP.equals(currentBalanceType))
                    && srcBackend != null && srcBackend.isAlive()) {
                // direct switch, update fe meta directly, not send preheating task
                if (isConflict(srcBe, destBe, cloudReplica, balanceType, partitionToTablets, beToTabletsInTable)) {
                    continue;
                }
                boolean moved = transferTablet(pickedTablet, srcBe, destBe, clusterId, balanceType, infos);
                if (moved) {
                    updateBalanceStatus(balanceType);
                }
                if (BalanceTypeEnum.PEER_READ_ASYNC_WARMUP.equals(currentBalanceType)) {
                    LOG.debug("directly switch {} from {} to {}, cluster {}", pickedTablet.getId(), srcBe, destBe,
                            clusterId);
                    // send sync cache rpc, best effort
                    try {
                        sendPreHeatingRpc(pickedTablet, srcBe, destBe);
                    } catch (Exception e) {
                        LOG.debug("Failed to preheat tablet {} from {} to {}, "
                                + "directly policy, just ignore the error",
                                pickedTablet.getId(), srcBe, destBe, e);
                        return;
                    }
                }
            } else {
                // cache warm up
                if (isConflict(srcBe, destBe, cloudReplica, balanceType,
                        futurePartitionToTablets, futureBeToTabletsInTable)) {
                    continue;
                }
                boolean moved = preheatAndUpdateTablet(pickedTablet, srcBe, destBe,
                        clusterId, balanceType, beToTablets);
                if (moved) {
                    updateBalanceStatus(balanceType);
                }
            }
        }
    }

    private long calculateTotalTablets(List<Long> bes, Map<Long, Set<Tablet>> beToTablets) {
        return bes.stream()
                .mapToLong(be -> beToTablets.getOrDefault(be, Collections.emptySet()).size())
                .sum();
    }

    private long countActiveBackends(List<Long> bes) {
        return bes.stream()
                .filter(be -> {
                    Backend backend = cloudSystemInfoService.getBackend(be);
                    return backend != null && !backend.isDecommissioning() && !backend.isDecommissioned();
                })
                .count();
    }

    private long calculateTransferNum(long avgNum, long beNum) {
        return Math.max(Math.round(avgNum * Config.cloud_balance_tablet_percent_per_run), beNum);
    }

    private void updateBalanceStatus(BalanceType balanceType) {
        if (balanceType == BalanceType.PARTITION) {
            indexBalanced = false;
        } else if (balanceType == BalanceType.TABLE) {
            tableBalanced = false;
        }
    }

    private Set<Long> getActiveTabletIds() {
        try {
            // get topN active tablets
            List<TabletSlidingWindowAccessStats.AccessStatsResult> active =
                    TabletSlidingWindowAccessStats.getInstance()
                        .getTopNActive(Config.cloud_active_partition_scheduling_topn);
            if (active == null || active.isEmpty()) {
                return Collections.emptySet();
            }
            Set<Long> ids = new HashSet<>(active.size() * 2);
            for (TabletSlidingWindowAccessStats.AccessStatsResult r : active) {
                ids.add(r.id);
            }
            return ids;
        } catch (Throwable t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to get active tablets from CloudTabletAccessStats, fallback to random pick", t);
            }
            return Collections.emptySet();
        }
    }

    // Choose non-active (cold) tablet first to re-balance, to reduce impact on hot tablets.
    // Fallback to active/random if no cold tablet is available.
    private Tablet pickTabletPreferCold(long srcBe, Set<Tablet> tablets, Set<Long> activeTabletIds,
                                        Set<Long> pickedTabletIds) {
        if (tablets == null || tablets.isEmpty()) {
            return null;
        }
        // Prefer cold tablets first (when active stats is available)
        boolean hasActiveStats = activeTabletIds != null && !activeTabletIds.isEmpty();
        boolean preferCold = Config.enable_cloud_active_tablet_priority_scheduling && hasActiveStats;

        if (preferCold) {
            Tablet cold = reservoirPick(tablets, pickedTabletIds, activeTabletIds, true);
            if (cold != null) {
                return cold;
            }
        }
        return reservoirPick(tablets, pickedTabletIds, activeTabletIds, false);
    }

    // Reservoir sampling to pick one element uniformly at random from candidates,
    // without allocating intermediate collections.
    private Tablet reservoirPick(Set<Tablet> tablets, Set<Long> pickedTabletIds,
                                 Set<Long> activeTabletIds, boolean requireCold) {
        Tablet chosen = null;
        int seen = 0;
        for (Tablet t : tablets) {
            if (pickedTabletIds.contains(t.getId())) {
                continue;
            }
            if (requireCold && activeTabletIds != null && activeTabletIds.contains(t.getId())) {
                continue;
            }
            seen++;
            if (rand.nextInt(seen) == 0) {
                chosen = t;
            }
        }
        return chosen;
    }

    private boolean preheatAndUpdateTablet(Tablet pickedTablet, long srcBe, long destBe, String clusterId,
                                     BalanceType balanceType, Map<Long, Set<Tablet>> beToTablets) {
        Backend srcBackend = cloudSystemInfoService.getBackend(srcBe);
        Backend destBackend = cloudSystemInfoService.getBackend(destBe);
        if (srcBackend == null || destBackend == null) {
            LOG.warn("backend missing when preheating tablet {} from {} to {}, cluster {}",
                    pickedTablet.getId(), srcBe, destBe, clusterId);
            return false;
        }

        InfightTask task = new InfightTask();
        task.pickedTablet = pickedTablet;
        task.srcBe = srcBe;
        task.destBe = destBe;
        task.balanceType = balanceType;
        task.beToTablets = beToTablets;
        task.startTimestamp = System.currentTimeMillis() / 1000;
        InfightTablet key = new InfightTablet(pickedTablet.getId(), clusterId);

        tabletToInfightTask.put(key, task);
        updateBeToTablets(pickedTablet, srcBe, destBe,
                futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
        LOG.debug("pre cache {} from {} to {}, cluster {}", pickedTablet.getId(), srcBe, destBe, clusterId);
        enqueueWarmupTask(new WarmupTabletTask(pickedTablet, srcBe, destBe, clusterId));
        return true;
    }

    private boolean transferTablet(Tablet pickedTablet, long srcBe, long destBe, String clusterId,
                            BalanceType balanceType, List<UpdateCloudReplicaInfo> infos) {
        LOG.debug("transfer {} from {} to {}, cluster {}, type {}",
                pickedTablet.getId(), srcBe, destBe, clusterId, balanceType);
        updateBeToTablets(pickedTablet, srcBe, destBe,
                beToTabletsGlobal, beToTabletsInTable, partitionToTablets);
        updateBeToTablets(pickedTablet, srcBe, destBe,
                futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
        updateClusterToBeMap(pickedTablet, destBe, clusterId, infos);
        return true;
    }

    public void addTabletMigrationTask(Long srcBe, Long dstBe) {
        tabletsMigrateTasks.offer(Pair.of(srcBe, dstBe));
    }

    /* Migrate tablet replicas from srcBe to dstBe
     * replica location info will be updated in both master and follower FEs.
     */
    private void migrateTablets(Long srcBe, Long dstBe) {
        // get tablets
        Set<Tablet> tablets = beToTabletsGlobal.get(srcBe);
        if (tablets == null || tablets.isEmpty()) {
            LOG.info("smooth upgrade srcBe={} does not have any tablets, set inactive", srcBe);
            ((CloudEnv) Env.getCurrentEnv()).getCloudUpgradeMgr().setBeStateInactive(srcBe);
            return;
        }
        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Tablet tablet : tablets) {
            // get replica
            CloudReplica cloudReplica = ((CloudTablet) tablet).getCloudReplica();
            Backend be = cloudSystemInfoService.getBackend(srcBe);
            if (be == null) {
                LOG.info("src backend {} not found", srcBe);
                continue;
            }
            // populate to followers
            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                long beId;
                try {
                    beId = cloudReplica.getBackendId();
                } catch (ComputeGroupException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get backend failed cloudReplica {}", cloudReplica, e);
                    }
                    beId = -1;
                }
                LOG.error("get null db from replica, tabletId={}, partitionId={}, beId={}",
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), beId);
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
            if (table == null) {
                continue;
            }

            String clusterId = be.getCloudClusterId();
            String clusterName = be.getCloudClusterName();

            table.readLock();
            try {
                if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                    continue;
                }
                // update replica location info: primary -> new BE (dstBe)
                cloudReplica.updateClusterToPrimaryBe(clusterId, dstBe);
                // Set old BE (srcBe) as secondary so queries can fall back to it when new BE
                // is not alive yet (e.g. new BE heartbeat not registered). Otherwise
                // hashReplicaToBe would exclude both: old BE (isSmoothUpgradeSrc) and new BE
                // (not alive), causing COMPUTE_GROUPS_NO_ALIVE_BE.
                cloudReplica.updateClusterToSecondaryBe(clusterId, srcBe);
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                        tablet.getId(), cloudReplica.getId(), clusterId, dstBe);
                infos.add(info);
            } finally {
                table.readUnlock();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("cloud be migrate tablet {} from srcBe={} to dstBe={}, clusterId={}, clusterName={}",
                        tablet.getId(), srcBe, dstBe, clusterId, clusterName);
            }
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos, StatType.SMOOTH_UPGRADE);
        LOG.info("collect to editlog migrate before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("update cloud replicas failed", e);
            // edit log failed, try next time
            throw new RuntimeException(e);
        }

        try {
            ((CloudEnv) Env.getCurrentEnv()).getCloudUpgradeMgr().registerWaterShedTxnId(srcBe);
        } catch (UserException e) {
            LOG.warn("registerWaterShedTxnId get exception", e);
            throw new RuntimeException(e);
        }
    }

    private List<UpdateCloudReplicaInfo> batchUpdateCloudReplicaInfoEditlogs(List<UpdateCloudReplicaInfo> infos,
                                                                             StatType type) {
        long start = System.currentTimeMillis();
        List<UpdateCloudReplicaInfo> rets = new ArrayList<>();
        // clusterId, infos
        Map<String, List<UpdateCloudReplicaInfo>> clusterIdToInfos = infos.stream()
                .collect(Collectors.groupingBy(UpdateCloudReplicaInfo::getClusterId));
        Set<String> notBalancedClusterIds = new HashSet<>(this.clusterToBes.keySet());
        for (Map.Entry<String, List<UpdateCloudReplicaInfo>> entry : clusterIdToInfos.entrySet()) {
            // same cluster
            String clusterId = entry.getKey();
            notBalancedClusterIds.remove(clusterId);
            List<UpdateCloudReplicaInfo> infoList = entry.getValue();
            String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                    .getClusterNameByClusterId(clusterId);
            if (!Strings.isNullOrEmpty(clusterName)) {
                MetricRepo.updateClusterCloudBalanceNum(clusterName, clusterId, type, infoList.size());
            }
            Map<LocationKey, List<UpdateCloudReplicaInfo>> sameLocationInfos = infoList.stream()
                    .collect(Collectors.groupingBy(
                            info -> new LocationKey(info.getDbId(), info.getTableId(),
                                info.getPartitionId(), info.getIndexId())));
            sameLocationInfos.forEach((locationKey, locationInfos) -> {
                UpdateCloudReplicaInfo newInfo = new UpdateCloudReplicaInfo();
                long dbId = -1;
                long tableId = -1;
                long partitionId = -1;
                long indexId = -1;
                for (UpdateCloudReplicaInfo info : locationInfos) {
                    Preconditions.checkState(clusterId.equals(info.getClusterId()),
                            "impossible, cluster id not eq outer=" + clusterId + ", inner=" + info.getClusterId());

                    dbId = info.getDbId();
                    tableId = info.getTableId();
                    partitionId = info.getPartitionId();
                    indexId = info.getIndexId();

                    long tabletId = info.getTabletId();
                    long replicaId = info.getReplicaId();
                    long beId = info.getBeId();
                    newInfo.getTabletIds().add(tabletId);
                    newInfo.getReplicaIds().add(replicaId);
                    newInfo.getBeIds().add(beId);
                }
                newInfo.setDbId(dbId);
                newInfo.setTableId(tableId);
                newInfo.setPartitionId(partitionId);
                newInfo.setIndexId(indexId);
                newInfo.setClusterId(clusterId);
                // ATTN: in unprotectUpdateCloudReplica, use batch must set tabletId = -1
                newInfo.setTabletId(-1);
                rets.add(newInfo);
            });
        }

        for (String clusterId : notBalancedClusterIds) {
            String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                    .getClusterNameByClusterId(clusterId);
            if (!Strings.isNullOrEmpty(clusterName)) {
                MetricRepo.updateClusterCloudBalanceNum(clusterName, clusterId, type, 0);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("batchUpdateCloudReplicaInfoEditlogs old size {}, cur size {} cost {} ms",
                    infos.size(), rets.size(), System.currentTimeMillis() - start);
        }
        return rets;
    }

    public boolean isInited() {
        return inited;
    }
}
