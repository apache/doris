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

import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletChecker.CheckerCounter;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.AddResult;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ColocateTableBalancer is responsible for tablets' repair and balance of colocated tables.
 */
public class ColocateTableCheckerAndBalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ColocateTableCheckerAndBalancer.class);

    private ColocateTableCheckerAndBalancer(long intervalMs) {
        super("colocate group clone checker", intervalMs);
    }

    private static volatile ColocateTableCheckerAndBalancer INSTANCE = null;

    public static ColocateTableCheckerAndBalancer getInstance() {
        if (INSTANCE == null) {
            synchronized (ColocateTableCheckerAndBalancer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ColocateTableCheckerAndBalancer(FeConstants.tablet_checker_interval_ms);
                }
            }
        }
        return INSTANCE;
    }

    public static class BucketStatistic {
        public int tabletOrderIdx;
        public int totalReplicaNum;
        public long totalReplicaDataSize;

        public BucketStatistic(int tabletOrderIdx, int totalReplicaNum, long totalReplicaDataSize) {
            this.tabletOrderIdx = tabletOrderIdx;
            this.totalReplicaNum = totalReplicaNum;
            this.totalReplicaDataSize = totalReplicaDataSize;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BucketStatistic)) {
                return false;
            }

            BucketStatistic other = (BucketStatistic) obj;
            return tabletOrderIdx == other.tabletOrderIdx && totalReplicaNum == other.totalReplicaNum
                    && totalReplicaDataSize == other.totalReplicaDataSize;
        }

        @Override
        public String toString() {
            return "{ orderIdx: " + tabletOrderIdx + ", total replica num: " + totalReplicaNum
                    + ", total data size: " + totalReplicaDataSize + " }";
        }
    }

    public static class BackendBuckets {
        private long beId;
        private Map<GroupId, List<Integer>>  groupTabletOrderIndices = Maps.newHashMap();

        public BackendBuckets(long beId) {
            this.beId = beId;
        }

        // for test
        public Map<GroupId, List<Integer>> getGroupTabletOrderIndices() {
            return groupTabletOrderIndices;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BackendBuckets)) {
                return false;
            }

            BackendBuckets other = (BackendBuckets) obj;
            return beId == other.beId && groupTabletOrderIndices.equals(other.groupTabletOrderIndices);
        }

        @Override
        public String toString() {
            return "{ backendId: " + beId + ", group order index: " + groupTabletOrderIndices + " }";
        }

        public void addGroupTablet(GroupId groupId, int tabletOrderIdx) {
            List<Integer> indices = groupTabletOrderIndices.get(groupId);
            if (indices == null) {
                indices = Lists.newArrayList();
                groupTabletOrderIndices.put(groupId, indices);
            }
            indices.add(tabletOrderIdx);
        }

        public void removeGroupTablet(GroupId groupId, int tabletOrderIdx) {
            List<Integer> indices = groupTabletOrderIndices.get(groupId);
            if (indices == null) {
                return;
            }

            indices.remove(Integer.valueOf(tabletOrderIdx));
            if (indices.isEmpty()) {
                groupTabletOrderIndices.remove(groupId);
            }
        }

        public boolean containsGroupTablet(GroupId groupId, int tabletOrderIdx) {
            List<Integer> indices = groupTabletOrderIndices.get(groupId);
            if (indices == null) {
                return false;
            }

            return indices.indexOf(Integer.valueOf(tabletOrderIdx)) >= 0;
        }

        public int getTotalReplicaNum(Map<GroupId, List<BucketStatistic>> allGroupBucketsMap) {
            int totalReplicaNum = 0;
            for (Map.Entry<GroupId, List<Integer>> entry : groupTabletOrderIndices.entrySet()) {
                List<BucketStatistic> bucketStatistics = allGroupBucketsMap.get(entry.getKey());
                if (bucketStatistics != null) {
                    for (int tabletOrderIdx : entry.getValue()) {
                        if (tabletOrderIdx < bucketStatistics.size()) {
                            totalReplicaNum += bucketStatistics.get(tabletOrderIdx).totalReplicaNum;
                        }
                    }
                }
            }

            return totalReplicaNum;
        }

        public long getTotalReplicaDataSize(Map<GroupId, List<BucketStatistic>> allGroupBucketsMap) {
            long totalReplicaDataSize = 0;
            for (Map.Entry<GroupId, List<Integer>> entry : groupTabletOrderIndices.entrySet()) {
                List<BucketStatistic> bucketStatistics = allGroupBucketsMap.get(entry.getKey());
                if (bucketStatistics != null) {
                    for (int tabletOrderIdx : entry.getValue()) {
                        if (tabletOrderIdx < bucketStatistics.size()) {
                            totalReplicaDataSize += bucketStatistics.get(tabletOrderIdx).totalReplicaDataSize;
                        }
                    }
                }
            }

            return totalReplicaDataSize;
        }

        public int getTotalBucketsNum() {
            return groupTabletOrderIndices.values().stream().mapToInt(indices -> indices.size()).sum();
        }

        public int getGroupBucketsNum(GroupId groupId) {
            List<Integer> indices = groupTabletOrderIndices.get(groupId);
            if (indices == null) {
                return 0;
            } else {
                return indices.size();
            }
        }
    }

    public static class GlobalColocateStatistic {
        private Map<Long, BackendBuckets> backendBucketsMap = Maps.newHashMap();
        private Map<GroupId, List<BucketStatistic>> allGroupBucketsMap = Maps.newHashMap();
        private Map<Tag, Integer> allTagBucketNum = Maps.newHashMap();
        private static final BackendBuckets DUMMY_BE = new BackendBuckets(0);

        public GlobalColocateStatistic() {
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof GlobalColocateStatistic)) {
                return false;
            }

            GlobalColocateStatistic other = (GlobalColocateStatistic) obj;
            return backendBucketsMap.equals(other.backendBucketsMap)
                    && allGroupBucketsMap.equals(other.allGroupBucketsMap)
                    && allTagBucketNum.equals(other.allTagBucketNum);
        }

        @Override
        public String toString() {
            return "{ backends: " + backendBucketsMap + ", groups: " + allGroupBucketsMap
                    + ", tag bucket num: " + allTagBucketNum + " }";
        }

        Map<Long, BackendBuckets> getBackendBucketsMap() {
            return backendBucketsMap;
        }

        Map<GroupId, List<BucketStatistic>> getAllGroupBucketsMap() {
            return allGroupBucketsMap;
        }

        Map<Tag, Integer> getAllTagBucketNum() {
            return allTagBucketNum;
        }

        public boolean moveTablet(GroupId groupId, int tabletOrderIdx,
                long srcBeId, long destBeId) {
            BackendBuckets srcBackendBuckets = backendBucketsMap.get(srcBeId);
            if (srcBackendBuckets == null || !srcBackendBuckets.containsGroupTablet(groupId, tabletOrderIdx)) {
                return false;
            }

            BackendBuckets destBackendBuckets = backendBucketsMap.get(destBeId);
            if (destBackendBuckets == null) {
                destBackendBuckets = new BackendBuckets(destBeId);
                backendBucketsMap.put(destBeId, destBackendBuckets);
            }
            if (destBackendBuckets.containsGroupTablet(groupId, tabletOrderIdx)) {
                return false;
            }

            srcBackendBuckets.removeGroupTablet(groupId, tabletOrderIdx);
            destBackendBuckets.addGroupTablet(groupId, tabletOrderIdx);
            if (srcBackendBuckets.getTotalBucketsNum() == 0) {
                backendBucketsMap.remove(srcBeId);
            }

            return true;
        }

        public int getBackendTotalBucketNum(long backendId) {
            return backendBucketsMap.getOrDefault(backendId, DUMMY_BE).getTotalBucketsNum();
        }

        public long getBackendTotalReplicaDataSize(long backendId) {
            return backendBucketsMap.getOrDefault(backendId, DUMMY_BE)
                    .getTotalReplicaDataSize(allGroupBucketsMap);
        }

        public long getBucketTotalReplicaDataSize(GroupId groupId, int tabletOrderIdx) {
            List<BucketStatistic> bucketStatistics = allGroupBucketsMap.get(groupId);
            if (bucketStatistics != null && tabletOrderIdx < bucketStatistics.size()) {
                return bucketStatistics.get(tabletOrderIdx).totalReplicaDataSize;
            } else {
                return 0L;
            }
        }

        public void addGroup(GroupId groupId, ReplicaAllocation replicaAlloc, List<Set<Long>> backendBucketsSeq,
                List<Long> totalReplicaDataSizes, int totalReplicaNumPerBucket) {
            Preconditions.checkState(backendBucketsSeq.size() == totalReplicaDataSizes.size(),
                    backendBucketsSeq.size() + " vs. " + totalReplicaDataSizes.size());
            List<BucketStatistic> bucketStatistics = Lists.newArrayList();
            for (int tabletOrderIdx = 0; tabletOrderIdx < backendBucketsSeq.size(); tabletOrderIdx++) {
                BucketStatistic bucket = new BucketStatistic(tabletOrderIdx, totalReplicaNumPerBucket,
                        totalReplicaDataSizes.get(tabletOrderIdx));
                bucketStatistics.add(bucket);
                for (long backendId : backendBucketsSeq.get(tabletOrderIdx)) {
                    BackendBuckets backendBuckets = backendBucketsMap.get(backendId);
                    if (backendBuckets == null) {
                        backendBuckets = new BackendBuckets(backendId);
                        backendBucketsMap.put(backendId, backendBuckets);
                    }
                    backendBuckets.addGroupTablet(groupId, tabletOrderIdx);
                }
            }
            int bucketNum = backendBucketsSeq.size();
            replicaAlloc.getAllocMap().forEach((tag, count) -> {
                allTagBucketNum.put(tag, allTagBucketNum.getOrDefault(tag, 0) + bucketNum * count);
            });
            allGroupBucketsMap.put(groupId, bucketStatistics);
        }

    }

    @Override
    /*
     * Each round, we do 2 steps:
     * 1. Relocate and balance group:
     *      Backend is not available, find a new backend to replace it.
     *      and after all unavailable has been replaced, balance the group
     *
     * 2. Match group:
     *      If replica mismatch backends in a group, that group will be marked as unstable, and pass that
     *      tablet to TabletScheduler.
     *      Otherwise, mark the group as stable
     */
    protected void runAfterCatalogReady() {
        relocateAndBalanceGroups();
        matchGroups();
    }

    /*
     * relocate and balance group
     *  here we just let replicas in colocate table evenly distributed in cluster, not consider the
     *  cluster load statistic.
     *  for example:
     *  currently there are 4 backends A B C D with following load:
     *
     *                +-+
     *                | |
     * +-+  +-+  +-+  | |
     * | |  | |  | |  | |
     * +-+  +-+  +-+  +-+
     *  A    B    C    D
     *
     *  And colocate group balancer will still evenly distribute the replicas to all 4 backends, not
     *  just 3 low load backends.
     *
     *                 X
     *                 X
     *  X    X    X   +-+
     *  X    X    X   | |
     * +-+  +-+  +-+  | |
     * | |  | |  | |  | |
     * +-+  +-+  +-+  +-+
     * A    B    C    D
     *
     *  So After colocate balance, the cluster may still 'unbalanced' from a global perspective.
     *  And the LoadBalancer will balance the non-colocate table's replicas to make the
     *  cluster balance, eventually.
     *
     *  X    X    X    X
     *  X    X    X    X
     * +-+  +-+  +-+  +-+
     * | |  | |  | |  | |
     * | |  | |  | |  | |
     * +-+  +-+  +-+  +-+
     *  A    B    C    D
     */
    private void relocateAndBalanceGroups() {
        Set<GroupId> groupIds = Sets.newHashSet(Env.getCurrentEnv().getColocateTableIndex().getAllGroupIds());

        // balance only inside each group, excluded balance between all groups
        Set<GroupId> changeGroups = relocateAndBalanceGroup(groupIds, false);

        if (!Config.disable_colocate_balance_between_groups
                && !changeGroups.isEmpty()) {
            // balance both inside each group and between all groups
            relocateAndBalanceGroup(changeGroups, true);
        }
    }

    private Set<GroupId> relocateAndBalanceGroup(Set<GroupId> groupIds, boolean balanceBetweenGroups) {
        Set<GroupId> changeGroups = Sets.newHashSet();
        if (Config.disable_colocate_balance) {
            return changeGroups;
        }

        Env env = Env.getCurrentEnv();
        ColocateTableIndex colocateIndex = env.getColocateTableIndex();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        GlobalColocateStatistic globalColocateStatistic = buildGlobalColocateStatistic();

        // get all groups
        for (GroupId groupId : groupIds) {
            Map<Tag, LoadStatisticForTag> statisticMap = env.getTabletScheduler().getStatisticMap();
            if (statisticMap == null) {
                continue;
            }

            ColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
            ReplicaAllocation replicaAlloc = groupSchema.getReplicaAlloc();
            try {
                Env.getCurrentSystemInfo().checkReplicaAllocation(replicaAlloc);
            } catch (DdlException e) {
                colocateIndex.setErrMsgForGroup(groupId, e.getMessage());
                continue;
            }
            Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();

            for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
                Tag tag = entry.getKey();
                LoadStatisticForTag statistic = statisticMap.get(tag);
                if (statistic == null) {
                    continue;
                }
                List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeqByTag(groupId, tag);
                if (backendsPerBucketSeq.isEmpty()) {
                    continue;
                }

                // get all unavailable backends in the backend bucket sequence of this group
                Set<Long> unavailableBeIdsInGroup = getUnavailableBeIdsInGroup(
                        infoService, colocateIndex, groupId, tag);
                // get all available backends for this group
                Set<Long> beIdsInOtherTag = colocateIndex.getBackendIdsExceptForTag(groupId, tag);
                List<Long> availableBeIds = getAvailableBeIds(SystemInfoService.DEFAULT_CLUSTER, tag, beIdsInOtherTag,
                        infoService);
                // try relocate or balance this group for specified tag
                List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
                if (relocateAndBalance(groupId, tag, unavailableBeIdsInGroup, availableBeIds, colocateIndex,
                        infoService, statistic, globalColocateStatistic, balancedBackendsPerBucketSeq,
                        balanceBetweenGroups)) {
                    colocateIndex.addBackendsPerBucketSeqByTag(groupId, tag, balancedBackendsPerBucketSeq);
                    changeGroups.add(groupId);
                    Map<Tag, List<List<Long>>> balancedBackendsPerBucketSeqMap = Maps.newHashMap();
                    balancedBackendsPerBucketSeqMap.put(tag, balancedBackendsPerBucketSeq);
                    ColocatePersistInfo info = ColocatePersistInfo
                            .createForBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeqMap);
                    env.getEditLog().logColocateBackendsPerBucketSeq(info);
                    LOG.info("balance group {}. now backends per bucket sequence for tag {} is: {}",
                            groupId, tag, balancedBackendsPerBucketSeq);
                }
            }
        }

        return changeGroups;
    }

    /*
     * Check every tablet of a group, if replica's location does not match backends in group, relocating those
     * replicas, and mark that group as unstable.
     * If every replicas match the backends in group, mark that group as stable.
     */
    private void matchGroups() {
        long start = System.currentTimeMillis();
        CheckerCounter counter = new CheckerCounter();

        Env env = Env.getCurrentEnv();
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        ColocateTableIndex colocateIndex = env.getColocateTableIndex();
        TabletScheduler tabletScheduler = env.getTabletScheduler();

        // check each group
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : groupIds) {
            List<Long> tableIds = colocateIndex.getAllTableIds(groupId);
            List<Set<Long>> backendBucketsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
            if (backendBucketsSeq.isEmpty()) {
                continue;
            }

            String unstableReason = null;
            OUT:
            for (Long tableId : tableIds) {
                long dbId = groupId.dbId;
                if (dbId == 0) {
                    dbId = groupId.getDbIdByTblId(tableId);
                }
                Database db = env.getInternalCatalog().getDbNullable(dbId);
                if (db == null) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
                if (olapTable == null || !colocateIndex.isColocateTable(olapTable.getId())) {
                    continue;
                }
                olapTable.readLock();
                try {
                    for (Partition partition : olapTable.getPartitions()) {
                        ReplicaAllocation replicaAlloc
                                = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId());
                        short replicationNum = replicaAlloc.getTotalReplicaNum();
                        long visibleVersion = partition.getVisibleVersion();
                        // Here we only get VISIBLE indexes. All other indexes are not queryable.
                        // So it does not matter if tablets of other indexes are not matched.
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            Preconditions.checkState(backendBucketsSeq.size() == index.getTablets().size(),
                                    backendBucketsSeq.size() + " vs. " + index.getTablets().size());
                            int idx = 0;
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                counter.totalTabletNum++;
                                Set<Long> bucketsSeq = backendBucketsSeq.get(idx);
                                Preconditions.checkState(bucketsSeq.size() == replicationNum,
                                        bucketsSeq.size() + " vs. " + replicationNum);
                                Tablet tablet = index.getTablet(tabletId);
                                TabletStatus st = tablet.getColocateHealthStatus(
                                        visibleVersion, replicaAlloc, bucketsSeq);
                                if (st != TabletStatus.HEALTHY) {
                                    counter.unhealthyTabletNum++;
                                    unstableReason = String.format("get unhealthy tablet %d in colocate table."
                                            + " status: %s", tablet.getId(), st);
                                    LOG.debug(unstableReason);

                                    if (!tablet.readyToBeRepaired(infoService, Priority.NORMAL)) {
                                        counter.tabletNotReady++;
                                        continue;
                                    }

                                    TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                            TabletSchedCtx.Type.REPAIR,
                                            db.getId(), tableId, partition.getId(), index.getId(), tablet.getId(),
                                            olapTable.getPartitionInfo().getReplicaAllocation(partition.getId()),
                                            System.currentTimeMillis());
                                    // the tablet status will be set again when being scheduled
                                    tabletCtx.setTabletStatus(st);
                                    tabletCtx.setPriority(Priority.NORMAL);
                                    tabletCtx.setTabletOrderIdx(idx);

                                    AddResult res = tabletScheduler.addTablet(tabletCtx, false /* not force */);
                                    if (res == AddResult.LIMIT_EXCEED || res == AddResult.DISABLED) {
                                        // tablet in scheduler exceed limit, or scheduler is disabled,
                                        // skip this group and check next one.
                                        LOG.info("tablet scheduler return: {}. stop colocate table check", res.name());
                                        break OUT;
                                    } else if (res == AddResult.ADDED) {
                                        counter.addToSchedulerTabletNum++;
                                    }  else {
                                        counter.tabletInScheduler++;
                                    }
                                }
                                idx++;
                            }
                        }
                    }
                } finally {
                    olapTable.readUnlock();
                }
            } // end for tables

            // mark group as stable or unstable
            if (Strings.isNullOrEmpty(unstableReason)) {
                colocateIndex.markGroupStable(groupId, true);
            } else {
                colocateIndex.markGroupUnstable(groupId, unstableReason, true);
            }
        } // end for groups

        long cost = System.currentTimeMillis() - start;
        LOG.info("finished to check tablets. unhealth/total/added/in_sched/not_ready: {}/{}/{}/{}/{}, cost: {} ms",
                counter.unhealthyTabletNum, counter.totalTabletNum, counter.addToSchedulerTabletNum,
                counter.tabletInScheduler, counter.tabletNotReady, cost);
    }

    private GlobalColocateStatistic buildGlobalColocateStatistic() {
        Env env = Env.getCurrentEnv();
        ColocateTableIndex colocateIndex = env.getColocateTableIndex();
        GlobalColocateStatistic globalColocateStatistic = new GlobalColocateStatistic();

        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : groupIds) {
            ColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
            if (groupSchema == null) {
                continue;
            }
            ReplicaAllocation replicaAlloc = groupSchema.getReplicaAlloc();
            List<Long> tableIds = colocateIndex.getAllTableIds(groupId);
            List<Set<Long>> backendBucketsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
            if (backendBucketsSeq.isEmpty()) {
                continue;
            }

            int totalReplicaNumPerBucket = 0;
            ArrayList<Long> totalReplicaDataSizes = Lists.newArrayList();
            for (int i = 0; i < backendBucketsSeq.size(); i++) {
                totalReplicaDataSizes.add(0L);
            }

            for (Long tableId : tableIds) {
                long dbId = groupId.dbId;
                if (dbId == 0) {
                    dbId = groupId.getDbIdByTblId(tableId);
                }
                Database db = env.getInternalCatalog().getDbNullable(dbId);
                if (db == null) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
                if (olapTable == null || !colocateIndex.isColocateTable(olapTable.getId())) {
                    continue;
                }

                olapTable.readLock();
                try {
                    for (Partition partition : olapTable.getPartitions()) {
                        short replicationNum = replicaAlloc.getTotalReplicaNum();

                        // Here we only get VISIBLE indexes. All other indexes are not queryable.
                        // So it does not matter if tablets of other indexes are not matched.

                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            Preconditions.checkState(backendBucketsSeq.size() == index.getTablets().size(),
                                    backendBucketsSeq.size() + " vs. " + index.getTablets().size());
                            int tabletOrderIdx = 0;
                            totalReplicaNumPerBucket++;
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                Set<Long> bucketsSeq = backendBucketsSeq.get(tabletOrderIdx);
                                Preconditions.checkState(bucketsSeq.size() == replicationNum,
                                        bucketsSeq.size() + " vs. " + replicationNum);
                                Tablet tablet = index.getTablet(tabletId);
                                totalReplicaDataSizes.set(tabletOrderIdx,
                                        totalReplicaDataSizes.get(tabletOrderIdx) + tablet.getDataSize(true));
                                tabletOrderIdx++;
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("build group {} colocate statistic error", groupId, e);
                    continue;
                } finally {
                    olapTable.readUnlock();
                }
            }

            globalColocateStatistic.addGroup(groupId, replicaAlloc, backendBucketsSeq, totalReplicaDataSizes,
                    totalReplicaNumPerBucket);
        }

        return globalColocateStatistic;
    }

    /*
     * Each balance is performed for a single workload group in a colocate group.
     * For example, if the replica allocation of a colocate group is {TagA: 2, TagB: 1},
     * So the backend bucket seq may be like:
     *
     *       0  1  2  3
     * TagA  A  B  C  A
     * TagA  B  C  A  B
     * TagB  D  D  D  D
     *
     * First, we will handle workload group of TagA, then TagB.
     *
     * For a single workload group, the balance logic is as follow
     * (Suppose there is only one workload group with 3 replicas):
     *
     * All backends: A,B,C,D,E,F,G,H,I,J
     *
     * One group's buckets sequence:
     *
     * Buckets sequence:    0  1  2  3
     * Backend set:         A  A  A  A
     *                      B  D  F  H
     *                      C  E  G  I
     *
     * Then each backend has different replica num:
     *
     * Backends:    A B C D E F G H I J
     * Replica num: 4 1 1 1 1 1 1 1 1 0
     *
     * The goal of balance is to evenly distribute replicas on all backends. For this example, we want the
     * following result (one possible result):
     *
     * Backends:    A B C D E F G H I J
     * Replica num: 2 2 1 1 1 1 1 1 1 1
     *
     * Algorithm:
     * 0. Generate the flat list of backends per bucket sequence:
     *      A B C A D E A F G A H I
     * 1. Sort backends order by replication num and load score for same replication num backends, descending:
     *      A B C D E F G H I J
     * 2. Check the diff of the first backend(A)'s replica num and last backend(J)'s replica num.
     *      If diff is less or equal than 1, we consider this group as balance. Jump to step 5.
     * 3. Else, Replace the first occurrence of Backend A in flat list with Backend J.
     *      J B C A D E A F G A H I
     * 4. Recalculate the replica num of each backend and go to step 1.
     * 5. We should get the following flat list(one possible result):
     *      J B C J D E A F G A H I
     *    Partition this flat list by replication num:
     *      [J B C] [J D E] [A F G] [A H I]
     *    And this is our new balanced backends per bucket sequence.
     *
     *  relocate is similar to balance, but choosing unavailable be as src, and move all bucketIds on unavailable be to
     *  low be
     *
     *  Return true if backends per bucket sequence change and new sequence is saved in balancedBackendsPerBucketSeq.
     *  Return false if nothing changed.
     */
    private boolean relocateAndBalance(GroupId groupId, Tag tag, Set<Long> unavailableBeIds, List<Long> availableBeIds,
            ColocateTableIndex colocateIndex, SystemInfoService infoService, LoadStatisticForTag statistic,
            GlobalColocateStatistic globalColocateStatistic, List<List<Long>> balancedBackendsPerBucketSeq,
            boolean balanceBetweenGroups) {
        ColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
        short replicaNum = groupSchema.getReplicaAlloc().getReplicaNumByTag(tag);
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList(
                colocateIndex.getBackendsPerBucketSeqByTag(groupId, tag));
        // [[A,B,C],[B,C,D]] -> [A,B,C,B,C,D]
        List<Long> flatBackendsPerBucketSeq = backendsPerBucketSeq.stream()
                .flatMap(List::stream).collect(Collectors.toList());

        int tagTotalBucketNum = globalColocateStatistic.getAllTagBucketNum().getOrDefault(tag, 0);
        int availableBeNum = availableBeIds.size();
        int highTotalBucketNumPerBe = availableBeNum == 0 ? 0 :
                (tagTotalBucketNum + availableBeNum - 1) / availableBeNum;
        int lowTotalBucketNumPerBe = availableBeNum == 0 ? 0 : tagTotalBucketNum / availableBeNum;

        boolean isChanged = false;
        int times = 0;
        List<RootPathLoadStatistic> resultPaths = Lists.newArrayList();

        OUT:
        while (true) {
            // update backends and hosts at each round
            backendsPerBucketSeq = Lists.partition(flatBackendsPerBucketSeq, replicaNum);
            List<List<String>> hostsPerBucketSeq = getHostsPerBucketSeq(backendsPerBucketSeq, infoService);
            if (hostsPerBucketSeq == null) {
                // error happens, change nothing
                return false;
            }
            Preconditions.checkState(backendsPerBucketSeq.size() == hostsPerBucketSeq.size());
            times++;
            if (times > 10 * backendsPerBucketSeq.size()) {
                LOG.warn("iterate too many times for relocate group: {}, times: {}, bucket num: {}",
                        groupId, times, backendsPerBucketSeq.size());
                break;
            }

            long srcBeId = -1;
            List<Integer> seqIndexes = null;
            boolean srcBeUnavailable = false;
            // first choose the unavailable be as src be
            for (Long beId : unavailableBeIds) {
                seqIndexes = getBeSeqIndexes(flatBackendsPerBucketSeq, beId);
                if (!seqIndexes.isEmpty()) {
                    srcBeId = beId;
                    srcBeUnavailable = true;
                    LOG.info("find unavailable backend {} in colocate group: {}", beId, groupId);
                    break;
                }
            }

            // sort backends with replica num in desc order
            List<Map.Entry<Long, Long>> backendWithReplicaNum =
                    getSortedBackendReplicaNumPairs(availableBeIds, unavailableBeIds, statistic,
                            globalColocateStatistic, flatBackendsPerBucketSeq);

            // if there is only one available backend and no unavailable bucketId to relocate, end the outer loop
            if (backendWithReplicaNum.size() <= 1 && !srcBeUnavailable) {
                break;
            }

            if (seqIndexes == null || seqIndexes.isEmpty()) {
                // choose max bucketId num be as src be
                Preconditions.checkState(backendsPerBucketSeq.size() > 0);
                srcBeId = backendWithReplicaNum.get(0).getKey();
                seqIndexes = getBeSeqIndexes(flatBackendsPerBucketSeq, srcBeId);
            }

            boolean isThisRoundChanged = false;
            for (int j = backendWithReplicaNum.size() - 1; j >= 0; j--) {
                // we try to use a low backend to replace the src backend.
                // if replace failed(eg: both backends are on some host), select next low backend and try(j--)
                Map.Entry<Long, Long> lowBackend = backendWithReplicaNum.get(j);
                long destBeId = lowBackend.getKey();
                if (!srcBeUnavailable) {
                    long diffThisGroup = seqIndexes.size() - lowBackend.getValue();
                    if (diffThisGroup < 1) {
                        // balanced
                        break OUT;
                    }

                    // src's group bucket num = dest's group bucket num + 1
                    // if move group bucket from src to dest, dest will be one more group num than src.
                    // check global view
                    //
                    // suppose bucket num = 3, three BE A/B/C,  two group group1/group2, then we have:
                    //
                    // A [ group1:bucket0,  group2:bucket0]
                    // B [ group1:bucket1,  group2:bucket1]
                    // C [ group1:bucket2,  group2:bucket2]
                    //
                    // if we add a new BE D, for each group: bucketNum(A)=bucketNum(B)=bucketNum(C)=1,  bucketNum(D)=0
                    // so each group is balance, but in global groups view, it's not balance.
                    // we should move one of the buckets to D
                    if (diffThisGroup == 1) {
                        if (!balanceBetweenGroups) {
                            break OUT;
                        }
                        int srcTotalBucketNum = globalColocateStatistic.getBackendTotalBucketNum(srcBeId);
                        int destTotalBucketNum = globalColocateStatistic.getBackendTotalBucketNum(destBeId);
                        if (srcTotalBucketNum <= highTotalBucketNumPerBe
                                || destTotalBucketNum >= lowTotalBucketNumPerBe) {
                            continue;
                        }
                    }
                }

                Backend destBe = infoService.getBackend(destBeId);
                if (destBe == null) {
                    LOG.info("backend {} does not exist", destBeId);
                    return false;
                }

                // if we found src_id == dst_id we skip to next
                if (srcBeId == destBeId) {
                    continue;
                }

                // Unavailable be has been removed from backendWithReplicaNum,
                // but the conditions for judging unavailable be by
                // getUnavailableBeIdsInGroup may be too loose. Under the
                // default configuration (colocate_group_relocate_delay_second =
                // 1800), a be that has been out of contact for 20 minutes can
                // still be selected as the dest be.
                if (!destBe.isAlive()) {
                    LOG.info("{} is not alive, not suitable as a dest be", destBe);
                    continue;
                }

                BackendLoadStatistic beStat = statistic.getBackendLoadStatistic(destBeId);
                if (beStat == null) {
                    LOG.warn("not found backend {} statistic", destBeId);
                    continue;
                }

                int targetSeqIndex = -1;
                long minDataSizeDiff = Long.MAX_VALUE;
                for (int seqIndex : seqIndexes) {
                    // the bucket index.
                    // eg: 0 / 3 = 0, so that the bucket index of the 4th backend id in flatBackendsPerBucketSeq is 0.
                    int bucketIndex = seqIndex / replicaNum;
                    List<Long> backendsSet = backendsPerBucketSeq.get(bucketIndex);
                    List<String> hostsSet = hostsPerBucketSeq.get(bucketIndex);
                    // the replicas of a tablet can not locate in same Backend or same host
                    if (backendsSet.contains(destBeId) || hostsSet.contains(destBe.getHost())) {
                        continue;
                    }

                    Preconditions.checkState(backendsSet.contains(srcBeId), srcBeId);
                    long bucketDataSize =
                            globalColocateStatistic.getBucketTotalReplicaDataSize(groupId, bucketIndex);

                    resultPaths.clear();
                    BalanceStatus st = beStat.isFit(bucketDataSize, null, resultPaths, true);
                    if (!st.ok()) {
                        LOG.debug("backend {} is unable to fit in group {}, tablet order idx {}, data size {}",
                                destBeId, groupId, bucketIndex, bucketDataSize);
                        continue;
                    }

                    long newSrcBeTotalReplicaDataSize = globalColocateStatistic.getBackendTotalReplicaDataSize(srcBeId)
                            - bucketDataSize;
                    long newDestBeTotalReplicaDataSize =
                            globalColocateStatistic.getBackendTotalReplicaDataSize(destBeId) + bucketDataSize;
                    long dataSizeDiff = Math.abs(newSrcBeTotalReplicaDataSize - newDestBeTotalReplicaDataSize);
                    if (targetSeqIndex < 0 || dataSizeDiff < minDataSizeDiff) {
                        targetSeqIndex = seqIndex;
                        minDataSizeDiff = dataSizeDiff;
                    }
                }

                if (targetSeqIndex < 0) {
                    // we use next node as dst node
                    LOG.info("unable to replace backend {} with backend {} in colocate group {}",
                            srcBeId, destBeId, groupId);
                    continue;
                }

                int tabletOrderIdx = targetSeqIndex / replicaNum;
                int oldSrcThisGroup = seqIndexes.size();
                long oldDestThisGroup = lowBackend.getValue();
                int oldSrcBucketNum = globalColocateStatistic.getBackendTotalBucketNum(srcBeId);
                int oldDestBucketNum = globalColocateStatistic.getBackendTotalBucketNum(destBeId);
                LOG.debug("OneMove: group {}, src {}, this group {}, all group {}, dest {}, this group {}, "
                        + "all group {}", groupId, srcBeId, oldSrcThisGroup, oldSrcBucketNum, destBeId,
                        oldDestThisGroup, oldDestBucketNum);
                Preconditions.checkState(
                        globalColocateStatistic.moveTablet(groupId, tabletOrderIdx, srcBeId, destBeId));
                Preconditions.checkState(oldSrcBucketNum - 1
                        == globalColocateStatistic.getBackendTotalBucketNum(srcBeId));
                Preconditions.checkState(oldDestBucketNum + 1
                        == globalColocateStatistic.getBackendTotalBucketNum(destBeId));
                flatBackendsPerBucketSeq.set(targetSeqIndex, destBeId);
                // just replace one backend at a time, src and dest BE id should be recalculated because
                // flatBackendsPerBucketSeq is changed.
                isChanged = true;
                isThisRoundChanged = true;
                LOG.info("replace backend {} with backend {} in colocate group {}, idx: {}",
                        srcBeId, destBeId, groupId, targetSeqIndex);
                break;
            }

            if (!isThisRoundChanged) {
                // if all backends are checked but this round is not changed,
                // we should end the loop
                LOG.info("all backends are checked but this round is not changed, "
                        + "end outer loop in colocate group {}", groupId);
                break;
            }
            // end inner loop
        }

        if (isChanged) {
            balancedBackendsPerBucketSeq.addAll(Lists.partition(flatBackendsPerBucketSeq, replicaNum));
        }
        return isChanged;
    }

    // change the backend id to backend host
    // return null if some of backends do not exist
    private List<List<String>> getHostsPerBucketSeq(List<List<Long>> backendsPerBucketSeq,
                                                    SystemInfoService infoService) {
        List<List<String>> hostsPerBucketSeq = Lists.newArrayList();
        for (List<Long> backendIds : backendsPerBucketSeq) {
            List<String> hosts = Lists.newArrayList();
            for (Long beId : backendIds) {
                Backend be = infoService.getBackend(beId);
                if (be == null) {
                    // For non-exist BE(maybe dropped), add a ip 0.0.0.0
                    // And the following logic will handle the non-exist host.
                    hosts.add(Backend.DUMMY_IP);
                } else {
                    hosts.add(be.getHost());
                }
            }
            hostsPerBucketSeq.add(hosts);
        }
        return hostsPerBucketSeq;
    }

    private List<Map.Entry<Long, Long>> getSortedBackendReplicaNumPairs(List<Long> allAvailBackendIds,
            Set<Long> unavailBackendIds, LoadStatisticForTag statistic,
            GlobalColocateStatistic globalColocateStatistic,
            List<Long> flatBackendsPerBucketSeq) {
        // backend id -> replica num, and sorted by replica num, descending.
        Map<Long, Long> backendToReplicaNum = flatBackendsPerBucketSeq.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        // remove unavailable backend
        for (Long backendId : unavailBackendIds) {
            backendToReplicaNum.remove(backendId);
        }
        // add backends which are not in flatBackendsPerBucketSeq, with replication number 0
        for (Long backendId : allAvailBackendIds) {
            if (!backendToReplicaNum.containsKey(backendId)) {
                backendToReplicaNum.put(backendId, 0L);
            }
        }

        return backendToReplicaNum
                .entrySet()
                .stream()
                .sorted((entry1, entry2) -> {
                    if (!entry1.getValue().equals(entry2.getValue())) {
                        return (int) (entry2.getValue() - entry1.getValue());
                    }

                    // From java 7, sorting needs to satisfy reflexivity, transitivity and symmetry.
                    // Otherwise it will raise exception "Comparison method violates its general contract".

                    BackendLoadStatistic beStat1 = statistic.getBackendLoadStatistic(entry1.getKey());
                    BackendLoadStatistic beStat2 = statistic.getBackendLoadStatistic(entry2.getKey());
                    if (beStat1 == null || beStat2 == null) {
                        if (beStat1 == null && beStat2 == null) {
                            return 0;
                        } else {
                            return beStat1 == null ? 1 : -1;
                        }
                    }
                    double loadScore1 = beStat1.getMixLoadScore();
                    double loadScore2 = beStat2.getMixLoadScore();
                    int cmp = Double.compare(loadScore2, loadScore1);
                    if (cmp != 0) {
                        return cmp;
                    }

                    return Long.compare(entry1.getKey(), entry2.getKey());
                })
                .collect(Collectors.toList());
    }

    /*
     * get the array indexes of elements in flatBackendsPerBucketSeq which equals to beId
     * eg:
     * flatBackendsPerBucketSeq:
     *      A B C A D E A F G A H I
     * and srcBeId is A.
     * so seqIndexes is:
     *      0 3 6 9
     */
    private List<Integer> getBeSeqIndexes(List<Long> flatBackendsPerBucketSeq, long beId) {
        return IntStream.range(0, flatBackendsPerBucketSeq.size()).boxed().filter(
                idx -> flatBackendsPerBucketSeq.get(idx).equals(beId)).collect(Collectors.toList());
    }

    private Set<Long> getUnavailableBeIdsInGroup(SystemInfoService infoService, ColocateTableIndex colocateIndex,
                                                 GroupId groupId, Tag tag) {
        Set<Long> backends = colocateIndex.getBackendsByGroup(groupId, tag);
        Set<Long> unavailableBeIds = Sets.newHashSet();
        for (Long backendId : backends) {
            if (!checkBackendAvailable(backendId, tag, Sets.newHashSet(), infoService,
                    Config.colocate_group_relocate_delay_second)) {
                unavailableBeIds.add(backendId);
            }
        }
        return unavailableBeIds;
    }

    private List<Long> getAvailableBeIds(String cluster, Tag tag, Set<Long> excludedBeIds,
            SystemInfoService infoService) {
        // get all backends to allBackendIds, and check be availability using checkBackendAvailable
        // backend stopped for a short period of time is still considered available
        List<Long> allBackendIds = infoService.getAllBackendIds(false);
        List<Long> availableBeIds = Lists.newArrayList();
        for (Long backendId : allBackendIds) {
            if (checkBackendAvailable(backendId, tag, excludedBeIds, infoService,
                    Config.colocate_group_relocate_delay_second)) {
                availableBeIds.add(backendId);
            }
        }
        return availableBeIds;
    }

    /**
     * check backend available
     * backend stopped within "delaySecond" is still considered available
     */
    private boolean checkBackendAvailable(Long backendId, Tag tag, Set<Long> excludedBeIds,
                                          SystemInfoService infoService, long delaySecond) {
        long currTime = System.currentTimeMillis();
        Backend be = infoService.getBackend(backendId);
        if (be == null) {
            return false;
        } else if (!be.isMixNode()) {
            return false;
        } else if (!be.getLocationTag().equals(tag) || excludedBeIds.contains(be.getId())) {
            return false;
        } else if (!be.isScheduleAvailable()) {
            // 1. BE is dead longer than "delaySecond"
            // 2. BE is under decommission
            if ((!be.isAlive() && (currTime - be.getLastUpdateMs()) > delaySecond * 1000L) || be.isDecommissioned()) {
                return false;
            }
        }
        return true;
    }
}
