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
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TenantLevelColocateGroupSchema;
import org.apache.doris.catalog.TenantLevelColocateTableIndex;
import org.apache.doris.clone.TabletChecker.CheckerCounter;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.AddResult;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Reference;
import org.apache.doris.persist.ModifyTenantLevelColocateMapInfo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TenantLevelColocateTableCheckerAndBalancer is responsible for tablets' repair and balance of colocated tables.
 */
public class TenantLevelColocateTableCheckerAndBalancer extends ColocateTableCheckerAndBalancer {
    private static final Logger LOG = LogManager.getLogger(TenantLevelColocateTableCheckerAndBalancer.class);

    private TenantLevelColocateTableCheckerAndBalancer(long intervalMs) {
        super("colocate group clone checker v2", intervalMs);
    }

    private static volatile TenantLevelColocateTableCheckerAndBalancer INSTANCE = null;

    public static TenantLevelColocateTableCheckerAndBalancer getInstance() {
        if (INSTANCE == null) {
            synchronized (TenantLevelColocateTableCheckerAndBalancer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TenantLevelColocateTableCheckerAndBalancer(Config.tablet_checker_interval_ms);
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public void runAfterCatalogReady() {
        relocateAndBalanceGroups();
        matchGroups();
    }

    private void relocateAndBalanceGroups() {
        Set<Long> groupIds = Env.getCurrentEnv().getTenantLevelColocateTableIndex().getAllGroupIds();

        // balance only inside each group, excluded balance between all groups
        Set<Long> changeGroups = relocateAndBalanceGroup(groupIds, false);

        if (!Config.disable_colocate_balance_between_groups
                && !changeGroups.isEmpty()) {
            // balance both inside each group and between all groups
            relocateAndBalanceGroup(changeGroups, true);
        }
    }

    private Set<Long> relocateAndBalanceGroup(Set<Long> groupIds, boolean balanceBetweenGroups) {
        Set<Long> changeGroups = Sets.newHashSet();
        if (Config.disable_colocate_balance) {
            return changeGroups;
        }

        GlobalColocateStatistic globalColocateStatistic = buildGlobalColocateStatistic();

        // get all groups
        for (Long groupId : groupIds) {
            try {
                relocateAndBalanceGroup(groupId, balanceBetweenGroups, changeGroups, globalColocateStatistic);
            } catch (Exception e) {
                LOG.error("relocate group {} failed.", groupId, e);
            }
        }

        return changeGroups;
    }

    private void relocateAndBalanceGroup(Long groupId, boolean balanceBetweenGroups, Set<Long> changeGroups,
            GlobalColocateStatistic globalColocateStatistic) {
        Env env = Env.getCurrentEnv();
        TenantLevelColocateTableIndex colocateIndex = env.getTenantLevelColocateTableIndex();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        Map<Tag, LoadStatisticForTag> statisticMap = env.getTabletScheduler().getStatisticMap();
        if (statisticMap == null) {
            return;
        }

        TenantLevelColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
        if (groupSchema == null) {
            LOG.info("Not found colocate group {}, maybe delete", groupId);
            return;
        }
        ReplicaAllocation replicaAlloc = groupSchema.getReplicaAlloc();
        try {
            Env.getCurrentSystemInfo().checkReplicaAllocation(replicaAlloc);
        } catch (DdlException e) {
            colocateIndex.setErrMsgForGroup(groupId, e.getMessage());
            return;
        }

        Tag tag = groupSchema.getTag();
        LoadStatisticForTag statistic = statisticMap.get(tag);
        if (statistic == null) {
            return;
        }
        List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeqByGroup(groupId);
        if (backendsPerBucketSeq.isEmpty()) {
            return;
        }

        // get all unavailable backends in the backend bucket sequence of this group
        Set<Long> unavailableBeIdsInGroup = getUnavailableBeIdsInGroup(
                infoService, colocateIndex, groupId, tag);
        // get all available backends for this group
        List<Long> availableBeIds = getAvailableBeIds(tag, Collections.emptySet(),
                infoService);
        // try relocate or balance this group for specified tag
        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        if (relocateAndBalance(groupId, tag, unavailableBeIdsInGroup, availableBeIds, colocateIndex,
                infoService, statistic, globalColocateStatistic, balancedBackendsPerBucketSeq,
                balanceBetweenGroups)) {
            if (!colocateIndex.addBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq, replicaAlloc)) {
                LOG.warn("relocate group {} succ, but replica allocation has change, old replica alloc {}",
                        groupId, replicaAlloc);
                return;
            }
            colocateIndex.markMasterGroupUnstable(groupId, "relocated", true);
            colocateIndex.markSlaveGroupUnstable(groupId, "master is unstable", true);
            changeGroups.add(groupId);
            Map<Long, List<List<Long>>> balancedBackendsPerBucketSeqMap = Maps.newHashMap();
            balancedBackendsPerBucketSeqMap.put(groupId, balancedBackendsPerBucketSeq);
            ModifyTenantLevelColocateMapInfo info = new ModifyTenantLevelColocateMapInfo(
                    balancedBackendsPerBucketSeqMap);
            env.getEditLog().logTenantLevelColocateBackendsPerBucketSeq(info);
            LOG.info("balance group {}. now backends per bucket sequence for tag {} is: {}",
                    groupId, tag, balancedBackendsPerBucketSeq);
        }
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
        TenantLevelColocateTableIndex colocateIndex = env.getTenantLevelColocateTableIndex();

        // check each group
        Set<Long> groupIds = colocateIndex.getAllGroupIds();
        for (Long groupId : groupIds) {
            TenantLevelColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
            if (groupSchema == null) {
                LOG.info("Not found colocate group {}, maybe delete", groupId);
                continue;
            }

            List<Set<Long>> backendBucketsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
            if (backendBucketsSeq.isEmpty()) {
                continue;
            }
            try {
                String unstableReason = matchMasterGroup(env, counter, groupId, groupSchema.getTag(),
                        backendBucketsSeq);
                // mark group as stable or unstable
                if (Strings.isNullOrEmpty(unstableReason)) {
                    colocateIndex.markMasterGroupStable(groupId, true);
                    unstableReason = matchSlaveGroup(env, counter, groupId, groupSchema.getTag(), backendBucketsSeq);
                    if (Strings.isNullOrEmpty(unstableReason)) {
                        colocateIndex.markSlaveGroupStable(groupId, true);
                    } else {
                        colocateIndex.markSlaveGroupUnstable(groupId, unstableReason, true);
                    }
                } else {
                    colocateIndex.markMasterGroupUnstable(groupId, unstableReason, true);
                    colocateIndex.markSlaveGroupUnstable(groupId, "mater is unstable", true);
                }
            } catch (Exception e) {
                LOG.error("check group {} failed.{}", groupId, e.getMessage());
            }
        } // end for groups

        long cost = System.currentTimeMillis() - start;
        LOG.info("finished to check tablets. unhealth/total/added/in_sched/not_ready/exceed_limit: {}/{}/{}/{}/{}/{}, "
                        + "cost: {} ms",
                counter.unhealthyTabletNum, counter.totalTabletNum, counter.addToSchedulerTabletNum,
                counter.tabletInScheduler, counter.tabletNotReady, counter.tabletExceedLimit, cost);
    }

    private String matchMasterGroup(Env env, CheckerCounter counter, Long groupId, Tag tag,
            List<Set<Long>> backendBucketsSeq) {
        TenantLevelColocateTableIndex colocateIndex = env.getTenantLevelColocateTableIndex();
        List<Long> tableIds = colocateIndex.getAllMasterTableIds(groupId);
        Set<Tag> colocateTags = Collections.singleton(tag);
        Reference<String> unstableReason = new Reference<>();
        for (Long tableId : tableIds) {
            Database db = env.getInternalCatalog().getDbNullableByTable(tableId);
            if (db == null) {
                continue;
            }
            counter.totalTabletNum++;
            OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
            if (olapTable == null || !colocateIndex.isColocateMasterTable(olapTable.getId())) {
                continue;
            }
            if (matchTable(env, counter, db, olapTable, colocateTags, backendBucketsSeq, unstableReason)) {
                break;
            }
        } // end for tables
        return unstableReason.getRef();
    }

    private String matchSlaveGroup(Env env, CheckerCounter counter, Long groupId, Tag tag,
            List<Set<Long>> masterBackendBucketsSeq) {
        TenantLevelColocateTableIndex colocateIndex = env.getTenantLevelColocateTableIndex();
        List<Long> tableIds = colocateIndex.getAllSlaveTableIds(groupId);
        Set<Tag> colocateTags = Collections.singleton(tag);
        Reference<String> unstableReason = new Reference<>();
        for (Long tableId : tableIds) {
            Database db = env.getInternalCatalog().getDbNullableByTable(tableId);
            if (db == null) {
                continue;
            }
            counter.totalTabletNum++;
            OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
            if (olapTable == null || !colocateIndex.isColocateSlaveTable(olapTable.getId())) {
                continue;
            }
            List<Set<Long>> backendBucketsSeq = TenantLevelColocateTableIndex.getSlaveBackendsPerBucketSeqSet(
                    masterBackendBucketsSeq, olapTable.getDefaultDistributionInfo().getBucketNum());
            if (backendBucketsSeq.isEmpty()) {
                continue;
            }
            if (matchTable(env, counter, db, olapTable, colocateTags, backendBucketsSeq, unstableReason)) {
                break;
            }
        } // end for tables
        return unstableReason.getRef();
    }

    /**
     * return true if skip this group and check next one
     */
    private boolean matchTable(Env env, CheckerCounter counter, Database db,
            OlapTable olapTable, Set<Tag> colocateTags,
            List<Set<Long>> backendBucketsSeq, Reference<String> unstableReason) {
        olapTable.readLock();
        try {
            for (Partition partition : olapTable.getPartitions()) {
                if (matchPartition(env, counter, db, olapTable, partition, colocateTags,
                        backendBucketsSeq, unstableReason)) {
                    return true;
                }
            }
        } finally {
            olapTable.readUnlock();
        }
        return false;
    }

    private boolean matchPartition(Env env, CheckerCounter counter, Database db,
            OlapTable olapTable, Partition partition, Set<Tag> colocateTags,
            List<Set<Long>> backendBucketsSeq, Reference<String> unstableReason) {
        TabletScheduler tabletScheduler = env.getTabletScheduler();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo()
                .getReplicaAllocation(partition.getId()).getSubMap(colocateTags);

        short replicationNum = replicaAlloc.getTotalReplicaNum();
        long visibleVersion = partition.getVisibleVersion();
        // Here we only get VISIBLE indexes. All other indexes are not queryable.
        // So it does not matter if tablets of other indexes are not matched.
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            Preconditions.checkState(backendBucketsSeq.size() == index.getTablets().size(),
                    backendBucketsSeq.size() + " vs. " + index.getTablets().size());
            List<Long> tabletIdsInOrder = index.getTabletIdsInOrder();
            for (int idx = 0; idx < tabletIdsInOrder.size(); idx++) {
                Long tabletId = tabletIdsInOrder.get(idx);
                counter.totalTabletNum++;
                Set<Long> bucketsSeq = backendBucketsSeq.get(idx);
                Preconditions.checkState(bucketsSeq.size() == replicationNum,
                        bucketsSeq.size() + " vs. " + replicationNum);
                Tablet tablet = index.getTablet(tabletId);
                TabletHealth tabletHealth = tablet.getColocateHealth(visibleVersion,
                        replicaAlloc, bucketsSeq, colocateTags);
                if (tabletHealth.status != TabletStatus.HEALTHY) {
                    counter.unhealthyTabletNum++;
                    unstableReason.setRef(String.format("get unhealthy tablet %d in colocate table."
                            + " status: %s", tablet.getId(), tabletHealth.status));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(unstableReason);
                    }

                    if (tabletHealth.status == TabletStatus.UNRECOVERABLE) {
                        continue;
                    }

                    if (!tablet.readyToBeRepaired(infoService, Priority.NORMAL)) {
                        counter.tabletNotReady++;
                        continue;
                    }

                    TabletSchedCtx tabletCtx = new TabletSchedCtx(
                            TabletSchedCtx.Type.REPAIR,
                            db.getId(), olapTable.getId(), partition.getId(), index.getId(), tablet.getId(),
                            olapTable.getPartitionInfo().getReplicaAllocation(partition.getId()),
                            System.currentTimeMillis());
                    // the tablet status will be set again when being scheduled
                    tabletCtx.setTabletHealth(tabletHealth);
                    tabletCtx.setTabletOrderIdx(idx);
                    tabletCtx.setIsUniqKeyMergeOnWrite(olapTable.isUniqKeyMergeOnWrite());

                    AddResult res = tabletScheduler.addTablet(tabletCtx, false /* not force */);
                    if (res == AddResult.DISABLED) {
                        // tablet in scheduler exceed limit, or scheduler is disabled,
                        // skip this group and check next one.
                        LOG.info("tablet scheduler return: {}. stop colocate table check", res.name());
                        return true;
                    } else if (res == AddResult.ADDED) {
                        counter.addToSchedulerTabletNum++;
                    } else if (res == AddResult.ALREADY_IN) {
                        counter.tabletInScheduler++;
                    } else if (res == AddResult.REPLACE_ADDED || res == AddResult.LIMIT_EXCEED) {
                        counter.tabletExceedLimit++;
                    }
                }
            }
        }
        return false;
    }

    private GlobalColocateStatistic buildGlobalColocateStatistic() {
        Env env = Env.getCurrentEnv();
        TenantLevelColocateTableIndex colocateIndex = env.getTenantLevelColocateTableIndex();
        GlobalColocateStatistic globalColocateStatistic = new GlobalColocateStatistic();

        Set<Long> groupIds = colocateIndex.getAllGroupIds();
        for (Long groupId : groupIds) {
            TenantLevelColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
            if (groupSchema == null) {
                LOG.info("Not found colocate group {}, maybe delete", groupId);
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
                Database db = env.getInternalCatalog().getDbNullableByTable(tableId);
                if (db == null) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
                if (olapTable == null || !colocateIndex.isColocateMasterTable(olapTable.getId())) {
                    continue;
                }

                olapTable.readLock();
                try {
                    for (Partition partition : olapTable.getPartitions()) {
                        short replicationNum = replicaAlloc.getTotalReplicaNum();

                        // Here we only get VISIBLE indexes. All other indexes are not queryable.
                        // So it does not matter if tablets of other indexes are not matched.

                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            totalReplicaNumPerBucket++;
                            List<Long> tabletIdsInOrders = index.getTabletIdsInOrder();
                            for (int tabletOrderIdx = 0; tabletOrderIdx < tabletIdsInOrders.size(); tabletOrderIdx++) {
                                Long tabletId = tabletIdsInOrders.get(tabletOrderIdx);
                                int bucketIndex = tabletOrderIdx % backendBucketsSeq.size();
                                Set<Long> bucketsSeq = backendBucketsSeq.get(bucketIndex);
                                Preconditions.checkState(bucketsSeq.size() == replicationNum,
                                        bucketsSeq.size() + " vs. " + replicationNum);
                                Tablet tablet = index.getTablet(tabletId);
                                totalReplicaDataSizes.set(bucketIndex,
                                        totalReplicaDataSizes.get(bucketIndex)
                                                + tablet.getDataSize(true, false));
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("build group {} colocate statistic error", groupId, e);
                } finally {
                    olapTable.readUnlock();
                }
            }

            globalColocateStatistic.addGroup(groupId, replicaAlloc, backendBucketsSeq, totalReplicaDataSizes,
                    totalReplicaNumPerBucket);
        }

        return globalColocateStatistic;
    }

    private boolean relocateAndBalance(Long groupId, Tag tag, Set<Long> unavailableBeIds,
            List<Long> availableBeIds, TenantLevelColocateTableIndex colocateIndex, SystemInfoService infoService,
            LoadStatisticForTag statistic, GlobalColocateStatistic globalColocateStatistic,
            List<List<Long>> balancedBackendsPerBucketSeq, boolean balanceBetweenGroups) {
        TenantLevelColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
        if (groupSchema == null) {
            LOG.info("Not found colocate group {}, maybe delete", groupId);
            return false;
        }
        short replicaNum = groupSchema.getReplicaNum();
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList(
                colocateIndex.getBackendsPerBucketSeqByGroup(groupId));
        return super.relocateAndBalance(groupId, tag, unavailableBeIds,
                availableBeIds, replicaNum, backendsPerBucketSeq, infoService, statistic,
                globalColocateStatistic, balancedBackendsPerBucketSeq, balanceBetweenGroups);
    }


    private Set<Long> getUnavailableBeIdsInGroup(SystemInfoService infoService,
            TenantLevelColocateTableIndex colocateIndex, Long groupId, Tag tag) {
        Set<Long> backends = colocateIndex.getBackendsByGroup(groupId);
        return super.getUnavailableBeIdsInGroup(infoService, tag, backends);
    }
}
