// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.clone;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.DataProperty;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Database.DbState;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PartitionInfo;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.clone.CloneJob.JobPriority;
import com.baidu.palo.clone.CloneJob.JobState;
import com.baidu.palo.clone.CloneJob.JobType;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.common.util.Util;
import com.baidu.palo.persist.DatabaseInfo;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.task.AgentBatchTask;
import com.baidu.palo.task.AgentTaskExecutor;
import com.baidu.palo.task.CloneTask;
import com.baidu.palo.thrift.TBackend;
import com.baidu.palo.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CloneChecker for replica supplement, migration and deletion
 * 
 * replica supplement: select src tablets that have not enough online replicas
 * replica migration: select src tablets from high distribution or high capacity
 * backends dest backends: low distribution and low capacity backends replica
 * deletion: offline > clone > low version > high distribution backends
 */
public class CloneChecker extends Daemon {
    private static final Logger LOG = LogManager.getLogger(CloneChecker.class);
    private static final int CHECK_TABLE_TABLET_NUM_PER_MIGRATION_CYCLE = 5;

    private static CloneChecker INSTANCE = null;

    private CloneChecker(long intervalMs) {
        super("clone checker", intervalMs);
    }

    public static CloneChecker getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CloneChecker(Config.clone_checker_interval_second * 1000L);
        }
        return INSTANCE;
    }

    /**
     * check tablet for supplement when be reports bad replica
     */
    public boolean checkTabletForSupplement(long dbId, long tableId, long partitionId, long indexId, long tabletId) {
        // 1. check tablet has clone job
        Catalog catalog = Catalog.getInstance();
        SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();
        Set<Long> cloneTabletIds = catalog.getCloneInstance().getCloneTabletIds();
        if (cloneTabletIds.contains(tabletId)) {
            LOG.debug("tablet already have clone job. tablet id: {}", tabletId);
            return true;
        }

        Database db = catalog.getDb(dbId);
        if (db == null) {
            LOG.warn("db does not exist. id: {}", dbId);
            return false;
        }
        
        // 2. init backends and capacity info
        // beId -> beInfo
        Map<Long, BackendInfo> backendInfos = initBackendInfos(db.getClusterName());
        if (backendInfos == null || backendInfos.isEmpty()) {
            LOG.warn("init backend infos error");
            return false;
        }
        Map<CapacityLevel, Set<List<Long>>> capacityLevelToBackendIds = initBackendCapacityInfos(backendInfos);
        if (capacityLevelToBackendIds == null || capacityLevelToBackendIds.isEmpty()) {
            LOG.warn("init backend capacity infos error");
            return false;
        }

        // 3. init backends distribution info
        TabletInfo tabletInfo = null;
        db.readLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                LOG.warn("table does not exist. id: {}", tableId);
                return false;
            }

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                LOG.warn("partition does not exist. id: {}", partitionId);
                return false;
            }

            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                LOG.warn("index does not exist. id: {}", indexId);
                return false;
            }

            boolean foundTablet = false;
            for (Tablet tablet : index.getTablets()) {
                List<Replica> replicas = tablet.getReplicas();
                short onlineReplicaNum = 0;
                long tabletSizeB = 0L;
                Set<Long> backendIds = new HashSet<Long>();
                for (Replica replica : replicas) {
                    long backendId = replica.getBackendId();
                    Backend backend = clusterInfoService.getBackend(backendId);
                    if (backend == null) {
                        continue;
                    }
                    BackendInfo backendInfo = backendInfos.get(backendId);
                    if (backendInfo == null) {
                        continue;
                    }

                    backendIds.add(backendId);
                    if (replica.getDataSize() > tabletSizeB) {
                        tabletSizeB = replica.getDataSize();
                    }
                    if (backend.isAlive() && replica.getState() != ReplicaState.CLONE) {
                        // use isAlive instead of isAvailable.
                        // in Clone routine, we do not handle decommissioned
                        // situation
                        ++onlineReplicaNum;
                    }
                    backendInfo.setTableReplicaNum(backendInfo.getTableReplicaNum() + 1);
                }

                if (tablet.getId() == tabletId) {
                    foundTablet = true;
                    tabletInfo = new TabletInfo(dbId, tableId, partitionId, indexId, tabletId, replicationNum,
                            onlineReplicaNum, tabletSizeB, backendIds);
                }
            }
            if (!foundTablet) {
                LOG.warn("tablet does not exist. id: {}", tabletId);
                return false;
            }
        } finally {
            db.readUnlock();
        }
        Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds = initBackendDistributionInfos(backendInfos);
        if (distributionLevelToBackendIds == null || distributionLevelToBackendIds.isEmpty()) {
            LOG.warn("init backend distribution infos error");
            return false;
        }

        // 4. add clone job
        addCloneJob(tabletInfo, distributionLevelToBackendIds, capacityLevelToBackendIds, backendInfos,
                JobType.SUPPLEMENT);
        return true;
    }

    @Override
    protected void runOneCycle() {
        Clone clone = Catalog.getInstance().getCloneInstance();
        LOG.debug("start to check clone. job num: {}", clone.getJobNum());

        // 1. check tablet for supplement, migration and deletion
        checkTablets();

        // 2. check timeout
        clone.checkTimeout();

        // 3. run pending job
        List<CloneJob> jobs = clone.getCloneJobs(JobState.PENDING);
        for (CloneJob job : jobs) {
            // select src backends and submit clone task to backend
            runCloneJob(job);
        }

        // 4. remove cancelled and finished jobs
        clone.removeCloneJobs();
    }

    private void checkTablets() {
        Catalog catalog = Catalog.getInstance();
        SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();

        // 1. get all tablets which are in Clone process.
        // NOTICE: this is only a copy of tablet under Clone process.
        // It will change any time during this method.
        // So DO NOT severely depend on it make any decision!
        Set<Long> cloneTabletIds = catalog.getCloneInstance().getCloneTabletIds();

        // check tablet database by database.
        List<String> dbNames = catalog.getDbNames();
        for (String dbName : dbNames) {
            Database db = catalog.getDb(dbName);
            if (db == null) {
                LOG.warn("db does not exist. name: {}", dbName);
                continue;
            }

            final String clusterName = db.getClusterName();

            if (Strings.isNullOrEmpty(clusterName)) {
                LOG.error("database {} has no cluster name", dbName);
                continue;
            }

            // in Multi-Tenancy, we check tablet in all backends
            // but our dest will only select backend in cluster
            // init cluster's backends and capacity info

            // beId -> BackendInfo
            final Map<Long, BackendInfo> backendInfosInCluster = initBackendInfos(clusterName);
            if (backendInfosInCluster.isEmpty()) {
                LOG.warn("failed to init backend infos of cluster: {}", clusterName);
                continue;
            }

            final Map<CapacityLevel, Set<List<Long>>> clusterCapacityLevelToBackendIds =
                    initBackendCapacityInfos(backendInfosInCluster);
            if (clusterCapacityLevelToBackendIds == null || clusterCapacityLevelToBackendIds.isEmpty()) {
                LOG.warn("failed to init capacity level map of cluster: {}", clusterName);
                continue;
            }

            long dbId = db.getId();
            Set<String> tableNames = db.getTableNamesWithLock();
            boolean hasMigrations = false;
            // check table by table
            for (String tableName : tableNames) {
                long tableId = -1L;
                Multimap<Long, MaterializedIndex> partitionIdToIndices = LinkedHashMultimap.create();
                Map<Long, Short> partitionIdToReplicationNumMap = Maps.newHashMap();
                db.readLock();
                try {
                    // get all olap tables
                    Table table = db.getTable(tableName);
                    if (table == null || table.getType() != TableType.OLAP) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    tableId = table.getId();
                    for (Partition partition : olapTable.getPartitions()) {
                        long partitionId = partition.getId();
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                            // only check NORMAL index
                            if (materializedIndex.getState() != IndexState.NORMAL) {
                                continue;
                            }

                            partitionIdToIndices.put(partitionId, materializedIndex);
                        }

                        short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                        partitionIdToReplicationNumMap.put(partitionId, replicationNum);
                    }
                } finally {
                    db.readUnlock();
                }

                // check olap table
                for (Map.Entry<Long, MaterializedIndex> entry : partitionIdToIndices.entries()) {
                    long partitionId = entry.getKey();
                    MaterializedIndex index = entry.getValue();
                    // init backend table replica num to zero
                    initBackendTableReplicaNum(backendInfosInCluster);

                    // init table clone info
                    // backend id -> tablet info set, to gather statistics of tablet infos of each backends
                    Map<Long, Set<TabletInfo>> backendToTablets = Maps.newHashMap();
                    // tablet id -> tablet info, tablets which need to be cloned.
                    Map<Long, TabletInfo> cloneTabletMap = Maps.newHashMap();
                    // tablets which have redundant replicas.
                    Set<TabletInfo> deleteTabletSet = Sets.newHashSet();
                    db.readLock();
                    try {
                        long indexId = index.getId();
                        short replicationNum = partitionIdToReplicationNumMap.get(partitionId);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            List<Replica> replicas = tablet.getReplicas();
                            short onlineReplicaNum = 0;
                            short onlineReplicaNumInCluster = 0;
                            // choose the largest replica's size as this tablet's size
                            long tabletSizeB = 0L;

                            // save ids of all backends which the replicas of this tablet belongs to.
                            Set<Long> beIdsOfReplica = Sets.newHashSet();

                            for (Replica replica : replicas) {
                                long backendId = replica.getBackendId();
                                Backend backend = clusterInfoService.getBackend(backendId);
                                if (backend == null) {
                                    continue;
                                }

                                beIdsOfReplica.add(backendId);
                                if (replica.getDataSize() > tabletSizeB) {
                                    tabletSizeB = replica.getDataSize();
                                }

                                if (backend.isAlive() && replica.getState() != ReplicaState.CLONE) {
                                    ++onlineReplicaNum;
                                    // only if 
                                    if (backendInfosInCluster.containsKey(backendId)) {
                                        ++onlineReplicaNumInCluster;
                                    }
                                }
                            }

                            TabletInfo tabletInfo = new TabletInfo(dbId, tableId, partitionId, indexId, tabletId,
                                                                   replicationNum, onlineReplicaNum,
                                                                   tabletSizeB, beIdsOfReplica);
                            tabletInfo.setDbState(db.getDbState());
                            
                            // gather statistics of tablet infos of each backends
                            for (long backendId : beIdsOfReplica) {
                                Set<TabletInfo> tabletInfos = backendToTablets.get(backendId);
                                if (tabletInfos == null) {
                                    tabletInfos = new HashSet<TabletInfo>();
                                    backendToTablets.put(backendId, tabletInfos);
                                }
                                tabletInfos.add(tabletInfo);
                            }

                            // when we migration 
                            if (onlineReplicaNumInCluster < replicationNum) {
                                hasMigrations = true;
                            }

                            if (replicas.size() > replicationNum && onlineReplicaNum >= replicationNum) {
                                // in Multi-Tenancy, we will have priority to
                                // guarantee replica in cluster
                                if (onlineReplicaNumInCluster < replicationNum && !cloneTabletIds.contains(tabletId)) {
                                    cloneTabletMap.put(tabletId, tabletInfo);
                                } else {
                                    // need delete tablet
                                    deleteTabletSet.add(tabletInfo);
                                }
                            } else if (onlineReplicaNumInCluster < replicationNum
                                    && !cloneTabletIds.contains(tabletId)) {
                                cloneTabletMap.put(tabletId, tabletInfo);
                            }
                        }
                    } finally {
                        db.readUnlock();
                    }

                    // init backend tablet distribution
                    for (Map.Entry<Long, Set<TabletInfo>> mapEntry : backendToTablets.entrySet()) {
                        long backendId = mapEntry.getKey();
                        if (backendInfosInCluster.containsKey(backendId)) {
                            final BackendInfo backendInfo = backendInfosInCluster.get(backendId);
                            backendInfo.setTableReplicaNum(mapEntry.getValue().size());
                        }
                    }

                    // tablet distribution level
                    final Map<CapacityLevel, Set<List<Long>>> clusterDistributionLevelToBackendIds =
                            initBackendDistributionInfos(backendInfosInCluster);
                    if (clusterDistributionLevelToBackendIds != null
                            && !clusterDistributionLevelToBackendIds.isEmpty()) {
                        // supplement
                        checkSupplement(cloneTabletMap, clusterDistributionLevelToBackendIds,
                                        clusterCapacityLevelToBackendIds, backendInfosInCluster);
                        // migration
                        checkMigration(backendToTablets, clusterDistributionLevelToBackendIds,
                                       clusterCapacityLevelToBackendIds, backendInfosInCluster);
                    } else {
                        LOG.warn("init backend distribution infos error");
                    }

                    // tablet distribution level
                    final Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds =
                            initBackendDistributionInfos(backendInfosInCluster);
                    if (distributionLevelToBackendIds != null && !distributionLevelToBackendIds.isEmpty()) {
                        // delete redundant replicas
                        for (TabletInfo tabletInfo : deleteTabletSet) {
                            deleteRedundantReplicas(db, tabletInfo, distributionLevelToBackendIds);
                        }
                    } else {
                        LOG.warn("init backend distribution infos error");
                    }
                } // end for partition -> index
            } // end for tables

            // db migrations finish
            // when migrate finish ,next circle reset dbstate 
            if (!hasMigrations && db.getDbState() == DbState.MOVE) {
                db.setDbState(DbState.NORMAL);
                final DatabaseInfo info = new DatabaseInfo(db.getFullName(), db.getFullName(), db.getDataQuota());
                info.setDbState(DbState.NORMAL);
                info.setClusterName(db.getClusterName());
                Catalog.getInstance().getEditLog().logUpdateDatabase(info);
            }
        } // end for dbs
    }

    private Map<Long, BackendInfo> initBackendInfos(String clusterName) {
        Map<Long, BackendInfo> backendInfos = Maps.newHashMap();
        SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();
        List<Long> backendIds = null;
        // get all backends which belong to the cluster
        if (Strings.isNullOrEmpty(clusterName)) {
            backendIds = clusterInfoService.getBackendIds(true /* need alive */);
        } else {
            backendIds = clusterInfoService.getClusterBackendIds(clusterName, true /* need alive */);
        }

        for (long backendId : backendIds) {
            Backend backend = clusterInfoService.getBackend(backendId);
            if (backend == null || backend.isDecommissioned()) {
                continue;
            }
            BackendInfo beInfo = new BackendInfo(backend.getHost(), backendId, backend.getTotalCapacityB(), 
                                                 backend.getAvailableCapacityB());
            backendInfos.put(backendId, beInfo);
        }
        return backendInfos;
    }

    /**
     * maybe more than one backend in same host, so capacity of backend are saved into one
     * list with same host. it need to randomly select one backend in migration and supplement,
     * and select all in delete.
     * 
     * @param backendInfos
     * @return CapLvl to Set of BE List, each BE list represents all BE in one host
     */
    private Map<CapacityLevel, Set<List<Long>>> initBackendCapacityInfos(Map<Long, BackendInfo> backendInfos) {
        Preconditions.checkNotNull(backendInfos);
        if (backendInfos.size() == 0) {
            return null;
        }
        
        // hostname -> list of backendInfo
        Map<String, List<BackendInfo>> backendInfosMap = Maps.newHashMap();
        for (BackendInfo info : backendInfos.values()) {
            List<BackendInfo> infoList = backendInfosMap.get(info.getHost());
            if (infoList == null) {
                infoList = Lists.newArrayList();
                backendInfosMap.put(info.getHost(), infoList);
            }
            infoList.add(info);
        }

        // maybe backendInfosMap size == 1 when migrate db
        // calculate avg used ratio of the entire Palo system.
        long totalCapacityB = 0;
        long availableCapacityB = 0;
        for (BackendInfo backendInfo : backendInfos.values()) {
            totalCapacityB += backendInfo.getTotalCapacityB();
            availableCapacityB += backendInfo.getAvailableCapacityB();
        }
        if (totalCapacityB == 0L) {
            return null;
        }
        double avgUsedRatio = (double) (totalCapacityB - availableCapacityB) / totalCapacityB;
        double lowRatioThreshold = avgUsedRatio * (1 - Config.clone_capacity_balance_threshold);
        double highRatioThreshold = avgUsedRatio * (1 + Config.clone_capacity_balance_threshold);
        LOG.debug("capacity ratio. average used ratio: {}, low ratio threshold: {}, high ratio threshold: {}",
                  avgUsedRatio, lowRatioThreshold, highRatioThreshold);

        // CapacityLevel -> ids of BE in same host
        Map<CapacityLevel, Set<List<Long>>> capacityLevelToHostBackendIds = Maps.newHashMap();
        for (CapacityLevel level : CapacityLevel.values()) {
            capacityLevelToHostBackendIds.put(level, new HashSet<List<Long>>());
        }

        // handle backends host by host
        for (List<BackendInfo> infoList : backendInfosMap.values()) {
            long hostTotalCapacityB = 0;
            long hostAvailableCapacityB = 0;
            List<Long> beIds = Lists.newArrayList();
            for (BackendInfo backendInfo : infoList) {
                hostTotalCapacityB += backendInfo.getTotalCapacityB();
                hostAvailableCapacityB += backendInfo.getAvailableCapacityB();
                beIds.add(backendInfo.getBackendId());
            }
            double usedRatio = (double) (hostTotalCapacityB - hostAvailableCapacityB) / hostTotalCapacityB;
            Set<List<Long>> backendIds = null;
            if (usedRatio < lowRatioThreshold) {
                backendIds = capacityLevelToHostBackendIds.get(CapacityLevel.LOW);
            } else if (usedRatio < highRatioThreshold) {
                backendIds = capacityLevelToHostBackendIds.get(CapacityLevel.MID);
            } else {
                backendIds = capacityLevelToHostBackendIds.get(CapacityLevel.HIGH);
            }
            
            // add all backends in host to the set of the specified CapacityLevel
            backendIds.add(beIds);

            // *hostCloneCapacityB* is the capacity of a host that can be used to store cloned data.
            // which is [totalCapacity * avgUsedRatio * (1 + threshold) - usedCapacity]
            double hostCloneCapacityB = hostTotalCapacityB * highRatioThreshold
                    - (hostTotalCapacityB - hostAvailableCapacityB);

            // We calculate the capacity at host granularity, but use it at backend granularity.
            // Just for simplicity.
            for (BackendInfo backendInfo : infoList) {
                backendInfo.setCloneCapacityB((long) hostCloneCapacityB);
            }
        }

        LOG.info("capacity level map: {}", capacityLevelToHostBackendIds);
        return capacityLevelToHostBackendIds;
    }

    /**
     * maybe more than one backend in same host, so distribution of tablet are saved into one 
     * list with same host. it need to randomly select one backend in migration and supplement, 
     * and select all in delete.
     * 
     * @param backendInfos
     * @return
     */
    private Map<CapacityLevel, Set<List<Long>>> initBackendDistributionInfos(Map<Long, BackendInfo> backendInfos) {
        Preconditions.checkNotNull(backendInfos);
        if (backendInfos.size() == 0) {
            return null;
        }

        Map<String, List<BackendInfo>> backendInfosMap = Maps.newHashMap();
        for (BackendInfo info : backendInfos.values()) {
            if (backendInfosMap.containsKey(info.getHost())) {
                backendInfosMap.get(info.getHost()).add(info);
            } else {
                List<BackendInfo> list = Lists.newArrayList();
                list.add(info);
                backendInfosMap.put(info.getHost(), list);
            }
        }

        // maybe backendInfosMap size == 1 when migrate db
        // init distributionLevelToBackendIds
        Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds =
                new HashMap<CapacityLevel, Set<List<Long>>>();
        for (CapacityLevel level : CapacityLevel.values()) {
            distributionLevelToBackendIds.put(level, Sets.newHashSet());
        }

        int totalReplicaNum = 0;
        for (BackendInfo backendInfo : backendInfos.values()) {
            totalReplicaNum += backendInfo.getTableReplicaNum();
        }
        double avgReplicaNum = (double) totalReplicaNum / backendInfos.size();
        double lowReplicaNumThreshold = avgReplicaNum * (1 - Config.clone_distribution_balance_threshold);
        double highReplicaNumThreshold = avgReplicaNum * (1 + Config.clone_distribution_balance_threshold);

        for (List<BackendInfo> list : backendInfosMap.values()) {
            int backendReplicaNum = 0;
            List<Long> ids = Lists.newArrayList();
            for (BackendInfo backendInfo: list) {
                backendReplicaNum += backendInfo.getTableReplicaNum();
                ids.add(backendInfo.getBackendId());
            }
            Set<List<Long>> backendIds = null;
            if (backendReplicaNum < lowReplicaNumThreshold) {
                backendIds = distributionLevelToBackendIds.get(CapacityLevel.LOW);
            } else if (backendReplicaNum < highReplicaNumThreshold) {
                backendIds = distributionLevelToBackendIds.get(CapacityLevel.MID);
            } else {
                backendIds = distributionLevelToBackendIds.get(CapacityLevel.HIGH);
            }
            
            backendIds.add(ids);
            // set clone replica num
            int cloneReplicaNum = (int) highReplicaNumThreshold - backendReplicaNum;
            for (BackendInfo backendInfo: list) {
                backendInfo.setCloneReplicaNum(cloneReplicaNum);
            }

        }
        LOG.debug("backend distribution infos. level map: {}", distributionLevelToBackendIds);
        return distributionLevelToBackendIds;
    }

    private void initBackendTableReplicaNum(Map<Long, BackendInfo> backendInfos) {
        for (BackendInfo backendInfo : backendInfos.values()) {
            backendInfo.setTableReplicaNum(0);
        }
    }

    private long selectRandomBackendId(List<Long> candidateBackendIds, Set<String> excludeBackendHosts,
            Map<Long, BackendInfo> backendInfos) {
        Collections.shuffle(candidateBackendIds);
        for (long backendId : candidateBackendIds) {
            if (!excludeBackendHosts.contains(backendInfos.get(backendId).getHost())) {
                return backendId;
            }
        }
        return -1;
    }

    /**
     * 0. random select backend if priority is HIGH, recover tablet as soon as
     * possible 1. select from low distribution and capacity, and canCloneByBoth
     * 2. if supplement, select from 2.1 low distribution and capacity 2.2 low
     * distribution 2.3 all order by distribution
     */
    private long selectCloneReplicaBackendId(
            Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds,
            Map<CapacityLevel, Set<List<Long>>> capacityLevelToBackendIds, 
            Map<Long, BackendInfo> backendInfos, TabletInfo tabletInfo,
            JobType jobType, JobPriority priority) {
        Set<Long> existBackendIds = tabletInfo.getBackendIds();
        long tabletSizeB = tabletInfo.getTabletSizeB();
        long candidateBackendId = -1;
        BackendInfo candidateBackendInfo = null;
        // candidate backend from which step for debug
        String step = "-1";

        final SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        Set<String> existBackendHosts = Sets.newHashSet();
        for (Long id : existBackendIds) {
            existBackendHosts.add(infoService.getBackend(id).getHost());
        }
 
        if (priority == JobPriority.HIGH || priority == JobPriority.NORMAL) {
            // 1. HIGH priority
            List<Long> allBackendIds = Lists.newArrayList();
            for (Set<List<Long>> backendIds : distributionLevelToBackendIds.values()) {
                // select one backend from each host
                for (List<Long> list : backendIds) {
                    Collections.shuffle(list);
                    allBackendIds.add(list.get(0));
                }
            }

            candidateBackendId = selectRandomBackendId(allBackendIds, existBackendHosts, backendInfos);
            step = "0";
        } else {
            // candidate backendIds:
            // low distribution and low capacity backends
            Set<List<Long>> candidateBackendIdsByDistribution = distributionLevelToBackendIds.get(CapacityLevel.LOW);
            LOG.debug("candidate backends by distribution: {}", candidateBackendIdsByDistribution);
            Set<List<Long>> candidateBackendIdsByCapacity = capacityLevelToBackendIds.get(CapacityLevel.LOW);
            LOG.debug("candidate backends by capacity: {}", candidateBackendIdsByCapacity);

            // select dest backendId from candidates
            // 2. check canCloneByCapacity && canCloneByDistribution from
            // candidateBackendIds
            List<Long> candidateBackendIds = Lists.newArrayList();
            // select one backend from each host
            for (List<Long> list : candidateBackendIdsByDistribution) {
                Collections.shuffle(list);
                candidateBackendIds.add(list.get(0));
            }
            // here we choose candidate backends which are both in Low capacity and low distribution.
            Set<Long> candidateBackendIdsByCapacitySet = Sets.newHashSet();
            for (List<Long> list : candidateBackendIdsByCapacity) {
                candidateBackendIdsByCapacitySet.addAll(list);
            }
            candidateBackendIds.retainAll(candidateBackendIdsByCapacitySet);
            Collections.shuffle(candidateBackendIds);

            for (long backendId : candidateBackendIds) {
                if (existBackendHosts.contains(backendInfos.get(backendId).getHost())) {
                    continue;
                }
                candidateBackendInfo = backendInfos.get(backendId);
                if (candidateBackendInfo.canCloneByDistribution()
                        && candidateBackendInfo.canCloneByCapacity(tabletSizeB)) {
                    candidateBackendId = backendId;
                    step = "1";
                    break;
                }
            }

            // for SUPPLEMENT
            if (jobType == JobType.SUPPLEMENT) {
                // 3.1 random from candidateBackendIds
                if (candidateBackendId == -1 && !candidateBackendIds.isEmpty()) {
                    candidateBackendId = selectRandomBackendId(candidateBackendIds, existBackendHosts, backendInfos);
                    step = "2.1";
                }

                // 3.2 random from candidateBackendIdsByDistribution
                if (candidateBackendId == -1 && !candidateBackendIdsByDistribution.isEmpty()) {
                    for (List<Long> list : candidateBackendIdsByDistribution) {
                        // select one backend in same host
                        Collections.shuffle(list);
                        candidateBackendIds.add(list.get(0));
                    }
                    candidateBackendId = selectRandomBackendId(candidateBackendIds, existBackendHosts, backendInfos);
                    step = "2.2";
                }

                // 3.3 select order by distribution level
                if (candidateBackendId == -1) {
                    for (CapacityLevel level : CapacityLevel.values()) {
                        candidateBackendIdsByDistribution = distributionLevelToBackendIds.get(level);
                        if (candidateBackendIdsByDistribution.isEmpty()) {
                            continue;
                        }

                        for (List<Long> list : candidateBackendIdsByDistribution) {
                            // select one backend in same host
                            Collections.shuffle(list);
                            candidateBackendIds.add(list.get(0));
                        }
                        candidateBackendId = selectRandomBackendId(candidateBackendIds, 
                                         existBackendHosts, backendInfos);
                        if (candidateBackendId != -1) {
                            step = "2.3";
                            break;
                        }
                    }
                }
            }
        }

        LOG.debug("select backend for tablet: {}. type: {}, priority: {}, dest backend id: {}, step: {}", tabletInfo,
                jobType.name(), priority.name(), candidateBackendId, step);

        // decrease clone info
        if (candidateBackendId != -1) {
            candidateBackendInfo = backendInfos.get(candidateBackendId);
            candidateBackendInfo.decreaseCloneCapacityB(tabletSizeB);
            candidateBackendInfo.decreaseCloneReplicaNum();
        }
        return candidateBackendId;
    }

    /**
     * (1) offline > clone > low version > high distribution out of cluster (2) offline > clone > low version > high
     * distribution in cluster
     */
    private void deleteRedundantReplicas(Database db, TabletInfo tabletInfo,
            Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds) {
        long tableId = tabletInfo.getTableId();
        long partitionId = tabletInfo.getPartitionId();
        long indexId = tabletInfo.getIndexId();
        long tabletId = tabletInfo.getTabletId();
        final String clusterName = db.getClusterName();

        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                LOG.warn("table does not exist. id: {}", tableId);
                return;
            }

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                LOG.warn("partition does not exist. id: {}", partitionId);
                return;
            }

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                LOG.warn("index does not exist. id: {}", indexId);
                return;
            }

            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                LOG.warn("tablet does not exist. id: {}", tabletId);
                return;
            }

            List<Replica> replicas = tablet.getReplicas();
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            int realReplicaNum = 0;
            for (Replica replica : replicas) {
                if (replica.getState() != ReplicaState.CLONE) {
                    ++realReplicaNum;
                }
            }

            if (realReplicaNum <= replicationNum) {
                LOG.info("no redundant replicas in tablet[{}]", tabletId);
                return;
            }
            final Map<Long, BackendInfo> backendInfos = initBackendInfos(clusterName);
            // out of cluster and in cluster
            Collections.sort(replicas, new Comparator<Replica>() {
                public int compare(Replica arg0, Replica arg1) {
                    if (backendInfos.containsKey(arg0.getBackendId())) {
                        return 1;
                    }
                    return -1;
                }
            });

            long committedVersion = partition.getCommittedVersion();
            long committedVersionHash = partition.getCommittedVersionHash();
            int deleteNum = realReplicaNum - replicationNum;
            Replica deletedReplica = null;
            while (deleteNum > 0) {
                Iterator<Replica> replicaIterator = replicas.iterator();
                deletedReplica = null;
                while (replicaIterator.hasNext()) {
                    Replica replica = replicaIterator.next();
                    long backendId = replica.getBackendId();
                    // delete offline
                    if (!backendInfos.containsKey(replica.getBackendId())) {
                        replicaIterator.remove();
                        --deleteNum;
                        deletedReplica = replica;
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(db.getId(), tableId, partitionId,
                                indexId, tabletId, backendId);
                        Catalog.getInstance().getEditLog().logDeleteReplica(info);
                        LOG.info("delete replica [outOfCluster], backendId: {}, tablet info: {}, replica: {}",
                                backendId, tabletInfo, replica);
                        break;
                    }
                    
                    if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                        replicaIterator.remove();
                        --deleteNum;
                        deletedReplica = replica;

                        // Write edit log
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(db.getId(), tableId, partitionId,
                                indexId, tabletId, backendId);
                        Catalog.getInstance().getEditLog().logDeleteReplica(info);

                        LOG.info("delete replica [offline], backendId: {}, tablet info: {}, replica: {}", backendId,
                                tabletInfo, replica);
                        break;
                    }

                    // delete clone
                    if (replica.getState() == ReplicaState.CLONE) {
                        replicaIterator.remove();
                        --deleteNum;
                        deletedReplica = replica;

                        LOG.info("delete replica [clone], backendId: {}, tablet info: {}, replica: {}", backendId,
                                tabletInfo, replica);
                        break;
                    }

                    // delete low version
                    long replicaVersion = replica.getVersion();
                    long replicaVersionHash = replica.getVersionHash();
                    if ((replicaVersion == committedVersion && replicaVersionHash != committedVersionHash)
                            || replicaVersion < committedVersion) {
                        replicaIterator.remove();
                        --deleteNum;
                        deletedReplica = replica;

                        // Write edit log
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(db.getId(), tableId, partitionId,
                                indexId, tabletId, backendId);
                        Catalog.getInstance().getEditLog().logDeleteReplica(info);

                        LOG.info("delete replica [low version], backendId: {}, tablet info: {}, replica: {}", backendId,
                                tabletInfo, replica);
                        break;
                    }
                    
                    if (!backendInfos.containsKey(backendId)) {
                        break;
                    }
                    
                } // end for replicas

                if (deletedReplica != null) {
                    Catalog.getInstance().handleJobsWhenDeleteReplica(tableId, partitionId, indexId, tabletId,
                            deletedReplica.getId(), deletedReplica.getBackendId());
                    // delete from inverted index
                    Catalog.getCurrentInvertedIndex().deleteReplica(tabletId, deletedReplica.getBackendId());

                    continue;
                }

                // delete high distribution backend, order by level desc and
                // shuffle in level
                List<Long> backendIds = Lists.newArrayList();

                List<Long> levelBackendIds = Lists.newArrayList();
                // select from each distribution level
                for (Set<List<Long>> sets : distributionLevelToBackendIds.values()) {
                    // select one BE from each host
                    for (List<Long> list : sets) {
                        Collections.shuffle(list);
                        levelBackendIds.add(list.get(0));
                    }
                }
                Collections.shuffle(levelBackendIds);
                backendIds.addAll(levelBackendIds);

                for (long backendId : backendIds) {
                    Replica replica = tablet.getReplicaByBackendId(backendId);
                    if (tablet.deleteReplica(replica)) {
                        --deleteNum;
                        Catalog.getInstance().handleJobsWhenDeleteReplica(tableId, partitionId, indexId, tabletId,
                                replica.getId(), backendId);

                        // write edit log
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(db.getId(), tableId, partitionId,
                                indexId, tabletId, backendId);
                        Catalog.getInstance().getEditLog().logDeleteReplica(info);

                        LOG.info("delete replica [high distribution], backendId: {}, tablet info: {}", backendId,
                                tabletInfo);
                        break;
                    }
                }
            } // end for deleteNum
        } finally {
            db.writeUnlock();
        }
        
    }

    private void checkSupplement(Map<Long, TabletInfo> cloneTabletMap,
            Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds, 
            Map<CapacityLevel, Set<List<Long>>> capacityLevelToBackendIds,
            Map<Long, BackendInfo> backendInfos) {
        for (TabletInfo tabletInfo : cloneTabletMap.values()) {
            addCloneJob(tabletInfo, distributionLevelToBackendIds, capacityLevelToBackendIds, backendInfos,
                    JobType.SUPPLEMENT);
        }
    }

    private void checkMigration(Map<Long, Set<TabletInfo>> backendToTablets,
            Map<CapacityLevel, Set<List<Long>>> distributionLevelToBackendIds, Map<CapacityLevel, 
            Set<List<Long>>> capacityLevelToBackendIds,
            Map<Long, BackendInfo> backendInfos) {
        // select src tablet from high distribution or high capacity backends
        Set<List<Long>> highBackendIds = Sets.newHashSet();
        highBackendIds.addAll(distributionLevelToBackendIds.get(CapacityLevel.HIGH));
        highBackendIds.addAll(capacityLevelToBackendIds.get(CapacityLevel.HIGH));
        if (highBackendIds.isEmpty()) {
            LOG.debug("no high distribution or capacity backend tablets for migration");
            return;
        }

        Set<TabletInfo> candidateMigrationTablets = Sets.newHashSet();
        for (List<Long> backendIds : highBackendIds) {
            // select one backend in same host
            Collections.shuffle(backendIds);
            if (backendToTablets.containsKey(backendIds.get(0))) {
                candidateMigrationTablets.addAll(backendToTablets.get(backendIds.get(0)));
            }
        }
        if (candidateMigrationTablets.isEmpty()) {
            LOG.debug("no tablets for migration");
            return;
        }
        List<TabletInfo> migrationTablets = null;
        List<TabletInfo> candidateTablets = Lists.newArrayList(candidateMigrationTablets);
        if (candidateTablets.size() <= CHECK_TABLE_TABLET_NUM_PER_MIGRATION_CYCLE) {
            migrationTablets = candidateTablets;
        } else {
            migrationTablets = Util.sample(candidateTablets, CHECK_TABLE_TABLET_NUM_PER_MIGRATION_CYCLE);
            if (migrationTablets == null) {
                LOG.debug("no tablets for migration");
                return;
            }
        }

        // add clone job
        for (TabletInfo tabletInfo : migrationTablets) {
            addCloneJob(tabletInfo, distributionLevelToBackendIds, capacityLevelToBackendIds, backendInfos,
                    JobType.MIGRATION);
        }
    }

    private void addCloneJob(TabletInfo tabletInfo, Map<CapacityLevel,
                             Set<List<Long>>> distributionLevelToBackendIds,
                             Map<CapacityLevel, Set<List<Long>>> capacityLevelToBackendIds,
                             Map<Long, BackendInfo> backendInfos, JobType jobType) {
        // priority
        short onlineReplicaNum = tabletInfo.getOnlineReplicaNum();
        short replicationNum = tabletInfo.getReplicationNum();
        JobPriority priority;
        if (tabletInfo.getDbState() == DbState.MOVE) {
            priority = JobPriority.HIGH;
        } else {
            priority = Clone.calculatePriority(onlineReplicaNum, replicationNum);
        }
        LOG.debug("clone priority: {}, tablet: {}", priority.name(), tabletInfo);

        // select dest backend
        long cloneReplicaBackendId = selectCloneReplicaBackendId(distributionLevelToBackendIds,
                capacityLevelToBackendIds, backendInfos, tabletInfo, jobType, priority);
        if (cloneReplicaBackendId == -1) {
            LOG.debug("fail to select clone replica backend. tablet: {}", tabletInfo);
            return;
        }
        LOG.debug("select clone replica dest backend id[{}] for tablet[{}]", cloneReplicaBackendId, tabletInfo);

        // add new clone job
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.addCloneJob(tabletInfo.getDbId(), tabletInfo.getTableId(), tabletInfo.getPartitionId(),
                tabletInfo.getIndexId(), tabletInfo.getTabletId(), cloneReplicaBackendId, jobType, priority,
                Config.clone_job_timeout_second * 1000L);
    }

    private void runCloneJob(CloneJob job) {
        // check delay
        if (!checkPassDelayTime(job)) {
            return;
        }

        // check clone backend alive
        Catalog catalog = Catalog.getInstance();
        SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();
        Clone clone = catalog.getCloneInstance();
        Backend backend = clusterInfoService.getBackend(job.getDestBackendId());
        if (!clusterInfoService.checkBackendAvailable(job.getDestBackendId())) {
            clone.cancelCloneJob(job, "backend is not available. backend: " + backend);
            return;
        }

        // get db
        long dbId = job.getDbId();
        long tableId = job.getTableId();
        long partitionId = job.getPartitionId();
        long indexId = job.getIndexId();
        long tabletId = job.getTabletId();
        TStorageMedium storageMedium = TStorageMedium.HDD;
        Database db = catalog.getDb(dbId);
        if (db == null) {
            clone.cancelCloneJob(job, "db does not exist");
            return;
        }

        // check online replica num
        short replicationNum = 0;
        short onlineReplicaNum = 0;
        short onlineReplicaNumInCluster = 0;
        long committedVersion = -1L;
        long committedVersionHash = -1L;
        int schemaHash = 0;
        List<TBackend> srcBackends = new ArrayList<TBackend>();
        Tablet tablet = null;
        Replica cloneReplica = null;
        final Map<Long, BackendInfo> clusterBackendInfos = initBackendInfos(db.getClusterName());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            
            if (olapTable == null) {
                throw new MetaNotFoundException("table[" + tableId + "] does not exist");
            }

            if (job.getType() == JobType.MIGRATION && olapTable.getState() != OlapTableState.NORMAL) {
                throw new MetaNotFoundException(
                        "MIGRATION is not allowed when table[" + tableId + "]'s state is not NORMAL");
            }

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("partition[" + partitionId + "] does not exist");
            }

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
            storageMedium = dataProperty.getStorageMedium();

            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                throw new MetaNotFoundException("index[" + indexId + "] does not exist");
            }
            tablet = materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                throw new MetaNotFoundException("tablet[" + tabletId + "] does not exist");
            }

            replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            schemaHash = olapTable.getSchemaHashByIndexId(indexId);
            Preconditions.checkState(schemaHash != -1);

            List<Replica> sortedReplicas = new ArrayList<Replica>();
            for (Replica replica : tablet.getReplicas()) {
                backend = clusterInfoService.getBackend(replica.getBackendId());
                if (backend == null || !backend.isAlive()) {
                    continue;
                }

                ReplicaState replicaState = replica.getState();
                Preconditions.checkState(replicaState != ReplicaState.ROLLUP);
                // here we pass NORMAL / CLONE / SCHEMA_CHANGE
                // ATTN(cmy): if adding other state, update here

                if (replica.getBackendId() == job.getDestBackendId() && replicaState != ReplicaState.CLONE) {
                    String failMsg = "backend[" + replica.getBackendId() + "] already exists in tablet[" + tabletId
                            + "]. replica id: " + replica.getId() + ". state: " + replicaState;
                    clone.cancelCloneJob(job, failMsg);
                    return;
                }

                ++onlineReplicaNum;
                sortedReplicas.add(replica);
                if (clusterBackendInfos.containsKey(backend.getId())) {
                    onlineReplicaNumInCluster++;
                }
                if (replicaState == ReplicaState.CLONE) {
                    cloneReplica = replica;
                }
            }

            // check online replicas is enough
            if (onlineReplicaNum >= replicationNum && job.getType() == JobType.SUPPLEMENT 
                    && onlineReplicaNumInCluster >= replicationNum) {
                String failMsg = "online replicas is enough, online replica num: " + onlineReplicaNum;
                clone.cancelCloneJob(job, failMsg);
                return;
            }

            // sort replica by version desc
            committedVersion = partition.getCommittedVersion();
            committedVersionHash = partition.getCommittedVersionHash();
            Tablet.sortReplicaByVersionDesc(sortedReplicas);
            for (Replica replica : sortedReplicas) {
                backend = clusterInfoService.getBackend(replica.getBackendId());
                if (backend == null || !backend.isAlive()) {
                    continue;
                }

                // DO NOT choose replica with stale version or invalid version hash
                if (replica.getVersion() > committedVersion || (replica.getVersion() == committedVersion
                            && replica.getVersionHash() == committedVersionHash)) {
                    srcBackends.add(new TBackend(backend.getHost(), backend.getBePort(), backend.getHttpPort()));
                }
            }

            // no src backend found
            if (srcBackends.isEmpty()) {
                clone.cancelCloneJob(job, "no source backends");
                return;
            }

            if (cloneReplica != null) {
                tablet.deleteReplica(cloneReplica);
            }

            // add clone replica in meta
            long replicaId = catalog.getNextId();
            cloneReplica = new Replica(replicaId, job.getDestBackendId(), ReplicaState.CLONE);
            tablet.addReplica(cloneReplica);
        } catch (MetaNotFoundException e) {
            clone.cancelCloneJob(job, e.getMessage());
            return;
        } finally {
            db.writeUnlock();
        }

        // add clone task
        AgentBatchTask batchTask = new AgentBatchTask();
        CloneTask task = new CloneTask(job.getDestBackendId(), dbId, tableId, partitionId, indexId, tabletId,
                                       schemaHash, srcBackends, storageMedium, committedVersion, committedVersionHash);
        batchTask.addTask(task);
        if (clone.runCloneJob(job, task)) {
            AgentTaskExecutor.submit(batchTask);
            LOG.info("submit clone task success. job: {}", job);
        }
    }

    private boolean checkPassDelayTime(CloneJob job) {
        boolean isPassed = false;
        JobPriority priority = job.getPriority();
        long pendingTimeMs = System.currentTimeMillis() - job.getCreateTimeMs();
        switch (priority) {
            case HIGH:
                isPassed = pendingTimeMs > Config.clone_high_priority_delay_second * 1000L;
                break;
            case NORMAL:
                isPassed = pendingTimeMs > Config.clone_normal_priority_delay_second * 1000L;
                break;
            case LOW:
                isPassed = pendingTimeMs > Config.clone_low_priority_delay_second * 1000L;
                break;
            default:
                LOG.error("unknown priority: " + priority.name());
                isPassed = true;
                break;
        }
        return isPassed;
    }

    // capacity level or table tablet distribution level of backend
    public enum CapacityLevel {
        HIGH, MID, LOW
    }

    private class TabletInfo {
        private long dbId;
        private long tableId;
        private long partitionId;
        private long indexId;
        private long tabletId;
        private short replicationNum;
        private short onlineReplicaNum;
        private long tabletSizeB;
        private Set<Long> backendIds;
        private DbState dbState;

        public TabletInfo(long dbId, long tableId, long partitionId, long indexId, long tabletId, short replicationNum,
                short onlineReplicaNum, long tabletSizeB, Set<Long> backendIds) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.indexId = indexId;
            this.tabletId = tabletId;
            this.replicationNum = replicationNum;
            this.onlineReplicaNum = onlineReplicaNum;
            this.tabletSizeB = tabletSizeB;
            this.backendIds = backendIds;
            this.dbState = DbState.NORMAL;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
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

        public short getReplicationNum() {
            return replicationNum;
        }

        public short getOnlineReplicaNum() {
            return onlineReplicaNum;
        }

        public long getTabletSizeB() {
            return tabletSizeB;
        }

        public Set<Long> getBackendIds() {
            return backendIds;
        }

        @Override
        public String toString() {
            return "TabletInfo [dbId=" + dbId + ", tableId=" + tableId + ", partitionId=" + partitionId + ", indexId="
                    + indexId + ", tabletId=" + tabletId + ", replicationNum=" + replicationNum + ", onlineReplicaNum="
                    + onlineReplicaNum + ", tabletSizeB=" + tabletSizeB + ", backendIds=" + backendIds + "]";
        }

        public DbState getDbState() {
            return dbState;
        }

        public void setDbState(DbState dbState) {
            this.dbState = dbState;
        }
    }

    private class BackendInfo {
        private long backendId;
        private String host;
        
        private long totalCapacityB;
        private long availableCapacityB;
        // capacity for clone
        private long cloneCapacityB;

        private int tableReplicaNum;
        // replica num for clone
        private int cloneReplicaNum;

        public BackendInfo(String host, long backendId, long totalCapacityB, long availableCapacityB) {
            this.backendId = backendId;
            this.totalCapacityB = totalCapacityB;
            this.availableCapacityB = availableCapacityB;
            this.host = host;
            this.cloneCapacityB = 0L;
            this.tableReplicaNum = 0;
            this.cloneReplicaNum = 0;
        }

        public String getHost() {
            return host;
        }

        public long getBackendId() {
            return backendId;
        }

        public long getTotalCapacityB() {
            return totalCapacityB;
        }

        public long getAvailableCapacityB() {
            return availableCapacityB;
        }

        public void setCloneCapacityB(long cloneCapacityB) {
            this.cloneCapacityB = cloneCapacityB;
        }

        public boolean canCloneByCapacity(long tabletSizeB) {
            if (cloneCapacityB <= tabletSizeB) {
                return false;
            }
            return true;
        }

        public void decreaseCloneCapacityB(long tabletSizeB) {
            cloneCapacityB -= tabletSizeB;
        }

        public int getTableReplicaNum() {
            return tableReplicaNum;
        }

        public void setTableReplicaNum(int tableReplicaNum) {
            this.tableReplicaNum = tableReplicaNum;
        }

        public void setCloneReplicaNum(int cloneReplicaNum) {
            this.cloneReplicaNum = cloneReplicaNum;
        }

        public boolean canCloneByDistribution() {
            if (cloneReplicaNum <= 1) {
                return false;
            }
            return true;
        }

        public void decreaseCloneReplicaNum() {
            cloneReplicaNum -= 1;
        }
    }

}

