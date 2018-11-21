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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Database.DbState;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.clone.CloneJob.JobPriority;
import org.apache.doris.clone.CloneJob.JobState;
import org.apache.doris.clone.CloneJob.JobType;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.DatabaseInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TStorageMedium;
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
        LOG.info("start to check clone. job num: {}", clone.getJobNum());

        // yiguolei: check whether the replica's version is less than last failed version
        checkFailedReplicas();

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
    
    // check if a replica is failed during loading, add it as a clone job to catch up
    private void checkFailedReplicas() {
        Catalog catalog = Catalog.getInstance();
        SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();

        // 1. get all tablets which are in Clone process.
        // NOTICE: this is only a copy of tablet under Clone process.
        // It will change any time during this method.
        // So DO NOT severely depend on it to make any decision!
        Set<Long> cloneTabletIds = catalog.getCloneInstance().getCloneTabletIds();

        // check tablet database by database.
        List<String> dbNames = catalog.getDbNames();
        for (String dbName : dbNames) {
            Database db = catalog.getDb(dbName);
            if (db == null) {
                LOG.debug("db does not exist. name: {}", dbName);
                continue;
            }

            final String clusterName = db.getClusterName();

            if (Strings.isNullOrEmpty(clusterName)) {
                LOG.debug("database {} has no cluster name", dbName);
                continue;
            }

            long dbId = db.getId();
            Set<String> tableNames = db.getTableNamesWithLock();
            // check table by table
            for (String tableName : tableNames) {
                long tableId = -1L;
                db.readLock();
                try {
                    Table table = db.getTable(tableName);
                    if (table == null || table.getType() != TableType.OLAP) {
                        LOG.debug("table {} is null or is not olap table, skip repair process", table);
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    tableId = table.getId();
                    for (Partition partition : olapTable.getPartitions()) {
                        long partitionId = partition.getId();
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                            // only check NORMAL index
                            if (materializedIndex.getState() != IndexState.NORMAL) {
                                LOG.debug("index {} is not normal state, so that skip repair" 
                                        + " all tablets belongs this index", materializedIndex);
                                continue;
                            }
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                long tabletId = tablet.getId();
                                if (cloneTabletIds.contains(tabletId)) {
                                    LOG.debug("tablet {} is under clone, so that skip repair it", tablet);
                                    continue;
                                }
                                Replica replicaToCatchup = null;
                                for (Replica replica : tablet.getReplicas()) {
                                    long backendId = replica.getBackendId();
                                    Backend backend = clusterInfoService.getBackend(backendId);
                                    if (backend == null) {
                                        continue;
                                    }
                                    if (backend.isAlive() 
                                            && replica.getState() != ReplicaState.CLONE
                                            && replica.getLastFailedVersion() > 0) {

                                        long elapsedAfterFailed = System.currentTimeMillis() - replica.getLastFailedTimestamp();
                                        // if not check it, the replica may be failed at version 1,3,4,6,8, then we will run 5 clone jobs
                                        // wait some seconds then the replica maybe stable, and we could run single clone job to repair the 
                                        // replica
                                        if (elapsedAfterFailed < Config.replica_delay_recovery_second * 1000L) {
                                            LOG.info("{} is down at {}, less than minimal delay second {}, not clone", 
                                                     replica, replica.getLastFailedTimestamp(), Config.replica_delay_recovery_second);
                                            continue;
                                        }

                                        // check if there exists a replica in this tablet which have larger version
                                        // if not any replica in this tablet has larger version then not clone, ignore it
                                        boolean hasCloneSrcReplica = false;
                                        for (Replica srcReplica : tablet.getReplicas()) {
                                            // the src clone replica has to be normal
                                            if (srcReplica.getLastFailedVersion() > 0) {
                                                continue;
                                            }
                                            // the src clone replica's version >= current replica's version
                                            if (srcReplica.getVersion() > replica.getVersion() 
                                                    || srcReplica.getVersion() == replica.getVersion() 
                                                        && srcReplica.getVersionHash() != replica.getVersionHash()) {
                                                hasCloneSrcReplica = true;
                                                break;
                                            }
                                        }
                                        if (!hasCloneSrcReplica) {
                                            LOG.info("{} could not find clone src replica meets the " 
                                                    + "condition, ignore this replica", replica);
                                            continue;
                                        }
                                        if (replicaToCatchup == null) {
                                            replicaToCatchup = replica;
                                        } else if (replica.getLastSuccessVersion() > replica.getLastFailedVersion()) {
                                            // because there is only one catchup clone task for one tablet, so that we should 
                                            // select one replica to catch up according to this priority
                                            replicaToCatchup = replica;
                                            // its perfect to select this replica, no need to check others
                                            break;
                                        } else if (replicaToCatchup.getLastFailedVersion() > replica.getLastFailedVersion()) {
                                            // its better to select a low last failed version replica
                                            replicaToCatchup = replica;
                                        }
                                    } 
                                }
                                if (replicaToCatchup != null) {
                                    LOG.info("select replica [{}] to send clone task", replicaToCatchup);
                                    Clone clone = Catalog.getInstance().getCloneInstance();
                                    clone.addCloneJob(dbId, tableId, partitionId, materializedIndex.getId(), 
                                                      tabletId, replicaToCatchup.getBackendId(), 
                                                      JobType.CATCHUP, JobPriority.HIGH,
                                                      Config.clone_job_timeout_second * 1000L);
                                }
                            }

                        }
                    }
                } finally {
                    db.readUnlock();
                }
            } // end for tables
        } // end for dbs
    
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
                LOG.debug("db does not exist. name: {}", dbName);
                continue;
            }

            final String clusterName = db.getClusterName();

            if (Strings.isNullOrEmpty(clusterName)) {
                LOG.debug("database {} has no cluster name", dbName);
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
                                LOG.debug("partition [{}] index [{}] state is {}, not normal, skip check tablets", 
                                        partitionId, materializedIndex.getId(), materializedIndex.getState());
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
                            short healthyReplicaNum = 0;
                            short healthyReplicaNumInCluster = 0;
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
                                    // has to check replica's last failed version, because a tablet may contains
                                    // A,B,C,D 4 replica, A,B is normal, C,D is abnormal
                                    // but replica num = 3, then it may drop B, the cluster will comes into fatal error state
                                    ++onlineReplicaNum;
                                    if (backendInfosInCluster.containsKey(backendId)) {
                                        ++onlineReplicaNumInCluster;
                                    }
                                    if (replica.getLastFailedVersion() < 0) {
                                        ++ healthyReplicaNum;
                                        if (backendInfosInCluster.containsKey(backendId)) {
                                            ++ healthyReplicaNumInCluster;
                                        }
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
                                LOG.debug("partition {} index {} tablet {} online replica num is {} > replica num {}, " 
                                        + "should delete on replica", 
                                        partitionId, index.getId(), tableId, onlineReplicaNum, replicationNum);
                                // in Multi-Tenancy, we will have priority to
                                // guarantee replica in cluster
                                if (onlineReplicaNumInCluster < replicationNum && !cloneTabletIds.contains(tabletId)) {
                                    cloneTabletMap.put(tabletId, tabletInfo);
                                } else {
                                    if (healthyReplicaNum >= replicationNum && healthyReplicaNumInCluster >= replicationNum) {
                                        // need delete tablet
                                        LOG.debug("add tablet {} to delete list", tabletInfo);
                                        deleteTabletSet.add(tabletInfo);
                                    }
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
        // LOG.debug("capacity ratio. average used ratio: {}, low ratio threshold: {}, high ratio threshold: {}",
        // avgUsedRatio, lowRatioThreshold, highRatioThreshold);

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

        // LOG.info("capacity level map: {}", capacityLevelToHostBackendIds);
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
        // LOG.debug("backend distribution infos. level map: {}", distributionLevelToBackendIds);
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
            // LOG.debug("candidate backends by distribution: {}", candidateBackendIdsByDistribution);
            Set<List<Long>> candidateBackendIdsByCapacity = capacityLevelToBackendIds.get(CapacityLevel.LOW);
            // LOG.debug("candidate backends by capacity: {}", candidateBackendIdsByCapacity);

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

        // LOG.debug("select backend for tablet: {}. type: {}, priority: {}, dest backend id: {}, step: {}", tabletInfo,
        // jobType.name(), priority.name(), candidateBackendId, step);

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
                // also check if the replica is a health replica or we will drop a health replica 
                // and the remaining replica is not quorum
                if (replica.getState() != ReplicaState.CLONE 
                        && replica.getLastFailedVersion() < 0
                        && (replica.getVersion() == partition.getVisibleVersion() 
                            && replica.getVersionHash() == partition.getVisibleVersionHash() 
                            || replica.getVersion() > partition.getVisibleVersionHash())) {
                    ++realReplicaNum;
                }
            }

            // if health replica num less than required num, then skip
            // if health replica num == required num and == total num, then skip
            if (realReplicaNum <= replicationNum
                    || replicas.size() <= replicationNum) {
                LOG.info("no redundant replicas in tablet[{}]", tabletId);
                return;
            }
            final Map<Long, BackendInfo> backendInfos = initBackendInfos(clusterName);
            // out of cluster and in cluster
            // out cluster replica rise to the top
            Collections.sort(replicas, new Comparator<Replica>() {
                public int compare(Replica arg0, Replica arg1) {
                    if (backendInfos.containsKey(arg0.getBackendId())) {
                        return 1;
                    }
                    return -1;
                }
            });

            long committedVersion = partition.getVisibleVersion();
            long committedVersionHash = partition.getVisibleVersionHash();
            int deleteNum = replicas.size() - replicationNum;
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

                        // actually should write edit log when it is a catchup clone, but we could not distinguish them
                        // write edit for both
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(db.getId(), tableId, partitionId,
                                indexId, tabletId, backendId);
                        Catalog.getInstance().getEditLog().logDeleteReplica(info);
                        
                        LOG.info("delete replica [clone], backendId: {}, tablet info: {}, replica: {}", backendId,
                                tabletInfo, replica);
                        break;
                    }
                    
                    // delete unhealthy replica
                    if (replica.getLastFailedVersion() > 0) {
                        replicaIterator.remove();
                        --deleteNum;
                        deletedReplica = replica;

                        // actually should write edit log when it is a catchup clone, but we could not distinguish them
                        // write edit for both
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(db.getId(), tableId, partitionId,
                                indexId, tabletId, backendId);
                        Catalog.getInstance().getEditLog().logDeleteReplica(info);
                        
                        LOG.info("delete replica with last failed version > 0, backendId: {}, " 
                                + "tablet info: {}, replica: {}", backendId, tabletInfo, replica);
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
        // LOG.debug("clone priority: {}, tablet: {}", priority.name(), tabletInfo);

        // select dest backend
        long cloneReplicaBackendId = selectCloneReplicaBackendId(distributionLevelToBackendIds,
                capacityLevelToBackendIds, backendInfos, tabletInfo, jobType, priority);
        if (cloneReplicaBackendId == -1) {
            // LOG.debug("fail to select clone replica backend. tablet: {}", tabletInfo);
            return;
        }
        // LOG.debug("select clone replica dest backend id[{}] for tablet[{}]", cloneReplicaBackendId, tabletInfo);

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
        long visibleVersion = -1L;
        long visibleVersionHash = -1L;
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
                // if rollup starts then base replcia has errors, the rollup replcia is dropped, then base replica could run clone job
                // if schema change starts, replica state is schema change, will not run clone job
                Preconditions.checkState(replicaState != ReplicaState.ROLLUP);
                // yiguolei: schema change, clone, rollup could not run concurrently on a replica
                Preconditions.checkState(replicaState != ReplicaState.SCHEMA_CHANGE);
                // here we pass NORMAL / CLONE / SCHEMA_CHANGE
                // ATTN(cmy): if adding other state, update here
                if (job.getType() != JobType.CATCHUP) {
                    if (replica.getBackendId() == job.getDestBackendId() && replicaState != ReplicaState.CLONE) {
                        String failMsg = "backend[" + replica.getBackendId() + "] already exists in tablet[" + tabletId
                                + "]. replica id: " + replica.getId() + ". state: " + replicaState;
                        clone.cancelCloneJob(job, failMsg);
                        return;
                    }
                }

                ++onlineReplicaNum;
                sortedReplicas.add(replica);
                if (clusterBackendInfos.containsKey(backend.getId())) {
                    onlineReplicaNumInCluster++;
                }
                if (replica.getBackendId() == job.getDestBackendId()) {
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
            visibleVersion = partition.getVisibleVersion();
            visibleVersionHash = partition.getVisibleVersionHash();
            Tablet.sortReplicaByVersionDesc(sortedReplicas);
            for (Replica replica : sortedReplicas) {
                backend = clusterInfoService.getBackend(replica.getBackendId());
                if (backend == null || !backend.isAlive()) {
                    continue;
                }
                
                // this is an abnormal replica, skip it
                if (replica.getLastFailedVersion() > 0) {
                    LOG.debug("replica's last failed version > 0, ignore this replica [{}]", replica);
                    continue;
                }
                // DO NOT choose replica with stale version or invalid version hash
                if (job.getType() != JobType.CATCHUP) {
                    if (replica.getVersion() > visibleVersion || (replica.getVersion() == visibleVersion
                            && replica.getVersionHash() == visibleVersionHash)) {
                        srcBackends.add(new TBackend(backend.getHost(), backend.getBePort(), backend.getHttpPort()));
                    } else {
                        LOG.debug("replica [{}] the version not equal to large than visible version {}"
                                + " or commit version hash {}, ignore this replica", 
                                replica, visibleVersion, visibleVersionHash);
                    }
                } else {
                    // deal with this case
                    // A, B, C 3 replica, A,B verison is 10, C is done, its version is 5
                    // A, B is normal during load for version 11
                    // but B failed to publish and B is crashed, A is successful
                    // then C comes up, the partition's committed version is 10, then C try to clone 10, then clone finished
                    // but last failed version is 11, it is abnormal
                    // the publish will still fail
                    if (replica.getVersion() > visibleVersion 
                            || replica.getVersion() == visibleVersion 
                                && replica.getVersionHash() != visibleVersionHash) {
                        visibleVersion = replica.getVersion();
                        visibleVersionHash = replica.getVersionHash();
                    }
                    // if this is a catchup job, then should exclude the dest backend id from src backends
                    if (job.getDestBackendId() != backend.getId() 
                            && (replica.getVersion() > cloneReplica.getVersion()
                                    || replica.getVersion() == cloneReplica.getVersion()
                                        && replica.getVersionHash() != cloneReplica.getVersionHash())) {
                        srcBackends.add(new TBackend(backend.getHost(), backend.getBePort(), backend.getHttpPort()));
                    }
                }
            }

            // no src backend found
            if (srcBackends.isEmpty()) {
                clone.cancelCloneJob(job, "no source backends");
                return;
            }
            
            if (job.getType() != JobType.CATCHUP) {
                // yiguolei: in catch up clone, the clone replica is not null
                if (cloneReplica != null) {
                    tablet.deleteReplica(cloneReplica);
                    ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(dbId, tableId, partitionId,
                            indexId, tabletId, cloneReplica.getBackendId());
                    Catalog.getInstance().getEditLog().logDeleteReplica(info);
                    LOG.info("remove clone replica. tablet id: {}, backend id: {}", 
                            tabletId, cloneReplica.getBackendId());
                }
                // add clone replica in meta
                long replicaId = catalog.getNextId();
                // for a new replica to add to the tablet
                // first set its state to clone and set last failed version to the largest version in the partition
                // wait the catchup clone task to catch up.
                // but send the clone task to partition's commit version, although the clone task maybe success but the replica is abnormal
                // and another clone task will send to the replica to clone again
                // not find a more sufficient method
                cloneReplica = new Replica(replicaId, job.getDestBackendId(), -1, 0, 
                        -1, -1, ReplicaState.CLONE, partition.getCommittedVersion(), 
                        partition.getCommittedVersionHash(), -1, 0);
                tablet.addReplica(cloneReplica);
            }
            // set the replica's state to clone
            cloneReplica.setState(ReplicaState.CLONE);
        } catch (MetaNotFoundException e) {
            clone.cancelCloneJob(job, e.getMessage());
            return;
        } finally {
            db.writeUnlock();
        }

        // add clone task
        AgentBatchTask batchTask = new AgentBatchTask();
        // very important, it is partition's commit version here
        CloneTask task = new CloneTask(job.getDestBackendId(), dbId, tableId, partitionId, indexId, tabletId,
                                       schemaHash, srcBackends, storageMedium, visibleVersion, visibleVersionHash);
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

