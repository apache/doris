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


import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * when backend remove, down, and add, balance colocate tablets
 * some work delegate to {@link CloneChecker}
 * CloneChecker don't handle colocate tablets
 */
public class ColocateTableBalancer extends Daemon {
    private static final Logger LOG = LogManager.getLogger(ColocateTableBalancer.class);

    private ColocateTableBalancer(long intervalMs) {
        super("colocate group clone checker", intervalMs);
    }

    private static ColocateTableBalancer INSTANCE = null;

    public static ColocateTableBalancer getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ColocateTableBalancer(Config.clone_checker_interval_second * 1000L);
        }
        return INSTANCE;
    }

    @Override
    /**
     * firstly, try marking balancing group to stable if the balance has finished
     * secondly, try deleting redundant replicas for colocate group balance finished just now
     *          we delay to delete redundant replicas until all clone job is done.
     * thirdly, try balancing colocate group if we found backend removed, down or added.
     */
    protected void runOneCycle() {
        tryMarkBalancingGroupStable();

        tryDeleteRedundantReplicas();

        tryBalanceWhenBackendChange();
    }

    /**
     * check all balancing colocate group tables
     * if all tables in a colocate group are stable, mark the colocate group stable
     */
    private synchronized void tryMarkBalancingGroupStable() {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        Catalog catalog = Catalog.getInstance();

        Set<Long> allGroups = colocateIndex.getAllGroupIds();
        for (Long group : allGroups) {
            LOG.info("colocate group: {} backendsPerBucketSeq is {}", group, colocateIndex.getBackendsPerBucketSeq(group));
        }

        Set<Long> balancingGroupIds = colocateIndex.getBalancingGroupIds();
        if (balancingGroupIds.size() == 0) {
            LOG.info("All colocate groups are stable. Skip");
            return;
        }

        for (Long groupId : balancingGroupIds) {
            Database db = catalog.getDb(colocateIndex.getDB(groupId));

            boolean isBalancing = false;
            List<Long> allTableIds = colocateIndex.getAllTableIds(groupId);
            for (long tableId : allTableIds) {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (checkTableBalancing(db, olapTable, colocateIndex.getBackendsPerBucketSeq(groupId))) {
                    isBalancing = true;
                    break;
                }
            }

            if (!isBalancing) {
                colocateIndex.markGroupStable(groupId);
                ColocatePersistInfo info = ColocatePersistInfo.CreateForMarkStable(groupId);
                Catalog.getInstance().getEditLog().logColocateMarkStable(info);
                LOG.info("colocate group : {} become stable!", db.getTable(groupId).getName());
            }
        }
    }

    /**
     * 1 check the colocate table whether balancing
     *      A colocate table is stable means:
     *      a: all replica state is not clone
     *      b: the tablet backendIds are consistent with ColocateTableIndex's backendsPerBucketSeq
     *
     * 2 if colocate table is balancing , we will try adding a clone job
     *      handle the FE restart when colocate groups balancing case:
     *      After FE restart, the clone job meta will lose
     */
    private boolean checkTableBalancing(Database db, OlapTable olapTable, List<List<Long>> backendsPerBucketSeq) {
        boolean isBalancing = false;
        out: for (Partition partition : olapTable.getPartitions()) {
            short replicateNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                List<Tablet> tablets = index.getTablets();
                for (int i = 0; i < tablets.size(); i++) {
                    Tablet tablet = tablets.get(i);
                    //1 check all replica state is not clone
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.getState().equals(Replica.ReplicaState.CLONE)) {
                            isBalancing = true;
                            LOG.info("colocate group : {} is still balancing, there is clone Replica", olapTable.getColocateTable());
                            break out;
                        }
                    }

                    List<Long> groupBackends = new ArrayList<>(backendsPerBucketSeq.get(i));
                    Set<Long> tabletBackends = tablet.getBackendIds();
                    //2 check the tablet backendIds are consistent with ColocateTableIndex's backendsPerBucketSeq
                    if (!tabletBackends.containsAll(groupBackends)) {
                        isBalancing = true;
                        LOG.info("colocate group : {} is still balancing, may be clone job hasn't run, try adding a clone job", olapTable.getColocateTable());

                        //try adding a clone job
                        //clone.addCloneJob has duplicated check, so there isn't side-effect
                        List<Long> clusterAliveBackendIds = getAliveClusterBackendIds(db.getClusterName());
                        groupBackends.removeAll(tabletBackends);

                        //for backend added;
                        if (clusterAliveBackendIds.containsAll(tabletBackends)) {
                            //we can ignore tabletSizeB parameter here
                            CloneTabletInfo tabletInfo = new CloneTabletInfo(db.getId(), olapTable.getId(), partition.getId(),
                                    index.getId(), tablet.getId(), replicateNum, replicateNum, 0, tabletBackends);

                            for (Long cloneBackend : groupBackends) {
                                AddMigrationJob(tabletInfo, cloneBackend);
                            }
                        } else { //for backend down or removed
                            short onlineReplicaNum = (short) (replicateNum - groupBackends.size());
                            CloneTabletInfo tabletInfo = new CloneTabletInfo(db.getId(), olapTable.getId(), partition.getId(),
                                    index.getId(), tablet.getId(), replicateNum, onlineReplicaNum, 0, tabletBackends);

                            for (Long cloneBackend : groupBackends) {
                                AddSupplementJob(tabletInfo, cloneBackend);
                            }
                        }
                    }
                } //end tablet
            } //end index
        } //end partition
        return isBalancing;
    }

    /**
     * firstly, check backend removed or down
     * secondly, check backend added
     */
    private synchronized void tryBalanceWhenBackendChange() {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        Catalog catalog = Catalog.getInstance();

        Set<Long> allGroupIds = colocateIndex.getAllGroupIds();
        for (Long groupId : allGroupIds) {
            Database db = catalog.getDb(colocateIndex.getDB(groupId));
            List<Long> clusterAliveBackendIds = getAliveClusterBackendIds(db.getClusterName());
            Set<Long> allGroupBackendIds = colocateIndex.getBackendsByGroup(groupId);
            List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);

            //1 check backend removed or down
            if (!clusterAliveBackendIds.containsAll(allGroupBackendIds)) {
                Set<Long> removedBackendIds = Sets.newHashSet(allGroupBackendIds);
                removedBackendIds.removeAll(clusterAliveBackendIds);

                //A backend in Colocate group but not alive, which means the backend is removed or down
                Iterator removedBackendIdsIterator = removedBackendIds.iterator();
                while (removedBackendIdsIterator.hasNext()) {
                    Long removedBackendId = (Long) removedBackendIdsIterator.next();
                    Backend removedBackend = Catalog.getCurrentSystemInfo().getBackend(removedBackendId);
                    if (removedBackend != null && !removedBackend.isDecommissioned() && System.currentTimeMillis() - removedBackend.getLastUpdateMs() < Config.max_backend_down_time_second * 1000) {
                        LOG.info("backend[{}-{}] is down for a short time. ignore", removedBackend, removedBackend.getHost());
                        removedBackendIdsIterator.remove();
                    }
                }

                if (!removedBackendIds.isEmpty()) {
                    LOG.info("removedBackendIds {} for colocate group {}", removedBackendIds, groupId);
                    //multiple backend removed is unusual, so we handle one by one
                    for (Long backendId : removedBackendIds) {
                        balanceForBackendRemoved(db, groupId, backendId);
                    }
                    continue; //for one colocate group, only handle backend removed or added event once
                }
            }

            //2 check backend added
            int replicateNum = backendsPerBucketSeq.get(0).size();
            if (backendsPerBucketSeq.size() * replicateNum <= allGroupBackendIds.size()) {
                //if each tablet replica has a different backend, which means the colocate group
                //has fully balanced. we can ignore the new backend added.
                LOG.info("colocate group {} has already fully balanced. skip", groupId);
                continue;
            }

            if (clusterAliveBackendIds.size() > allGroupBackendIds.size()) {
                clusterAliveBackendIds.removeAll(allGroupBackendIds);

                if (!clusterAliveBackendIds.isEmpty()) {
                    LOG.info("new backends for colocate group {} are {}", groupId, clusterAliveBackendIds);
                    balanceForBackendAdded(groupId, db, clusterAliveBackendIds);
                }
            }
        }
    }

    //get the backends: 1 belong to this cluster; 2 alive; 3 not decommissioned
    private List<Long> getAliveClusterBackendIds(String clusterName) {
        SystemInfoService systemInfo = Catalog.getCurrentSystemInfo();
        List<Long> clusterBackendIds = systemInfo.getClusterBackendIds(clusterName, true);
        List<Long> decommissionedBackendIds = systemInfo.getDecommissionedBackendIds();
        clusterBackendIds.removeAll(decommissionedBackendIds);
        return clusterBackendIds;
    }

    private synchronized void tryDeleteRedundantReplicas() {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        Catalog catalog = Catalog.getInstance();

        Set<Long> allGroupIds = colocateIndex.getAllGroupIds();
        for (Long groupId : allGroupIds) {
            Set<Long> balancingGroups = colocateIndex.getBalancingGroupIds();
            //only delete reduntdant replica when group is stable
            if (!balancingGroups.contains(groupId)) {
                Database db = catalog.getDb(colocateIndex.getDB(groupId));
                List<Long> allTableIds = colocateIndex.getAllTableIds(groupId);
                Set<CloneTabletInfo> deleteTabletSet = Sets.newHashSet();//keep tablets which have redundant replicas.
                int replicateNum = -1;
                for (long tableId : allTableIds) {
                    db.readLock();
                    try {
                        OlapTable olapTable = (OlapTable) db.getTable(tableId);
                        for (Partition partition : olapTable.getPartitions()) {
                            replicateNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                                // only check NORMAL index
                                if (index.getState() != MaterializedIndex.IndexState.NORMAL) {
                                    continue;
                                }
                                for (Tablet tablet : index.getTablets()) {
                                    List<Replica> replicas = tablet.getReplicas();
                                    if (replicas.size() > replicateNum) {
                                        CloneTabletInfo tableInfo = new CloneTabletInfo(db.getId(), tableId, partition.getId(), index.getId(), tablet.getId(), (short) replicateNum, (short) replicas.size(), 0, new HashSet<>());
                                        deleteTabletSet.add(tableInfo);
                                    }
                                }
                            }
                        }
                    } finally {
                        db.readUnlock();
                    }
                }

                if (deleteTabletSet.size() > 0) {
                    LOG.info("colocate group {} will delete tablet {}", groupId, deleteTabletSet);
                    //delete tablet will affect colocate table local query schedule,
                    //so make colocate group balancing again
                    colocateIndex.markGroupBalancing(groupId);
                    ColocatePersistInfo info = ColocatePersistInfo.CreateForMarkBalancing(groupId);
                    Catalog.getInstance().getEditLog().logColocateMarkBalancing(info);
                    for (CloneTabletInfo tabletInfo : deleteTabletSet) {
                        deleteRedundantReplicas(db, tabletInfo);
                    }
                }

            }
        }
    }

    private void deleteRedundantReplicas(Database db, CloneTabletInfo tabletInfo) {
        long tableId = tabletInfo.getTableId();
        long partitionId = tabletInfo.getPartitionId();
        long indexId = tabletInfo.getIndexId();
        long tabletId = tabletInfo.getTabletId();

        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex index = partition.getIndex(indexId);
            Tablet tablet = index.getTablet(tabletId);
            List<Replica> replicas = tablet.getReplicas();

            //delete replica for backend removed
            List<Replica> copyReplicas = new ArrayList<>(replicas);
            for (Replica replica : copyReplicas) {
                long backendId = replica.getBackendId();
                if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                    deleteReplica(tablet, replica, db.getId(), tableId, partitionId, indexId);
                }
            }

            //delete replica for backend added
            List<Replica> updatedReplicas = tablet.getReplicas();
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            if (updatedReplicas.size() <= replicationNum) {
                return;
            }

            int deleteNum = updatedReplicas.size() - replicationNum;
            List<Long> sortedReplicaIds = sortReplicaId(updatedReplicas);

            //always delete replica which id is minimum
            for (int i = 0; i < deleteNum; i++) {
                Replica deleteReplica = tablet.getReplicaById(sortedReplicaIds.get(i));
                deleteReplica(tablet, deleteReplica, db.getId(), tableId, partitionId, indexId);
            }

        } finally {
            db.writeUnlock();
        }
    }

    private List<Long> sortReplicaId(List<Replica> replicas) {
        List<Long> replicaIds = new ArrayList<>();
        for (Replica replica : replicas) {
            replicaIds.add(replica.getId());
        }
        Collections.sort(replicaIds);
        return replicaIds;
    }


    private void deleteReplica(Tablet tablet, Replica deleteReplica, long dbId, long tableId, long partitionId, long indexId) {
        if (tablet.deleteReplica(deleteReplica)) {
            ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(dbId, tableId, partitionId, indexId, tablet.getId(), deleteReplica.getBackendId());
            Catalog.getInstance().getEditLog().logDeleteReplica(info);

            Catalog.getInstance().handleJobsWhenDeleteReplica(tableId, partitionId, indexId, tablet.getId(), deleteReplica.getId(), deleteReplica.getBackendId());
            LOG.info("delete replica {} for tablet: {}", deleteReplica, tablet.getId());
        }
    }

    /**
     * 1 compute need delete BucketSeqs in the removedBackend
     * 2 select clone replica BackendId for the new Replica
     * 3 mark colocate group balancing
     * 4 add a Supplement Job
     * 5 update the TableColocateIndex backendsPerBucketSeq metadata
     */
    private void balanceForBackendRemoved(Database db, Long groupId, Long removedBackendId) {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        com.google.common.collect.Table<Long, Integer, Long> newGroup2BackendsPerBucketSeq = HashBasedTable.create();

        List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
        List<Integer> needDeleteBucketSeqs = new ArrayList<>();

        int size = backendsPerBucketSeq.size();
        for (int i = 0; i < size; i++) {
            List<Long> backends = backendsPerBucketSeq.get(i);
            if (backends.contains(removedBackendId)) {
                needDeleteBucketSeqs.add(i);
            }
        }

        List<List<Long>> newBackendsPerBucketSeq = deepCopy(backendsPerBucketSeq);

        db.readLock();
        try {
            List<Long> allTableIds = colocateIndex.getAllTableIds(groupId);
            for (long tableId : allTableIds) {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                for (Partition partition : olapTable.getPartitions()) {
                    int replicateNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        List<Tablet> tablets = index.getTablets();
                        for (int i : needDeleteBucketSeqs) {
                            Tablet tablet = tablets.get(i);
                            Replica deleteReplica = null;
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getBackendId() == removedBackendId) {
                                    deleteReplica = replica;
                                }
                            }

                            long tabletSizeB = deleteReplica.getDataSize() * partition.getMaterializedIndices().size() * olapTable.getPartitions().size() * allTableIds.size();
                            CloneTabletInfo tabletInfo = new CloneTabletInfo(db.getId(), tableId, partition.getId(),
                            index.getId(), tablet.getId(), (short) replicateNum, (short) (replicateNum - 1),
                            tabletSizeB, tablet.getBackendIds());

                            long cloneReplicaBackendId = selectCloneBackendIdForRemove(newGroup2BackendsPerBucketSeq, groupId, i, db.getClusterName(), tabletInfo);

                            if (cloneReplicaBackendId != -1L) {
                                if (!colocateIndex.isGroupBalancing(groupId)) {
                                    colocateIndex.markGroupBalancing(groupId);
                                    ColocatePersistInfo info = ColocatePersistInfo.CreateForMarkBalancing(groupId);
                                    Catalog.getInstance().getEditLog().logColocateMarkBalancing(info);
                                }

                                //update TableColocateIndex groupBucket2BEs
                                List<Long> backends = newBackendsPerBucketSeq.get(i);
                                backends.remove(removedBackendId);
                                if (!backends.contains(cloneReplicaBackendId)) {
                                    backends.add(cloneReplicaBackendId);
                                }

                                AddSupplementJob(tabletInfo, cloneReplicaBackendId);
                            }
                        }
                    }
                }
            }
            colocateIndex.addBackendsPerBucketSeq(groupId, newBackendsPerBucketSeq);
            ColocatePersistInfo info = ColocatePersistInfo.CreateForBackendsPerBucketSeq(groupId, newBackendsPerBucketSeq);
            Catalog.getInstance().getEditLog().logColocateBackendsPerBucketSeq(info);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            db.readUnlock();
        }
    }

    private List<List<Long>> deepCopy(List<List<Long>> backendsPerBucketSeq) {
        List<List<Long>> newBackendsPerBucketSeq = new ArrayList<>();
        for (List<Long> backends : backendsPerBucketSeq) {
            newBackendsPerBucketSeq.add(new ArrayList<>(backends));
        }
        return newBackendsPerBucketSeq;
    }

    //this logic is like CloneChecker.checkTabletForSupplement and CloneChecker.addCloneJob
    private Long selectCloneBackendIdForRemove(com.google.common.collect.Table<Long, Integer, Long> newGroup2BackendsPerBucketSeq, long group, int bucketSeq, String clusterName, CloneTabletInfo tabletInfo) {
        Long cloneReplicaBackendId = null;
        cloneReplicaBackendId = newGroup2BackendsPerBucketSeq.get(group, bucketSeq);
        if (cloneReplicaBackendId == null) {
            // beId -> BackendInfo
            final Map<Long, BackendInfo> backendInfosInCluster = CloneChecker.getInstance().initBackendInfos(clusterName);
            if (backendInfosInCluster.isEmpty()) {
                LOG.warn("failed to init backend infos of cluster: {}", clusterName);
                return -1L;
            }

            // tablet distribution level
            final Map<CloneChecker.CapacityLevel, Set<List<Long>>> clusterDistributionLevelToBackendIds = CloneChecker.getInstance().initBackendDistributionInfos(backendInfosInCluster);

            final Map<CloneChecker.CapacityLevel, Set<List<Long>>> clusterCapacityLevelToBackendIds = CloneChecker.getInstance().initBackendCapacityInfos(backendInfosInCluster);
            if (clusterCapacityLevelToBackendIds == null || clusterCapacityLevelToBackendIds.isEmpty()) {
                LOG.warn("failed to init capacity level map of cluster: {}", clusterName);
                return -1L;
            }

            // select dest backend
            cloneReplicaBackendId = CloneChecker.getInstance().selectCloneReplicaBackendId(
                    clusterDistributionLevelToBackendIds, clusterCapacityLevelToBackendIds,
                    backendInfosInCluster, tabletInfo, CloneJob.JobType.SUPPLEMENT, CloneJob.JobPriority.HIGH);

            if (cloneReplicaBackendId == -1) {
                LOG.warn("fail to select clone replica backend. tablet: {}", tabletInfo);
                return -1L;
            }

            newGroup2BackendsPerBucketSeq.put(group, bucketSeq, cloneReplicaBackendId);
        }

        LOG.info("select clone replica dest backend id: {} for groop: {} TabletInfo: {}", cloneReplicaBackendId, group, tabletInfo);
        return cloneReplicaBackendId;
    }

    /**
     * balance after new backend added
     *
     * 1 compute the the number of bucket seqs need to move from the each old backend and
     *   the number of bucket seqs need to move to the each new backend
     * 2 select the clone target Backend for the new Replica
     * 3 mark colocate group balancing
     * 4 add a Migration Job
     * 5 update the ColocateTableIndex's backendsPerBucketSeq
     *
     * For example:
     * There are 3 backend and 4 tablet, and replicateNum is 3.
     *
     * the mapping from tablet to backend to is following:
     *
     * tablet1 : [1, 2, 3]
     * tablet2 : [2, 1, 3]
     * tablet3 : [3, 2, 1]
     * tablet4 : [1, 2, 3]
     *
     * After Adding a new backend:
     *
     * the needMoveBucketSeqs  = 4 * 3 / (3 + 1) = 3
     * the bucketSeqsPerNewBackend = 3 / 1 = 1
     *
     * After balancing, the mapping from tablet to backend to is following:
     *
     * tablet1 : [4, 2, 3]
     * tablet2 : [4, 1, 3]
     * tablet3 : [4, 2, 1]
     * tablet4 : [1, 2, 3]
     *
     */
    private void balanceForBackendAdded(Long groupId, Database db, List<Long> addedBackendIds) {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        com.google.common.collect.Table<Long, Integer, Long> newGroup2BackendsPerBucketSeq = HashBasedTable.create();

        List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
        int replicateNum = backendsPerBucketSeq.get(0).size();
        Set<Long> allGroupBackendIds = colocateIndex.getBackendsByGroup(groupId);

        List<List<Long>> newBackendsPerBucketSeq = deepCopy(backendsPerBucketSeq);

        int needMoveBucketSeqs = backendsPerBucketSeq.size() * replicateNum / (allGroupBackendIds.size() + addedBackendIds.size());
        int bucketSeqsPerNewBackend = needMoveBucketSeqs / addedBackendIds.size();
        LOG.info("for colocate group {}, needMoveBucketSeqs : {} , bucketSeqPerNewBackend: {}", groupId, needMoveBucketSeqs, bucketSeqsPerNewBackend);

        db.readLock();
        try {
            List<Long> allTableIds = colocateIndex.getAllTableIds(groupId);
            for (long tableId : allTableIds) {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                for (Partition partition : olapTable.getPartitions()) {
                    replicateNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        List<Tablet> tablets = index.getTablets();
                        for (int i = 0; i < tablets.size() && i < needMoveBucketSeqs; i++) {
                            Tablet tablet = tablets.get(i);
                            List<Replica> replicas = tablet.getReplicas();
                            List<Long> sortedReplicaIds = sortReplicaId(replicas);
                            //always delete replica which id is minimum
                            Replica deleteReplica = tablet.getReplicaById(sortedReplicaIds.get(0));

                            long tabletSizeB = deleteReplica.getDataSize() * partition.getMaterializedIndices().size()
                             * olapTable.getPartitions().size() * allTableIds.size();
                            CloneTabletInfo tabletInfo = new CloneTabletInfo(db.getId(), tableId, partition.getId(),
                            index.getId(), tablet.getId(), (short) replicateNum, (short) replicateNum,
                            tabletSizeB, tablet.getBackendIds());

                            Long cloneReplicaBackendId = newGroup2BackendsPerBucketSeq.get(groupId, i);
                            if (cloneReplicaBackendId == null) {
                                cloneReplicaBackendId = addedBackendIds.get(i / bucketSeqsPerNewBackend);
                                newGroup2BackendsPerBucketSeq.put(groupId, i, cloneReplicaBackendId);
                            }

                            if (!colocateIndex.isGroupBalancing(groupId)) {
                                colocateIndex.markGroupBalancing(groupId);
                                ColocatePersistInfo info = ColocatePersistInfo.CreateForMarkBalancing(groupId);
                                Catalog.getInstance().getEditLog().logColocateMarkBalancing(info);
                            }

                            //update ColocateTableIndex backendsPerBucketSeq
                            List<Long> backends = newBackendsPerBucketSeq.get(i);
                            backends.remove(deleteReplica.getBackendId());
                            if (!backends.contains(cloneReplicaBackendId)) {
                                backends.add(cloneReplicaBackendId);
                            }

                            AddMigrationJob(tabletInfo, cloneReplicaBackendId);
                        }
                    }
                }
            }
            colocateIndex.addBackendsPerBucketSeq(groupId, newBackendsPerBucketSeq);
            ColocatePersistInfo info = ColocatePersistInfo.CreateForBackendsPerBucketSeq(groupId, newBackendsPerBucketSeq);
            Catalog.getInstance().getEditLog().logColocateBackendsPerBucketSeq(info);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            db.readUnlock();
        }
    }

    //for backend down or removed
    private void AddSupplementJob(CloneTabletInfo tabletInfo, long cloneBackendId) {
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.addCloneJob(tabletInfo.getDbId(), tabletInfo.getTableId(), tabletInfo.getPartitionId(), tabletInfo.getIndexId(), tabletInfo.getTabletId(), cloneBackendId, CloneJob.JobType.SUPPLEMENT, CloneJob.JobPriority.HIGH, Config.clone_job_timeout_second * 1000L);
    }

    //for backend added
    private void AddMigrationJob(CloneTabletInfo tabletInfo, long cloneBackendId) {
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.addCloneJob(tabletInfo.getDbId(), tabletInfo.getTableId(), tabletInfo.getPartitionId(), tabletInfo.getIndexId(), tabletInfo.getTabletId(), cloneBackendId, CloneJob.JobType.MIGRATION, CloneJob.JobPriority.HIGH, Config.clone_job_timeout_second * 1000L);
    }
}
