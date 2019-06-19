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
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.AddResult;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ColocateTableBalancer is responsible for tablets' repair and balance of colocated tables
 */
public class ColocateTableBalancer extends Daemon {
    private static final Logger LOG = LogManager.getLogger(ColocateTableBalancer.class);

    private static final long CHECK_INTERVAL_MS = 20 * 1000L; // 20 second

    private ColocateTableBalancer(long intervalMs) {
        super("colocate group clone checker", intervalMs);
    }

    private static ColocateTableBalancer INSTANCE = null;
    public static ColocateTableBalancer getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ColocateTableBalancer(CHECK_INTERVAL_MS);
        }
        return INSTANCE;
    }

    @Override
    /*
     * Each round, we do 2 steps:
     * 1. Relocate group:
     *      Backend is not available, find a new backend to replace it.
     *      Relocate at most one group at a time
     *      
     * 2. Match group:
     *      If replica mismatch backends in a group, that group will be marked as unstable.
     * 
     * 3. Balance group:
     *      Skip groups which contains unavailable backends.
     */
    protected void runOneCycle() {
        relocateGroup();
        matchGroup();
        balanceGroup();
    }

    /*
     * Traverse all colocate groups, for each group:
     * Check if there are backends dead or unavailable(decommission)
     * If yes, for each buckets in this group, if the unavailable backend belongs to this bucket, we will find
     * a new backend to replace the unavailable one.
     * 
     * eg:
     *      original bucket backends sequence is
     *      [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
     *      
     *      and backend 3 is dead, so we will find an available backend(eg. backend 4) to replace backend 3.
     *      
     *      [[1, 2, 4], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
     *      
     *      NOTICE that we only replace the backends in first buckets. that is, we only replace
     *      one bucket in one group at each round. because we need to use newly-updated cluster load statistic to
     *      find next available backend. and cluster load statistic is updated every 20 seconds.
     */
    private void relocateGroup() {
        Catalog catalog = Catalog.getCurrentCatalog();
        ColocateTableIndex colocateIndex = catalog.getColocateTableIndex();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        Map<String, ClusterLoadStatistic> statisticMap = Catalog.getCurrentCatalog().getTabletScheduler().getStatisticMap();
        long currTime = System.currentTimeMillis();
        
        // get all groups
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : groupIds) {
            // get all backends in this group
            Set<Long> backends = colocateIndex.getBackendsByGroup(groupId);
            long unavailableBeId = -1;
            for (Long backendId : backends) {
                // find an unavailable backend. even if there are than one unavailable backend,
                // we just handle the first one.
                Backend be = infoService.getBackend(backendId);
                if (be == null) {
                    unavailableBeId = backendId;
                    break;
                } else if (!be.isAvailable()) {
                    // 1. BE is dead for a long time
                    // 2. BE is under decommission
                    if ((!be.isAlive() && (currTime - be.getLastUpdateMs()) > Config.tablet_repair_delay_factor_second * 1000 * 2)
                        || be.isDecommissioned()) {
                        unavailableBeId = backendId;
                        break;
                    }
                }
            }

            if (unavailableBeId == -1) {
                // all backends in this group are available, check next group
                continue;
            }

            // find the first bucket which contains the unavailable backend
            LOG.info("backend {} is unavailable in colocate group {}", unavailableBeId, groupId);
            List<Set<Long>> bucketBackendsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
            int tabletOrderIdx = 0;
            for (Set<Long> set : bucketBackendsSeq) {
                if (set.contains(unavailableBeId)) {
                    break;
                }
                tabletOrderIdx++;
            }

            // select a new backend to replace the unavailable one
            long newBackendId = selectSubstituteBackend(tabletOrderIdx, groupId, unavailableBeId, 
                    bucketBackendsSeq.get(tabletOrderIdx), statisticMap);
            if (newBackendId != -1) {
                // replace backend
                bucketBackendsSeq.get(tabletOrderIdx).remove(unavailableBeId);
                bucketBackendsSeq.get(tabletOrderIdx).add(newBackendId);
                colocateIndex.setBackendsSetByIdxForGroup(groupId, tabletOrderIdx, bucketBackendsSeq.get(tabletOrderIdx));
                LOG.info("select backend {} to replace backend {} for bucket {} in group {}. now backends set is: {}",
                        newBackendId, unavailableBeId, tabletOrderIdx, groupId, bucketBackendsSeq.get(tabletOrderIdx));
            }

            // only handle one backend at a time
            break;
        }
    }

    /*
     * Select a substitute backend for specified bucket and colocate group.
     * return -1 if backend not found.
     * we need to move all replicas of this bucket to the new backend, so we have to check if the new
     * backend can save all these replicas.
     */
    private long selectSubstituteBackend(int tabletOrderIdx, GroupId groupId, long unavailableBeId, 
            Set<Long> excludeBeIds, Map<String, ClusterLoadStatistic> statisticMap) {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        Database db = Catalog.getInstance().getDb(groupId.dbId);
        if (db == null) {
            LOG.info("db {} does not exist", groupId.dbId);
            return -1;
        }
        ClusterLoadStatistic statistic = statisticMap.get(db.getClusterName());
        if (statistic == null) {
            LOG.info("cluster {} statistic does not exist", db.getClusterName());
            return -1;
        }
        
        // calculate the total replica size of this bucket
        List<Long> tableIds = colocateIndex.getAllTableIds(groupId);
        long totalReplicaNum = 0;
        long totalReplicaSize = 0;
        db.readLock();
        try {
            for (Long tblId : tableIds) {
                OlapTable tbl = (OlapTable) db.getTable(tblId);
                if (tbl == null) {
                    continue;
                }

                for (Partition partition : tbl.getPartitions()) {
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        long tabletId = index.getTabletIdsInOrder().get(tabletOrderIdx);
                        Tablet tablet = index.getTablet(tabletId);
                        Replica replica = tablet.getReplicaByBackendId(unavailableBeId);
                        if (replica != null) {
                            totalReplicaNum++;
                            totalReplicaSize += replica.getDataSize();
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
        LOG.debug("the number and size of replicas on backend {} of bucket {} is: {} and {}",
                unavailableBeId, tabletOrderIdx, totalReplicaNum, totalReplicaSize);
        
        /*
         * There is an unsolved problem of finding a new backend for data migration:
         *    Different table(partition) in this group may in different storage medium(SSD or HDD). If one backend
         *    is down, the best solution is to find a backend which has both SSD and HDD, and replicas can be
         *    relocated in corresponding medium.
         *    But in fact, backends can be heterogeneous, which may only has SSD or HDD. If we choose to strictly
         *    find backends with expecting storage medium, this may lead to a consequence that most of replicas
         *    are gathered in a small portion of backends.
         *    
         *    So for simplicity, we ignore the storage medium property, just find a low load backend which has
         *    capacity to save these replicas.
         */
        List<BackendLoadStatistic> beStats = statistic.getSortedBeLoadStats(null /* mix medium */);
        if (beStats.isEmpty()) {
            LOG.warn("failed to relocate backend for colocate group: {}, no backends found", groupId);
            return -1;
        }
        // beStats is ordered by load score, ascend. so finding the available from first to last
        BackendLoadStatistic choosenBe = null;
        for (BackendLoadStatistic beStat : beStats) {
            if (beStat.isAvailable() && beStat.getBeId() != unavailableBeId && !excludeBeIds.contains(beStat.getBeId())) {
                choosenBe = beStat;
                break;
            }
        }
        if (choosenBe == null) {
            LOG.warn("failed to find an available backend to relocate for colocate group: {}", groupId);
            return -1;
        }

        // check if there is enough capacity to save all these replicas
        if (!choosenBe.canFitInColocate(totalReplicaSize)) {
            LOG.warn("no backend has enough capacity to save replics in group {} with bucket: {}", groupId, tabletOrderIdx);
            return -1;
        }

        return choosenBe.getBeId();
    }

    /*
     * Check every tablet of a group, if replica's location does not match backends in group, relocating those
     * replicas, and mark that group as unstable.
     * If every replicas match the backends in group, mark that group as stable.
     */
    private void matchGroup() {
        Catalog catalog = Catalog.getCurrentCatalog();
        ColocateTableIndex colocateIndex = catalog.getColocateTableIndex();
        TabletScheduler tabletScheduler = catalog.getTabletScheduler();

        // check each group
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : groupIds) {
            List<Long> tableIds = colocateIndex.getAllTableIds(groupId);
            Database db = catalog.getDb(groupId.dbId);
            if (db == null) {
                continue;
            }

            List<Set<Long>> backendBucketsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
            boolean isGroupStable = true;
            db.readLock();
            try {
                OUT: for (Long tableId : tableIds) {
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null || !colocateIndex.isColocateTable(olapTable.getId())) {
                        continue;
                    }

                    for (Partition partition : olapTable.getPartitions()) {
                        short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                        long visibleVersion = partition.getVisibleVersion();
                        long visibleVersionHash = partition.getVisibleVersionHash();
                        for (MaterializedIndex index : partition.getMaterializedIndices()) {
                            Preconditions.checkState(backendBucketsSeq.size() == index.getTablets().size(),
                                    backendBucketsSeq.size() + " vs. " + index.getTablets().size());
                            int idx = 0;
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                Set<Long> bucketsSeq = backendBucketsSeq.get(idx);
                                Preconditions.checkState(bucketsSeq.size() == replicationNum, bucketsSeq.size() + " vs. " + replicationNum);
                                Tablet tablet = index.getTablet(tabletId);
                                TabletStatus st = tablet.getColocateHealthStatus(visibleVersion, visibleVersionHash, replicationNum, bucketsSeq);
                                if (st != TabletStatus.HEALTHY) {
                                    isGroupStable = false;
                                    LOG.debug("get unhealthy tablet {} in colocate table. status: {}", tablet.getId(), st);

                                    TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                            TabletSchedCtx.Type.REPAIR, db.getClusterName(),
                                            db.getId(), tableId, partition.getId(), index.getId(), tablet.getId(),
                                            System.currentTimeMillis());
                                    // the tablet status will be set again when being scheduled
                                    tabletCtx.setTabletStatus(st);
                                    // using HIGH priority, cause we want to stabilize the colocate group as soon as possible
                                    tabletCtx.setOrigPriority(Priority.HIGH);
                                    tabletCtx.setTabletOrderIdx(idx);

                                    AddResult res = tabletScheduler.addTablet(tabletCtx, false /* not force */);
                                    if (res == AddResult.LIMIT_EXCEED) {
                                        // tablet in scheduler exceed limit, skip this group and check next one.
                                        LOG.info("number of scheduling tablets in tablet scheduler"
                                                + " exceed to limit. stop colocate table check");
                                        break OUT;
                                    }
                                }
                                idx++;
                            }
                        }
                    }
                } // end for tables

                // mark group as stable or unstable
                LOG.debug("begin to mark group {} as {}", groupId, isGroupStable ? "stable" : "unstable");
                colocateIndex.markGroupStable(groupId, isGroupStable);
            } finally {
                db.readUnlock();
            }
        } // end for group
    }

    /*
     * Balance colocate groups which are unstable
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
    private void balanceGroup() {
        Catalog catalog = Catalog.getCurrentCatalog();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        ColocateTableIndex colocateIndex = catalog.getColocateTableIndex();

        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : groupIds) {
            // skip unstable groups
            if (colocateIndex.isGroupUnstable(groupId)) {
                continue;
            }

            // skip backend unavailable groups
            Set<Long> backendIds = colocateIndex.getBackendsByGroup(groupId);
            boolean isAllBackendsAvailable = true;
            for (Long beId : backendIds) {
                Backend be = infoService.getBackend(beId);
                if (be == null || !be.isAvailable()) {
                    isAllBackendsAvailable = false;
                    break;
                }
            }
            if (!isAllBackendsAvailable) {
                continue;
            }

            // all backends are good
            Database db = catalog.getDb(groupId.dbId);
            if (db == null) {
                continue;
            }

            List<Long> allBackendIds = infoService.getClusterBackendIds(db.getClusterName(), true);
            List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
            if (balance(groupId, allBackendIds, colocateIndex, infoService, balancedBackendsPerBucketSeq)) {
                colocateIndex.addBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq);
                ColocatePersistInfo info = ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq);
                Catalog.getCurrentCatalog().getEditLog().logColocateBackendsPerBucketSeq(info);
                LOG.info("balance group {}. now backends per bucket sequence is: {}", groupId, balancedBackendsPerBucketSeq);
            }
        }
    }

    /*
     * The balance logic is as follow:
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
     * 1. Sort the backend in this order by replication num, descending:
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
     *  Return true if backends per bucket sequence change and new sequence is saved in balancedBackendsPerBucketSeq.
     *  Return false if nothing changed.
     */
    private boolean balance(GroupId groupId, List<Long> allAvailBackendIds, ColocateTableIndex colocateIndex,
            SystemInfoService infoService, List<List<Long>> balancedBackendsPerBucketSeq) {
        ColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
        int replicationNum = groupSchema.getReplicationNum();
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList(colocateIndex.getBackendsPerBucketSeq(groupId));
        // [[A,B,C],[B,C,D]] -> [A,B,C,B,C,D]
        List<Long> flatBackendsPerBucketSeq = backendsPerBucketSeq.stream().flatMap(List::stream).collect(Collectors.toList());

        boolean isChanged = false;
        OUT: while (true) {
            // update backends and hosts at each round
            backendsPerBucketSeq = Lists.partition(flatBackendsPerBucketSeq, replicationNum);
            List<List<String>> hostsPerBucketSeq = getHostsPerBucketSeq(backendsPerBucketSeq, infoService);
            if (hostsPerBucketSeq == null) {
                // error happens, change nothing
                return false;
            }
            Preconditions.checkState(backendsPerBucketSeq.size() == hostsPerBucketSeq.size());

            // sort backends with replica num
            List<Map.Entry<Long, Long>> backendWithReplicaNum =
                    getSortedBackendReplicaNumPairs(allAvailBackendIds, flatBackendsPerBucketSeq);

            int i = 0;
            int j = backendWithReplicaNum.size() - 1;
            while (i < j) {
                // we try to use a low backend to replace the high backend.
                // if replace failed(eg: both backends are on some host), select next low backend and try(j--)
                Map.Entry<Long, Long> highBackend = backendWithReplicaNum.get(i);
                Map.Entry<Long, Long> lowBackend = backendWithReplicaNum.get(j);
                if (highBackend.getValue() - lowBackend.getValue() <= 1) {
                    // balanced
                    break OUT;
                }

                long srcBeId = highBackend.getKey();
                long destBeId = lowBackend.getKey();
                Backend destBe = infoService.getBackend(destBeId);
                if (destBe == null) {
                    LOG.info("backend {} does not exist", destBeId);
                    return false;
                }

                /* 
                 * get the array indexes of elements in flatBackendsPerBucketSeq which equals to srcBeId
                 * eg:
                 * flatBackendsPerBucketSeq:
                 *      A B C A D E A F G A H I
                 * and srcBeId is A.
                 * so seqIndexes is:
                 *      0 3 6 9
                 */
                List<Integer> seqIndexes = IntStream.range(0, flatBackendsPerBucketSeq.size()).boxed().filter(
                        idx -> flatBackendsPerBucketSeq.get(idx).equals(srcBeId)).collect(Collectors.toList());

                for (int seqIndex : seqIndexes) {
                    // the bucket index.
                    // eg: 0 / 3 = 0, so that the bucket index of the 4th backend id in flatBackendsPerBucketSeq is 0.
                    int bucketIndex = seqIndex / replicationNum;
                    List<Long> backendsSet = backendsPerBucketSeq.get(bucketIndex);
                    List<String> hostsSet = hostsPerBucketSeq.get(bucketIndex);
                    // the replicas of a tablet can not locate in same Backend or same host
                    if (!backendsSet.contains(destBeId) && !hostsSet.contains(destBe.getHost())) {
                        Preconditions.checkState(backendsSet.contains(srcBeId), srcBeId);
                        flatBackendsPerBucketSeq.set(seqIndex, destBeId);
                        LOG.info("replace backend {} with backend {} in colocate group {}", srcBeId, destBeId, groupId);
                        // just replace one backend at a time, src and dest BE id should be recalculated because
                        // flatBackendsPerBucketSeq is changed.
                        isChanged = true;
                        break;
                    }
                }

                if (!isChanged) {
                    // select another load backend and try again
                    LOG.info("unable to replace backend {} with backend {} in colocate group {}",
                            srcBeId, destBeId, groupId);
                    j--;
                    continue;
                }

                break;
            } // end inner loop
        }

        if (isChanged) {
            balancedBackendsPerBucketSeq.addAll(Lists.partition(flatBackendsPerBucketSeq, replicationNum));
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
                    LOG.info("backend {} does not exist", beId);
                    return null;
                }
                hosts.add(be.getHost());
            }
            hostsPerBucketSeq.add(hosts);
        }
        return hostsPerBucketSeq;
    }

    private List<Map.Entry<Long, Long>> getSortedBackendReplicaNumPairs(List<Long> allAvailBackendIds,
            List<Long> flatBackendsPerBucketSeq) {
        // backend id -> replica num, and sorted by replica num, descending.
        Map<Long, Long> backendToReplicaNum = flatBackendsPerBucketSeq.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        // add backends which are not in flatBackendsPerBucketSeq, with replication number 0
        for (Long backendId : allAvailBackendIds) {
            if (!backendToReplicaNum.containsKey(backendId)) {
                backendToReplicaNum.put(backendId, 0L);
            }
        }
        List<Map.Entry<Long, Long>> backendWithReplicaNum = backendToReplicaNum.entrySet().stream().sorted(
                Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toList());
        return backendWithReplicaNum;
    }

    @Deprecated
    /**
     * The colocate table balance flow:
     *
     * 1 balance start when found backend removed, down or added
     * 2 compute which bucket seq need to migrate, the migrate source backend, the migrate target backend
     * 3 mark colocate group balancing in colocate meta
     * 4 update colocate backendsPerBucketSeq meta
     * 5 do real data migration by clone job
     * 6 delete redundant replicas after all clone job done
     * 7 mark colocate group stable in colocate meta and balance done
     */
    protected void runOneCycle_D() {
        checkAndCloneBalancingGroup();

        tryDeleteRedundantReplicas();

        tryBalanceWhenBackendChange();
    }

    /**
     * check all balancing colocate group tables
     * if all tables in a colocate group are stable, mark the colocate group stable
     * else add a clone job for balancing colocate group tables
     */
    private synchronized void checkAndCloneBalancingGroup() {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        Catalog catalog = Catalog.getInstance();

        Set<GroupId> allGroups = colocateIndex.getAllGroupIds();
        for (GroupId group : allGroups) {
            LOG.info("colocate group: {} backendsPerBucketSeq is {}", group, colocateIndex.getBackendsPerBucketSeq(group));
        }

        Set<GroupId> balancingGroupIds = colocateIndex.getUnstableGroupIds();
        if (balancingGroupIds.size() == 0) {
            LOG.info("All colocate groups are stable. Skip");
            return;
        }

        for (GroupId groupId : balancingGroupIds) {
            Database db = catalog.getDb(groupId.dbId);
            if (db == null) {
                continue;
            }

            boolean isBalancing = false;
            List<Long> allTableIds = colocateIndex.getAllTableIds(groupId);
            for (long tableId : allTableIds) {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (olapTable == null) {
                    continue;
                }
                if (checkAndCloneTable(db, olapTable, colocateIndex.getBackendsPerBucketSeq(groupId))) {
                    isBalancing = true;
                    break;
                }
            }

            if (!isBalancing) {
                colocateIndex.markGroupStable(groupId);
                ColocatePersistInfo info = ColocatePersistInfo.createForMarkStable(groupId);
                Catalog.getInstance().getEditLog().logColocateMarkStable(info);
                LOG.info("colocate group : {} become stable!", groupId);
            }
        }
    }

    /**
     * 1 check the colocate table whether balancing
     *      A colocate table is stable means:
     *      a: all replica state is not clone
     *      b: the tablet backendIds are consistent with ColocateTableIndex's backendsPerBucketSeq
     *
     * 2 if colocate table is balancing , we will try adding a clone job.
     *   clone.addCloneJob has duplicated check, so we could try many times
     */
    private boolean checkAndCloneTable(Database db, OlapTable olapTable, List<List<Long>> backendsPerBucketSeq) {
        boolean isBalancing = false;
        out: for (Partition partition : olapTable.getPartitions()) {
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                List<Tablet> tablets = index.getTablets();
                for (int i = 0; i < tablets.size(); i++) {
                    Tablet tablet = tablets.get(i);
                    // 1 check all replica state is not clone
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.getState().equals(Replica.ReplicaState.CLONE)) {
                            isBalancing = true;
                            LOG.info("colocate group : {} is still balancing, there is clone Replica", olapTable.getColocateGroup());
                            break out;
                        }
                    }

                    List<Long> groupBackends = new ArrayList<>(backendsPerBucketSeq.get(i));
                    Set<Long> tabletBackends = tablet.getBackendIds();
                    // 2 check the tablet backendIds are consistent with ColocateTableIndex's backendsPerBucketSeq
                    if (!tabletBackends.containsAll(groupBackends)) {
                        isBalancing = true;
                        LOG.info("colocate group : {} is still balancing, may be clone job hasn't run, try adding a clone job", olapTable.getColocateGroup());

                        // try adding a clone job
                        // clone.addCloneJob has duplicated check, so there isn't side-effect
                        List<Long> clusterAliveBackendIds = getAliveClusterBackendIds(db.getClusterName());
                        groupBackends.removeAll(tabletBackends);

                        // for backend added;
                        if (clusterAliveBackendIds.containsAll(tabletBackends)) {
                            // we can ignore tabletSizeB parameter here
                            CloneTabletInfo tabletInfo = new CloneTabletInfo(db.getId(), olapTable.getId(), partition.getId(),
                                    index.getId(), tablet.getId(), replicationNum, replicationNum, 0, tabletBackends);

                            for (Long cloneBackend : groupBackends) {
                                AddMigrationJob(tabletInfo, cloneBackend);
                            }
                        } else { // for backend down or removed
                            short onlineReplicaNum = (short) (replicationNum - groupBackends.size());
                            CloneTabletInfo tabletInfo = new CloneTabletInfo(db.getId(), olapTable.getId(), partition.getId(),
                                    index.getId(), tablet.getId(), replicationNum, onlineReplicaNum, 0, tabletBackends);

                            for (Long cloneBackend : groupBackends) {
                                AddSupplementJob(tabletInfo, cloneBackend);
                            }
                        }
                    }
                } // end tablet
            } // end index
        } // end partition
        return isBalancing;
    }

    /**
     * firstly, check backend removed or down
     * secondly, check backend added
     */
    private synchronized void tryBalanceWhenBackendChange() {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        Catalog catalog = Catalog.getInstance();

        Set<GroupId> allGroupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : allGroupIds) {
            if (colocateIndex.isGroupUnstable(groupId)) {
                LOG.info("colocate group {} is balancing", groupId);
                continue;
            }

            Database db = catalog.getDb(groupId.dbId);
            List<Long> clusterAliveBackendIds = getAliveClusterBackendIds(db.getClusterName());
            Set<Long> allGroupBackendIds = colocateIndex.getBackendsByGroup(groupId);
            List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);

            // 1 check backend removed or down
            if (!clusterAliveBackendIds.containsAll(allGroupBackendIds)) {
                Set<Long> removedBackendIds = Sets.newHashSet(allGroupBackendIds);
                removedBackendIds.removeAll(clusterAliveBackendIds);

                // A backend in Colocate group but not alive, which means the backend is removed or down
                Iterator<Long> removedBackendIdsIterator = removedBackendIds.iterator();
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
                    // multiple backend removed is unusual, so we handle one by one
                    for (Long backendId : removedBackendIds) {
                        balanceForBackendRemoved(db, groupId, backendId);
                    }
                    continue; // for one colocate group, only handle backend removed or added event once
                }
            }

            //2 check backend added
            int replicateNum = backendsPerBucketSeq.get(0).size();
            if (backendsPerBucketSeq.size() * replicateNum <= allGroupBackendIds.size()) {
                // if each tablet replica has a different backend, which means the colocate group
                // has fully balanced. we can ignore the new backend added.
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

    // get the backends: 1 belong to this cluster; 2 alive; 3 not decommissioned
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

        Set<GroupId> allGroupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : allGroupIds) {
            Set<GroupId> balancingGroups = colocateIndex.getUnstableGroupIds();
            // only delete redundant replica when group is stable
            if (!balancingGroups.contains(groupId)) {
                Database db = catalog.getDb(groupId.dbId);
                if (db == null) {
                    continue;
                }
                List<Long> allTableIds = colocateIndex.getAllTableIds(groupId);
                Set<CloneTabletInfo> deleteTabletSet = Sets.newHashSet();//keep tablets which have redundant replicas.
                int replicateNum = -1;
                for (long tableId : allTableIds) {
                    db.readLock();
                    try {
                        OlapTable olapTable = (OlapTable) db.getTable(tableId);
                        if (olapTable == null) {
                            continue;
                        }
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
                    // delete tablet will affect colocate table local query schedule,
                    // so make colocate group balancing again
                    markGroupBalancing(groupId);
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

            // delete replica for backend removed
            List<Replica> copyReplicas = new ArrayList<>(replicas);
            for (Replica replica : copyReplicas) {
                long backendId = replica.getBackendId();
                if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                    deleteReplica(tablet, replica, db.getId(), tableId, partitionId, indexId);
                }
            }

            // delete replica for backend added
            List<Replica> updatedReplicas = tablet.getReplicas();
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            if (updatedReplicas.size() <= replicationNum) {
                return;
            }

            int deleteNum = updatedReplicas.size() - replicationNum;
            List<Long> sortedReplicaIds = sortReplicaId(updatedReplicas);

            // always delete replica which id is minimum
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
     * 1 compute which bucket seq need to migrate, the migrate target backend
     * 2 mark colocate group balancing in colocate meta
     * 3 update colcate backendsPerBucketSeq meta
     */
    private void balanceForBackendRemoved(Database db, GroupId groupId, Long removedBackendId) {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        com.google.common.collect.Table<GroupId, Integer, Long> newGroup2BackendsPerBucketSeq = HashBasedTable.create();

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
                                markGroupBalancing(groupId);

                                List<Long> backends = newBackendsPerBucketSeq.get(i);
                                backends.remove(removedBackendId);
                                if (!backends.contains(cloneReplicaBackendId)) {
                                    backends.add(cloneReplicaBackendId);
                                }

                                Preconditions.checkState(replicateNum == backends.size(), replicateNum + " vs. " + backends.size());
                            }
                        }
                    }
                }
            }
            persistBackendsToBucketSeqMeta(groupId, newBackendsPerBucketSeq);
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

    // this logic is like CloneChecker.checkTabletForSupplement and CloneChecker.addCloneJob
    private Long selectCloneBackendIdForRemove(
            com.google.common.collect.Table<GroupId, Integer, Long> newGroup2BackendsPerBucketSeq,
            GroupId groupId, int bucketSeq, String clusterName, CloneTabletInfo tabletInfo) {
        Long cloneReplicaBackendId = null;
        cloneReplicaBackendId = newGroup2BackendsPerBucketSeq.get(groupId, bucketSeq);
        if (cloneReplicaBackendId == null) {
            // beId -> BackendInfo
            final Map<Long, BackendInfo> backendInfosInCluster = CloneChecker.getInstance().initBackendInfos(clusterName);
            if (backendInfosInCluster.isEmpty()) {
                LOG.warn("failed to init backend infos of cluster: {}", clusterName);
                return -1L;
            }

            // tablet distribution level
            final Map<CloneChecker.CapacityLevel, Set<List<Long>>> clusterDistributionLevelToBackendIds = 
                    CloneChecker.getInstance().initBackendDistributionInfos(backendInfosInCluster);

            final Map<CloneChecker.CapacityLevel, Set<List<Long>>> clusterCapacityLevelToBackendIds =
                    CloneChecker.getInstance().initBackendCapacityInfos(backendInfosInCluster);
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

            newGroup2BackendsPerBucketSeq.put(groupId, bucketSeq, cloneReplicaBackendId);
        }

        LOG.info("select clone replica dest backend id: {} for group: {} TabletInfo: {}", cloneReplicaBackendId,
                groupId, tabletInfo);
        return cloneReplicaBackendId;
    }

    /**
     * 1 compute which bucket seq need to migrate, the migrate source backend, the migrate target backend
     * 2 mark colocate group balancing in colocate meta
     * 3 update colocate backendsPerBucketSeq meta
     *
     * For example:
     *
     * the old backendsPerBucketSeq is:
     * [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
     * 
     * after we add two new backends: [5, 6]
     * 
     * the balanced backendsPerBucketSeq will become:
     * [[5, 6, 3], [6, 1, 2], [5, 4, 1], [2, 3, 4], [1, 2, 3]]
     *
     */
    private void balanceForBackendAdded(GroupId groupId, Database db, List<Long> addedBackendIds) {
        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
        List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);

        db.readLock();
        try {
            List<List<Long>> newBackendsPerBucketSeq = balance(backendsPerBucketSeq, addedBackendIds);
            markGroupBalancing(groupId);
            persistBackendsToBucketSeqMeta(groupId, newBackendsPerBucketSeq);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            db.readUnlock();
        }
    }

    /**
     * Returns a map that the key is backend id, the value is replica num in the backend
     * The map will sort by replica num in descending order
     *
     * @param backends the backend id list
     * @return a descending sorted map
     */
    private static Map<Long, Long> getBackendToReplicaNums(List<Long> backends) {
        Map<Long, Long> backendCounter = backends.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        return backendCounter.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * balance the bucket seq according to the new backends
     *
     * the balance logic is simple:
     *
     * 1 compute the replica num in each old backend
     * 2 compute the avg replica num after new backend added
     * 3 migrate the replica from the old backend to new backend,
     *  we preferred select the old backend has more replica num
     *
     * @param backendsPerBucketSeq the mapping from bucket seq to backend
     * @param newBackends the new backends need to balance
     * @return the balanced mapping from bucket seq to backend
     */
    public static List<List<Long>> balance(List<List<Long>> backendsPerBucketSeq, List<Long> newBackends) {
        int replicaNum = backendsPerBucketSeq.get(0).size();
        // the replicateNum for a partition in a colocate group
        int allReplicaNum = backendsPerBucketSeq.size() * replicaNum;
        Set<Long> groupBackendSet = backendsPerBucketSeq.stream().flatMap(List::stream).collect(Collectors.toSet());
        List<Long> flatBackendsPerBucketSeq = backendsPerBucketSeq.stream().flatMap(List::stream).collect(Collectors.toList());
        Map<Long, Long> backendToReplicaNums = getBackendToReplicaNums(flatBackendsPerBucketSeq);

        int allBackendSize = groupBackendSet.size() + newBackends.size();

        // normally avgReplicaNum equal allReplicaNum / allBackendSize,
        // but when allBackendSize larger than allReplicaNum: the avgReplicaNum should be 1, not 0.
        int avgReplicaNum = Math.max(allReplicaNum / allBackendSize, 1);

        // normally needBalanceNum equal avgReplicaNum * newBackends num
        // but when allBackendSize larger than allReplicaNum: we should ensure the old backend at least has one replica
        // for example: when the allReplicaNum is 15, the old backend num is 4, the new backend num is 12
        // the needBalanceNum should be 15 - 4 = 11, not 12
        int needBalanceNum = Math.min(avgReplicaNum * newBackends.size(), allReplicaNum - groupBackendSet.size());

        LOG.info("avg ReplicaNum: " + avgReplicaNum);
        LOG.info("need BalanceNum: " + needBalanceNum);

        int hasBalancedNum = 0;
        // keep which BucketSeq will migrate to the new target backend
        Map<Long, List<Integer>> targetBackendsToBucketSeqs = Maps.newHashMap();
        while (hasBalancedNum < needBalanceNum) {
            // in one loop, we only migrate newBackends.size() num BucketSeq, because the backendToReplicaNums will change
            for(Map.Entry<Long, Long> backendToReplicaNum: backendToReplicaNums.entrySet()) {
                long sourceBackend = backendToReplicaNum.getKey();
                long sourceReplicaNum = backendToReplicaNum.getValue();

                // new backend should not as sourceBackend, after one loop, new backend will add to backendToReplicaNums
                if (newBackends.contains(sourceBackend)) {
                    continue;
                }

                if (sourceReplicaNum > avgReplicaNum) {
                    Long targetBackend = newBackends.get (hasBalancedNum % newBackends.size());

                    List<Integer> sourceIndexes = IntStream.range(0, flatBackendsPerBucketSeq.size()).boxed()
                            .filter(i -> flatBackendsPerBucketSeq.get(i).equals(sourceBackend))
                            .collect(Collectors.toList());

                    for(int sourceIndex: sourceIndexes) {
                        int sourceBucketSeq = sourceIndex / replicaNum;

                        // for one bucket seq, all replica should in different Backend
                        List<Integer> choseSourceBucketSeq = targetBackendsToBucketSeqs.getOrDefault(targetBackend, Lists.newArrayList());
                        if (!choseSourceBucketSeq.contains(sourceBucketSeq)) {
                            flatBackendsPerBucketSeq.set(sourceIndex, targetBackend);

                            choseSourceBucketSeq.add(sourceBucketSeq);
                            targetBackendsToBucketSeqs.put(targetBackend, choseSourceBucketSeq);

                            hasBalancedNum++;
                            break;
                        }
                    }
                }

                if (hasBalancedNum >= needBalanceNum) {
                    break;
                }
            }
            // reorder because the replica num in each backend has changed
            backendToReplicaNums = getBackendToReplicaNums(flatBackendsPerBucketSeq);
        }
        return  Lists.partition(flatBackendsPerBucketSeq, replicaNum);
    }

    private void markGroupBalancing(GroupId groupId) {
        if (!Catalog.getCurrentColocateIndex().isGroupUnstable(groupId)) {
            Catalog.getCurrentColocateIndex().markGroupBalancing(groupId);
            ColocatePersistInfo info = ColocatePersistInfo.createForMarkBalancing(groupId);
            Catalog.getInstance().getEditLog().logColocateMarkBalancing(info);
        }
    }

    private void persistBackendsToBucketSeqMeta(GroupId groupId, List<List<Long>> newBackendsPerBucketSeq) {
        Catalog.getCurrentColocateIndex().addBackendsPerBucketSeq(groupId, newBackendsPerBucketSeq);
        ColocatePersistInfo info = ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, newBackendsPerBucketSeq);
        Catalog.getInstance().getEditLog().logColocateBackendsPerBucketSeq(info);
        LOG.info("persist backendsPerBucketSeq {} for group {}", newBackendsPerBucketSeq, groupId);
    }

    // for backend down or removed
    private void AddSupplementJob(CloneTabletInfo tabletInfo, long cloneBackendId) {
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.addCloneJob(tabletInfo.getDbId(), tabletInfo.getTableId(), tabletInfo.getPartitionId(), tabletInfo.getIndexId(), tabletInfo.getTabletId(), cloneBackendId, CloneJob.JobType.SUPPLEMENT, CloneJob.JobPriority.HIGH, Config.clone_job_timeout_second * 1000L);
    }

    // for backend added
    private void AddMigrationJob(CloneTabletInfo tabletInfo, long cloneBackendId) {
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.addCloneJob(tabletInfo.getDbId(), tabletInfo.getTableId(), tabletInfo.getPartitionId(), tabletInfo.getIndexId(), tabletInfo.getTabletId(), cloneBackendId, CloneJob.JobType.MIGRATION, CloneJob.JobPriority.HIGH, Config.clone_job_timeout_second * 1000L);
    }
}
