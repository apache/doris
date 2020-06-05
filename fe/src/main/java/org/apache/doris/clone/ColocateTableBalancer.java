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
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.AddResult;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ColocateTableBalancer is responsible for tablets' repair and balance of colocated tables.
 */
public class ColocateTableBalancer extends MasterDaemon {
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
     * Each round, we do 3 steps:
     * 1. Relocate group:
     *      Backend is not available, find a new backend to replace it.
     *      Relocate at most one bucket in one group at a time.
     *      
     * 2. Match group:
     *      If replica mismatch backends in a group, that group will be marked as unstable, and pass that 
     *      tablet to TabletScheduler.
     *      Otherwise, mark the group as stable
     * 
     * 3. Balance group:
     *      Try balance group, and skip groups which contains unavailable backends.
     */
    protected void runAfterCatalogReady() {
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
     *      original bucket backends sequence is:
     *      [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
     *      
     *      and backend 3 is dead, so we will find an available backend(eg. backend 4) to replace backend 3.
     *      [[1, 2, 4], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
     *      
     *      NOTICE that in this example, we only replace the #3 backend in first bucket. That is, we only replace
     *      one bucket in one group at each round. because we need to use newly-updated cluster load statistic to
     *      find next available backend. and cluster load statistic is updated every 20 seconds.
     */
    private void relocateGroup() {
        if (Config.disable_colocate_relocate) {
            return;
        }
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
                // all backends in this group are available.
                // But in previous version we had a bug that replicas of a tablet may be located on same host.
                // we have to check it.
                List<List<Long>> backendsPerBucketsSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
                OUT: for (List<Long> backendIds : backendsPerBucketsSeq) {
                    Set<String> hosts = Sets.newHashSet();
                    for (Long beId : backendIds) {
                        Backend be = infoService.getBackend(beId);
                        if (be == null) {
                            // backend can be dropped any time, just skip this bucket
                            break;
                        }
                        if (!hosts.add(be.getHost())) {
                            // find replicas on same host. simply mark this backend as unavailable,
                            // so that following step will find another backend
                            unavailableBeId = beId;
                            break OUT;
                        }
                    }
                }

                if (unavailableBeId == -1) {
                    // if everything is ok, continue
                    continue;
                }
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
        Database db = Catalog.getCurrentCatalog().getDb(groupId.dbId);
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
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
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
         *    relocated in corresponding storage medium.
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

        // the selected backend should not be on same host of other backends of this bucket.
        // here we generate a host set for further checking.
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        Set<String> excludeHosts = Sets.newHashSet();
        for (Long excludeBeId : excludeBeIds) {
            Backend be = infoService.getBackend(excludeBeId);
            if (be == null) {
                LOG.info("Backend {} has been dropped when finding backend for colocate group {}", excludeBeId, groupId);
                continue;
            }
            excludeHosts.add(be.getHost());
        }
        Preconditions.checkState(excludeBeIds.size() >= excludeHosts.size());

        // beStats is ordered by load score, ascend. so finding the available from first to last
        BackendLoadStatistic choosenBe = null;
        for (BackendLoadStatistic beStat : beStats) {
            if (beStat.isAvailable() && beStat.getBeId() != unavailableBeId && !excludeBeIds.contains(beStat.getBeId())) {
                Backend be = infoService.getBackend(beStat.getBeId());
                if (be == null) {
                    continue;
                }
                if (excludeHosts.contains(be.getHost())) {
                    continue;
                }
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
                        // Here we only get VISIBLE indexes. All other indexes are not queryable.
                        // So it does not matter if tablets of other indexes are not matched.
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
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
                if (isGroupStable) {
                    colocateIndex.markGroupStable(groupId, true);
                } else {
                    colocateIndex.markGroupUnstable(groupId, true);
                }
            } finally {
                db.readUnlock();
            }
        } // end for groups
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
        if (Config.disable_colocate_balance) {
            return;
        }
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
                boolean isThisRoundChanged = false;
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
                        isThisRoundChanged = true;
                        break;
                    }
                }

                if (!isThisRoundChanged) {
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
}
