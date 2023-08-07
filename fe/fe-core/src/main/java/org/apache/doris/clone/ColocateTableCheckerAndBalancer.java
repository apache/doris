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
        relocateAndBalanceGroup();
        matchGroup();
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
    private void relocateAndBalanceGroup() {
        if (Config.disable_colocate_balance) {
            return;
        }

        Env env = Env.getCurrentEnv();
        ColocateTableIndex colocateIndex = env.getColocateTableIndex();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        // get all groups
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
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
                        infoService, statistic, balancedBackendsPerBucketSeq)) {
                    colocateIndex.addBackendsPerBucketSeqByTag(groupId, tag, balancedBackendsPerBucketSeq);
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
    }

    /*
     * Check every tablet of a group, if replica's location does not match backends in group, relocating those
     * replicas, and mark that group as unstable.
     * If every replicas match the backends in group, mark that group as stable.
     */
    private void matchGroup() {
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
            ColocateTableIndex colocateIndex, SystemInfoService infoService,
            LoadStatisticForTag statistic, List<List<Long>> balancedBackendsPerBucketSeq) {
        ColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
        short replicaNum = groupSchema.getReplicaAlloc().getReplicaNumByTag(tag);
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList(
                colocateIndex.getBackendsPerBucketSeqByTag(groupId, tag));
        // [[A,B,C],[B,C,D]] -> [A,B,C,B,C,D]
        List<Long> flatBackendsPerBucketSeq = backendsPerBucketSeq.stream()
                .flatMap(List::stream).collect(Collectors.toList());

        boolean isChanged = false;
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

            long srcBeId = -1;
            List<Integer> seqIndexes = null;
            boolean hasUnavailableBe = false;
            // first choose the unavailable be as src be
            for (Long beId : unavailableBeIds) {
                seqIndexes = getBeSeqIndexes(flatBackendsPerBucketSeq, beId);
                if (!seqIndexes.isEmpty()) {
                    srcBeId = beId;
                    hasUnavailableBe = true;
                    LOG.info("find unavailable backend {} in colocate group: {}", beId, groupId);
                    break;
                }
            }
            // sort backends with replica num in desc order
            List<Map.Entry<Long, Long>> backendWithReplicaNum =
                    getSortedBackendReplicaNumPairs(availableBeIds,
                            unavailableBeIds, statistic, flatBackendsPerBucketSeq);

            // if there is only one available backend and no unavailable bucketId to relocate, end the outer loop
            if (backendWithReplicaNum.size() <= 1 && !hasUnavailableBe) {
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
                if ((!hasUnavailableBe) && (seqIndexes.size() - lowBackend.getValue()) <= 1) {
                    // balanced
                    break OUT;
                }

                long destBeId = lowBackend.getKey();
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

                for (int seqIndex : seqIndexes) {
                    // the bucket index.
                    // eg: 0 / 3 = 0, so that the bucket index of the 4th backend id in flatBackendsPerBucketSeq is 0.
                    int bucketIndex = seqIndex / replicaNum;
                    List<Long> backendsSet = backendsPerBucketSeq.get(bucketIndex);
                    List<String> hostsSet = hostsPerBucketSeq.get(bucketIndex);
                    // the replicas of a tablet can not locate in same Backend or same host
                    if (!backendsSet.contains(destBeId) && !hostsSet.contains(destBe.getHost())) {
                        Preconditions.checkState(backendsSet.contains(srcBeId), srcBeId);
                        flatBackendsPerBucketSeq.set(seqIndex, destBeId);
                        LOG.info("replace backend {} with backend {} in colocate group {}, idx: {}",
                                srcBeId, destBeId, groupId, seqIndex);
                        // just replace one backend at a time, src and dest BE id should be recalculated because
                        // flatBackendsPerBucketSeq is changed.
                        isChanged = true;
                        isThisRoundChanged = true;
                        break;
                    }
                }

                if (isThisRoundChanged) {
                    // we found a change
                    break;
                }
                // we use next node as dst node
                LOG.info("unable to replace backend {} with backend {} in colocate group {}",
                        srcBeId, destBeId, groupId);
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
            Set<Long> unavailBackendIds, LoadStatisticForTag statistic, List<Long> flatBackendsPerBucketSeq) {
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
                    BackendLoadStatistic beStat1 = statistic.getBackendLoadStatistic(entry1.getKey());
                    BackendLoadStatistic beStat2 = statistic.getBackendLoadStatistic(entry2.getKey());
                    if (beStat1 == null || beStat2 == null) {
                        return 0;
                    }
                    double loadScore1 = beStat1.getMixLoadScore();
                    double loadScore2 = beStat2.getMixLoadScore();
                    if (Math.abs(loadScore1 - loadScore2) < 1e-6) {
                        return 0;
                    } else if (loadScore2 > loadScore1) {
                        return 1;
                    } else {
                        return -1;
                    }
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
