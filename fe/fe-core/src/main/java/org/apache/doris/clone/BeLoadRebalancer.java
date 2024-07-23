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

import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.BackendLoadStatistic.BePathLoadStatPair;
import org.apache.doris.clone.BackendLoadStatistic.BePathLoadStatPairComparator;
import org.apache.doris.clone.BackendLoadStatistic.Classification;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.SchedException.SubCode;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * BeLoadRebalancer strategy:
 * 1. selecting alternative tablets from high load backends, and return them to tablet scheduler.
 * 2. given a tablet, find a backend to migration.
 * 3. deleting the redundant replica in high load, so don't override getCachedSrcBackendId().
 */
public class BeLoadRebalancer extends Rebalancer {
    private static final Logger LOG = LogManager.getLogger(BeLoadRebalancer.class);

    public BeLoadRebalancer(SystemInfoService infoService, TabletInvertedIndex invertedIndex,
            Map<Long, PathSlot> backendsWorkingSlots) {
        super(infoService, invertedIndex, backendsWorkingSlots);
    }

    /*
     * Try to select alternative tablets to balance the specified cluster.
     * 1. Classify the backend into low, mid and high class by load score.
     * 2. Try to select tablets from high load backends.
     *      1. Here we only select alternative tablets, without considering selected tablets' status,
     *         and whether it is benefit for balance (All these will be checked in tablet scheduler)
     *      2. Only select tablets from 'high' backends.
     *      3. Only select tablets from 'high' and 'mid' paths.
     *
     * Here we only select tablets from high load node, do not set its src or dest, all this will be set
     * when this tablet is being scheduled in tablet scheduler.
     *
     * NOTICE that we may select any available tablets here, ignore their state.
     * The state will be checked when being scheduled in tablet scheduler.
     */
    @Override
    protected List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            LoadStatisticForTag clusterStat, TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        List<BackendLoadStatistic> lowBEs = Lists.newArrayList();
        List<BackendLoadStatistic> highBEs = Lists.newArrayList();
        boolean isUrgent = clusterStat.getLowHighBEsWithIsUrgent(lowBEs, highBEs, medium);

        if (lowBEs.isEmpty() && highBEs.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cluster is balance with medium: {}. skip", medium);
            }
            return alternativeTablets;
        }

        // first we should check if low backends is available.
        // if all low backends is not available, we should not start balance
        if (lowBEs.stream().noneMatch(BackendLoadStatistic::isAvailable)) {
            LOG.info("all low load backends is dead: {} with medium: {}. skip",
                    lowBEs.stream().mapToLong(BackendLoadStatistic::getBeId).toArray(), medium);
            return alternativeTablets;
        }

        if (lowBEs.stream().noneMatch(BackendLoadStatistic::hasAvailDisk)) {
            LOG.info("all low load backends {} have no available disk with medium: {}. skip",
                    lowBEs.stream().mapToLong(BackendLoadStatistic::getBeId).toArray(), medium);
            return alternativeTablets;
        }

        long numOfLowPaths = 0;
        for (BackendLoadStatistic backendLoadStatistic : lowBEs) {
            if (!backendLoadStatistic.isAvailable()) {
                continue;
            }
            PathSlot pathSlot = backendsWorkingSlots.get(backendLoadStatistic.getBeId());
            if (pathSlot != null) {
                numOfLowPaths += pathSlot.getTotalAvailBalanceSlotNum();
            }
        }
        LOG.info("get number of low load paths: {}, with medium: {}", numOfLowPaths, medium);

        List<String> alternativeTabletInfos = Lists.newArrayList();

        // Clone ut mocked env, but CatalogRecycleBin is not mockable (it extends from Thread)
        // so in clone ut recycleBin need to set to null.
        CatalogRecycleBin recycleBin = null;
        if (!FeConstants.runningUnitTest) {
            recycleBin = Env.getCurrentRecycleBin();
        }
        int clusterAvailableBEnum = infoService.getAllBackendIds(true).size();
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        List<Set<Long>> lowBETablets = lowBEs.stream()
                .map(beStat -> Sets.newHashSet(invertedIndex.getTabletIdsByBackendId(beStat.getBeId())))
                .collect(Collectors.toList());

        // choose tablets from high load backends.
        // BackendLoadStatistic is sorted by load score in ascend order,
        // so we need to traverse it from last to first
        OUTER:
        for (int i = highBEs.size() - 1; i >= 0; i--) {
            BackendLoadStatistic beStat = highBEs.get(i);
            PathSlot pathSlot = backendsWorkingSlots.get(beStat.getBeId());
            if (pathSlot == null) {
                continue;
            }

            boolean choseHighDisk = isUrgent && beStat.getMaxDiskClazz(medium) == Classification.HIGH;

            // for each path, we try to select at most BALANCE_SLOT_NUM_FOR_PATH tablets
            Map<Long, Integer> remainingPaths = Maps.newHashMap();
            Set<Long> pathHigh = null;
            if (choseHighDisk) {
                pathHigh = beStat.getAvailPaths(medium).stream().filter(RootPathLoadStatistic::isGlobalHighUsage)
                        .map(RootPathLoadStatistic::getPathHash).collect(Collectors.toSet());
            } else {
                // classify the paths.
                pathHigh = Sets.newHashSet();
                Set<Long> pathLow = Sets.newHashSet();
                Set<Long> pathMid = Sets.newHashSet();
                beStat.getPathStatisticByClass(pathLow, pathMid, pathHigh, medium);
                // we only select tablets from available mid and high load path
                pathHigh.addAll(pathMid);
            }

            double highDiskMaxUsage = 0;
            for (Long pathHash : pathHigh) {
                int availBalanceNum = pathSlot.getAvailableBalanceNum(pathHash);
                if (availBalanceNum > 0) {
                    remainingPaths.put(pathHash, availBalanceNum);
                }

                RootPathLoadStatistic pathStat = beStat.getPathStatisticByPathHash(pathHash);
                if (pathStat != null) {
                    highDiskMaxUsage = Math.max(highDiskMaxUsage, pathStat.getUsedPercent());
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("high be {}, medium: {}, path high: {}, remainingPaths: {}, chose high disk: {}",
                        beStat.getBeId(), medium, pathHigh, remainingPaths, choseHighDisk);
            }

            if (remainingPaths.isEmpty()) {
                continue;
            }

            // get all tablets on this backend, and shuffle them for random selection
            List<Pair<Long, Long>> tabletIdSizes = invertedIndex.getTabletSizeByBackendIdAndStorageMedium(
                    beStat.getBeId(), medium);
            if (!isUrgent
                    || tabletIdSizes.size() < Config.urgent_balance_pick_large_tablet_num_threshold
                    || highDiskMaxUsage < (double) Config.urgent_balance_pick_large_disk_usage_percentage / 100.0
                    || Config.urgent_balance_shuffle_large_tablet_percentage >= 100
                    || Config.urgent_balance_shuffle_large_tablet_percentage < 0) {
                Collections.shuffle(tabletIdSizes);
            } else {
                Collections.sort(tabletIdSizes, new Pair.PairComparator<Pair<Long, Long>>());
                if (Config.urgent_balance_shuffle_large_tablet_percentage > 0) {
                    int startIndex = (int) (tabletIdSizes.size()
                            * (1 - (double) Config.urgent_balance_shuffle_large_tablet_percentage / 100.0));
                    Collections.shuffle(tabletIdSizes.subList(startIndex, tabletIdSizes.size()));
                }
            }

            // select tablet from shuffled tablets
            for (int j = tabletIdSizes.size() - 1; j >= 0; j--) {
                long tabletId = tabletIdSizes.get(j).key();
                if (clusterAvailableBEnum <= invertedIndex.getReplicasByTabletId(tabletId).size()) {
                    continue;
                }

                if (alternativeTablets.stream().anyMatch(tabletCtx -> tabletId == tabletCtx.getTabletId())) {
                    continue;
                }

                Replica replica = null;
                try {
                    replica = invertedIndex.getReplica(tabletId, beStat.getBeId());
                } catch (IllegalStateException e) {
                    continue;
                }
                if (replica == null) {
                    continue;
                }

                // check if replica's is on 'high' or 'mid' path.
                // and only select it if the selected tablets num of this path
                // does not exceed the limit (BALANCE_SLOT_NUM_FOR_PATH).
                long replicaPathHash = replica.getPathHash();
                long replicaDataSize = replica.getDataSize();
                if (remainingPaths.containsKey(replicaPathHash)) {
                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (tabletMeta == null) {
                        continue;
                    }

                    if (colocateTableIndex.isColocateTable(tabletMeta.getTableId())) {
                        continue;
                    }

                    // for urgent disk, pick tablets order by size,
                    // then it may always pick tablets that was on the low backends.
                    if (!lowBETablets.isEmpty()
                            && lowBETablets.stream().allMatch(tablets -> tablets.contains(tabletId))) {
                        continue;
                    }

                    if (recycleBin != null && recycleBin.isRecyclePartition(tabletMeta.getDbId(),
                            tabletMeta.getTableId(), tabletMeta.getPartitionId())) {
                        continue;
                    }

                    boolean isFit = lowBEs.stream().anyMatch(be -> be.isFit(replicaDataSize,
                            medium, null, false) == BalanceStatus.OK);
                    if (!isFit) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("tablet {} with size {} medium {} not fit in low backends",
                                    tabletId, replica.getDataSize(), medium);
                        }
                        continue;
                    }

                    TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                            tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                            tabletMeta.getIndexId(), tabletId, null /* replica alloc is not used for balance*/,
                            System.currentTimeMillis());
                    tabletCtx.setTag(clusterStat.getTag());
                    // balance task's priority is always LOW
                    tabletCtx.setPriority(isUrgent ? Priority.NORMAL : Priority.LOW);

                    alternativeTablets.add(tabletCtx);
                    alternativeTabletInfos.add("{ tabletId=" + tabletId + ", beId=" + beStat.getBeId()
                            + ", pathHash=" + replica.getPathHash()
                            + ", replicaLocalSize=" + replica.getDataSize() + " }");
                    if (--numOfLowPaths <= 0) {
                        // enough
                        break OUTER;
                    }

                    // update remaining paths
                    int remaining = remainingPaths.get(replicaPathHash) - 1;
                    if (remaining <= 0) {
                        remainingPaths.remove(replicaPathHash);
                    } else {
                        remainingPaths.put(replicaPathHash, remaining);
                    }
                }
            }
        } // end for high backends

        if (!alternativeTablets.isEmpty()) {
            LOG.info("select alternative tablets, medium: {}, is urgent: {}, num: {}, detail: {}",
                    medium, isUrgent, alternativeTablets.size(), alternativeTabletInfos);
        }
        return alternativeTablets;
    }


    /*
     * Create a clone task of this selected tablet for balance.
     * 1. Check if this tablet has replica on high load backend. If not, the balance will be cancelled.
     *    If yes, select a replica as source replica.
     * 2. Select a low load backend as destination. And tablet should not has replica on this backend.
     */
    @Override
    public void completeSchedCtx(TabletSchedCtx tabletCtx) throws SchedException {
        LoadStatisticForTag clusterStat = statisticMap.get(tabletCtx.getTag());
        if (clusterStat == null) {
            throw new SchedException(Status.UNRECOVERABLE,
                    String.format("tag %s does not exist", tabletCtx.getTag()));
        }

        // get classification of backends
        List<BackendLoadStatistic> lowBEs = Lists.newArrayList();
        List<BackendLoadStatistic> highBEs = Lists.newArrayList();
        boolean isUrgent = clusterStat.getLowHighBEsWithIsUrgent(lowBEs, highBEs, tabletCtx.getStorageMedium());
        String isUrgentInfo = isUrgent ? " for urgent" : " for non-urgent";

        if (lowBEs.isEmpty() && highBEs.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE, "cluster is balance");
        }

        // if all low backends is not available, return
        if (lowBEs.stream().noneMatch(BackendLoadStatistic::isAvailable)) {
            throw new SchedException(Status.UNRECOVERABLE, "all low load backends is unavailable");
        }

        List<Replica> replicas = tabletCtx.getReplicas();

        // Check if this tablet has replica on high load backend.
        // Also create a set to save hosts of this tablet.
        Set<String> hosts = Sets.newHashSet();
        List<BackendLoadStatistic> replicaHighBEs = Lists.newArrayList();
        for (BackendLoadStatistic beStat : highBEs) {
            if (replicas.stream().anyMatch(replica -> beStat.getBeId() == replica.getBackendId())) {
                replicaHighBEs.add(beStat);
            }
            Backend be = infoService.getBackend(beStat.getBeId());
            if (be == null) {
                throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                        "backend is dropped: " + beStat.getBeId());
            }
            hosts.add(be.getHost());
        }
        if (replicaHighBEs.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                    "no replica on high load backend" + isUrgentInfo);
        }

        // select a replica as source
        boolean setSource = false;
        for (Replica replica : replicas) {
            PathSlot slot = backendsWorkingSlots.get(replica.getBackendId());
            if (slot == null) {
                continue;
            }
            long pathHash = slot.takeBalanceSlot(replica.getPathHash());
            if (pathHash != -1) {
                tabletCtx.setSrc(replica);
                setSource = true;
                break;
            }
        }
        if (!setSource) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to take src slot" + isUrgentInfo);
        }

        // Select a low load backend as destination.
        List<BackendLoadStatistic> candidates = Lists.newArrayList();
        for (BackendLoadStatistic beStat : lowBEs) {
            if (beStat.isAvailable() && replicas.stream().noneMatch(r -> r.getBackendId() == beStat.getBeId())) {
                // check if on same host.
                Backend lowBackend = infoService.getBackend(beStat.getBeId());
                if (lowBackend == null) {
                    continue;
                }
                if (!Config.allow_replica_on_same_host && hosts.contains(lowBackend.getHost())) {
                    continue;
                }

                // no replica on this low load backend
                // 1. check if this clone task can make the cluster more balance.
                BalanceStatus bs = beStat.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(), null,
                        false /* not supplement */);
                if (bs != BalanceStatus.OK) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("tablet not fit in BE {}, reason: {}, {}",
                                beStat.getBeId(), bs.getErrMsgs(), isUrgentInfo);
                    }
                    continue;
                }

                if (!Config.be_rebalancer_fuzzy_test && !isUrgent) {
                    boolean moreBalanced = replicaHighBEs.stream().anyMatch(highBeStat ->
                            clusterStat.isMoreBalanced(highBeStat.getBeId(), beStat.getBeId(),
                            tabletCtx.getTabletId(), tabletCtx.getTabletSize(),
                            tabletCtx.getStorageMedium()));
                    if (!moreBalanced) {
                        continue;
                    }
                }

                PathSlot slot = backendsWorkingSlots.get(beStat.getBeId());
                if (slot == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("BE does not have slot: {}", beStat.getBeId());
                    }
                    continue;
                }

                candidates.add(beStat);
            }
        }

        if (candidates.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                    "unable to find low dest backend" + isUrgentInfo);
        }

        List<BePathLoadStatPair> candFitPaths = Lists.newArrayList();
        for (BackendLoadStatistic beStat : candidates) {
            PathSlot slot = backendsWorkingSlots.get(beStat.getBeId());
            if (slot == null) {
                continue;
            }

            List<RootPathLoadStatistic> pathLow = null;
            if (isUrgent) {
                pathLow = beStat.getAvailPaths(tabletCtx.getStorageMedium()).stream()
                        .filter(RootPathLoadStatistic::isGlobalLowUsage)
                        .collect(Collectors.toList());
            } else {
                // classify the paths.
                // And we only select path from 'low' and 'mid' paths
                pathLow = Lists.newArrayList();
                List<RootPathLoadStatistic> pathMid = Lists.newArrayList();
                List<RootPathLoadStatistic> pathHigh = Lists.newArrayList();
                beStat.getPathStatisticByClass(pathLow, pathMid, pathHigh, tabletCtx.getStorageMedium());

                pathLow.addAll(pathMid);
            }
            pathLow.forEach(path -> candFitPaths.add(new BePathLoadStatPair(beStat, path)));
        }

        if (candFitPaths.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, SubCode.DIAGNOSE_IGNORE,
                    "unable to find low dest backend to fit in paths" + isUrgentInfo);
        }

        BePathLoadStatPairComparator comparator = new BePathLoadStatPairComparator(candFitPaths);
        Collections.sort(candFitPaths, comparator);
        for (BePathLoadStatPair bePathLoadStat : candFitPaths) {
            BackendLoadStatistic beStat = bePathLoadStat.getBackendLoadStatistic();
            RootPathLoadStatistic pathStat = bePathLoadStat.getPathLoadStatistic();

            PathSlot slot = backendsWorkingSlots.get(beStat.getBeId());
            if (slot == null) {
                continue;
            }
            if (slot.takeBalanceSlot(pathStat.getPathHash()) != -1) {
                tabletCtx.setDest(beStat.getBeId(), pathStat.getPathHash());
                return;
            }
        }

        throw new SchedException(Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                "unable to take dest slot" + isUrgentInfo);
    }

}
