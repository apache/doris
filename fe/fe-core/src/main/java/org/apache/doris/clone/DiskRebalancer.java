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

import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.TabletSchedCtx.BalanceType;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*

 * This DiskBalancer is different from other Balancers which takes care of cluster-wide data balancing.
 * This DiskBalancer chooses a backend and moves tablet from one disk to another.
 * DiskRebalancer strategy:
 * 1. only works while the cluster is balanced(which means the cluster has no high and mid load backends)
 * 1.1 if user has given prio backends, then select tablets from prio backends no matter cluster is balanced or not.
 * 2. selecting alternative tablets from mid load backends, and return them to tablet scheduler.
 * 3. given a tablet which has src path(disk), find a path(disk) to migration.
 */
public class DiskRebalancer extends Rebalancer {
    private static final Logger LOG = LogManager.getLogger(DiskRebalancer.class);

    public DiskRebalancer(SystemInfoService infoService, TabletInvertedIndex invertedIndex,
            Map<Long, PathSlot> backendsWorkingSlots) {
        super(infoService, invertedIndex, backendsWorkingSlots);
    }

    public List<BackendLoadStatistic> filterByPrioBackends(List<BackendLoadStatistic> bes) {
        List<BackendLoadStatistic> stats = Lists.newArrayList();
        for (BackendLoadStatistic backend : bes) {
            long backendId = backend.getBeId();
            Long timeoutS = prioBackends.getOrDefault(backendId, 0L);
            if (timeoutS != 0) {
                if (timeoutS > System.currentTimeMillis()) {
                    // remove backends from prio if timeout
                    prioBackends.remove(backendId);
                    continue;
                }
                stats.add(backend);
            }
        }
        return stats;
    }

    // true means BE has low and high paths for balance after reclassification
    private boolean checkAndReclassifyPaths(Set<Long> pathLow, Set<Long> pathMid, Set<Long> pathHigh) {
        if (pathLow.isEmpty() && pathHigh.isEmpty()) {
            // balanced
            return false;
        }
        if (pathLow.isEmpty()) {
            // mid => low
            pathLow.addAll(pathMid);
        } else if (pathHigh.isEmpty()) {
            // mid => high
            pathHigh.addAll(pathMid);
        }
        if (pathLow.isEmpty() || pathHigh.isEmpty()) {
            // check again
            return false;
        }
        return true;
    }

    /*
     * Try to select alternative tablets to balance the disks.
     * 1. Classify the backend into low, mid and high class by load score.
     * 2. Try to select tablets from mid load backends.
     *      1. Here we only select alternative tablets, without considering selected tablets' status,
     *         and whether it is benefit for balance (All these will be checked in tablet scheduler)
     *      2. Only select tablets from 'mid' backends.
     *      3. Only select tablets from 'high' paths.
     * 3. Try to select tablets from prio backends.
     *
     * Here we only select tablets from mid load node, do not set its dest, all this will be set
     * when this tablet is being scheduled in tablet scheduler.
     *
     * NOTICE that we may select any available tablets here, ignore their state.
     * The state will be checked when being scheduled in tablet scheduler.
     */
    @Override
    protected List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            LoadStatisticForTag clusterStat, TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();

        // get classification of backends
        List<BackendLoadStatistic> lowBEs = Lists.newArrayList();
        List<BackendLoadStatistic> midBEs = Lists.newArrayList();
        List<BackendLoadStatistic> highBEs = Lists.newArrayList();
        clusterStat.getBackendStatisticByClass(lowBEs, midBEs, highBEs, medium);

        if (Config.tablet_rebalancer_type.equalsIgnoreCase("partition")) {
            PartitionRebalancer rebalancer = (PartitionRebalancer) Env.getCurrentEnv()
                    .getTabletScheduler().getRebalancer();
            if (rebalancer != null && rebalancer.checkCacheEmptyForLong()) {
                midBEs.addAll(lowBEs);
                midBEs.addAll(highBEs);
            }
        } else if (!(lowBEs.isEmpty() && highBEs.isEmpty())) {
            // the cluster is not balanced
            if (prioBackends.isEmpty()) {
                LOG.info("cluster is not balanced with medium: {}. skip", medium);
                return alternativeTablets;
            } else {
                // prioBEs are not empty, we only schedule prioBEs' disk balance task
                midBEs.addAll(lowBEs);
                midBEs.addAll(highBEs);
                midBEs = filterByPrioBackends(midBEs);
            }
        }

        // first we should check if mid backends is available.
        // if all mid backends is not available, we should not start balance
        if (midBEs.stream().noneMatch(BackendLoadStatistic::isAvailable)) {
            LOG.debug("all mid load backends is dead: {} with medium: {}. skip",
                    midBEs.stream().mapToLong(BackendLoadStatistic::getBeId).toArray(), medium);
            return alternativeTablets;
        }

        if (midBEs.stream().noneMatch(BackendLoadStatistic::hasAvailDisk)) {
            LOG.info("all mid load backends {} have no available disk with medium: {}. skip",
                    midBEs.stream().mapToLong(BackendLoadStatistic::getBeId).toArray(), medium);
            return alternativeTablets;
        }

        Set<Long> alternativeTabletIds = Sets.newHashSet();
        Set<Long> unbalancedBEs = Sets.newHashSet();
        // choose tablets from backends randomly.
        Collections.shuffle(midBEs);
        for (int i = midBEs.size() - 1; i >= 0; i--) {
            BackendLoadStatistic beStat = midBEs.get(i);
            PathSlot pathSlot = backendsWorkingSlots.get(beStat.getBeId());
            if (pathSlot == null) {
                continue;
            }

            // classify the paths.
            Set<Long> pathLow = Sets.newHashSet();
            Set<Long> pathMid = Sets.newHashSet();
            Set<Long> pathHigh = Sets.newHashSet();
            // we only select tablets from available high load path
            beStat.getPathStatisticByClass(pathLow, pathMid, pathHigh, medium);
            // check if BE has low and high paths for balance after reclassification
            if (!checkAndReclassifyPaths(pathLow, pathMid, pathHigh)) {
                continue;
            }

            // get all tablets on this backend, and shuffle them for random selection
            List<Long> tabletIds = invertedIndex.getTabletIdsByBackendIdAndStorageMedium(beStat.getBeId(), medium);
            Collections.shuffle(tabletIds);

            // for each path, we try to select at most BALANCE_SLOT_NUM_FOR_PATH tablets
            Map<Long, Integer> remainingPaths = Maps.newHashMap();
            for (Long pathHash : pathHigh) {
                int availBalanceNum = pathSlot.getAvailableBalanceNum(pathHash);
                if (availBalanceNum > 0) {
                    remainingPaths.put(pathHash, availBalanceNum);
                }
            }

            if (remainingPaths.isEmpty()) {
                continue;
            }

            // select tablet from shuffled tablets
            for (Long tabletId : tabletIds) {
                if (alternativeTabletIds.contains(tabletId)) {
                    continue;
                }
                Replica replica = invertedIndex.getReplica(tabletId, beStat.getBeId());
                if (replica == null) {
                    continue;
                }
                // ignore empty replicas as they do not make disk more balance. (disk usage)
                if (replica.getDataSize() == 0) {
                    continue;
                }

                // check if replica's is on 'high' path.
                // and only select it if the selected tablets num of this path
                // does not exceed the limit (BALANCE_SLOT_NUM_FOR_PATH).
                long replicaPathHash = replica.getPathHash();
                if (remainingPaths.containsKey(replicaPathHash)) {
                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (tabletMeta == null) {
                        continue;
                    }

                    TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                            tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                            tabletMeta.getIndexId(), tabletId, null /* replica alloc is not used for balance*/,
                            System.currentTimeMillis());
                    // we set temp src here to simplify completeSchedCtx method, and avoid take slot here
                    tabletCtx.setTempSrc(replica);
                    tabletCtx.setTag(clusterStat.getTag());
                    if (prioBackends.containsKey(beStat.getBeId())) {
                        // priority of balance task of prio BE is NORMAL
                        tabletCtx.setPriority(Priority.NORMAL);
                    } else {
                        // balance task's default priority is LOW
                        tabletCtx.setPriority(Priority.LOW);
                    }
                    // we must set balanceType to DISK_BALANCE for create migration task
                    tabletCtx.setBalanceType(BalanceType.DISK_BALANCE);

                    alternativeTablets.add(tabletCtx);
                    alternativeTabletIds.add(tabletId);
                    unbalancedBEs.add(beStat.getBeId());
                    // update remaining paths
                    int remaining = remainingPaths.get(replicaPathHash) - 1;
                    if (remaining <= 0) {
                        remainingPaths.remove(replicaPathHash);
                    } else {
                        remainingPaths.put(replicaPathHash, remaining);
                    }
                }
            }
        } // end for mid backends

        // remove balanced BEs from prio backends
        prioBackends.keySet().removeIf(id -> !unbalancedBEs.contains(id));
        if (!alternativeTablets.isEmpty()) {
            LOG.info("select alternative tablets, medium: {}, num: {}, detail: {}",
                    medium, alternativeTablets.size(),
                    alternativeTablets.stream().mapToLong(TabletSchedCtx::getTabletId).toArray());
        }
        return alternativeTablets;
    }

    /*
     * Create a StorageMediaMigrationTask of this selected tablet for balance.
     * 1. Check if the cluster is balanced. if not, the balance will be cancelled.
     * 2. Check if the src replica still on high load path. If not, the balance will be cancelled.
     * 3. Select a low load path from this backend as destination.
     */
    @Override
    public void completeSchedCtx(TabletSchedCtx tabletCtx) throws SchedException {
        LoadStatisticForTag clusterStat = statisticMap.get(tabletCtx.getTag());
        if (clusterStat == null) {
            throw new SchedException(Status.UNRECOVERABLE,
                    String.format("tag %s does not exist", tabletCtx.getTag()));
        }
        if (tabletCtx.getTempSrcBackendId() == -1 || tabletCtx.getTempSrcPathHash() == -1) {
            throw new SchedException(Status.UNRECOVERABLE,
                "src does not appear to be set correctly, something goes wrong");
        }
        Replica replica = invertedIndex.getReplica(tabletCtx.getTabletId(), tabletCtx.getTempSrcBackendId());
        // check src replica still there
        if (replica == null || replica.getPathHash() != tabletCtx.getTempSrcPathHash()) {
            throw new SchedException(Status.UNRECOVERABLE, "src replica may be rebalanced");
        }
        // ignore empty replicas as they do not make disk more balance
        if (replica.getDataSize() == 0) {
            throw new SchedException(Status.UNRECOVERABLE, "size of src replica is zero");
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrException(tabletCtx.getDbId(),
                s -> new SchedException(Status.UNRECOVERABLE, "db " + tabletCtx.getDbId() + " does not exist"));
        OlapTable tbl = (OlapTable) db.getTableOrException(tabletCtx.getTblId(),
                s -> new SchedException(Status.UNRECOVERABLE, "tbl " + tabletCtx.getTblId() + " does not exist"));
        DataProperty dataProperty = tbl.getPartitionInfo().getDataProperty(tabletCtx.getPartitionId());
        if (dataProperty == null) {
            throw new SchedException(Status.UNRECOVERABLE, "data property is null");
        }
        String storagePolicy = dataProperty.getStoragePolicy();
        if (!Strings.isNullOrEmpty(storagePolicy)) {
            throw new SchedException(Status.UNRECOVERABLE, "disk balance not support for cooldown storage");
        }

        // check src slot
        PathSlot slot = backendsWorkingSlots.get(replica.getBackendId());
        if (slot == null) {
            LOG.debug("BE does not have slot: {}", replica.getBackendId());
            throw new SchedException(Status.UNRECOVERABLE, "unable to take src slot");
        }
        long pathHash = slot.takeBalanceSlot(replica.getPathHash());
        if (pathHash == -1) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to take src slot");
        }
        // after take src slot, we can set src replica now
        tabletCtx.setSrc(replica);

        BackendLoadStatistic beStat = clusterStat.getBackendLoadStatistic(replica.getBackendId());
        if (!beStat.isAvailable()) {
            throw new SchedException(Status.UNRECOVERABLE, "the backend is not available");
        }
        // classify the paths.
        // If src path is 'high', then we can select path from 'low' and 'mid'
        // If src path is 'mid', then we can only select path from 'low'
        // If src path is 'low', then we have nothing to do
        Set<Long> pathLow = Sets.newHashSet();
        Set<Long> pathMid = Sets.newHashSet();
        Set<Long> pathHigh = Sets.newHashSet();
        beStat.getPathStatisticByClass(pathLow, pathMid, pathHigh, tabletCtx.getStorageMedium());
        if (pathHigh.contains(replica.getPathHash())) {
            pathLow.addAll(pathMid);
        } else if (!pathMid.contains(replica.getPathHash())) {
            throw new SchedException(Status.UNRECOVERABLE, "src path is low load");
        }
        // check if this migration task can make the be's disks more balance.
        List<RootPathLoadStatistic> availPaths = Lists.newArrayList();
        BalanceStatus bs;
        if ((bs = beStat.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(), availPaths,
                false /* not supplement */)) != BalanceStatus.OK) {
            LOG.debug("tablet not fit in BE {}, reason: {}", beStat.getBeId(), bs.getErrMsgs());
            throw new SchedException(Status.UNRECOVERABLE, "tablet not fit in BE");
        }
        // Select a low load path as destination.
        boolean setDest = false;
        for (RootPathLoadStatistic stat : availPaths) {
            // check if avail path is src path
            if (stat.getPathHash() == replica.getPathHash()) {
                continue;
            }
            // check if avail path is low path
            if (!pathLow.contains(stat.getPathHash())) {
                LOG.debug("the path :{} is not low load", stat.getPathHash());
                continue;
            }
            if (!beStat.isMoreBalanced(tabletCtx.getSrcPathHash(), stat.getPathHash(),
                    tabletCtx.getTabletId(), tabletCtx.getTabletSize(), tabletCtx.getStorageMedium())) {
                LOG.debug("the path :{} can not make more balance", stat.getPathHash());
                continue;
            }
            long destPathHash = slot.takeBalanceSlot(stat.getPathHash());
            if (destPathHash == -1) {
                continue;
            }
            tabletCtx.setDest(beStat.getBeId(), destPathHash, stat.getPath());
            setDest = true;
            break;
        }

        if (!setDest) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to find low load path");
        }
    }
}
