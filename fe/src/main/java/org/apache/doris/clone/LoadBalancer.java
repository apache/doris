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
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
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

/*
 * LoadBalancer is responsible for 
 * 1. selecting alternative tablets from high load backends, and return them to tablet scheduler.
 * 2. given a tablet, find a backend to migration.
 */
public class LoadBalancer {
    private static final Logger LOG = LogManager.getLogger(LoadBalancer.class);

    private Map<String, ClusterLoadStatistic> statisticMap;
    private TabletInvertedIndex invertedIndex;
    private SystemInfoService infoService;

    public LoadBalancer(Map<String, ClusterLoadStatistic> statisticMap) {
        this.statisticMap = statisticMap;
        this.invertedIndex = Catalog.getCurrentInvertedIndex();
        this.infoService = Catalog.getCurrentSystemInfo();
    }

    public List<TabletSchedCtx> selectAlternativeTablets() {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        for (Map.Entry<String, ClusterLoadStatistic> entry : statisticMap.entrySet()) {
            for (TStorageMedium medium : TStorageMedium.values()) {
                alternativeTablets.addAll(selectAlternativeTabletsForCluster(entry.getKey(),
                        entry.getValue(), medium));
            }
        }
        return alternativeTablets;
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
    private List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            String clusterName, ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();

        // get classification of backends
        List<BackendLoadStatistic> lowBEs = Lists.newArrayList();
        List<BackendLoadStatistic> midBEs = Lists.newArrayList();
        List<BackendLoadStatistic> highBEs = Lists.newArrayList();
        clusterStat.getBackendStatisticByClass(lowBEs, midBEs, highBEs, medium);
        
        if (lowBEs.isEmpty() && highBEs.isEmpty()) {
            LOG.info("cluster is balance: {} with medium: {}. skip", clusterName, medium);
            return alternativeTablets;
        }

        // first we should check if low backends is available.
        // if all low backends is not available, we should not start balance
        if (lowBEs.stream().allMatch(b -> !b.isAvailable())) {
            LOG.info("all low load backends is dead: {} with medium: {}. skip",
                    lowBEs.stream().mapToLong(b -> b.getBeId()).toArray(), medium);
            return alternativeTablets;
        }
        
        if (lowBEs.stream().allMatch(b -> !b.hasAvailDisk())) {
            LOG.info("all low load backends have no available disk with medium: {}. skip",
                    lowBEs.stream().mapToLong(b -> b.getBeId()).toArray(), medium);
            return alternativeTablets;
        }

        // get the number of low load paths. and we should at most select this number of tablets
        long numOfLowPaths = lowBEs.stream().filter(b -> b.isAvailable() && b.hasAvailDisk()).mapToLong(
                b -> b.getAvailPathNum(medium)).sum();
        LOG.info("get number of low load paths: {}, with medium: {}", numOfLowPaths, medium);

        ColocateTableIndex colocateTableIndex = Catalog.getCurrentColocateIndex();
        // choose tablets from high load backends.
        // BackendLoadStatistic is sorted by load score in ascend order,
        // so we need to traverse it from last to first
        OUTER: for (int i = highBEs.size() - 1; i >= 0; i--) {
            BackendLoadStatistic beStat = highBEs.get(i);

            // classify the paths.
            Set<Long> pathLow = Sets.newHashSet();
            Set<Long> pathMid = Sets.newHashSet();
            Set<Long> pathHigh = Sets.newHashSet();
            beStat.getPathStatisticByClass(pathLow, pathMid, pathHigh, medium);
            // we only select tablets from available mid and high load path
            pathHigh.addAll(pathMid);
            
            // get all tablets on this backend, and shuffle them for random selection
            List<Long> tabletIds = invertedIndex.getTabletIdsByBackendIdAndStorageMedium(beStat.getBeId(), medium);
            Collections.shuffle(tabletIds);

            // for each path, we try to select at most BALANCE_SLOT_NUM_FOR_PATH tablets
            Map<Long, Integer> remainingPaths = Maps.newHashMap();
            for (Long pathHash : pathHigh) {
                remainingPaths.put(pathHash, TabletScheduler.BALANCE_SLOT_NUM_FOR_PATH);
            }

            // select tablet from shuffled tablets
            for (Long tabletId : tabletIds) {
                if (remainingPaths.isEmpty()) {
                    break;
                }

                Replica replica = invertedIndex.getReplica(tabletId, beStat.getBeId());
                if (replica == null) {
                    continue;
                }

                // check if replica's is on 'high' or 'mid' path.
                // and only select it if the selected tablets num of this path
                // does not exceed the limit (BALANCE_SLOT_NUM_FOR_PATH).
                long replicaPathHash = replica.getPathHash();
                if (remainingPaths.containsKey(replicaPathHash)) {
                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (tabletMeta == null) {
                        continue;
                    }
                    
                    if (colocateTableIndex.isColocateTable(tabletMeta.getTableId())) {
                        continue;
                    }

                    TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, clusterName,
                            tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                            tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
                    // balance task's priority is always LOW
                    tabletCtx.setOrigPriority(Priority.LOW);

                    alternativeTablets.add(tabletCtx);
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

        LOG.info("select alternative tablets for cluster: {}, medium: {}, num: {}, detail: {}",
                clusterName, medium, alternativeTablets.size(),
                alternativeTablets.stream().mapToLong(t -> t.getTabletId()).toArray());
        return alternativeTablets;
    }

    /*
     * Create a clone task of this selected tablet for balance.
     * 1. Check if this tablet has replica on high load backend. If not, the balance will be cancelled.
     *    If yes, select a replica as source replica.
     * 2. Select a low load backend as destination. And tablet should not has replica on this backend.
     * 3. Create a clone task.
     */
    public void createBalanceTask(TabletSchedCtx tabletCtx, Map<Long, PathSlot> backendsWorkingSlots,
            AgentBatchTask batchTask) throws SchedException {
        ClusterLoadStatistic clusterStat = statisticMap.get(tabletCtx.getCluster());
        if (clusterStat == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }

        // get classification of backends
        List<BackendLoadStatistic> lowBe = Lists.newArrayList();
        List<BackendLoadStatistic> midBe = Lists.newArrayList();
        List<BackendLoadStatistic> highBe = Lists.newArrayList();
        clusterStat.getBackendStatisticByClass(lowBe, midBe, highBe, tabletCtx.getStorageMedium());

        if (lowBe.isEmpty() && highBe.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster is balance");
        }

        // if all low backends is not available, return
        if (lowBe.stream().allMatch(b -> !b.isAvailable())) {
            throw new SchedException(Status.UNRECOVERABLE, "all low load backends is unavailable");
        }

        List<Replica> replicas = tabletCtx.getReplicas();

        // Check if this tablet has replica on high load backend.
        // Also create a set to save hosts of this tablet.
        Set<String> hosts = Sets.newHashSet();
        boolean hasHighReplica = false;
        for (Replica replica : replicas) {
            if (highBe.stream().anyMatch(b -> b.getBeId() == replica.getBackendId())) {
                hasHighReplica = true;
            }
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                throw new SchedException(Status.UNRECOVERABLE, "backend is dropped: " + replica.getBackendId());
            }
            hosts.add(be.getHost());
        }
        if (!hasHighReplica) {
            throw new SchedException(Status.UNRECOVERABLE, "no replica on high load backend");
        }

        // select a replica as source
        boolean setSource = false;
        for (Replica replica : replicas) {
            PathSlot slot = backendsWorkingSlots.get(replica.getBackendId());
            if (slot == null) {
                continue;
            }
            long pathHash = slot.takeBalanceSlot(replica.getPathHash());
            if (pathHash == -1) {
                continue;
            } else {
                tabletCtx.setSrc(replica);
                setSource = true;
                break;
            }
        }
        if (!setSource) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to take src slot");
        }

        // Select a low load backend as destination.
        boolean setDest = false;
        for (BackendLoadStatistic beStat : lowBe) {
            if (beStat.isAvailable() && !replicas.stream().anyMatch(r -> r.getBackendId() == beStat.getBeId())) {
                // check if on same host.
                Backend lowBackend = infoService.getBackend(beStat.getBeId());
                if (lowBackend == null) {
                    continue;
                }
                if (hosts.contains(lowBackend.getHost())) {
                    continue;
                }
                
                // no replica on this low load backend
                // 1. check if this clone task can make the cluster more balance.
                List<RootPathLoadStatistic> availPaths = Lists.newArrayList();
                BalanceStatus bs;
                if ((bs = beStat.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(), availPaths,
                        false /* not supplement */)) != BalanceStatus.OK) {
                    LOG.debug("tablet not fit in BE {}, reason: {}", beStat.getBeId(), bs.getErrMsgs());
                    continue;
                }

                if (!clusterStat.isMoreBalanced(tabletCtx.getSrcBackendId(), beStat.getBeId(),
                        tabletCtx.getTabletId(), tabletCtx.getTabletSize(), tabletCtx.getStorageMedium())) {
                    continue;
                }

                PathSlot slot = backendsWorkingSlots.get(beStat.getBeId());
                if (slot == null) {
                    LOG.debug("BE does not have slot: {}", beStat.getBeId());
                    continue;
                }

                // classify the paths.
                // And we only select path from 'low' and 'mid' paths
                Set<Long> pathLow = Sets.newHashSet();
                Set<Long> pathMid = Sets.newHashSet();
                Set<Long> pathHigh = Sets.newHashSet();
                beStat.getPathStatisticByClass(pathLow, pathMid, pathHigh, tabletCtx.getStorageMedium());
                pathLow.addAll(pathMid);

                long pathHash = slot.takeAnAvailBalanceSlotFrom(pathLow);
                if (pathHash == -1) {
                    LOG.debug("paths has no available balance slot: {}", pathLow);
                    continue;
                } else {
                    tabletCtx.setDest(beStat.getBeId(), pathHash);
                    setDest = true;
                    break;
                }
            }
        }

        if (!setDest) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to find low backend");
        }

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }
}
