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

import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.SchedException.SubCode;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/*
 * PartitionRebalancer will decrease the skew of partitions. The skew of the partition is defined as the difference
 * between the maximum replica count of the partition over all bes and the minimum replica count over all bes.
 * Only consider about the replica count for each partition, never consider the replica size(disk usage).
 *
 * We use TwoDimensionalGreedyRebalanceAlgo to get partition moves(one PartitionMove is <partition id, from be, to be>).
 * It prefers a move that reduce the skew of the cluster when we want to rebalance a max skew partition.
 *
 * selectAlternativeTabletsForCluster() must set the tablet id, so we need to select tablet for each move in this phase
 * (as TabletMove).
 */
public class PartitionRebalancer extends Rebalancer {
    private static final Logger LOG = LogManager.getLogger(PartitionRebalancer.class);

    private final TwoDimensionalGreedyRebalanceAlgo algo = new TwoDimensionalGreedyRebalanceAlgo();

    private final MovesCacheMap movesCacheMap = new MovesCacheMap();

    private final AtomicLong counterBalanceMoveCreated = new AtomicLong(0);
    private final AtomicLong counterBalanceMoveSucceeded = new AtomicLong(0);

    public PartitionRebalancer(SystemInfoService infoService, TabletInvertedIndex invertedIndex,
            Map<Long, PathSlot> backendsWorkingSlots) {
        super(infoService, invertedIndex, backendsWorkingSlots);
    }

    @Override
    protected List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            LoadStatisticForTag clusterStat, TStorageMedium medium) {
        MovesCacheMap.MovesCache movesInProgress = movesCacheMap.getCache(clusterStat.getTag(), medium);
        Preconditions.checkNotNull(movesInProgress,
                "clusterStat is got from statisticMap, movesCacheMap should have the same entry");

        // Iterating through Cache.asMap().values() does not reset access time for the entries you retrieve.
        List<TabletMove> movesInProgressList = movesInProgress.get().asMap().values()
                .stream().map(p -> p.first).collect(Collectors.toList());
        List<Long> toDeleteKeys = Lists.newArrayList();

        // The problematic movements will be found in buildClusterInfo(), so here is a simply move completion check
        // of moves which have valid ToDeleteReplica.
        List<TabletMove> movesNeedCheck = movesInProgress.get().asMap().values()
                .stream().filter(p -> p.second != -1L).map(p -> p.first).collect(Collectors.toList());
        checkMovesCompleted(movesNeedCheck, toDeleteKeys);

        ClusterBalanceInfo clusterBalanceInfo = new ClusterBalanceInfo();
        // We should assume the in-progress moves have been succeeded to avoid producing the same moves.
        // Apply in-progress moves to current cluster stats, use TwoDimensionalGreedyAlgo.ApplyMove for simplicity.
        if (!buildClusterInfo(clusterStat, medium, movesInProgressList, clusterBalanceInfo, toDeleteKeys)) {
            return Lists.newArrayList();
        }

        // Just delete the completed or problematic moves
        if (!toDeleteKeys.isEmpty()) {
            movesInProgress.get().invalidateAll(toDeleteKeys);
            movesInProgressList = movesInProgressList.stream()
                    .filter(m -> !toDeleteKeys.contains(m.tabletId)).collect(Collectors.toList());
        }

        // The balancing tasks of other cluster or medium might have failed. We use the upper limit value
        // `total num of in-progress moves` to avoid useless selections.
        if (movesCacheMap.size() > Config.max_balancing_tablets) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Total in-progress moves > {}", Config.max_balancing_tablets);
            }
            return Lists.newArrayList();
        }

        NavigableSet<Long> skews = clusterBalanceInfo.partitionInfoBySkew.keySet();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Medium {}: peek max skew {}, assume {} in-progress moves are succeeded {}", medium,
                    skews.isEmpty() ? 0 : skews.last(), movesInProgressList.size(), movesInProgressList);
        }

        List<TwoDimensionalGreedyRebalanceAlgo.PartitionMove> moves
                = algo.getNextMoves(clusterBalanceInfo, Config.partition_rebalance_max_moves_num_per_selection);

        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        Set<Long> inProgressIds = movesInProgressList.stream().map(m -> m.tabletId).collect(Collectors.toSet());
        Random rand = new SecureRandom();
        for (TwoDimensionalGreedyRebalanceAlgo.PartitionMove move : moves) {
            // Find all tablets of the specified partition that would have a replica at the source be,
            // but would not have a replica at the destination be. That is to satisfy the restriction
            // of having no more than one replica of the same tablet per be.
            List<Long> tabletIds = invertedIndex.getTabletIdsByBackendIdAndStorageMedium(move.fromBe, medium);
            if (tabletIds.isEmpty()) {
                continue;
            }

            Set<Long> invalidIds = Sets.newHashSet(
                    invertedIndex.getTabletIdsByBackendIdAndStorageMedium(move.toBe, medium));

            BiPredicate<Long, TabletMeta> canMoveTablet = (Long tabletId, TabletMeta tabletMeta) -> {
                return canBalanceTablet(tabletMeta)
                        && tabletMeta.getPartitionId() == move.partitionId
                        && tabletMeta.getIndexId() == move.indexId
                        && !invalidIds.contains(tabletId)
                        && !inProgressIds.contains(tabletId);
            };

            // Random pick one candidate to create tabletSchedCtx
            int startIdx = rand.nextInt(tabletIds.size());
            long pickedTabletId = -1L;
            TabletMeta pickedTabletMeta = null;
            for (int i = startIdx; i < tabletIds.size(); i++) {
                long tabletId = tabletIds.get(i);
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (canMoveTablet.test(tabletId, tabletMeta)) {
                    pickedTabletId = tabletId;
                    pickedTabletMeta = tabletMeta;
                    break;
                }
            }

            if (pickedTabletId == -1L) {
                for (int i = 0; i < startIdx; i++) {
                    long tabletId = tabletIds.get(i);
                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (canMoveTablet.test(tabletId, tabletMeta)) {
                        pickedTabletId = tabletId;
                        pickedTabletMeta = tabletMeta;
                        break;
                    }
                }
            }

            if (pickedTabletId == -1L) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cann't picked tablet id for move {}", move);
                }
                continue;
            }

            TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                    pickedTabletMeta.getDbId(), pickedTabletMeta.getTableId(), pickedTabletMeta.getPartitionId(),
                    pickedTabletMeta.getIndexId(), pickedTabletId, null /* replica alloc is not used for balance*/,
                    System.currentTimeMillis());
            tabletCtx.setTag(clusterStat.getTag());
            // Balance task's priority is always LOW
            tabletCtx.setPriority(TabletSchedCtx.Priority.LOW);
            alternativeTablets.add(tabletCtx);
            // Pair<Move, ToDeleteReplicaId>, ToDeleteReplicaId should be -1L before scheduled successfully
            movesInProgress.get().put(pickedTabletId,
                    Pair.of(new TabletMove(pickedTabletId, move.fromBe, move.toBe), -1L));
            counterBalanceMoveCreated.incrementAndGet();
            // Synchronize with movesInProgress
            inProgressIds.add(pickedTabletId);
        }

        if (moves.isEmpty()) {
            // Balanced cluster should not print too much log messages, so we log it with level debug.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Medium {}: cluster is balanced.", medium);
            }
        } else {
            LOG.info("Medium {}: get {} moves, actually select {} alternative tablets to move. Tablets detail: {}",
                    medium, moves.size(), alternativeTablets.size(),
                    alternativeTablets.stream().mapToLong(TabletSchedCtx::getTabletId).toArray());
        }
        return alternativeTablets;
    }

    private boolean buildClusterInfo(LoadStatisticForTag clusterStat, TStorageMedium medium,
            List<TabletMove> movesInProgress, ClusterBalanceInfo info, List<Long> toDeleteKeys) {
        Preconditions.checkState(info.beByTotalReplicaCount.isEmpty() && info.partitionInfoBySkew.isEmpty(), "");

        // If we wanna modify the PartitionBalanceInfo in info.beByTotalReplicaCount, deep-copy it
        info.beByTotalReplicaCount.putAll(clusterStat.getBeByTotalReplicaMap(medium));
        info.partitionInfoBySkew.putAll(clusterStat.getSkewMap(medium));

        // Skip the toDeleteKeys
        List<TabletMove> filteredMoves = movesInProgress.stream()
                .filter(m -> !toDeleteKeys.contains(m.tabletId)).collect(Collectors.toList());

        for (TabletMove move : filteredMoves) {
            TabletMeta meta = invertedIndex.getTabletMeta(move.tabletId);
            if (meta == null) {
                // Move's tablet is invalid, need delete it
                toDeleteKeys.add(move.tabletId);
                continue;
            }

            TwoDimensionalGreedyRebalanceAlgo.PartitionMove partitionMove
                    = new TwoDimensionalGreedyRebalanceAlgo.PartitionMove(
                            meta.getPartitionId(), meta.getIndexId(), move.fromBe, move.toBe);
            boolean st = TwoDimensionalGreedyRebalanceAlgo.applyMove(
                    partitionMove, info.beByTotalReplicaCount, info.partitionInfoBySkew);
            if (!st) {
                // Can't apply this move, mark it failed, continue to apply the next.
                toDeleteKeys.add(move.tabletId);
            }
        }
        return true;
    }

    private void checkMovesCompleted(List<TabletMove> moves, List<Long> toDeleteKeys) {
        boolean moveIsComplete;
        for (TabletMove move : moves) {
            moveIsComplete = checkMoveCompleted(move);
            // If the move was completed, remove it
            if (moveIsComplete) {
                toDeleteKeys.add(move.tabletId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Move {} is completed. The cur dist: {}", move,
                            invertedIndex.getReplicasByTabletId(move.tabletId).stream()
                                    .map(Replica::getBackendIdWithoutException).collect(Collectors.toList()));
                }
                counterBalanceMoveSucceeded.incrementAndGet();
            }
        }
    }

    // Move completed: fromBe doesn't have a replica and toBe has a replica
    private boolean checkMoveCompleted(TabletMove move) {
        Long tabletId = move.tabletId;
        List<Long> bes = invertedIndex.getReplicasByTabletId(tabletId).stream()
                .map(Replica::getBackendIdWithoutException).collect(Collectors.toList());
        return !bes.contains(move.fromBe) && bes.contains(move.toBe);
    }

    @Override
    protected void completeSchedCtx(TabletSchedCtx tabletCtx)
            throws SchedException {
        MovesCacheMap.MovesCache movesInProgress = movesCacheMap.getCache(tabletCtx.getTag(),
                tabletCtx.getStorageMedium());
        Preconditions.checkNotNull(movesInProgress,
                "clusterStat is got from statisticMap, movesInProgressMap should have the same entry");

        try {
            Pair<TabletMove, Long> pair = movesInProgress.get().getIfPresent(tabletCtx.getTabletId());
            Preconditions.checkNotNull(pair, "No cached move for tablet: " + tabletCtx.getTabletId());

            TabletMove move = pair.first;
            checkMoveValidation(move);

            // Check src replica's validation
            Replica srcReplica = tabletCtx.getTablet().getReplicaByBackendId(move.fromBe);
            Preconditions.checkNotNull(srcReplica);
            TabletScheduler.PathSlot slot = backendsWorkingSlots.get(srcReplica.getBackendIdWithoutException());
            Preconditions.checkNotNull(slot, "unable to get fromBe "
                    + srcReplica.getBackendIdWithoutException() + " slot");
            if (slot.takeBalanceSlot(srcReplica.getPathHash()) != -1) {
                tabletCtx.setSrc(srcReplica);
            } else {
                throw new SchedException(SchedException.Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                        "no slot for src replica " + srcReplica + ", pathHash " + srcReplica.getPathHash());
            }

            // Choose a path in destination
            LoadStatisticForTag loadStat = statisticMap.get(tabletCtx.getTag());
            Preconditions.checkNotNull(loadStat, "tag does not exist: " + tabletCtx.getTag());
            BackendLoadStatistic beStat = loadStat.getBackendLoadStatistic(move.toBe);
            Preconditions.checkNotNull(beStat);
            slot = backendsWorkingSlots.get(move.toBe);
            Preconditions.checkNotNull(slot, "unable to get slot of toBe " + move.toBe);

            List<RootPathLoadStatistic> paths = beStat.getPathStatistics();
            List<Long> availPath = paths.stream().filter(path -> path.getStorageMedium() == tabletCtx.getStorageMedium()
                            && path.isFit(tabletCtx.getTabletSize(), false) == BalanceStatus.OK)
                    .map(RootPathLoadStatistic::getPathHash).collect(Collectors.toList());
            long pathHash = slot.takeAnAvailBalanceSlotFrom(availPath, tabletCtx.getTag(),
                    tabletCtx.getStorageMedium());
            if (pathHash == -1) {
                throw new SchedException(SchedException.Status.SCHEDULE_FAILED, SubCode.WAITING_SLOT,
                        "paths has no available balance slot: " + availPath);
            }

            tabletCtx.setDest(beStat.getBeId(), pathHash);

            // ToDeleteReplica is the source replica
            pair.second = srcReplica.getId();
        } catch (IllegalStateException | NullPointerException e) {
            // Problematic move should be invalidated immediately
            movesInProgress.get().invalidate(tabletCtx.getTabletId());
            throw new SchedException(SchedException.Status.UNRECOVERABLE, e.getMessage());
        }
    }

    // The validation check cannot be accurate, cuz the production of moves do have ordering.
    // If some moves failed, the cluster & partition skew is different to the skew when we getNextMove.
    // So we can't do skew check.
    // Just do some basic checks, e.g. server available.
    private void checkMoveValidation(TabletMove move) throws IllegalStateException {
        boolean fromAvailable = infoService.checkBackendScheduleAvailable(move.fromBe);
        boolean toAvailable = infoService.checkBackendScheduleAvailable(move.toBe);
        Preconditions.checkState(fromAvailable && toAvailable,
                move + "'s bes are not all available: from " + fromAvailable + ", to " + toAvailable);
        // To be improved
    }

    @Override
    public void onTabletFailed(TabletSchedCtx tabletCtx) {
        movesCacheMap.invalidateTablet(tabletCtx);
    }

    @Override
    public Long getToDeleteReplicaId(TabletSchedCtx tabletCtx) {
        // We don't invalidate the cached move here, cuz the redundant repair progress is just started.
        // The move should be invalidated by TTL or Algo.CheckMoveCompleted()
        Pair<TabletMove, Long> pair = movesCacheMap.getTabletMove(tabletCtx);
        if (pair != null) {
            //Preconditions.checkState(pair.second != -1L);
            return pair.second;
        } else {
            return (long) -1;
        }
    }

    @Override
    public void invalidateToDeleteReplicaId(TabletSchedCtx tabletCtx) {
        movesCacheMap.invalidateTablet(tabletCtx);
    }

    @Override
    public void updateLoadStatistic(Map<Tag, LoadStatisticForTag> statisticMap) {
        super.updateLoadStatistic(statisticMap);
        movesCacheMap.updateMapping(statisticMap, Config.partition_rebalance_move_expire_after_access);
        // Perform cache maintenance
        movesCacheMap.maintain();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Move succeeded/total :{}/{}, current {}",
                    counterBalanceMoveSucceeded.get(), counterBalanceMoveCreated.get(), movesCacheMap);
        }
    }

    public Map<Long, Pair<TabletMove, Long>> getMovesInProgress() {
        Map<Long, Pair<TabletMove, Long>> moves = Maps.newHashMap();
        movesCacheMap.getCacheMap().values().forEach(
                m -> m.values().forEach(cache -> moves.putAll(cache.get().asMap())));

        return moves;
    }

    // Represents a concrete move of a tablet from one be to another.
    // Formed logically from a PartitionMove by specifying a tablet for the move.
    public static class TabletMove {
        public Long tabletId;
        public Long fromBe;
        public Long toBe;

        TabletMove(Long id, Long from, Long to) {
            this.tabletId = id;
            this.fromBe = from;
            this.toBe = to;
        }

        @Override
        public String toString() {
            return "ReplicaMove{"
                    + "tabletId=" + tabletId
                    + ", fromBe=" + fromBe
                    + ", toBe=" + toBe
                    + '}';
        }
    }

    // Balance information for a cluster(one medium), excluding decommissioned/dead bes and replicas on them.
    // Natural ordering, so the last key is the max key.
    public static class ClusterBalanceInfo {
        TreeMultimap<Long, TabletInvertedIndex.PartitionBalanceInfo> partitionInfoBySkew
                = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
        TreeMultimap<Long, Long> beByTotalReplicaCount = TreeMultimap.create();

        @Override
        public String toString() {
            return "[partitionSkew=" + partitionInfoBySkew + ", totalReplicaNum2Be=" + beByTotalReplicaCount + "]";
        }
    }

}
