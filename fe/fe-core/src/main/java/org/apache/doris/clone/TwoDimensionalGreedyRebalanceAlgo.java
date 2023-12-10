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

import org.apache.doris.catalog.TabletInvertedIndex.PartitionBalanceInfo;
import org.apache.doris.clone.PartitionRebalancer.ClusterBalanceInfo;
import org.apache.doris.common.Pair;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A two-dimensional greedy rebalancing algorithm. The two dims are cluster and partition.
 * It'll generate multiple `PartitionMove`, only decide which partition to move, fromBe, toBe.
 * The next step is to select a tablet to move.
 *
 * <p>From among moves that decrease the skew of a most skewed partition,
 * it prefers ones that reduce the skew of the cluster.
 * A cluster is considered balanced when the skew of every partition is <= 1 and the skew of the cluster is <= 1.
 * The skew of the cluster is defined as the difference between the maximum total replica count over all bes and the
 * minimum total replica count over all bes.
 *
 * This class is modified from kudu TwoDimensionalGreedyAlgo.
 */
public class TwoDimensionalGreedyRebalanceAlgo {
    private static final Logger LOG = LogManager.getLogger(TwoDimensionalGreedyRebalanceAlgo.class);

    private final EqualSkewOption equalSkewOption;
    private static final Random rand = new SecureRandom();

    public static class PartitionMove {
        Long partitionId;
        Long indexId;
        Long fromBe;
        Long toBe;

        PartitionMove(Long p, Long i, Long f, Long t) {
            this.partitionId = p;
            this.indexId = i;
            this.fromBe = f;
            this.toBe = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionMove that = (PartitionMove) o;
            return Objects.equal(partitionId, that.partitionId)
                    && Objects.equal(indexId, that.indexId)
                    && Objects.equal(fromBe, that.fromBe)
                    && Objects.equal(toBe, that.toBe);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(partitionId, indexId, fromBe, toBe);
        }

        @Override
        public String toString() {
            return "ReplicaMove{"
                    + "pid=" + partitionId + "-" + indexId
                    + ", from=" + fromBe
                    + ", to=" + toBe
                    + '}';
        }
    }

    public enum EqualSkewOption {
        // generally only be used on unit test
        PICK_FIRST,
        PICK_RANDOM
    }

    public enum ExtremumType {
        MAX,
        MIN
    }

    public static class IntersectionResult {
        Long replicaCountPartition;
        Long replicaCountTotal;
        List<Long> beWithExtremumCount;
        List<Long> intersection;
    }

    TwoDimensionalGreedyRebalanceAlgo() {
        this(EqualSkewOption.PICK_RANDOM);
    }

    TwoDimensionalGreedyRebalanceAlgo(EqualSkewOption equalSkewOption) {
        this.equalSkewOption = equalSkewOption;
    }

    // maxMovesNum: Value of '0' is a shortcut for 'the possible maximum'.
    // May modify the ClusterBalanceInfo
    public List<PartitionMove> getNextMoves(ClusterBalanceInfo info, int maxMovesNum) {
        Preconditions.checkArgument(maxMovesNum >= 0);
        if (maxMovesNum == 0) {
            maxMovesNum = Integer.MAX_VALUE;
        }

        if (info.partitionInfoBySkew.isEmpty()) {
            // Check for the consistency of the 'ClusterBalanceInfo' parameter: if no information is given on
            // the partition skew, partition count for all the be should be 0.
            // Keys are ordered by the natural ordering, so we can get the last(max) key to know if all keys are 0.
            NavigableSet<Long> keySet = info.beByTotalReplicaCount.keySet();
            LOG.debug(keySet);
            Preconditions.checkState(keySet.isEmpty() || keySet.last() == 0L,
                    "non-zero replica count on be while no partition skew information in skewMap");
            // Nothing to balance: cluster is empty.
            return Lists.newArrayList();
        }

        NavigableSet<Long> keySet = info.beByTotalReplicaCount.keySet();
        if (keySet.isEmpty() || keySet.last() == 0L) {
            // the number of replica on specified medium we get from getReplicaNumByBeIdAndStorageMedium() is
            // defined by table properties,but in fact there may not has SSD/HDD disk on this backend.
            // So if we found that no SSD/HDD disk on this backend, set the replica number to 0,
            // but the partitionInfoBySkew doesn't consider this scene, medium has no SSD/HDD disk also skew,
            // cause rebalance exception
            return Lists.newArrayList();
        }


        List<PartitionMove> moves = Lists.newArrayList();
        for (int i = 0; i < maxMovesNum; ++i) {
            PartitionMove move = getNextMove(info.beByTotalReplicaCount, info.partitionInfoBySkew);
            if (move == null || !(applyMove(move, info.beByTotalReplicaCount, info.partitionInfoBySkew))) {
                // 1. No replicas to move.
                // 2. Apply to info failed, it's useless to get next move from the same info.
                break;
            }
            moves.add(move);
        }

        return moves;
    }

    private PartitionMove getNextMove(TreeMultimap<Long, Long> beByTotalReplicaCount,
                                      TreeMultimap<Long, PartitionBalanceInfo> skewMap) {
        PartitionMove move = null;
        if (skewMap.isEmpty() || beByTotalReplicaCount.isEmpty()) {
            return null;
        }
        long maxPartitionSkew = skewMap.keySet().last();
        long maxBeSkew = beByTotalReplicaCount.keySet().last() - beByTotalReplicaCount.keySet().first();

        // 1. Every partition is balanced(maxPartitionSkew<=1) and any move will unbalance a partition, so there
        // is no potential for the greedy algorithm to balance the cluster.
        // 2. Every partition is balanced(maxPartitionSkew<=1) and the cluster as a whole is balanced(maxBeSkew<=1).
        if (maxPartitionSkew == 0L || (maxPartitionSkew <= 1L && maxBeSkew <= 1L)) {
            return null;
        }

        // Among the partitions with maximum skew, attempt to pick a partition where there is
        // a move that improves the partition skew and the cluster skew, if possible. If
        // not, attempt to pick a move that improves the partition skew. If all partitions
        // are balanced, attempt to pick a move that preserves partition balance and
        // improves cluster skew.
        NavigableSet<PartitionBalanceInfo> maxSet = skewMap.get(maxPartitionSkew);
        for (PartitionBalanceInfo pbi : maxSet) {
            Preconditions.checkArgument(!pbi.beByReplicaCount.isEmpty(), "no information on replicas of "
                    + "partition " + pbi.partitionId + "-" + pbi.indexId);

            Long minReplicaCount = pbi.beByReplicaCount.keySet().first();
            Long maxReplicaCount = pbi.beByReplicaCount.keySet().last();
            LOG.debug("balancing partition {}-{} with replica count skew {}"
                            + " (min_replica_count: {}, max_replica_count: {})",
                    pbi.partitionId, pbi.indexId, maxPartitionSkew,
                    minReplicaCount, maxReplicaCount);

            // Compute the intersection of the bes most loaded for the table
            // with the bes most loaded overall, and likewise for least loaded.
            // These are our ideal candidates for moving from and to, respectively.
            IntersectionResult maxLoaded = getIntersection(ExtremumType.MAX,
                    pbi.beByReplicaCount, beByTotalReplicaCount);
            IntersectionResult minLoaded = getIntersection(ExtremumType.MIN,
                    pbi.beByReplicaCount, beByTotalReplicaCount);
            LOG.debug("partition-wise: min_count: {}, max_count: {}",
                    minLoaded.replicaCountPartition, maxLoaded.replicaCountPartition);
            LOG.debug("cluster-wise: min_count: {}, max_count: {}",
                    minLoaded.replicaCountTotal, maxLoaded.replicaCountTotal);
            LOG.debug("min_loaded_intersection: {}, max_loaded_intersection: {}",
                    minLoaded.intersection.toString(), maxLoaded.intersection.toString());

            // Do not move replicas of a balanced table if the least (most) loaded
            // servers overall do not intersect the servers hosting the least (most)
            // replicas of the table. Moving a replica in that case might keep the
            // cluster skew the same or make it worse while keeping the table balanced.
            if ((maxLoaded.replicaCountPartition <= minLoaded.replicaCountPartition + 1)
                    && (minLoaded.intersection.isEmpty() || maxLoaded.intersection.isEmpty())) {
                continue;
            }

            Long minLoadedBe;
            Long maxLoadedBe;
            if (equalSkewOption == EqualSkewOption.PICK_FIRST) {
                // beWithExtremumCount lists & intersection lists are natural ordering
                minLoadedBe = minLoaded.intersection.isEmpty()
                        ? minLoaded.beWithExtremumCount.get(0) : minLoaded.intersection.get(0);
                maxLoadedBe = maxLoaded.intersection.isEmpty()
                        ? maxLoaded.beWithExtremumCount.get(0) : maxLoaded.intersection.get(0);
            } else {
                minLoadedBe = minLoaded.intersection.isEmpty() ? getRandomListElement(minLoaded.beWithExtremumCount)
                        : getRandomListElement(minLoaded.intersection);
                maxLoadedBe = maxLoaded.intersection.isEmpty() ? getRandomListElement(maxLoaded.beWithExtremumCount)
                        : getRandomListElement(maxLoaded.intersection);
            }

            LOG.debug("min_loaded_be: {}, max_loaded_be: {}", minLoadedBe, maxLoadedBe);
            if (minLoadedBe.equals(maxLoadedBe)) {
                // Nothing to move.
                continue;
            }
            // Move a replica of the selected partition from a most loaded server to a
            // least loaded server.
            move = new PartitionMove(pbi.partitionId, pbi.indexId, maxLoadedBe, minLoadedBe);
            break;
        }
        return move;
    }

    public static <T> T getRandomListElement(List<T> items) {
        Preconditions.checkArgument(!items.isEmpty());
        return items.get(rand.nextInt(items.size()));
    }

    public static IntersectionResult getIntersection(ExtremumType extremumType,
            TreeMultimap<Long, Long> beByReplicaCount, TreeMultimap<Long, Long> beByTotalReplicaCount) {
        Pair<Long, Set<Long>> beSelectedByPartition = getMinMaxLoadedServers(beByReplicaCount, extremumType);
        Pair<Long, Set<Long>> beSelectedByTotal = getMinMaxLoadedServers(beByTotalReplicaCount, extremumType);
        Preconditions.checkNotNull(beSelectedByPartition);
        Preconditions.checkNotNull(beSelectedByTotal);

        IntersectionResult res = new IntersectionResult();
        res.replicaCountPartition = beSelectedByPartition.first;
        res.replicaCountTotal = beSelectedByTotal.first;
        res.beWithExtremumCount = Lists.newArrayList(beSelectedByPartition.second);
        res.intersection = Lists.newArrayList(
                Sets.intersection(beSelectedByPartition.second, beSelectedByTotal.second));
        return res;
    }

    private static Pair<Long, Set<Long>> getMinMaxLoadedServers(
            TreeMultimap<Long, Long> multimap, ExtremumType extremumType) {
        if (multimap.isEmpty()) {
            return null;
        }
        Long count = (extremumType == ExtremumType.MIN) ? multimap.keySet().first() : multimap.keySet().last();
        return Pair.of(count, multimap.get(count));
    }

    /** Update the balance state in 'ClusterBalanceInfo'(the two maps) with the outcome of the move 'move'.
     * To support apply in-progress moves to current cluster balance info,
     * if apply failed, the maps should not be modified.
     */
    public static boolean applyMove(PartitionMove move, TreeMultimap<Long, Long> beByTotalReplicaCount,
                                    TreeMultimap<Long, PartitionBalanceInfo> skewMap) {
        try {
            // Update the total counts
            moveOneReplica(move.fromBe, move.toBe, beByTotalReplicaCount);
        } catch (IllegalStateException e) {
            LOG.info("{} apply failed, {}", move, e.getMessage());
            return false;
        }

        try {
            PartitionBalanceInfo partitionBalanceInfo = null;
            Long skew = -1L;
            for (Long key : skewMap.keySet()) {
                NavigableSet<PartitionBalanceInfo> pbiSet = skewMap.get(key);
                List<PartitionBalanceInfo> pbis = pbiSet.stream()
                        .filter(info -> info.partitionId.equals(move.partitionId) && info.indexId.equals(move.indexId))
                        .collect(Collectors.toList());
                Preconditions.checkState(pbis.size() <= 1, "skew map has dup partition info");
                if (pbis.size() == 1) {
                    partitionBalanceInfo = pbis.get(0);
                    skew = key;
                    break;
                }
            }

            Preconditions.checkState(skew != -1L, "partition is not in skew map");
            PartitionBalanceInfo newInfo = new PartitionBalanceInfo(partitionBalanceInfo);
            moveOneReplica(move.fromBe, move.toBe, newInfo.beByReplicaCount);

            skewMap.remove(skew, partitionBalanceInfo);
            long minCount = newInfo.beByReplicaCount.keySet().first();
            long maxCount = newInfo.beByReplicaCount.keySet().last();
            skewMap.put(maxCount - minCount, newInfo);
        } catch (IllegalStateException e) {
            // If touch IllegalState, the skew map doesn't be modified,
            // so we should rollback the move of beByTotalReplicaCount
            moveOneReplica(move.toBe, move.fromBe, beByTotalReplicaCount);
            LOG.info("{} apply failed, {}", move, e.getMessage());
            return false;
        } catch (Exception e) {
            // Rollback the move of beByTotalReplicaCount is meaningless here
            LOG.warn("got unexpected exception when apply {}, the skew may be broken. {}", move, e.toString());
            throw e;
        }
        return true;
    }

    // Applies to 'm' a move of a replica from the be with id 'src' to the be with id 'dst' by decrementing
    // the count of 'src' and incrementing the count of 'dst'.
    // If check failed, won't modify the map.
    private static void moveOneReplica(Long fromBe, Long toBe,
            TreeMultimap<Long, Long> m) throws IllegalStateException {
        boolean foundSrc = false;
        boolean foundDst = false;
        Long countSrc = 0L;
        Long countDst = 0L;
        for (Long key : m.keySet()) {
            // set is arbitrary ordering, need to convert
            Set<Long> values = m.get(key);
            if (values.contains(fromBe)) {
                foundSrc = true;
                countSrc = key;
            }
            if (values.contains(toBe)) {
                foundDst = true;
                countDst = key;
            }
        }

        Preconditions.checkState(foundSrc, "fromBe " + fromBe + " is not in the map");
        Preconditions.checkState(foundDst, "toBe " + toBe + " is not in the map");
        Preconditions.checkState(countSrc > 0, "fromBe has no replica in the map, can't move");

        m.remove(countSrc, fromBe);
        m.remove(countDst, toBe);
        m.put(countSrc - 1, fromBe);
        m.put(countDst + 1, toBe);
    }
}
