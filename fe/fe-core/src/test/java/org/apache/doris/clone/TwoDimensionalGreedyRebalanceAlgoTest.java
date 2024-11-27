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
import org.apache.doris.clone.TwoDimensionalGreedyRebalanceAlgo.PartitionMove;
import org.apache.doris.common.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public class TwoDimensionalGreedyRebalanceAlgoTest {
    private static final Logger LOG = LogManager.getLogger(TwoDimensionalGreedyRebalanceAlgoTest.class);

    TwoDimensionalGreedyRebalanceAlgo algo = new TwoDimensionalGreedyRebalanceAlgo(
            TwoDimensionalGreedyRebalanceAlgo.EqualSkewOption.PICK_FIRST);

    // Structure to describe rebalancing-related state of the cluster expressively
    // enough for the tests.
    private static class TestClusterConfig {
        static class PartitionPerBeReplicas {
            Long partitionId;
            Long indexId;

            // Number of replicas of this partition on each server in the cluster.
            // By definition, the indices in this container correspond to indices
            // in TestClusterConfig::beIds.
            List<Long> numReplicasByServer;

            PartitionPerBeReplicas(Long p, Long i, List<Long> l) {
                this.partitionId = p;
                this.indexId = i;
                this.numReplicasByServer = l;
            }
        }

        // IDs of bes; every element must be unique.
        List<Long> beIds = Lists.newArrayList();

        // Distribution of partition replicas across the bes. The following
        // constraints should be in place:
        //   * for each p in partitionReplicas:
        //       p.numReplicasByServer.size() == beIds.size()
        List<PartitionPerBeReplicas> partitionReplicas = Lists.newArrayList();

        // The expected replica movements: the reference output of the algorithm
        // to compare with.
        List<PartitionMove> expectedMoves = Lists.newArrayList();

        // TODO MovesOrderingComparison: Options controlling how the reference and the actual results are compared.
        // PartitionBalanceInfos in skew map are arbitrary ordering, so we can't get the fixed moves
        // when more than one partition have the maxSkew.
    }

    // Transform the definition of the test cluster into the ClusterInfo
    // that is consumed by the rebalancing algorithm.
    private ClusterBalanceInfo clusterConfigToClusterBalanceInfo(TestClusterConfig tcc) {
        // First verify that the configuration of the test cluster is valid.
        Set<Pair<Long, Long>> partitionIds = Sets.newHashSet();
        for (TestClusterConfig.PartitionPerBeReplicas p : tcc.partitionReplicas) {
            Assert.assertEquals(tcc.beIds.size(), p.numReplicasByServer.size());
            partitionIds.add(Pair.of(p.partitionId, p.indexId));
        }
        Assert.assertEquals(partitionIds.size(), tcc.partitionReplicas.size());

        // Check for uniqueness of the tablet servers' identifiers.
        Set<Long> beIdSet = new HashSet<>(tcc.beIds);
        Assert.assertEquals(tcc.beIds.size(), beIdSet.size());

        ClusterBalanceInfo balance = new ClusterBalanceInfo();

        for (int beIdx = 0; beIdx < tcc.beIds.size(); ++beIdx) {
            // Total replica count at the tablet server.
            long count = 0;
            for (TestClusterConfig.PartitionPerBeReplicas p : tcc.partitionReplicas) {
                count += p.numReplicasByServer.get(beIdx);
            }
            balance.beByTotalReplicaCount.put(count, tcc.beIds.get(beIdx));
        }

        for (int pIdx = 0; pIdx < tcc.partitionReplicas.size(); ++pIdx) {
            // Replicas of the current partition per be.
            TestClusterConfig.PartitionPerBeReplicas distribution = tcc.partitionReplicas.get(pIdx);
            PartitionBalanceInfo info = new PartitionBalanceInfo(distribution.partitionId, distribution.indexId);
            List<Long> replicaCount = distribution.numReplicasByServer;
            IntStream.range(0, replicaCount.size())
                    .forEach(i -> info.beByReplicaCount.put(replicaCount.get(i), tcc.beIds.get(i)));

            Long maxCount = info.beByReplicaCount.keySet().last();
            Long minCount = info.beByReplicaCount.keySet().first();
            Assert.assertTrue(maxCount >= minCount);
            balance.partitionInfoBySkew.put(maxCount - minCount, info);
        }
        return balance;
    }

    private void verifyMoves(List<TestClusterConfig> configs) {
        for (TestClusterConfig config : configs) {
            List<PartitionMove> moves = algo.getNextMoves(clusterConfigToClusterBalanceInfo(config), 0);
            Assert.assertEquals(moves, config.expectedMoves);
        }
    }

    @Before
    public void setUp() {
        Configurator.setLevel("org.apache.doris.clone.TwoDimensionalGreedyAlgo", Level.WARN);
    }

    @Test
    public void testApplyMoveFailed() {
        PartitionMove move = new PartitionMove(11L, 22L, 10001L, 10002L);
        // total count is valid
        TreeMultimap<Long, Long> beByTotalReplicaCount = TreeMultimap.create();
        beByTotalReplicaCount.put(10L, 10001L);
        beByTotalReplicaCount.put(10L, 10002L);
        // no info of partition
        TreeMultimap<Long, PartitionBalanceInfo> skewMap
                = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
        try {
            TwoDimensionalGreedyRebalanceAlgo.applyMove(move, beByTotalReplicaCount, skewMap);
        } catch (Exception e) {
            Assert.assertSame(e.getClass(), IllegalStateException.class);
            LOG.info(e.getMessage());
        }
        // beByTotalReplicaCount should be modified
        Assert.assertEquals(0, beByTotalReplicaCount.keySet().stream().filter(skew -> skew != 10L).count());

        // invalid info of partition
        skewMap.put(6L, new PartitionBalanceInfo(11L, 22L));
        try {
            TwoDimensionalGreedyRebalanceAlgo.applyMove(move, beByTotalReplicaCount, skewMap);
        } catch (Exception e) {
            Assert.assertSame(e.getClass(), IllegalStateException.class);
            LOG.warn(e.getMessage());
        }
        // beByTotalReplicaCount should be modified
        Assert.assertEquals(0, beByTotalReplicaCount.keySet().stream().filter(skew -> skew != 10L).count());
    }

    @Test
    public void testInvalidClusterBalanceInfo() {
        Configurator.setLevel("org.apache.doris.clone.TwoDimensionalGreedyAlgo", Level.DEBUG);
        try {
            algo.getNextMoves(new ClusterBalanceInfo(), 0);
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            algo.getNextMoves(new ClusterBalanceInfo() {
                {
                    beByTotalReplicaCount.put(0L, 10001L);
                }
            }, 0);
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            // Invalid balance info will cause IllegalStateException
            algo.getNextMoves(new ClusterBalanceInfo() {
                {
                    beByTotalReplicaCount.put(0L, 10001L);
                    beByTotalReplicaCount.put(1L, 10002L);
                }
            }, -1);
            Assert.fail("Exception will be thrown in GetNextMoves");
        } catch (Exception e) {
            Assert.assertSame(e.getClass(), IllegalArgumentException.class);
            LOG.info(e.getMessage());
        }
    }

    // Partition- and cluster-wise balanced configuration with one-off skew.
    // Algorithm won't consider about the tablet health
    @Test
    public void testAlreadyBalanced() {
        List<TestClusterConfig> configs = Lists.newArrayList(
                // A single be with a single replica of the only partition.
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(1L)));
                    // expectedMoves is empty
                }},
                // A single be in the cluster that hosts all replicas.
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(1L)));
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 44L, Lists.newArrayList(10L)));
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 55L, Lists.newArrayList(10L)));
                }},
                // Single partition and 2 be: 100 and 99 replicas at each.
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    beIds.add(10002L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(100L, 99L)));
                }}
        );
        verifyMoves(configs);
    }

    // TODO after MovesOrderingComparison supported
    // Set of scenarios where the distribution of replicas is partition-wise balanced
    // but not yet cluster-wise balanced, requiring just a few replica moves
    // to achieve both partition- and cluster-wise balance state.

    // TODO add more tests after MovesOrderingComparison supported
    // Set of scenarios where the distribution of table replicas is cluster-wise
    // balanced, but not table-wise balanced, requiring just few moves to make it
    // both table- and cluster-wise balanced.
    @Test
    public void testClusterWiseBalanced() {
        List<TestClusterConfig> configs = Lists.newArrayList(
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    beIds.add(10002L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(2L, 0L)));
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 44L, Lists.newArrayList(1L, 2L)));
                    expectedMoves.add(new PartitionMove(22L, 33L, 10001L, 10002L));
                }}
        );
        verifyMoves(configs);
    }

    // Unbalanced (both table- and cluster-wise) and simple enough configurations
    // to make them balanced moving just few replicas.
    @Test
    public void testFewMoves() {
        List<TestClusterConfig> configs = Lists.newArrayList(
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    beIds.add(10002L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(2L, 0L)));
                    expectedMoves.add(new PartitionMove(22L, 33L, 10001L, 10002L));
                }},
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    beIds.add(10002L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(3L, 0L)));
                    expectedMoves.add(new PartitionMove(22L, 33L, 10001L, 10002L));
                }},
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    beIds.add(10002L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(4L, 0L)));
                    expectedMoves.add(new PartitionMove(22L, 33L, 10001L, 10002L));
                    expectedMoves.add(new PartitionMove(22L, 33L, 10001L, 10002L));
                }}
        );
        verifyMoves(configs);
    }

    // Unbalanced (both table- and cluster-wise) and simple enough configurations to
    // make them balanced moving many replicas around.
    @Test
    public void testManyMoves() {
        List<TestClusterConfig> configs = Lists.newArrayList(
                new TestClusterConfig() {{
                    beIds.add(10001L);
                    beIds.add(10002L);
                    beIds.add(10003L);
                    partitionReplicas.add(new PartitionPerBeReplicas(22L, 33L, Lists.newArrayList(100L, 400L, 100L)));
                    for (int i = 0; i < 200; i++) {
                        if (i % 2 == 1) {
                            expectedMoves.add(new PartitionMove(22L, 33L, 10002L, 10003L));
                        } else {
                            expectedMoves.add(new PartitionMove(22L, 33L, 10002L, 10001L));
                        }
                    }

                }}
        );
        verifyMoves(configs);
    }
}
