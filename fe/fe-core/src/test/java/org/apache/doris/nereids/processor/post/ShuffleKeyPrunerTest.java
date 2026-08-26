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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

/** Unit tests for {@link ShuffleKeyPruner}. */
class ShuffleKeyPrunerTest extends TestWithFeService {

    private SlotReference slotA;
    private PhysicalEmptyRelation empty;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setParallelResultSink(false);
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;
        connectContext.getSessionVariable().parallelPipelineTaskNum = 2;

        createTable("CREATE TABLE `t1` (\n"
                + "  `a` int(11) NULL,\n"
                + "  `b` int(11) NULL,\n"
                + "  `d` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`a`, `b`, `d`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`b`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + "  `a` int(11) NULL,\n"
                + "  `b` int(11) NULL,\n"
                + "  `d` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`a`, `b`, `d`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`b`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        slotA = new SlotReference(new ExprId(0), "a", IntegerType.INSTANCE, true, ImmutableList.of());
        empty = new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(slotA), null);
    }

    @Test
    void testSkewJoinHintShouldNotTriggerShuffleKeyPrune() {
        String sql = "select t1.a,t2.b from t1 join [shuffle[skew(t1.b(1,2))]]t2 "
                + "on t1.b=t2.b and t1.d=t2.d order by 1,2";
        int[] pruneOn = extractJoinShuffleKeySizes(sql, true);
        int[] pruneOff = extractJoinShuffleKeySizes(sql, false);
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;

        Assertions.assertArrayEquals(pruneOff, pruneOn);
    }

    @Test
    void testDistinctSkewAggShouldNotPruneDistinctGlobalDistribute() {
        String sql = "select a,count(distinct [skew] b) from t1 group by a";
        int keySizeOn = extractDistinctGlobalChildDistributeKeySize(sql, true);
        int keySizeOff = extractDistinctGlobalChildDistributeKeySize(sql, false);
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;

        Assertions.assertEquals(keySizeOff, keySizeOn);
        Assertions.assertEquals(1, keySizeOn);
    }

    @Test
    void testGlobalAggPostProcessorUsesExchangeInputStatisticsForSafety() {
        SlotReference stringKey = new SlotReference(new ExprId(10), "string_key",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference hotKey = new SlotReference(new ExprId(11), "hot_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Statistics scanStats = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(stringKey, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(1)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .putColumnStatistics(hotKey, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.001f))
                        .build())
                .build();
        Statistics localAggStats = new StatisticsBuilder()
                .setRowCount(2_000_000)
                .putColumnStatistics(stringKey, new ColumnStatisticBuilder(2_000_000)
                        .setNdv(1)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .putColumnStatistics(hotKey, new ColumnStatisticBuilder(2_000_000)
                        .setNdv(2_000_000)
                        .build())
                .build();
        PhysicalEmptyRelation relation = (PhysicalEmptyRelation) new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(stringKey, hotKey), null)
                .withPhysicalPropertiesAndStats(PhysicalProperties.ANY, scanStats);
        PhysicalHashAggregate<PhysicalEmptyRelation> localAgg = new PhysicalHashAggregate<>(
                ImmutableList.of(stringKey, hotKey),
                ImmutableList.of(stringKey, hotKey),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                true, null, false, relation);
        localAgg = localAgg.withPhysicalPropertiesAndStats(PhysicalProperties.ANY, localAggStats);
        DistributionSpecHash hashSpec = new DistributionSpecHash(
                ImmutableList.of(stringKey.getExprId(), hotKey.getExprId()),
                DistributionSpecHash.ShuffleType.EXECUTION_BUCKETED);
        PhysicalDistribute<PhysicalHashAggregate<PhysicalEmptyRelation>> distribute = new PhysicalDistribute<>(
                hashSpec, Optional.empty(), localAgg.getLogicalProperties(),
                PhysicalProperties.createHash(hashSpec), localAggStats, localAgg);
        PhysicalHashAggregate<PhysicalDistribute<PhysicalHashAggregate<PhysicalEmptyRelation>>> globalAgg =
                new PhysicalHashAggregate<>(
                        ImmutableList.of(stringKey, hotKey),
                        ImmutableList.of(stringKey, hotKey),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, distribute);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        int previousBeNumberForTest = connectContext.getSessionVariable().getBeNumberForTest();
        int previousParallelPipelineTaskNum = connectContext.getSessionVariable().parallelPipelineTaskNum;
        boolean previousEnableShuffleKeyPrune = connectContext.getSessionVariable().enableShuffleKeyPrune;
        connectContext.getSessionVariable().setBeNumberForTest(3);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;
        try {
            Plan output = new ShuffleKeyPruner().processRoot(globalAgg, cascadesContext);

            Assertions.assertInstanceOf(PhysicalHashAggregate.class, output);
            Assertions.assertInstanceOf(PhysicalDistribute.class, output.child(0));
            DistributionSpecHash outputSpec = (DistributionSpecHash) ((PhysicalDistribute<?>) output.child(0))
                    .getDistributionSpec();
            Assertions.assertEquals(hashSpec.getOrderedShuffledColumns(), outputSpec.getOrderedShuffledColumns());
        } finally {
            connectContext.getSessionVariable().setBeNumberForTest(previousBeNumberForTest);
            connectContext.getSessionVariable().parallelPipelineTaskNum = previousParallelPipelineTaskNum;
            connectContext.getSessionVariable().enableShuffleKeyPrune = previousEnableShuffleKeyPrune;
        }
    }

    @Test
    void testGlobalAggPostProcessorKeepsPlanWithoutExchangeStatistics() {
        SlotReference stringKey = new SlotReference(new ExprId(12), "string_key",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference numericKey = new SlotReference(new ExprId(13), "numeric_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Statistics scanStats = shufflePrunableStats(stringKey, numericKey);
        PhysicalEmptyRelation relation = (PhysicalEmptyRelation) new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(stringKey, numericKey), null)
                .withPhysicalPropertiesAndStats(PhysicalProperties.ANY, scanStats);
        PhysicalHashAggregate<PhysicalEmptyRelation> localAgg = new PhysicalHashAggregate<>(
                ImmutableList.of(stringKey, numericKey),
                ImmutableList.of(stringKey, numericKey),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                true, null, false, relation);
        DistributionSpecHash hashSpec = new DistributionSpecHash(
                ImmutableList.of(stringKey.getExprId(), numericKey.getExprId()),
                DistributionSpecHash.ShuffleType.EXECUTION_BUCKETED);
        PhysicalDistribute<PhysicalHashAggregate<PhysicalEmptyRelation>> distribute = new PhysicalDistribute<>(
                hashSpec, Optional.empty(), localAgg.getLogicalProperties(),
                PhysicalProperties.createHash(hashSpec), null, localAgg);
        PhysicalHashAggregate<PhysicalDistribute<PhysicalHashAggregate<PhysicalEmptyRelation>>> globalAgg =
                new PhysicalHashAggregate<>(
                        ImmutableList.of(stringKey, numericKey),
                        ImmutableList.of(stringKey, numericKey),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, distribute);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        boolean previousEnableShuffleKeyPrune = connectContext.getSessionVariable().enableShuffleKeyPrune;
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;
        try {
            Plan output = new ShuffleKeyPruner().processRoot(globalAgg, cascadesContext);

            Assertions.assertInstanceOf(PhysicalHashAggregate.class, output);
            Assertions.assertInstanceOf(PhysicalDistribute.class, output.child(0));
            PhysicalDistribute<?> outputDistribute = (PhysicalDistribute<?>) output.child(0);
            Assertions.assertNull(outputDistribute.getStats());
            Assertions.assertEquals(hashSpec.getOrderedShuffledColumns(),
                    ((DistributionSpecHash) outputDistribute.getDistributionSpec()).getOrderedShuffledColumns());
        } finally {
            connectContext.getSessionVariable().enableShuffleKeyPrune = previousEnableShuffleKeyPrune;
        }
    }

    @Test
    void testGlobalAggPostProcessorMarksPruningFromFullAggregateShuffle() {
        SlotReference retainedKey = new SlotReference(new ExprId(14), "retained_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference droppedKey = new SlotReference(new ExprId(15), "dropped_key",
                new VarcharType(64), true, ImmutableList.of());
        Statistics inputStatistics = pruningStatistics(retainedKey, droppedKey);
        PhysicalHashAggregate<?> aggregate = aggregateWithShuffle(
                ImmutableList.of(retainedKey, droppedKey),
                ImmutableList.of(retainedKey, droppedKey), inputStatistics);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        Plan output = new ShuffleKeyPruner().processRoot(aggregate, cascadesContext);

        PhysicalDistribute<?> outputDistribute = (PhysicalDistribute<?>) output.child(0);
        Assertions.assertEquals(ImmutableList.of(retainedKey.getExprId()),
                ((DistributionSpecHash) outputDistribute.getDistributionSpec()).getOrderedShuffledColumns());
        Assertions.assertTrue(outputDistribute.isPrunedFromFullAggregateKeys());
    }

    @Test
    void testGlobalAggPostProcessorDoesNotMarkPruningFromExistingSubset() {
        SlotReference retainedKey = new SlotReference(new ExprId(16), "retained_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference droppedKey = new SlotReference(new ExprId(17), "dropped_key",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference otherGroupKey = new SlotReference(new ExprId(18), "other_group_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Statistics inputStatistics = pruningStatistics(retainedKey, droppedKey, otherGroupKey);
        PhysicalHashAggregate<?> aggregate = aggregateWithShuffle(
                ImmutableList.of(retainedKey, droppedKey, otherGroupKey),
                ImmutableList.of(retainedKey, droppedKey), inputStatistics);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        Plan output = new ShuffleKeyPruner().processRoot(aggregate, cascadesContext);

        PhysicalDistribute<?> outputDistribute = (PhysicalDistribute<?>) output.child(0);
        Assertions.assertEquals(ImmutableList.of(retainedKey.getExprId()),
                ((DistributionSpecHash) outputDistribute.getDistributionSpec()).getOrderedShuffledColumns());
        Assertions.assertFalse(outputDistribute.isPrunedFromFullAggregateKeys());
    }

    @Test
    void testGlobalAggPostProcessorKeepsExchangeEnforcedOverEquivalentSubset() {
        SlotReference key1 = new SlotReference(new ExprId(30), "key1",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference key2 = new SlotReference(new ExprId(31), "key2",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference key3 = new SlotReference(new ExprId(32), "key3",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference joinKey = new SlotReference(new ExprId(33), "join_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Plan globalAgg = globalAggBranchWithEquivalentSubset(key1, key2, key3, joinKey);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        int previousBeNumberForTest = connectContext.getSessionVariable().getBeNumberForTest();
        int previousParallelPipelineTaskNum = connectContext.getSessionVariable().parallelPipelineTaskNum;
        boolean previousEnableShuffleKeyPrune = connectContext.getSessionVariable().enableShuffleKeyPrune;
        connectContext.getSessionVariable().setBeNumberForTest(3);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;
        try {
            Plan output = new ShuffleKeyPruner().processRoot(globalAgg, cascadesContext);

            Assertions.assertInstanceOf(PhysicalHashAggregate.class, output);
            Assertions.assertInstanceOf(PhysicalDistribute.class, output.child(0));
            DistributionSpecHash outputSpec = (DistributionSpecHash) ((PhysicalDistribute<?>) output.child(0))
                    .getDistributionSpec();
            Assertions.assertEquals(ImmutableList.of(key1.getExprId(), key2.getExprId(), key3.getExprId()),
                    outputSpec.getOrderedShuffledColumns());
        } finally {
            connectContext.getSessionVariable().setBeNumberForTest(previousBeNumberForTest);
            connectContext.getSessionVariable().parallelPipelineTaskNum = previousParallelPipelineTaskNum;
            connectContext.getSessionVariable().enableShuffleKeyPrune = previousEnableShuffleKeyPrune;
        }
    }

    @Test
    void testShuffleJoinKeepsGlobalAggExchangesEnforcedOverEquivalentSubsets() {
        SlotReference leftKey1 = new SlotReference(new ExprId(40), "left_key1",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference leftKey2 = new SlotReference(new ExprId(41), "left_key2",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference leftKey3 = new SlotReference(new ExprId(42), "left_key3",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference leftJoinKey = new SlotReference(new ExprId(43), "left_join_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference rightKey1 = new SlotReference(new ExprId(44), "right_key1",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference rightKey2 = new SlotReference(new ExprId(45), "right_key2",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference rightKey3 = new SlotReference(new ExprId(46), "right_key3",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference rightJoinKey = new SlotReference(new ExprId(47), "right_join_key",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Plan left = globalAggBranchWithEquivalentSubset(leftKey1, leftKey2, leftKey3, leftJoinKey);
        Plan right = globalAggBranchWithEquivalentSubset(rightKey1, rightKey2, rightKey3, rightJoinKey);
        PhysicalHashJoin<Plan, Plan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(
                        new EqualTo(leftKey1, rightKey1),
                        new EqualTo(leftKey2, rightKey2),
                        new EqualTo(leftKey3, rightKey3)),
                ImmutableList.of(), new DistributeHint(DistributeType.NONE), Optional.empty(), null, left, right);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        int previousBeNumberForTest = connectContext.getSessionVariable().getBeNumberForTest();
        int previousParallelPipelineTaskNum = connectContext.getSessionVariable().parallelPipelineTaskNum;
        boolean previousEnableShuffleKeyPrune = connectContext.getSessionVariable().enableShuffleKeyPrune;
        connectContext.getSessionVariable().setBeNumberForTest(3);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;
        try {
            Plan output = new ShuffleKeyPruner().processRoot(join, cascadesContext);

            Assertions.assertInstanceOf(PhysicalHashJoin.class, output);
            PhysicalHashJoin<?, ?> outputJoin = (PhysicalHashJoin<?, ?>) output;
            Assertions.assertEquals(3,
                    getHashSpecFromJoinChild(outputJoin.left()).getOrderedShuffledColumns().size());
            Assertions.assertEquals(3,
                    getHashSpecFromJoinChild(outputJoin.right()).getOrderedShuffledColumns().size());
        } finally {
            connectContext.getSessionVariable().setBeNumberForTest(previousBeNumberForTest);
            connectContext.getSessionVariable().parallelPipelineTaskNum = previousParallelPipelineTaskNum;
            connectContext.getSessionVariable().enableShuffleKeyPrune = previousEnableShuffleKeyPrune;
        }
    }

    @Test
    void testShuffleJoinUsesGlobalAggExchangeInputStatisticsForSafety() {
        SlotReference leftString = new SlotReference(new ExprId(20), "left_string",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference leftHot = new SlotReference(new ExprId(21), "left_hot",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference rightString = new SlotReference(new ExprId(22), "right_string",
                new VarcharType(64), true, ImmutableList.of());
        SlotReference rightHot = new SlotReference(new ExprId(23), "right_hot",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Statistics leftScanStats = shufflePrunableStats(leftString, leftHot);
        Statistics rightScanStats = shufflePrunableStats(rightString, rightHot);
        Statistics leftExchangeStats = localAggOutputStats(leftString, leftHot);
        Statistics rightExchangeStats = localAggOutputStats(rightString, rightHot);
        Plan left = globalAggBranch(leftString, leftHot, leftScanStats, leftExchangeStats);
        Plan right = globalAggBranch(rightString, rightHot, rightScanStats, rightExchangeStats);
        PhysicalHashJoin<Plan, Plan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(leftString, rightString), new EqualTo(leftHot, rightHot)),
                ImmutableList.of(), new DistributeHint(DistributeType.NONE), Optional.empty(), null, left, right);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);

        Plan output = new ShuffleKeyPruner().processRoot(join, cascadesContext);

        Assertions.assertInstanceOf(PhysicalHashJoin.class, output);
        PhysicalHashJoin<?, ?> outputJoin = (PhysicalHashJoin<?, ?>) output;
        Assertions.assertEquals(2, getHashSpecFromJoinChild(outputJoin.left()).getOrderedShuffledColumns().size());
        Assertions.assertEquals(2, getHashSpecFromJoinChild(outputJoin.right()).getOrderedShuffledColumns().size());
    }

    private Statistics shufflePrunableStats(SlotReference stringKey, SlotReference hotKey) {
        return new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(stringKey, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(1)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .putColumnStatistics(hotKey, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.001f))
                        .build())
                .build();
    }

    private Statistics pruningStatistics(SlotReference retainedKey, SlotReference... otherKeys) {
        StatisticsBuilder statistics = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(retainedKey, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build());
        for (SlotReference key : otherKeys) {
            statistics.putColumnStatistics(key, new ColumnStatisticBuilder(1_000_000_000)
                    .setNdv(1)
                    .setHotValues(ImmutableMap.of())
                    .build());
        }
        return statistics.build();
    }

    private PhysicalHashAggregate<?> aggregateWithShuffle(List<SlotReference> groupByKeys,
            List<SlotReference> shuffleKeys, Statistics inputStatistics) {
        PhysicalEmptyRelation relation = (PhysicalEmptyRelation) new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(), groupByKeys, null)
                .withPhysicalPropertiesAndStats(PhysicalProperties.ANY, inputStatistics);
        DistributionSpecHash hashSpec = new DistributionSpecHash(
                shuffleKeys.stream().map(SlotReference::getExprId).collect(ImmutableList.toImmutableList()),
                DistributionSpecHash.ShuffleType.REQUIRE);
        PhysicalDistribute<PhysicalEmptyRelation> distribute = new PhysicalDistribute<>(
                hashSpec, Optional.empty(), relation.getLogicalProperties(),
                PhysicalProperties.createHash(hashSpec), inputStatistics, relation);
        return new PhysicalHashAggregate<>(
                ImmutableList.<Expression>copyOf(groupByKeys),
                ImmutableList.<NamedExpression>copyOf(groupByKeys),
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT),
                true, null, false, distribute);
    }

    private Statistics localAggOutputStats(SlotReference stringKey, SlotReference hotKey) {
        return new StatisticsBuilder()
                .setRowCount(2_000_000)
                .putColumnStatistics(stringKey, new ColumnStatisticBuilder(2_000_000)
                        .setNdv(1)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .putColumnStatistics(hotKey, new ColumnStatisticBuilder(2_000_000)
                        .setNdv(2_000_000)
                        .build())
                .build();
    }

    private Plan globalAggBranch(SlotReference stringKey, SlotReference hotKey,
            Statistics scanStats, Statistics exchangeStats) {
        PhysicalEmptyRelation relation = (PhysicalEmptyRelation) new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(stringKey, hotKey), null)
                .withPhysicalPropertiesAndStats(PhysicalProperties.ANY, scanStats);
        PhysicalHashAggregate<PhysicalEmptyRelation> localAgg = new PhysicalHashAggregate<>(
                ImmutableList.of(stringKey, hotKey), ImmutableList.of(stringKey, hotKey),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                true, null, false, relation);
        localAgg = localAgg.withPhysicalPropertiesAndStats(PhysicalProperties.ANY, exchangeStats);
        DistributionSpecHash hashSpec = new DistributionSpecHash(
                ImmutableList.of(stringKey.getExprId(), hotKey.getExprId()),
                DistributionSpecHash.ShuffleType.EXECUTION_BUCKETED);
        PhysicalDistribute<PhysicalHashAggregate<PhysicalEmptyRelation>> distribute = new PhysicalDistribute<>(
                hashSpec, Optional.empty(), localAgg.getLogicalProperties(),
                PhysicalProperties.createHash(hashSpec), exchangeStats, localAgg);
        return new PhysicalHashAggregate<>(
                ImmutableList.of(stringKey, hotKey), ImmutableList.of(stringKey, hotKey),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, null, false, distribute);
    }

    private Plan globalAggBranchWithEquivalentSubset(
            SlotReference key1, SlotReference key2, SlotReference key3, SlotReference joinKey) {
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(key1, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.001f))
                        .build())
                .putColumnStatistics(key2, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(100)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.001f))
                        .build())
                .putColumnStatistics(key3, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .putColumnStatistics(joinKey, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(100)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.001f))
                        .build())
                .build();
        DistributionSpecHash inputHashSpec = new DistributionSpecHash(
                ImmutableList.of(key1.getExprId(), key2.getExprId()),
                DistributionSpecHash.ShuffleType.NATURAL, -1L, ImmutableSet.of(),
                ImmutableList.of(
                        ImmutableSet.of(key1.getExprId(), joinKey.getExprId()),
                        ImmutableSet.of(key2.getExprId(), joinKey.getExprId())),
                ImmutableMap.of(key1.getExprId(), 0, key2.getExprId(), 1, joinKey.getExprId(), 1));
        PhysicalEmptyRelation relation = (PhysicalEmptyRelation) new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(key1, key2, key3, joinKey), null)
                .withPhysicalPropertiesAndStats(PhysicalProperties.createHash(inputHashSpec), inputStatistics);
        DistributionSpecHash enforcedHashSpec = new DistributionSpecHash(
                ImmutableList.of(key1.getExprId(), key2.getExprId(), key3.getExprId()),
                DistributionSpecHash.ShuffleType.EXECUTION_BUCKETED);
        PhysicalDistribute<PhysicalEmptyRelation> distribute = new PhysicalDistribute<>(
                enforcedHashSpec, Optional.empty(), relation.getLogicalProperties(),
                PhysicalProperties.createHash(enforcedHashSpec), inputStatistics, relation);
        return new PhysicalHashAggregate<>(
                ImmutableList.of(key1, key2, key3), ImmutableList.of(key1, key2, key3),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, null, false, distribute);
    }

    private int[] extractJoinShuffleKeySizes(String sql, boolean enablePrune) {
        connectContext.getSessionVariable().enableShuffleKeyPrune = enablePrune;
        int[] sizes = new int[2];
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            PhysicalHashJoin<? extends Plan, ? extends Plan> join = findFirstHashJoin(planner.getOptimizedPlan());
            Assertions.assertNotNull(join);
            Assertions.assertNotNull(join.getDistributeHint().getSkewInfo());

            DistributionSpecHash leftSpec = getHashSpecFromJoinChild(join.left());
            DistributionSpecHash rightSpec = getHashSpecFromJoinChild(join.right());
            sizes[0] = leftSpec.getOrderedShuffledColumns().size();
            sizes[1] = rightSpec.getOrderedShuffledColumns().size();
        });
        return sizes;
    }

    private int extractDistinctGlobalChildDistributeKeySize(String sql, boolean enablePrune) {
        connectContext.getSessionVariable().enableShuffleKeyPrune = enablePrune;
        int[] size = new int[] {-1};
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            PhysicalHashAggregate<? extends Plan> distinctGlobal =
                    findFirstDistinctGlobalAgg(planner.getOptimizedPlan());
            Assertions.assertNotNull(distinctGlobal);
            Assertions.assertInstanceOf(PhysicalDistribute.class, distinctGlobal.child());
            Assertions.assertInstanceOf(DistributionSpecHash.class,
                    ((PhysicalDistribute<?>) distinctGlobal.child()).getDistributionSpec());
            DistributionSpecHash spec = (DistributionSpecHash) ((PhysicalDistribute<?>) distinctGlobal.child())
                    .getDistributionSpec();
            size[0] = spec.getOrderedShuffledColumns().size();
        });
        return size[0];
    }

    private PhysicalHashAggregate<? extends Plan> findFirstDistinctGlobalAgg(Plan plan) {
        if (plan instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> agg = (PhysicalHashAggregate<? extends Plan>) plan;
            if (agg.getAggPhase() == AggPhase.DISTINCT_GLOBAL) {
                return agg;
            }
        }
        for (Plan child : plan.children()) {
            PhysicalHashAggregate<? extends Plan> found = findFirstDistinctGlobalAgg(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private PhysicalHashJoin<? extends Plan, ? extends Plan> findFirstHashJoin(Plan plan) {
        if (plan instanceof PhysicalHashJoin) {
            return (PhysicalHashJoin<? extends Plan, ? extends Plan>) plan;
        }
        for (Plan child : plan.children()) {
            PhysicalHashJoin<? extends Plan, ? extends Plan> found = findFirstHashJoin(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private DistributionSpecHash getHashSpecFromJoinChild(Plan joinChild) {
        Optional<PhysicalDistribute<Plan>> distOpt = ShuffleKeyPruner.findHashDistributeUnderJoinChild(joinChild);
        Assertions.assertTrue(distOpt.isPresent());
        Assertions.assertInstanceOf(DistributionSpecHash.class, distOpt.get().getDistributionSpec());
        return (DistributionSpecHash) distOpt.get().getDistributionSpec();
    }

    private PhysicalDistribute<PhysicalEmptyRelation> newDistribute() {
        DistributionSpecHash spec = new DistributionSpecHash(
                ImmutableList.of(slotA.getExprId()), DistributionSpecHash.ShuffleType.REQUIRE);
        return new PhysicalDistribute<>(spec, empty);
    }

    @Test
    void testFindHashDistributeUnderJoinChild_directDistribute() {
        PhysicalDistribute<?> dist = newDistribute();
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(dist);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testFindHashDistributeUnderJoinChild_globalAggOverDistribute() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(globalAgg);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testFindHashDistributeUnderJoinChild_localAggOverDistribute_empty() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> localAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                        true, null, false, dist);
        Assertions.assertFalse(ShuffleKeyPruner.findHashDistributeUnderJoinChild(localAgg).isPresent());
    }

    @Test
    void testReplaceDistributeUnderJoinChild_keepsAggWrapper() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalDistribute<PhysicalEmptyRelation> newDist = newDistribute();
        Plan out = ShuffleKeyPruner.replaceDistributeUnderJoinChild(globalAgg, newDist);
        Assertions.assertInstanceOf(PhysicalHashAggregate.class, out);
        Assertions.assertSame(newDist, out.child(0));
    }

    @Test
    void testFindHashDistributeUnderJoinChild_projectGlobalAggOverDistribute() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(project);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testFindHashDistributeUnderJoinChild_filterProjectGlobalAggOverDistribute() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        PhysicalFilter<PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>>> filter =
                new PhysicalFilter<>(ImmutableSet.of(), null, project);
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(filter);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testReplaceDistributeUnderJoinChild_keepsProjectWrapper() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        PhysicalDistribute<PhysicalEmptyRelation> newDist = newDistribute();
        Plan out = ShuffleKeyPruner.replaceDistributeUnderJoinChild(project, newDist);
        Assertions.assertInstanceOf(PhysicalProject.class, out);
        Assertions.assertSame(newDist, out.child(0).child(0));
    }

    @Test
    void testReplaceDistributeUnderJoinChild_keepsFilterAndProjectWrappers() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        PhysicalFilter<PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>>> filter =
                new PhysicalFilter<>(ImmutableSet.of(), null, project);
        PhysicalDistribute<PhysicalEmptyRelation> newDist = newDistribute();
        Plan out = ShuffleKeyPruner.replaceDistributeUnderJoinChild(filter, newDist);
        Assertions.assertInstanceOf(PhysicalFilter.class, out);
        Assertions.assertInstanceOf(PhysicalProject.class, out.child(0));
        Assertions.assertSame(newDist, out.child(0).child(0).child(0));
    }

    @Test
    void testRecomputePhysicalPropertiesPostProcessorShouldRefreshWrapperDistribution() {
        SlotReference slotB = new SlotReference(new ExprId(1), "b",
                IntegerType.INSTANCE, true, ImmutableList.of());
        PhysicalEmptyRelation relation = new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(slotA, slotB), null);
        DistributionSpecHash childSpec = new DistributionSpecHash(
                ImmutableList.of(slotA.getExprId()), DistributionSpecHash.ShuffleType.REQUIRE);
        DistributionSpecHash staleSpec = new DistributionSpecHash(
                ImmutableList.of(slotB.getExprId()), DistributionSpecHash.ShuffleType.REQUIRE);
        PhysicalDistribute<PhysicalEmptyRelation> distribute = new PhysicalDistribute<>(
                childSpec,
                Optional.empty(),
                relation.getLogicalProperties(),
                PhysicalProperties.createHash(childSpec),
                null,
                relation);
        PhysicalFilter<? extends Plan> wrapper = new PhysicalFilter<>(ImmutableSet.of(), null, distribute)
                .withPhysicalPropertiesAndStats(PhysicalProperties.createHash(staleSpec), null);

        CascadesContext cascadesContext = CascadesContext.initContext(
                new StatementContext(connectContext, new OriginStatement("", 0)),
                wrapper, PhysicalProperties.ANY);
        Plan output = wrapper.accept(RecomputePhysicalPropertiesPostProcessor.INSTANCE, cascadesContext);

        Assertions.assertInstanceOf(PhysicalFilter.class, output);
        DistributionSpec outputSpec = ((PhysicalFilter<?>) output).getPhysicalProperties().getDistributionSpec();
        Assertions.assertInstanceOf(DistributionSpecHash.class, outputSpec);
        Assertions.assertEquals(childSpec.getOrderedShuffledColumns(),
                ((DistributionSpecHash) outputSpec).getOrderedShuffledColumns());
    }
}
