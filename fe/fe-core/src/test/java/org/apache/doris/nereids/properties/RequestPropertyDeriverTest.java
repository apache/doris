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

package org.apache.doris.nereids.properties;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;

class RequestPropertyDeriverTest {

    GroupExpression ge = new GroupExpression(
            new LogicalOneRowRelation(
                    new RelationId(1),
                    ImmutableList.of(new Alias(Literal.of(1)))
            ),
            ImmutableList.of()
    );

    GroupPlan groupPlan = new GroupPlan(
            new Group(GroupId.createGenerator().getNextId(),
                    ge.getPlan().getLogicalProperties()
            )
    );

    LogicalProperties logicalProperties = Mockito.mock(LogicalProperties.class);

    ConnectContext connectContext = Mockito.mock(ConnectContext.class);

    Group group = Mockito.mock(Group.class);

    JobContext jobContext = Mockito.mock(JobContext.class);

    SlotReference aggregateKey1;
    SlotReference aggregateKey2;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setUp() {
        Mockito.when(jobContext.getRequiredProperties()).thenReturn(PhysicalProperties.ANY);
        aggregateKey1 = new SlotReference(new ExprId(0), "col1",
                IntegerType.INSTANCE, true, ImmutableList.of());
        aggregateKey2 = new SlotReference(new ExprId(1), "col2",
                IntegerType.INSTANCE, true, ImmutableList.of());
    }

    // #66112: enable_strict_consistency_dml is an OPTIONAL consistency knob for UPDATE, but the SQL MERGE
    // cardinality rule ("a target row matched by more than one source row is an error") is mandatory. BE can
    // only detect the duplicates when the plan keeps the sink's required distribution, so turning the knob off
    // must NOT relax a MERGE to PhysicalProperties.ANY. Both polarities are pinned: with the knob off, an
    // UPDATE still relaxes (otherwise the knob would be dead) and a MERGE still requires.
    @Test
    void testExternalRowLevelMergeSinkKeepsMergeDistributionWhenStrictConsistencyDmlIsOff() {
        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(PhysicalProperties.GATHER)),
                requestChildrenPropertiesForMergeSink(true, false),
                "a SQL MERGE must keep the sink's required distribution even with strict-consistency DML off");
        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(PhysicalProperties.ANY)),
                requestChildrenPropertiesForMergeSink(false, false),
                "an UPDATE has no cardinality rule, so strict-consistency DML off still relaxes it to ANY");
    }

    private List<List<PhysicalProperties>> requestChildrenPropertiesForMergeSink(
            boolean requireMergeCardinalityCheck, boolean enableStrictConsistencyDml) {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableStrictConsistencyDml = enableStrictConsistencyDml;
        ctx.setSessionVariable(sessionVariable);

        PhysicalExternalRowLevelMergeSink<GroupPlan> sink = new PhysicalExternalRowLevelMergeSink<>(
                Mockito.mock(ExternalDatabase.class), Mockito.mock(ExternalTable.class),
                ImmutableList.<Column>of(), ImmutableList.of(), requireMergeCardinalityCheck,
                Optional.empty(), logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sink);
        new Group(null, groupExpression, null);

        return new RequestPropertyDeriver(ctx, jobContext).getRequestChildrenPropertyList(groupExpression);
    }

    @Test
    void testNestedLoopJoin() {
        PhysicalNestedLoopJoin<GroupPlan, GroupPlan> join = new PhysicalNestedLoopJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, Optional.empty(), logicalProperties,
                groupPlan,
                groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testShuffleHashJoin() {
        SlotReference leftKey = new SlotReference("left", IntegerType.INSTANCE);
        SlotReference rightKey = new SlotReference("right", IntegerType.INSTANCE);
        GroupPlan leftPlan = new GroupPlan(new Group(GroupId.createGenerator().getNextId(),
                new GroupExpression(new LogicalOneRowRelation(new RelationId(2), ImmutableList.of(leftKey)))
                        .getPlan().getLogicalProperties()));
        GroupPlan rightPlan = new GroupPlan(new Group(GroupId.createGenerator().getNextId(),
                new GroupExpression(new LogicalOneRowRelation(new RelationId(3), ImmutableList.of(rightKey)))
                        .getPlan().getLogicalProperties()));
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(leftKey, rightKey)), ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties, leftPlan, rightPlan);
        GroupExpression groupExpression = new GroupExpression(join, Lists.newArrayList(group, group));
        new Group(null, groupExpression, null);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(
                new PhysicalProperties(
                        new DistributionSpecHash(Lists.newArrayList(leftKey.getExprId()), ShuffleType.REQUIRE)),
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(rightKey.getExprId()),
                        ShuffleType.REQUIRE))
        ));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testShuffleOrBroadcastHashJoin() {
        try (MockedStatic<ConnectContext> mockedConnectContext = Mockito.mockStatic(ConnectContext.class)) {
            ConnectContext testConnectContext = new ConnectContext();
            SessionVariable sessionVariable = new SessionVariable();
            testConnectContext.setSessionVariable(sessionVariable);
            mockedConnectContext.when(ConnectContext::get).thenReturn(testConnectContext);
            SlotReference leftKey = new SlotReference("left", IntegerType.INSTANCE);
            SlotReference rightKey = new SlotReference("right", IntegerType.INSTANCE);
            GroupPlan leftPlan = new GroupPlan(new Group(GroupId.createGenerator().getNextId(),
                    new GroupExpression(new LogicalOneRowRelation(new RelationId(4), ImmutableList.of(leftKey)))
                            .getPlan().getLogicalProperties()));
            GroupPlan rightPlan = new GroupPlan(new Group(GroupId.createGenerator().getNextId(),
                    new GroupExpression(new LogicalOneRowRelation(new RelationId(5), ImmutableList.of(rightKey)))
                            .getPlan().getLogicalProperties()));
            PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                    ImmutableList.of(new EqualTo(leftKey, rightKey)), ExpressionUtils.EMPTY_CONDITION,
                    new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties, leftPlan, rightPlan);
            Group leftGroup = Mockito.mock(Group.class);
            Group rightGroup = Mockito.mock(Group.class);
            org.apache.doris.statistics.Statistics stats = Mockito.mock(org.apache.doris.statistics.Statistics.class);
            Mockito.when(stats.computeSize(Mockito.anyList())).thenReturn(1D);
            Mockito.when(stats.getRowCount()).thenReturn(1D);
            Mockito.when(leftGroup.getStatistics()).thenReturn(stats);
            Mockito.when(rightGroup.getStatistics()).thenReturn(stats);
            GroupExpression groupExpression = new GroupExpression(join, Lists.newArrayList(leftGroup, rightGroup));
            new Group(null, groupExpression, null);

            RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
            List<List<PhysicalProperties>> actual
                    = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

            List<List<PhysicalProperties>> expected = Lists.newArrayList();
            expected.add(Lists.newArrayList(
                    new PhysicalProperties(
                            new DistributionSpecHash(Lists.newArrayList(leftKey.getExprId()), ShuffleType.REQUIRE)),
                    new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(rightKey.getExprId()),
                            ShuffleType.REQUIRE))
            ));
            expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
            Assertions.assertEquals(expected, actual);
        }
    }

    @Test
    void testLocalAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlanWithOutput(key)
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testGlobalAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlanWithOutput(key)
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.createHash(new DistributionSpecHash(
                Lists.newArrayList(key.getExprId()),
                ShuffleType.REQUIRE
        ))));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testGlobalAggregateWithoutGroupByUsesExplicitPartition() {
        SlotReference distinctKey = new SlotReference("distinct_key", IntegerType.INSTANCE);
        AggregateParam aggregateParam = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        Alias output = new Alias(new AggregateExpression(
                new MultiDistinctCount(distinctKey), aggregateParam));
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                ImmutableList.of(),
                ImmutableList.of(output),
                Optional.of(ImmutableList.of(distinctKey)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlanWithOutput(distinctKey)
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);

        List<List<PhysicalProperties>> actual = new RequestPropertyDeriver(null, jobContext)
                .getRequestChildrenPropertyList(groupExpression);

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(PhysicalProperties.createHash(
                ImmutableList.of(distinctKey.getExprId()), ShuffleType.REQUIRE))), actual);
    }

    @Test
    void testGlobalAggregateWithoutPartition() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testAssertNumRows() {
        PhysicalAssertNumRows<GroupPlan> assertNumRows = new PhysicalAssertNumRows<>(
                new AssertNumRowsElement(1, "", AssertNumRowsElement.Assertion.EQ),
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(assertNumRows);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testWindowWithPartitionKeyAndOrderKey() {
        SlotReference col1 = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference col2 = new SlotReference("col2", IntegerType.INSTANCE);
        Expression rowNumber = new RowNumber();
        WindowExpression windowExpression = new WindowExpression(rowNumber, ImmutableList.of(col1),
                ImmutableList.of(new OrderExpression(new OrderKey(col2, true, false))),
                new WindowFrame(FrameUnitsType.RANGE,
                        FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        Alias alias = new Alias(windowExpression);
        WindowFrameGroup windowFrameGroup = new WindowFrameGroup(alias);
        PhysicalWindow<GroupPlan> window = new PhysicalWindow<>(windowFrameGroup, null,
                ImmutableList.of(alias), false, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(window);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.createHash(ImmutableList.of(col1.getExprId()), ShuffleType.REQUIRE).withOrderSpec(
                new OrderSpec(ImmutableList.of(new OrderKey(col1, true, false), new OrderKey(col2, true, false)))
        )));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testWindowWithPartitionKeyAndNoOrderKey() {
        SlotReference col1 = new SlotReference("col1", IntegerType.INSTANCE);
        Expression rowNumber = new RowNumber();
        WindowExpression windowExpression = new WindowExpression(rowNumber, ImmutableList.of(col1),
                ImmutableList.of(),
                new WindowFrame(FrameUnitsType.RANGE,
                        FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        Alias alias = new Alias(windowExpression);
        WindowFrameGroup windowFrameGroup = new WindowFrameGroup(alias);
        PhysicalWindow<GroupPlan> window = new PhysicalWindow<>(windowFrameGroup, null,
                ImmutableList.of(alias), false, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(window);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.createHash(ImmutableList.of(col1.getExprId()), ShuffleType.REQUIRE).withOrderSpec(
                new OrderSpec(ImmutableList.of(new OrderKey(col1, true, false)))
        )));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testWindowWithNoPartitionKeyAndOrderKey() {
        SlotReference col2 = new SlotReference("col2", IntegerType.INSTANCE);
        Expression rowNumber = new RowNumber();
        WindowExpression windowExpression = new WindowExpression(rowNumber, ImmutableList.of(),
                ImmutableList.of(new OrderExpression(new OrderKey(col2, true, false))),
                new WindowFrame(FrameUnitsType.RANGE,
                        FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        Alias alias = new Alias(windowExpression);
        WindowFrameGroup windowFrameGroup = new WindowFrameGroup(alias);
        PhysicalWindow<GroupPlan> window = new PhysicalWindow<>(windowFrameGroup, null,
                ImmutableList.of(alias), false, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(window);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER.withOrderSpec(
                new OrderSpec(ImmutableList.of(new OrderKey(col2, true, false)))
        )));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testWindowWithNoPartitionKeyAndNoOrderKey() {
        Expression rowNumber = new RowNumber();
        WindowExpression windowExpression = new WindowExpression(rowNumber, ImmutableList.of(),
                ImmutableList.of(),
                new WindowFrame(FrameUnitsType.RANGE,
                        FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        Alias alias = new Alias(windowExpression);
        WindowFrameGroup windowFrameGroup = new WindowFrameGroup(alias);
        PhysicalWindow<GroupPlan> window = new PhysicalWindow<>(windowFrameGroup, null,
                ImmutableList.of(alias), false, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(window);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyDisabled() {
        Assertions.assertEquals(fullAggregateRequests(), requestAggregateProperties(false, null));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndMissingChildStats() {
        Assertions.assertEquals(fullAggregateRequests(), requestAggregateProperties(true, null));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndUnknownParentKeyStats() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, ColumnStatistic.UNKNOWN)
                .build();

        Assertions.assertEquals(fullAggregateRequests(), requestAggregateProperties(true, childStats));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndLowNdvStats() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(AggregateUtils.LOW_NDV_THRESHOLD)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(fullAggregateRequests(), requestAggregateProperties(true, childStats));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndNdvTooLowForInstanceCount() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(1_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(
                fullAggregateRequests(), requestAggregateProperties(true, childStats, 3000));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndHighNdvWithoutHotValueStats() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .build())
                .build();

        Assertions.assertEquals(parentAndFullAggregateRequests(),
                requestAggregateProperties(true, childStats));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndHighNdvWithCollectedNoHotValues() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(parentAggregateProperty()),
                        ImmutableList.of(fullAggregateProperty())),
                requestAggregateProperties(true, childStats));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndHotNullParentKeyStats() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .setNumNulls(5000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(fullAggregateRequests(), requestAggregateProperties(true, childStats));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledForJoinDerivedTpcdsKey() {
        double rowCount = 2_870_000_000D;
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(rowCount)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(rowCount)
                        .setNdv(10_000_000)
                        .setNumNulls(67_000_000)
                        .build())
                .build();

        Assertions.assertEquals(parentAndFullAggregateRequests(),
                requestAggregateProperties(true, childStats, 100));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndSmallNullFraction() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(1_000_000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(1_000_000)
                        .setNdv(500_000)
                        .setNumNulls(20)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(parentAggregateProperty()),
                        ImmutableList.of(fullAggregateProperty())),
                requestAggregateProperties(true, childStats));
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabledAndHotValueParentKeyStats() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.2f))
                        .build())
                .build();

        Assertions.assertEquals(fullAggregateRequests(), requestAggregateProperties(true, childStats));
    }

    @Test
    void testUnsafeExplicitPartitionExpressionsFallBackToFullGroupBy() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.005f))
                        .build())
                .build();

        Assertions.assertEquals(
                fullAggregateRequests(), requestAggregatePropertiesWithPartition(childStats, 3000));
    }

    @Test
    void testSafeExplicitPartitionExpressionsArePreserved() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(parentAggregateProperty())),
                requestAggregatePropertiesWithPartition(childStats, 3));
    }

    @Test
    void testDistinctGlobalKeepsStagePartitionExpressions() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(100)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(parentAggregateProperty())),
                requestAggregatePropertiesWithPartition(
                        childStats, 3, ImmutableList.of(aggregateKey1), AggPhase.DISTINCT_GLOBAL));
    }

    @Test
    void testDuplicateExplicitPartitionExpressionsDoNotBypassSafety() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(100)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(fullAggregateRequests(), requestAggregatePropertiesWithPartition(
                childStats, 3, ImmutableList.of(aggregateKey1, aggregateKey1)));
    }

    @Test
    void testKnownSmallNullFractionAllowsMultiKeyParentShuffle() {
        SlotReference aggregateKey3 = new SlotReference(new ExprId(2), "col3",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(1100)
                        .setNumNulls(5_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .putColumnStatistics(aggregateKey2, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(parentAndFullAggregateRequests(aggregateKey3),
                requestThreeKeyAggregateProperties(aggregateKey3, childStats, 3000));
    }

    @Test
    void testKnownSmallHotValueAllowsMultiKeyParentShuffle() {
        SlotReference aggregateKey3 = new SlotReference(new ExprId(2), "col3",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(1100)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.005f))
                        .build())
                .putColumnStatistics(aggregateKey2, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();

        Assertions.assertEquals(parentAndFullAggregateRequests(aggregateKey3),
                requestThreeKeyAggregateProperties(aggregateKey3, childStats, 3000));
    }

    @Test
    void testExplicitPartitionStillRejectsMissingHotValueStatistics() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(aggregateKey1, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .build())
                .build();

        Assertions.assertEquals(fullAggregateRequests(),
                requestAggregatePropertiesWithPartition(childStats, 3));
    }

    private List<List<PhysicalProperties>> requestAggregatePropertiesWithPartition(
            Statistics childStats, int beNumber) {
        return requestAggregatePropertiesWithPartition(
                childStats, beNumber, ImmutableList.of(aggregateKey1));
    }

    private List<List<PhysicalProperties>> requestAggregatePropertiesWithPartition(
            Statistics childStats, int beNumber, List<Expression> partitionExpressions) {
        return requestAggregatePropertiesWithPartition(
                childStats, beNumber, partitionExpressions, AggPhase.GLOBAL);
    }

    private List<List<PhysicalProperties>> requestAggregatePropertiesWithPartition(
            Statistics childStats, int beNumber, List<Expression> partitionExpressions, AggPhase aggPhase) {
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().setBeNumberForTest(beNumber);
        testConnectContext.getSessionVariable().parallelPipelineTaskNum = 1;

        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(aggregateKey1, aggregateKey2),
                Lists.newArrayList(aggregateKey1, aggregateKey2),
                Optional.of(partitionExpressions),
                new AggregateParam(aggPhase, AggMode.BUFFER_TO_RESULT),
                true, logicalProperties, false,
                groupPlanWithOutput(aggregateKey1, aggregateKey2));
        GroupExpression groupExpression = new GroupExpression(aggregate) {
            @Override
            public Statistics childStatistics(int idx) {
                return childStats;
            }
        };
        new Group(null, groupExpression, null);

        Mockito.when(jobContext.getRequiredProperties()).thenReturn(PhysicalProperties.ANY);
        return new RequestPropertyDeriver(testConnectContext, jobContext)
                .getRequestChildrenPropertyList(groupExpression);
    }

    private List<List<PhysicalProperties>> requestThreeKeyAggregateProperties(
            SlotReference aggregateKey3, Statistics childStats) {
        return requestThreeKeyAggregateProperties(aggregateKey3, childStats, 3);
    }

    private List<List<PhysicalProperties>> requestThreeKeyAggregateProperties(
            SlotReference aggregateKey3, Statistics childStats, int beNumber) {
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().aggShuffleUseParentKey = true;
        testConnectContext.getSessionVariable().setHotValueThreshold(0.1);
        testConnectContext.getSessionVariable().setBeNumberForTest(beNumber);
        testConnectContext.getSessionVariable().parallelPipelineTaskNum = 1;

        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(aggregateKey1, aggregateKey2, aggregateKey3),
                Lists.newArrayList(aggregateKey1, aggregateKey2, aggregateKey3),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, logicalProperties, false,
                groupPlanWithOutput(aggregateKey1, aggregateKey2, aggregateKey3));
        GroupExpression groupExpression = new GroupExpression(aggregate) {
            @Override
            public Statistics childStatistics(int idx) {
                return childStats;
            }
        };
        new Group(null, groupExpression, null);

        Mockito.when(jobContext.getRequiredProperties()).thenReturn(PhysicalProperties.createHash(
                ImmutableList.of(aggregateKey1.getExprId(), aggregateKey2.getExprId()), ShuffleType.REQUIRE));
        return new RequestPropertyDeriver(testConnectContext, jobContext)
                .getRequestChildrenPropertyList(groupExpression);
    }

    private List<List<PhysicalProperties>> fullAggregateRequests() {
        return ImmutableList.of(ImmutableList.of(fullAggregateProperty()));
    }

    private List<List<PhysicalProperties>> fullAggregateRequests(SlotReference aggregateKey3) {
        return ImmutableList.of(ImmutableList.of(PhysicalProperties.createHash(
                ImmutableList.of(aggregateKey1.getExprId(), aggregateKey2.getExprId(), aggregateKey3.getExprId()),
                ShuffleType.REQUIRE)));
    }

    private List<List<PhysicalProperties>> parentAndFullAggregateRequests() {
        return ImmutableList.of(
                ImmutableList.of(parentAggregateProperty()),
                ImmutableList.of(fullAggregateProperty()));
    }

    private List<List<PhysicalProperties>> parentAndFullAggregateRequests(SlotReference aggregateKey3) {
        return ImmutableList.of(
                ImmutableList.of(PhysicalProperties.createHash(
                        ImmutableList.of(aggregateKey1.getExprId(), aggregateKey2.getExprId()), ShuffleType.REQUIRE)),
                ImmutableList.of(PhysicalProperties.createHash(
                        ImmutableList.of(aggregateKey1.getExprId(), aggregateKey2.getExprId(),
                                aggregateKey3.getExprId()), ShuffleType.REQUIRE)));
    }

    private List<List<PhysicalProperties>> requestAggregateProperties(
            boolean aggShuffleUseParentKey, Statistics childStats) {
        return requestAggregateProperties(aggShuffleUseParentKey, childStats, 3);
    }

    private List<List<PhysicalProperties>> requestAggregateProperties(
            boolean aggShuffleUseParentKey, Statistics childStats, int beNumber) {
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().aggShuffleUseParentKey = aggShuffleUseParentKey;
        testConnectContext.getSessionVariable().setHotValueThreshold(0.1);
        testConnectContext.getSessionVariable().setBeNumberForTest(beNumber);
        testConnectContext.getSessionVariable().parallelPipelineTaskNum = 1;

        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(aggregateKey1, aggregateKey2),
                Lists.newArrayList(aggregateKey1, aggregateKey2),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, logicalProperties, false,
                groupPlanWithOutput(aggregateKey1, aggregateKey2));
        GroupExpression groupExpression = new GroupExpression(aggregate) {
            @Override
            public Statistics childStatistics(int idx) {
                return childStats;
            }
        };
        new Group(null, groupExpression, null);

        Mockito.when(jobContext.getRequiredProperties()).thenReturn(parentAggregateProperty());
        return new RequestPropertyDeriver(testConnectContext, jobContext)
                .getRequestChildrenPropertyList(groupExpression);
    }

    private PhysicalProperties parentAggregateProperty() {
        return PhysicalProperties.createHash(
                ImmutableList.of(aggregateKey1.getExprId()), ShuffleType.REQUIRE);
    }

    private PhysicalProperties fullAggregateProperty() {
        return PhysicalProperties.createHash(
                ImmutableList.of(aggregateKey1.getExprId(), aggregateKey2.getExprId()), ShuffleType.REQUIRE);
    }

    private GroupPlan groupPlanWithOutput(SlotReference... outputs) {
        GroupPlan child = Mockito.mock(GroupPlan.class);
        Mockito.when(child.getAllChildrenTypes()).thenReturn(new BitSet());
        Mockito.when(child.getOutput()).thenReturn(ImmutableList.copyOf(outputs));
        return child;
    }
}
