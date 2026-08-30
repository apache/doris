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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.Cast;
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
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setUp() {
        Mockito.when(jobContext.getRequiredProperties()).thenReturn(PhysicalProperties.ANY);
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

            RequestPropertyDeriver requestPropertyDeriver =
                    new RequestPropertyDeriver(testConnectContext, jobContext);
            List<List<PhysicalProperties>> actual =
                    requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

            List<List<PhysicalProperties>> expected = Lists.newArrayList();
            expected.add(Lists.newArrayList(
                    new PhysicalProperties(
                            new DistributionSpecHash(Lists.newArrayList(leftKey.getExprId()), ShuffleType.REQUIRE)),
                    new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(rightKey.getExprId()),
                            ShuffleType.REQUIRE))
            ));
            expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
            Assertions.assertEquals(expected, actual);

            sessionVariable.enableColocateMappingConstraint = true;
            List<List<PhysicalProperties>> enabled =
                    requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
            expected.add(1, Lists.newArrayList(
                    new PhysicalProperties(new DistributionSpecHash(
                            Lists.newArrayList(leftKey.getExprId()),
                            ShuffleType.COLOCATE_MAPPING_REQUIRE)),
                    new PhysicalProperties(new DistributionSpecHash(
                            Lists.newArrayList(rightKey.getExprId()),
                            ShuffleType.COLOCATE_MAPPING_REQUIRE))));
            Assertions.assertEquals(expected, enabled);

            PhysicalHashJoin<GroupPlan, GroupPlan> expressionJoin = new PhysicalHashJoin<>(
                    JoinType.INNER_JOIN,
                    ImmutableList.of(new EqualTo(leftKey, new Add(rightKey, Literal.of(1)))),
                    ExpressionUtils.EMPTY_CONDITION,
                    new DistributeHint(DistributeType.NONE),
                    Optional.empty(),
                    logicalProperties,
                    leftPlan,
                    rightPlan);
            GroupExpression expressionJoinGroup =
                    new GroupExpression(expressionJoin, Lists.newArrayList(leftGroup, rightGroup));
            new Group(null, expressionJoinGroup, null);

            List<List<PhysicalProperties>> expressionJoinRequests =
                    requestPropertyDeriver.getRequestChildrenPropertyList(expressionJoinGroup);
            Assertions.assertEquals(
                    Lists.newArrayList(expected.get(0), expected.get(2)),
                    expressionJoinRequests);
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
                groupPlan
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
                groupPlan
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
    void testGlobalAggregatePropagatesColocateMappingRequestWhenEnabled() {
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        SlotReference d1 = new SlotReference("d1", IntegerType.INSTANCE);
        SlotReference k2 = new SlotReference("k2", IntegerType.INSTANCE);
        SlotReference extra = new SlotReference("extra", IntegerType.INSTANCE);
        Alias outputD1 = new Alias(d1, "output_d1");
        Alias outputK2 = new Alias(k2, "output_k2");
        AggregateParam aggregateParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        Alias sum = new Alias(new AggregateExpression(new Sum(extra), aggregateParam), "sum_value");
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(d1, k2),
                Lists.newArrayList(outputD1, outputK2, sum),
                Optional.of(Lists.newArrayList(d1, k2)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlan);
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                Lists.newArrayList(outputD1.getExprId(), outputK2.getExprId(), sum.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);
        PhysicalProperties mappingRequest = PhysicalProperties.createHash(
                Lists.newArrayList(d1.getExprId(), k2.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);
        PhysicalProperties originalRequest = PhysicalProperties.createHash(
                Lists.newArrayList(d1.getExprId(), k2.getExprId()), ShuffleType.REQUIRE);

        testConnectContext.getSessionVariable().enableColocateMappingConstraint = true;
        List<List<PhysicalProperties>> enabled = new RequestPropertyDeriver(
                testConnectContext, parentProperties).getRequestChildrenPropertyList(groupExpression);
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of(mappingRequest), ImmutableList.of(originalRequest)), enabled);

        testConnectContext.getSessionVariable().enableColocateMappingConstraint = false;
        List<List<PhysicalProperties>> disabled = new RequestPropertyDeriver(
                testConnectContext, parentProperties).getRequestChildrenPropertyList(groupExpression);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(originalRequest)), disabled);

        testConnectContext.getSessionVariable().enableColocateMappingConstraint = true;
        PhysicalProperties partiallyMappableParent = PhysicalProperties.createHash(
                Lists.newArrayList(outputD1.getExprId(), sum.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);
        PhysicalProperties partialMappingRequest = PhysicalProperties.createHash(
                Lists.newArrayList(d1.getExprId()), ShuffleType.COLOCATE_MAPPING_REQUIRE);
        List<List<PhysicalProperties>> partiallyMappableRequests = new RequestPropertyDeriver(
                testConnectContext, partiallyMappableParent).getRequestChildrenPropertyList(groupExpression);
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of(partialMappingRequest), ImmutableList.of(originalRequest)),
                partiallyMappableRequests);

        PhysicalHashAggregate<GroupPlan> expressionGroupByAggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(new EqualTo(d1, k2), k2),
                Lists.newArrayList(outputD1, outputK2, sum),
                Optional.of(Lists.newArrayList(d1, k2)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlan);
        GroupExpression expressionGroupBy = new GroupExpression(expressionGroupByAggregate);
        new Group(null, expressionGroupBy, null);
        testConnectContext.getSessionVariable().enableColocateMappingConstraint = true;
        List<List<PhysicalProperties>> expressionGroupByRequests = new RequestPropertyDeriver(
                testConnectContext, parentProperties).getRequestChildrenPropertyList(expressionGroupBy);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(originalRequest)), expressionGroupByRequests);
    }

    @Test
    void testDistinctAndDeduplicateAggregatesDoNotPropagateColocateMappingRequest() {
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().enableColocateMappingConstraint = true;
        SlotReference d1 = new SlotReference("d1", IntegerType.INSTANCE);
        SlotReference k2 = new SlotReference("k2", IntegerType.INSTANCE);
        SlotReference extra = new SlotReference("extra", IntegerType.INSTANCE);
        Alias outputD1 = new Alias(d1, "output_d1");
        Alias outputK2 = new Alias(k2, "output_k2");
        AggregateParam aggregateParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        Alias distinctCount = new Alias(
                new AggregateExpression(new MultiDistinctCount(extra), aggregateParam), "distinct_count");
        Alias distinctSum = new Alias(
                new AggregateExpression(new Sum(true, extra), aggregateParam), "distinct_sum");
        Alias sum = new Alias(new AggregateExpression(new Sum(extra), aggregateParam), "sum_value");
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                Lists.newArrayList(outputD1.getExprId(), outputK2.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);
        PhysicalProperties originalRequest = PhysicalProperties.createHash(
                Lists.newArrayList(d1.getExprId(), k2.getExprId()), ShuffleType.REQUIRE);

        PhysicalHashAggregate<GroupPlan> distinctAggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(d1, k2),
                Lists.newArrayList(outputD1, outputK2, distinctCount),
                Optional.of(Lists.newArrayList(d1, k2)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlan);

        PhysicalHashAggregate<GroupPlan> mixedDistinctAggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(d1, k2),
                Lists.newArrayList(outputD1, outputK2, sum, distinctSum),
                Optional.of(Lists.newArrayList(d1, k2)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlan);

        AggregateParam distinctPhaseParam = new AggregateParam(
                AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT);
        Alias distinctPhaseSum = new Alias(
                new AggregateExpression(new Sum(extra), distinctPhaseParam), "distinct_phase_sum");
        PhysicalHashAggregate<GroupPlan> distinctPhaseAggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(d1, k2),
                Lists.newArrayList(outputD1, outputK2, distinctPhaseSum),
                Optional.of(Lists.newArrayList(d1, k2)),
                distinctPhaseParam,
                true,
                logicalProperties,
                false,
                groupPlan);

        PhysicalHashAggregate<GroupPlan> deduplicateAggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(d1, k2),
                Lists.newArrayList(outputD1, outputK2),
                Optional.of(Lists.newArrayList(d1, k2)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlan);

        for (PhysicalHashAggregate<GroupPlan> barrier
                : ImmutableList.of(distinctAggregate, mixedDistinctAggregate,
                        distinctPhaseAggregate, deduplicateAggregate)) {
            GroupExpression groupExpression = new GroupExpression(barrier);
            new Group(null, groupExpression, null);
            Assertions.assertEquals(ImmutableList.of(ImmutableList.of(originalRequest)),
                    new RequestPropertyDeriver(testConnectContext, parentProperties)
                            .getRequestChildrenPropertyList(groupExpression));
        }
    }

    @Test
    void testAggregateRemapsColocateMappingRequestThroughWideningVarcharCast() {
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().enableColocateMappingConstraint = true;
        SlotReference determinant = new SlotReference("determinant", new VarcharType(8));
        SlotReference value = new SlotReference("value", IntegerType.INSTANCE);
        Alias widenedOutput =
                new Alias(new Cast(determinant, new VarcharType(32)), "widened_determinant");
        AggregateParam aggregateParam =
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        Alias sum = new Alias(new AggregateExpression(new Sum(value), aggregateParam), "sum_value");
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(determinant),
                Lists.newArrayList(widenedOutput, sum),
                Optional.of(Lists.newArrayList(determinant)),
                aggregateParam,
                true,
                logicalProperties,
                false,
                groupPlan);
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                Lists.newArrayList(widenedOutput.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);

        List<List<PhysicalProperties>> requests = new RequestPropertyDeriver(
                testConnectContext, parentProperties)
                .getRequestChildrenPropertyList(groupExpression);

        Assertions.assertEquals(
                PhysicalProperties.createHash(
                        Lists.newArrayList(determinant.getExprId()),
                        ShuffleType.COLOCATE_MAPPING_REQUIRE),
                requests.get(0).get(0));
    }

    @Test
    void testUnionDoesNotPropagateColocateMappingRequest() {
        SlotReference outputD1 = new SlotReference("output_d1", IntegerType.INSTANCE);
        SlotReference outputK2 = new SlotReference("output_k2", IntegerType.INSTANCE);
        SlotReference leftD1 = new SlotReference("left_d1", IntegerType.INSTANCE);
        SlotReference leftK2 = new SlotReference("left_k2", IntegerType.INSTANCE);
        SlotReference rightD1 = new SlotReference("right_d1", IntegerType.INSTANCE);
        SlotReference rightK2 = new SlotReference("right_k2", IntegerType.INSTANCE);
        PhysicalUnion union = new PhysicalUnion(
                Qualifier.ALL,
                ImmutableList.of(outputD1, outputK2),
                ImmutableList.of(
                        ImmutableList.of(leftD1, leftK2),
                        ImmutableList.of(rightD1, rightK2)),
                ImmutableList.of(),
                logicalProperties,
                ImmutableList.of(groupPlan, groupPlan));
        GroupExpression groupExpression = new GroupExpression(union, Lists.newArrayList(group, group));
        new Group(null, groupExpression, null);
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                ImmutableList.of(outputD1.getExprId(), outputK2.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);

        List<List<PhysicalProperties>> actual = new RequestPropertyDeriver(
                MemoTestUtils.createConnectContext(), parentProperties)
                .getRequestChildrenPropertyList(groupExpression);

        Assertions.assertEquals(
                ImmutableList.of(ImmutableList.of(PhysicalProperties.ANY, PhysicalProperties.ANY)),
                actual);
    }

    @Test
    void testIntersectAndExceptDoNotPropagateColocateMappingRequest() {
        SlotReference outputD1 = new SlotReference("output_d1", IntegerType.INSTANCE);
        SlotReference outputK2 = new SlotReference("output_k2", IntegerType.INSTANCE);
        SlotReference leftD1 = new SlotReference("left_d1", IntegerType.INSTANCE);
        SlotReference leftK2 = new SlotReference("left_k2", IntegerType.INSTANCE);
        SlotReference rightD1 = new SlotReference("right_d1", IntegerType.INSTANCE);
        SlotReference rightK2 = new SlotReference("right_k2", IntegerType.INSTANCE);
        List<List<SlotReference>> childrenOutputs = ImmutableList.of(
                ImmutableList.of(leftD1, leftK2),
                ImmutableList.of(rightD1, rightK2));
        List<Plan> children = ImmutableList.of(groupPlan, groupPlan);
        List<PhysicalSetOperation> setOperations = ImmutableList.of(
                new PhysicalIntersect(Qualifier.DISTINCT,
                        ImmutableList.of(outputD1, outputK2), childrenOutputs, logicalProperties, children),
                new PhysicalExcept(Qualifier.DISTINCT,
                        ImmutableList.of(outputD1, outputK2), childrenOutputs, logicalProperties, children));
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                ImmutableList.of(outputD1.getExprId(), outputK2.getExprId()),
                ShuffleType.COLOCATE_MAPPING_REQUIRE);

        for (PhysicalSetOperation setOperation : setOperations) {
            GroupExpression groupExpression =
                    new GroupExpression(setOperation, Lists.newArrayList(group, group));
            new Group(null, groupExpression, null);
            List<List<PhysicalProperties>> actual = new RequestPropertyDeriver(
                    MemoTestUtils.createConnectContext(), parentProperties)
                    .getRequestChildrenPropertyList(groupExpression);

            Assertions.assertEquals(1, actual.size());
            Assertions.assertEquals(2, actual.get(0).size());
            for (PhysicalProperties childRequest : actual.get(0)) {
                Assertions.assertInstanceOf(DistributionSpecHash.class, childRequest.getDistributionSpec());
                Assertions.assertNotEquals(ShuffleType.COLOCATE_MAPPING_REQUIRE,
                        ((DistributionSpecHash) childRequest.getDistributionSpec()).getShuffleType());
            }
        }
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
        // Create ConnectContext with aggShuffleUseParentKey = false
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().aggShuffleUseParentKey = false;
        testConnectContext.getSessionVariable().setBeNumberForTest(3);

        SlotReference key1 = new SlotReference(new ExprId(0), "col1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference key2 = new SlotReference(new ExprId(1), "col2", IntegerType.INSTANCE, true, ImmutableList.of());
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key1, key2),
                Lists.newArrayList(key1, key2),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);

        // Create a parent hash distribution with key1 only
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                Lists.newArrayList(key1.getExprId()), ShuffleType.REQUIRE);

        Mockito.when(jobContext.getRequiredProperties()).thenReturn(parentProperties);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(testConnectContext, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        // When aggShuffleUseParentKey is false, should only use all groupByExpressions (key1, key2)
        // and not use parent key (key1) separately
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.createHash(
                Lists.newArrayList(key1.getExprId(), key2.getExprId()), ShuffleType.REQUIRE)));
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testAggregateWithAggShuffleUseParentKeyEnabled() {
        // Create ConnectContext with aggShuffleUseParentKey = true (default value)
        ConnectContext testConnectContext = MemoTestUtils.createConnectContext();
        testConnectContext.getSessionVariable().aggShuffleUseParentKey = true;
        testConnectContext.getSessionVariable().setBeNumberForTest(3);

        SlotReference key1 = new SlotReference(new ExprId(0), "col1", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference key2 = new SlotReference(new ExprId(1), "col2", IntegerType.INSTANCE, true, ImmutableList.of());
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key1, key2),
                Lists.newArrayList(key1, key2),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate) {
            @Override
            public org.apache.doris.statistics.Statistics childStatistics(int idx) {
                return null;
            }
        };
        new Group(null, groupExpression, null);

        // Create a parent hash distribution with key1 only
        PhysicalProperties parentProperties = PhysicalProperties.createHash(
                Lists.newArrayList(key1.getExprId()), ShuffleType.REQUIRE);

        Mockito.when(jobContext.getRequiredProperties()).thenReturn(parentProperties);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(testConnectContext, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        // When aggShuffleUseParentKey is true, shouldUseParent may return true
        // If shouldUseParent returns true, it will add parent key (key1) first, then all groupByExpressions (key1, key2)
        Assertions.assertEquals(2, actual.size(), "Should have at least one property request");
        PhysicalProperties parentProp = PhysicalProperties.createHash(
                Lists.newArrayList(key1.getExprId()), ShuffleType.REQUIRE);
        PhysicalProperties aggProp = PhysicalProperties.createHash(
                Lists.newArrayList(key1.getExprId(), key2.getExprId()), ShuffleType.REQUIRE);
        Assertions.assertTrue(actual.contains(ImmutableList.of(aggProp)) && actual.contains(ImmutableList.of(parentProp)));
    }
}
