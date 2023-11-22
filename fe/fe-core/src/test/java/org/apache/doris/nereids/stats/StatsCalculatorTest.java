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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StatsCalculatorTest {
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    private Group newFakeGroup() {
        GroupExpression groupExpression = new GroupExpression(scan);
        Group group = new Group(null, groupExpression, new LogicalProperties(Collections::emptyList));
        group.getLogicalExpressions().remove(0);
        return group;
    }

    // TODO: temporary disable this test, until we could get column stats
    // @Test
    // public void testAgg() {
    //     List<String> qualifier = new ArrayList<>();
    //     qualifier.add("test");
    //     qualifier.add("t");
    //     SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
    //     SlotReference slot2 = new SlotReference("c2", IntegerType.INSTANCE, true, qualifier);
    //     ColumnStats columnStats1 = new ColumnStats();
    //     columnStats1.setNdv(10);
    //     columnStats1.setNumNulls(5);
    //     ColumnStats columnStats2 = new ColumnStats();
    //     columnStats2.setNdv(20);
    //     columnStats1.setNumNulls(10);
    //     Map<Slot, ColumnStats> slotColumnStatsMap = new HashMap<>();
    //     slotColumnStatsMap.put(slot1, columnStats1);
    //     slotColumnStatsMap.put(slot2, columnStats2);
    //     List<Expression> groupByExprList = new ArrayList<>();
    //     groupByExprList.add(slot1);
    //     AggregateFunction sum = new Sum(slot2);
    //     Statistics childStats = new Statistics(20, slotColumnStatsMap);
    //     Alias alias = new Alias(sum, "a");
    //     Group childGroup = newGroup();
    //     childGroup.setLogicalProperties(new LogicalProperties(new Supplier<List<Slot>>() {
    //         @Override
    //         public List<Slot> get() {
    //             return Collections.emptyList();
    //         }
    //     }));
    //     GroupPlan groupPlan = new GroupPlan(childGroup);
    //     childGroup.setStatistics(childStats);
    //     LogicalAggregate logicalAggregate = new LogicalAggregate(groupByExprList, Arrays.asList(alias), groupPlan);
    //     GroupExpression groupExpression = new GroupExpression(logicalAggregate, Arrays.asList(childGroup));
    //     Group ownerGroup = newGroup();
    //     groupExpression.setOwnerGroup(ownerGroup);
    //     StatsCalculator.estimate(groupExpression);
    //     Assertions.assertEquals(groupExpression.getOwnerGroup().getStatistics().getRowCount(), 10);
    // }

    @Test
    public void testFilter() {
        List<String> qualifier = Lists.newArrayList();
        qualifier.add("test");
        qualifier.add("t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        SlotReference slot2 = new SlotReference("c2", IntegerType.INSTANCE, true, qualifier);

        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setMinValue(0);
        columnStat1.setMaxValue(1000);
        columnStat1.setNumNulls(10);
        ColumnStatisticBuilder columnStat2 = new ColumnStatisticBuilder();
        columnStat2.setNdv(20);
        columnStat2.setMinValue(0);
        columnStat2.setMaxValue(1000);
        columnStat2.setNumNulls(10);

        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        slotColumnStatsMap.put(slot2, columnStat2.build());
        Statistics childStats = new Statistics(10000, slotColumnStatsMap);

        EqualTo eq1 = new EqualTo(slot1, new IntegerLiteral(1));
        EqualTo eq2 = new EqualTo(slot2, new IntegerLiteral(2));

        ImmutableSet and = ImmutableSet.of(eq1, eq2);
        ImmutableSet or = ImmutableSet.of(new Or(eq1, eq2));

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalFilter<GroupPlan> logicalFilter = new LogicalFilter<>(and, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalFilter, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Assertions.assertEquals((10000 * 0.1 * 0.05), ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group(null, groupExpressionOr, null);
        StatsCalculator.estimate(groupExpressionOr, null);
        Assertions.assertEquals((long) (10000 * (0.1 + 0.05 - 0.1 * 0.05)),
                ownerGroupOr.getStatistics().getRowCount(), 0.001);
    }

    // a, b are in (0,100)
    // a=200 and b=300 => output: 0 rows
    @org.junit.Test
    public void testFilterOutofRange() {
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        SlotReference slot2 = new SlotReference("c2", IntegerType.INSTANCE, true, qualifier);

        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setMinValue(0);
        columnStat1.setMaxValue(100);
        columnStat1.setNumNulls(10);
        ColumnStatisticBuilder columnStat2 = new ColumnStatisticBuilder();
        columnStat2.setNdv(20);
        columnStat2.setMinValue(0);
        columnStat2.setMaxValue(100);
        columnStat2.setNumNulls(10);

        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        slotColumnStatsMap.put(slot2, columnStat2.build());
        Statistics childStats = new Statistics(10000, slotColumnStatsMap);

        EqualTo eq1 = new EqualTo(slot1, new IntegerLiteral(200));
        EqualTo eq2 = new EqualTo(slot2, new IntegerLiteral(300));

        ImmutableSet and = ImmutableSet.of(new And(eq1, eq2));
        ImmutableSet or = ImmutableSet.of(new Or(eq1, eq2));

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalFilter<GroupPlan> logicalFilter = new LogicalFilter<>(and, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalFilter, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        groupExpression.setOwnerGroup(ownerGroup);
        StatsCalculator.estimate(groupExpression, null);
        Assertions.assertEquals(0, ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group(null, groupExpressionOr, null);
        groupExpressionOr.setOwnerGroup(ownerGroupOr);
        StatsCalculator.estimate(groupExpressionOr, null);
        Assertions.assertEquals(0, ownerGroupOr.getStatistics().getRowCount(), 0.001);
    }
    // TODO: temporary disable this test, until we could get column stats
    // @Test
    // public void testHashJoin() {
    //     List<String> qualifier = ImmutableList.of("test", "t");
    //     SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
    //     SlotReference slot2 = new SlotReference("c2", IntegerType.INSTANCE, true, qualifier);
    //     ColumnStats columnStats1 = new ColumnStats();
    //     columnStats1.setNdv(10);
    //     columnStats1.setNumNulls(5);
    //     ColumnStats columnStats2 = new ColumnStats();
    //     columnStats2.setNdv(20);
    //     columnStats1.setNumNulls(10);
    //     Map<Slot, ColumnStats> slotColumnStatsMap1 = new HashMap<>();
    //     slotColumnStatsMap1.put(slot1, columnStats1);
    //
    //     Map<Slot, ColumnStats> slotColumnStatsMap2 = new HashMap<>();
    //     slotColumnStatsMap2.put(slot2, columnStats2);
    //
    //     final long leftRowCount = 5000;
    //     Statistics leftStats = new Statistics(leftRowCount, slotColumnStatsMap1);
    //
    //     final long rightRowCount = 10000;
    //     Statistics rightStats = new Statistics(rightRowCount, slotColumnStatsMap2);
    //
    //     EqualTo equalTo = new EqualTo(slot1, slot2);
    //
    //     LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t", 0);
    //     LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(0, "t", 0);
    //     LogicalJoin<LogicalOlapScan, LogicalOlapScan> fakeSemiJoin = new LogicalJoin<>(
    //             JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(equalTo), Optional.empty(), scan1, scan2);
    //     LogicalJoin<LogicalOlapScan, LogicalOlapScan> fakeInnerJoin = new LogicalJoin<>(
    //             JoinType.INNER_JOIN, Lists.newArrayList(equalTo), Optional.empty(), scan1, scan2);
    //     Statistics semiJoinStats = JoinEstimation.estimate(leftStats, rightStats, fakeSemiJoin);
    //     Assertions.assertEquals(leftRowCount, semiJoinStats.getRowCount());
    //     Statistics innerJoinStats = JoinEstimation.estimate(leftStats, rightStats, fakeInnerJoin);
    //     Assertions.assertEquals(2500000, innerJoinStats.getRowCount());
    // }

    @Test
    public void testOlapScan() {
        long tableId1 = 0;
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference(new ExprId(0),
                "c1", IntegerType.INSTANCE, true, qualifier, new Column("c1", PrimitiveType.INT));

        OlapTable table1 = PlanConstructor.newOlapTable(tableId1, "t1", 0);
        LogicalOlapScan logicalOlapScan1 = (LogicalOlapScan) new LogicalOlapScan(
                StatementScopeIdGenerator.newRelationId(), table1,
                Collections.emptyList()).withGroupExprLogicalPropChildren(Optional.empty(),
                Optional.of(new LogicalProperties(() -> ImmutableList.of(slot1))), ImmutableList.of());

        GroupExpression groupExpression = new GroupExpression(logicalOlapScan1, ImmutableList.of());
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Statistics stats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, stats.columnStatistics().size());
        Assertions.assertNotNull(stats.columnStatistics().get(slot1));
    }

    @Test
    public void testLimit() {
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference(new ExprId(0),
                "c1", IntegerType.INSTANCE, true, qualifier, new Column("c1", PrimitiveType.INT));
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        Statistics childStats = new Statistics(10, slotColumnStatsMap);

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalLimit<? extends Plan> logicalLimit = new LogicalLimit<>(1, 2,
                LimitPhase.GLOBAL, new LogicalLimit<>(1, 2, LimitPhase.LOCAL, groupPlan));
        GroupExpression groupExpression = new GroupExpression(logicalLimit, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Statistics limitStats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, limitStats.getRowCount());
        ColumnStatistic slot1Stats = limitStats.columnStatistics().get(slot1);
        Assertions.assertEquals(1, slot1Stats.ndv, 0.1);
        Assertions.assertEquals(0, slot1Stats.numNulls, 0.1);
    }

    @Test
    public void testTopN() {
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        Statistics childStats = new Statistics(10, slotColumnStatsMap);

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalTopN<GroupPlan> logicalTopN = new LogicalTopN<>(Collections.emptyList(), 1, 2, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalTopN, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Statistics topNStats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, topNStats.getRowCount());
        ColumnStatistic slot1Stats = topNStats.columnStatistics().get(slot1);
        Assertions.assertEquals(1, slot1Stats.ndv, 0.1);
        Assertions.assertEquals(0, slot1Stats.numNulls, 0.1);
    }
}
