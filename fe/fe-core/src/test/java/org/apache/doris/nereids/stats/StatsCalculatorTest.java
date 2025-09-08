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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.TableStatsMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StatsCalculatorTest {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    private Group newFakeGroup() {
        GroupExpression groupExpression = new GroupExpression(scan1);
        Group group = new Group(null, groupExpression,
                new LogicalProperties(Collections::emptyList, () -> DataTrait.EMPTY_TRAIT));
        group.getLogicalExpressions().remove(0);
        return group;
    }

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
        Assertions.assertEquals(49.90005, ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group(null, groupExpressionOr, null);
        StatsCalculator.estimate(groupExpressionOr, null);
        Assertions.assertEquals(1448.555,
                ownerGroupOr.getStatistics().getRowCount(), 0.1);
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

        ImmutableSet and = ImmutableSet.of(eq1, eq2);
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

    @Test
    public void testOlapScan() {
        long tableId1 = 0;
        OlapTable table1 = PlanConstructor.newOlapTable(tableId1, "t1", 0);
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference(new ExprId(0), "c1", IntegerType.INSTANCE, true, qualifier,
                table1, new Column("c1", PrimitiveType.INT),
                table1, new Column("c1", PrimitiveType.INT));

        LogicalOlapScan logicalOlapScan1 = (LogicalOlapScan) new LogicalOlapScan(
                StatementScopeIdGenerator.newRelationId(), table1,
                Collections.emptyList()).withGroupExprLogicalPropChildren(Optional.empty(),
                Optional.of(new LogicalProperties(() -> ImmutableList.of(slot1), () -> DataTrait.EMPTY_TRAIT)), ImmutableList.of());

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
        SlotReference slot1 = new SlotReference(new ExprId(0), "c1", IntegerType.INSTANCE, true, qualifier,
                null, new Column("c1", PrimitiveType.INT),
                null, new Column("c1", PrimitiveType.INT));
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        Statistics childStats = new Statistics(20, slotColumnStatsMap);

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
        Assertions.assertEquals(1, slot1Stats.numNulls, 0.1);
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
        Statistics childStats = new Statistics(20, slotColumnStatsMap);

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
        Assertions.assertEquals(1, slot1Stats.numNulls, 0.1);
    }

    @Test
    public void testHashJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"6", "7", "8", "9", "10"});

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(5, icStatsOut.getHotValues().size());

        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(1, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testHashJoinPkFkSkew() {
        double leftRowCount = 100;
        double rightRowCount = 10;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                leftRowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rightRowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(leftRowCount, ImmutableMap.of(ia, iaStats));
        Statistics rightStats = new Statistics(rightRowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(1, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testLeftOuterJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"4", "5", "6", "7"});

        LogicalJoin join = new LogicalJoin(JoinType.LEFT_OUTER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(4, icStatsOut.getHotValues().size());

        // left outer join,
        // ia.hotValues:  "2", "3", "4" -> "2", "3", "4"
        // ib.hotValues: "4", "5" -> "4"
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(2, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(1, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testRightOuterJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"4", "5", "6", "7"});

        LogicalJoin join = new LogicalJoin(JoinType.RIGHT_OUTER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(4, icStatsOut.getHotValues().size());

        // right outer join,
        // ia.hotValues:  "1", "2" -> "2"
        // ib.hotValues: "2", "3", "4" -> "2", "3", "4"
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(3, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testLeftSemiJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"0", "1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"4", "5", "6", "7"});

        LogicalJoin join = new LogicalJoin(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(4, icStatsOut.getHotValues().size());

        // left semi join,
        // ia.hotValues:  "0", "1", "2" -> "0", "1", "2"
        // ib.hotValues: "2", "3", "4"
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(3, iaStatsOut.getHotValues().size());
    }

    @Test
    public void testAggSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair.second.get(0);
        LogicalAggregate agg = new LogicalAggregate(
                ImmutableList.of(ia),
                ImmutableList.of(ia),
                new DummyPlan()
        );

        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"0", "1", "2"});

        Statistics inputStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeAggregate(agg, inputStats);
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertNull(iaStatsOut.getHotValues());
    }

    @Test
    public void testExceptSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair1 = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair1.second.get(0);
        SlotReference ib = StatsTestUtil.instance.createExpr("ia").second.get(0);
        LogicalExcept exceptAll = new LogicalExcept(
                Qualifier.ALL,
                ImmutableList.of(ia),
                ImmutableList.of(
                        ImmutableList.of(ia),
                        ImmutableList.of(ib)
                ),
                ImmutableList.of(new DummyPlan(), new DummyPlan())
        );
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"0", "1", "2"});
        Statistics child0Stats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeExcept(exceptAll, child0Stats);
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(3, iaStatsOut.getHotValues().size());
    }

    @Test
    public void testUnionSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair1 = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair1.second.get(0);
        SlotReference ib = StatsTestUtil.instance.createExpr("ia").second.get(0);
        LogicalUnion unionAll = new LogicalUnion(
                Qualifier.ALL,
                ImmutableList.of(ia),
                ImmutableList.of(
                        ImmutableList.of(ia),
                        ImmutableList.of(ib)
                ),
                ImmutableList.of(),
                true,
                ImmutableList.of(new DummyPlan(), new DummyPlan())
        );
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, ImmutableMap.of("0", 40.0f, "1", 50.0f));
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, ImmutableMap.of("1", 40.0f, "2", 40.0f));
        Statistics child0Stats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        Statistics child1Stats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));

        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeUnion(unionAll, ImmutableList.of(child0Stats, child1Stats));
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());
        Assertions.assertTrue(containsHotValue(iaStatsOut, "1"));
    }

    private boolean containsHotValue(ColumnStatistic columnStatistic, String value) {
        if (columnStatistic.getHotValues() == null) {
            return false;
        }
        return columnStatistic.getHotValues().keySet().stream()
                .anyMatch(literal -> literal.getStringValue().equals(value));
    }

    @Test
    public void testLimitSkew() {
        LogicalLimit limit = new LogicalLimit(10, 10, LimitPhase.GLOBAL, new DummyPlan());
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair1 = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair1.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, ImmutableMap.of("0", 40.0f, "1", 50.0f));

        Statistics childStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeLimit(limit, childStats);
        Assertions.assertNull(outputStats.findColumnStatistics(ia).getHotValues());
    }

    @Test
    public void testDisableJoinReorderIfStatsInvalid() throws IOException {
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .build();

        // mock StatsCalculator to return -1 for getTableRowCount
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCountForIndex(long indexId, boolean strict) {
                return -1;
            }
        };
        new MockUp<TableStatsMeta>() {
            @Mock
            public long getRowCount(long indexId) {
                return -1;
            }
        };
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(join);
        cascadesContext.getConnectContext().getSessionVariable()
                .setVarOnce(SessionVariable.DISABLE_JOIN_REORDER, "false");
        StatsCalculator.disableJoinReorderIfStatsInvalid(ImmutableList.of(scan1, scan2, scan3), cascadesContext);
        // because table row count is -1, so disable join reorder
        Assertions.assertTrue(cascadesContext.getConnectContext().getSessionVariable().isDisableJoinReorder());
    }
}
