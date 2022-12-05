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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Id;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStat;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.TableStats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StatsCalculatorTest {

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
    //     StatsDeriveResult childStats = new StatsDeriveResult(20, slotColumnStatsMap);
    //     Alias alias = new Alias(sum, "a");
    //     Group childGroup = new Group();
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
    //     Group ownerGroup = new Group();
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

        Map<Id, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1.getExprId(), columnStat1.build());
        slotColumnStatsMap.put(slot2.getExprId(), columnStat2.build());
        StatsDeriveResult childStats = new StatsDeriveResult(10000, slotColumnStatsMap);

        EqualTo eq1 = new EqualTo(slot1, new IntegerLiteral(1));
        EqualTo eq2 = new EqualTo(slot2, new IntegerLiteral(2));

        And and = new And(eq1, eq2);
        Or or = new Or(eq1, eq2);

        Group childGroup = new Group();
        childGroup.setLogicalProperties(new LogicalProperties(Collections::emptyList));
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalFilter<GroupPlan> logicalFilter = new LogicalFilter<>(and, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalFilter, ImmutableList.of(childGroup));
        Group ownerGroup = new Group();
        groupExpression.setOwnerGroup(ownerGroup);
        StatsCalculator.estimate(groupExpression);
        Assertions.assertEquals((long) (10000 * 0.1 * 0.05), ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group();
        groupExpressionOr.setOwnerGroup(ownerGroupOr);
        StatsCalculator.estimate(groupExpressionOr);
        Assertions.assertEquals((long) (10000 * (0.1 + 0.05 - 0.1 * 0.05)),
                ownerGroupOr.getStatistics().getRowCount(), 0.001);
    }

    // a, b are in (0,100)
    // a=200 and b=300 => output: 0 rows
    @org.junit.Test
    public void testFilterOutofRange() {
        List<String> qualifier = Lists.newArrayList();
        qualifier.add("test");
        qualifier.add("t");
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

        Map<Id, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1.getExprId(), columnStat1.build());
        slotColumnStatsMap.put(slot2.getExprId(), columnStat2.build());
        StatsDeriveResult childStats = new StatsDeriveResult(10000, slotColumnStatsMap);

        EqualTo eq1 = new EqualTo(slot1, new IntegerLiteral(200));
        EqualTo eq2 = new EqualTo(slot2, new IntegerLiteral(300));

        And and = new And(eq1, eq2);
        Or or = new Or(eq1, eq2);

        Group childGroup = new Group();
        childGroup.setLogicalProperties(new LogicalProperties(Collections::emptyList));
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalFilter<GroupPlan> logicalFilter = new LogicalFilter<>(and, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalFilter, ImmutableList.of(childGroup));
        Group ownerGroup = new Group();
        groupExpression.setOwnerGroup(ownerGroup);
        StatsCalculator.estimate(groupExpression);
        Assertions.assertEquals(0, ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group();
        groupExpressionOr.setOwnerGroup(ownerGroupOr);
        StatsCalculator.estimate(groupExpressionOr);
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
    //     StatsDeriveResult leftStats = new StatsDeriveResult(leftRowCount, slotColumnStatsMap1);
    //
    //     final long rightRowCount = 10000;
    //     StatsDeriveResult rightStats = new StatsDeriveResult(rightRowCount, slotColumnStatsMap2);
    //
    //     EqualTo equalTo = new EqualTo(slot1, slot2);
    //
    //     LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t", 0);
    //     LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(0, "t", 0);
    //     LogicalJoin<LogicalOlapScan, LogicalOlapScan> fakeSemiJoin = new LogicalJoin<>(
    //             JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(equalTo), Optional.empty(), scan1, scan2);
    //     LogicalJoin<LogicalOlapScan, LogicalOlapScan> fakeInnerJoin = new LogicalJoin<>(
    //             JoinType.INNER_JOIN, Lists.newArrayList(equalTo), Optional.empty(), scan1, scan2);
    //     StatsDeriveResult semiJoinStats = JoinEstimation.estimate(leftStats, rightStats, fakeSemiJoin);
    //     Assertions.assertEquals(leftRowCount, semiJoinStats.getRowCount());
    //     StatsDeriveResult innerJoinStats = JoinEstimation.estimate(leftStats, rightStats, fakeInnerJoin);
    //     Assertions.assertEquals(2500000, innerJoinStats.getRowCount());
    // }

    @Test
    public void testOlapScan(@Mocked ConnectContext context) {
        ColumnStat columnStat1 = new ColumnStat();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        long tableId1 = 0;
        TableStats tableStats1 = new TableStats();
        tableStats1.putColumnStats("c1", columnStat1);

        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);

        OlapTable table1 = PlanConstructor.newOlapTable(tableId1, "t1", 0);
        LogicalOlapScan logicalOlapScan1 = new LogicalOlapScan(RelationId.createGenerator().getNextId(), table1, Collections.emptyList())
                .withLogicalProperties(Optional.of(new LogicalProperties(() -> ImmutableList.of(slot1))));
        Group childGroup = new Group();
        GroupExpression groupExpression = new GroupExpression(logicalOlapScan1, ImmutableList.of(childGroup));
        Group ownerGroup = new Group();
        groupExpression.setOwnerGroup(ownerGroup);
        StatsCalculator.estimate(groupExpression);
        StatsDeriveResult stats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, stats.getSlotIdToColumnStats().size());
        Assertions.assertNotNull(stats.getSlotIdToColumnStats().get(slot1.getExprId()));
    }

    @Test
    public void testLimit() {
        List<String> qualifier = new ArrayList<>();
        qualifier.add("test");
        qualifier.add("t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Id, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1.getExprId(), columnStat1.build());
        StatsDeriveResult childStats = new StatsDeriveResult(10, slotColumnStatsMap);

        Group childGroup = new Group();
        childGroup.setLogicalProperties(new LogicalProperties(Collections::emptyList));
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalLimit<GroupPlan> logicalLimit = new LogicalLimit<>(1, 2, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalLimit, ImmutableList.of(childGroup));
        Group ownerGroup = new Group();
        ownerGroup.addGroupExpression(groupExpression);
        StatsCalculator.estimate(groupExpression);
        StatsDeriveResult limitStats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, limitStats.getRowCount());
        ColumnStatistic slot1Stats = limitStats.getSlotIdToColumnStats().get(slot1.getExprId());
        Assertions.assertEquals(1, slot1Stats.ndv);
        Assertions.assertEquals(1, slot1Stats.numNulls);
    }

    @Test
    public void testTopN() {
        List<String> qualifier = new ArrayList<>();
        qualifier.add("test");
        qualifier.add("t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Id, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1.getExprId(), columnStat1.build());
        StatsDeriveResult childStats = new StatsDeriveResult(10, slotColumnStatsMap);

        Group childGroup = new Group();
        childGroup.setLogicalProperties(new LogicalProperties(Collections::emptyList));
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalTopN<GroupPlan> logicalTopN = new LogicalTopN<>(Collections.emptyList(), 1, 2, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalTopN, ImmutableList.of(childGroup));
        Group ownerGroup = new Group();
        ownerGroup.addGroupExpression(groupExpression);
        StatsCalculator.estimate(groupExpression);
        StatsDeriveResult topNStats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, topNStats.getRowCount());
        ColumnStatistic slot1Stats = topNStats.getSlotIdToColumnStats().get(slot1.getExprId());
        Assertions.assertEquals(1, slot1Stats.ndv);
        Assertions.assertEquals(1, slot1Stats.numNulls);
    }
}
