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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.model.StatisticsBuilder;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JoinEstimateTest {

    /*
    L join R on a=b
    after join
    a.ndv = b.ndv
     */
    @Test
    public void testInnerJoinStats() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        EqualTo eq = new EqualTo(a, b);
        Statistics leftStats = new StatisticsBuilder().setRowCount(100).build();
        leftStats.addColumnStats(a,
                new ColumnStatisticBuilder(100)
                        .setNdv(10)
                        .build()
        );
        Statistics rightStats = new StatisticsBuilder().setRowCount(80).build();
        rightStats.addColumnStats(b,
                new ColumnStatisticBuilder(80)
                        .setNdv(5)
                        .build()
        );
        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan left = new GroupPlan(new Group(idGenerator.getNextId(), new LogicalProperties(
                new Supplier<List<Slot>>() {
                    @Override
                    public List<Slot> get() {
                        return Lists.newArrayList(a);
                    }
                }, () -> DataTrait.EMPTY_TRAIT)));
        GroupPlan right = new GroupPlan(new Group(idGenerator.getNextId(), new LogicalProperties(
                new Supplier<List<Slot>>() {
                    @Override
                    public List<Slot> get() {
                        return Lists.newArrayList(b);
                    }
                }, () -> DataTrait.EMPTY_TRAIT)));
        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN, Lists.newArrayList(eq),
                left, right, null);
        Statistics outputStats = JoinEstimation.estimate(leftStats, rightStats, join);
        ColumnStatistic outAStats = outputStats.findColumnStatistics(a);
        Assertions.assertNotNull(outAStats);
        Assertions.assertEquals(5, outAStats.ndv);
        ColumnStatistic outBStats = outputStats.findColumnStatistics(b);
        Assertions.assertNotNull(outAStats);
        Assertions.assertEquals(5, outBStats.ndv);
    }

    @Test
    public void testMixedJoinKeyStatistics() {
        SlotReference leftKey = new SlotReference("leftKey", IntegerType.INSTANCE);
        SlotReference rightKey = new SlotReference("rightKey", IntegerType.INSTANCE);
        SlotReference leftApp = new SlotReference("leftApp", IntegerType.INSTANCE);
        SlotReference rightApp = new SlotReference("rightApp", IntegerType.INSTANCE);
        SlotReference flag = new SlotReference("flag", IntegerType.INSTANCE);
        ColumnStatistic key = new ColumnStatisticBuilder(1_000)
                .setNdv(10).setMinValue(0).setMaxValue(9).build();
        Statistics left = new StatisticsBuilder().setRowCount(1_000)
                .putColumnStatistics(leftKey, key)
                .putColumnStatistics(leftApp, ColumnStatistic.UNKNOWN)
                .putColumnStatistics(flag, new ColumnStatisticBuilder(1_000)
                        .setNdv(2).setMinValue(0).setMaxValue(1).build())
                .build();
        Statistics right = new StatisticsBuilder().setRowCount(1_000)
                .putColumnStatistics(rightKey, key).build();

        EqualTo known = new EqualTo(rightKey, leftKey);
        EqualTo unknown = new EqualTo(leftApp, rightApp);
        LogicalJoin<Plan, Plan> mixed = new LogicalJoin<>(JoinType.INNER_JOIN, List.of(unknown, known),
                new DummyPlan(), new DummyPlan(), JoinReorderContext.EMPTY);
        LogicalJoin<Plan, Plan> allUnknown = new LogicalJoin<>(JoinType.INNER_JOIN, List.of(unknown),
                new DummyPlan(), new DummyPlan(), JoinReorderContext.EMPTY);
        LogicalJoin<Plan, Plan> residual = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN, List.of(known, unknown),
                List.of(new EqualTo(flag, new IntegerLiteral(0))),
                new DummyPlan(), new DummyPlan(), JoinReorderContext.EMPTY);

        Assertions.assertEquals(100_000, JoinEstimation.estimate(left, right, mixed).getRowCount());
        Assertions.assertEquals(1_000, JoinEstimation.estimate(left, right, allUnknown).getRowCount());
        Assertions.assertEquals(50_000, JoinEstimation.estimate(left, right, residual).getRowCount());
        Assertions.assertEquals(200_000, JoinEstimation.estimate(left, right, mixed, 2).getRowCount());
    }

    @Test
    public void testOuterJoinStats() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        EqualTo eq = new EqualTo(a, b);
        Statistics leftStats = new StatisticsBuilder().setRowCount(100).build();
        leftStats.addColumnStats(a,
                new ColumnStatisticBuilder(100)
                        .setNdv(10)
                        .build()
        );
        Statistics rightStats = new StatisticsBuilder().setRowCount(80).build();
        rightStats.addColumnStats(b,
                new ColumnStatisticBuilder(80)
                        .setNdv(0)
                        .build()
        ).addColumnStats(c,
                new ColumnStatisticBuilder(80)
                        .setNdv(20)
                        .build()
        );
        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan left = new GroupPlan(new Group(idGenerator.getNextId(), new LogicalProperties(
                new Supplier<List<Slot>>() {
                    @Override
                    public List<Slot> get() {
                        return Lists.newArrayList(a);
                    }
                }, () -> DataTrait.EMPTY_TRAIT)));
        GroupPlan right = new GroupPlan(new Group(idGenerator.getNextId(), new LogicalProperties(
                new Supplier<List<Slot>>() {
                    @Override
                    public List<Slot> get() {
                        return Lists.newArrayList(b, c);
                    }
                }, () -> DataTrait.EMPTY_TRAIT)));
        LogicalJoin join = new LogicalJoin(JoinType.LEFT_OUTER_JOIN, Lists.newArrayList(eq),
                left, right, null);
        Statistics outputStats = JoinEstimation.estimate(leftStats, rightStats, join);
        ColumnStatistic outAStats = outputStats.findColumnStatistics(a);
        Assertions.assertNotNull(outAStats);
        Assertions.assertEquals(10, outAStats.ndv);
        ColumnStatistic outBStats = outputStats.findColumnStatistics(b);
        Assertions.assertNotNull(outAStats);
        Assertions.assertEquals(0, outBStats.ndv);
        ColumnStatistic outCStats = outputStats.findColumnStatistics(c);
        Assertions.assertNotNull(outAStats);
        Assertions.assertEquals(20.0, outCStats.ndv);
    }

    @Test
    public void testAsofInnerJoinStats() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        EqualTo eq = new EqualTo(a, b);

        // left probe
        Statistics leftStats = new StatisticsBuilder().setRowCount(100).build();
        leftStats.addColumnStats(a,
                new ColumnStatisticBuilder(100)
                        .setNdv(50)
                        .build()
        );

        // right build with original ndv larger than current ndv
        Statistics rightStats = new StatisticsBuilder().setRowCount(80).build();
        ColumnStatistic original = new ColumnStatisticBuilder(80)
                .setNdv(20)
                .build();
        rightStats.addColumnStats(b,
                new ColumnStatisticBuilder(80)
                        .setNdv(10)
                        .setOriginal(original)
                        .build()
        );

        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan left = new GroupPlan(new Group(idGenerator.getNextId(), new LogicalProperties(
                (Supplier<List<Slot>>) () -> Lists.newArrayList(a), () -> DataTrait.EMPTY_TRAIT)));
        GroupPlan right = new GroupPlan(new Group(idGenerator.getNextId(), new LogicalProperties(
                (Supplier<List<Slot>>) () -> Lists.newArrayList(b), () -> DataTrait.EMPTY_TRAIT)));

        LogicalJoin join = new LogicalJoin(JoinType.ASOF_LEFT_INNER_JOIN, Lists.newArrayList(eq),
                left, right, null);
        Statistics outputStats = JoinEstimation.estimate(leftStats, rightStats, join);

        // expected rowCount = left.rowCount * build.ndv / build.originalNdv = 100 * 10 / 20 = 50
        Assertions.assertEquals(50.0, outputStats.getRowCount());
        ColumnStatistic outAStats = outputStats.findColumnStatistics(a);
        Assertions.assertNotNull(outAStats);
    }
}
