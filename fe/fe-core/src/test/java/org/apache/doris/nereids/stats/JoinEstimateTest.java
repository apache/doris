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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

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
                new ColumnStatisticBuilder()
                        .setCount(100)
                        .setNdv(10)
                        .build()
        );
        Statistics rightStats = new StatisticsBuilder().setRowCount(80).build();
        rightStats.addColumnStats(b,
                new ColumnStatisticBuilder()
                        .setCount(80)
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
    public void testOuterJoinStats() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        EqualTo eq = new EqualTo(a, b);
        Statistics leftStats = new StatisticsBuilder().setRowCount(100).build();
        leftStats.addColumnStats(a,
                new ColumnStatisticBuilder()
                        .setCount(100)
                        .setNdv(10)
                        .build()
        );
        Statistics rightStats = new StatisticsBuilder().setRowCount(80).build();
        rightStats.addColumnStats(b,
                new ColumnStatisticBuilder()
                        .setCount(80)
                        .setNdv(0)
                        .build()
        ).addColumnStats(c,
                new ColumnStatisticBuilder()
                        .setCount(80)
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
}
