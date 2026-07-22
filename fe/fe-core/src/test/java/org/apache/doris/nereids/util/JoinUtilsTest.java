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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class JoinUtilsTest {

    @Test
    public void testVolatileEqualPredicateIsNotHashCondition() {
        SlotReference leftKey = new SlotReference(new ExprId(1), "c1",
                TinyIntType.INSTANCE, false, Lists.newArrayList());
        SlotReference rightKey = new SlotReference(new ExprId(2), "c2",
                TinyIntType.INSTANCE, false, Lists.newArrayList());
        EqualTo equalTo = new EqualTo(leftKey, new Add(rightKey, new Random()));

        JoinUtils.JoinSlotCoverageChecker checker = new JoinUtils.JoinSlotCoverageChecker(
                Lists.newArrayList(leftKey), Lists.newArrayList(rightKey));

        Assertions.assertTrue(equalTo.containsVolatileExpression());
        Assertions.assertFalse(checker.isHashJoinCondition(equalTo));
        Assertions.assertTrue(JoinUtils.extractExpressionForHashTable(
                Lists.newArrayList(leftKey), Lists.newArrayList(rightKey), Lists.newArrayList(equalTo)).first.isEmpty());
    }

    @Test
    public void testCouldColocateJoinForSameTable() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        DistributionSpecHash left = new DistributionSpecHash(Lists.newArrayList(new ExprId(1)), ShuffleType.NATURAL,
                1L, 1L, Collections.emptySet());
        DistributionSpecHash right = new DistributionSpecHash(Lists.newArrayList(new ExprId(2)), ShuffleType.NATURAL,
                1L, 1L, Collections.emptySet());

        Expression leftKey1 = new SlotReference(new ExprId(1), "c1",
                TinyIntType.INSTANCE, false, Lists.newArrayList());
        Expression rightKey1 = new SlotReference(new ExprId(2), "c1",
                TinyIntType.INSTANCE, false, Lists.newArrayList());
        Expression leftKey2 = new SlotReference(new ExprId(3), "c1",
                TinyIntType.INSTANCE, false, Lists.newArrayList());
        Expression rightKey2 = new SlotReference(new ExprId(4), "c1",
                TinyIntType.INSTANCE, false, Lists.newArrayList());

        List<Expression> conjuncts;

        // key same with distribute key
        conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1));
        Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, conjuncts));

        // key contains distribute key, and have distribute key = distribute key
        conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1), new EqualTo(leftKey2, rightKey2));
        Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, conjuncts));

        // key contains distribute key, and NOT have distribute key = distribute key
        conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey2), new EqualTo(leftKey2, rightKey1));
        Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

        // key not contains distribute key
        conjuncts = Lists.newArrayList(new EqualTo(leftKey2, rightKey2));
        Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));
    }

    @Test
    public void testCouldColocateJoinForDiffTableInSameGroupAndGroupIsStable() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        // same group and group is statble
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            GroupId groupId = new GroupId(1L, 1L);
            ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
            Mockito.when(colocateIndex.isSameGroup(1L, 2L)).thenReturn(true);
            Mockito.when(colocateIndex.getGroup(1L)).thenReturn(groupId);
            Mockito.when(colocateIndex.isGroupUnstable(groupId)).thenReturn(false);
            mockedEnv.when(() -> Env.getCurrentColocateIndex()).thenReturn(colocateIndex);

            DistributionSpecHash left = new DistributionSpecHash(Lists.newArrayList(new ExprId(1)),
                    ShuffleType.NATURAL, 1L, 1L, Collections.emptySet());
            DistributionSpecHash right = new DistributionSpecHash(Lists.newArrayList(new ExprId(2)),
                    ShuffleType.NATURAL, 2L, 2L, Collections.emptySet());

            Expression leftKey1 = new SlotReference(new ExprId(1), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey1 = new SlotReference(new ExprId(2), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression leftKey2 = new SlotReference(new ExprId(3), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey2 = new SlotReference(new ExprId(4), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());

            List<Expression> conjuncts;

            // key same with distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1));
            Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1), new EqualTo(leftKey2, rightKey2));
            Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and NOT have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey2), new EqualTo(leftKey2, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key not contains distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));
        }
    }

    @Test
    public void testCouldColocateJoinForNotNaturalHashDstribution() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        // same group and group is statble
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            GroupId groupId = new GroupId(1L, 1L);
            ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
            Mockito.when(colocateIndex.isSameGroup(1L, 2L)).thenReturn(true);
            Mockito.when(colocateIndex.getGroup(1L)).thenReturn(groupId);
            Mockito.when(colocateIndex.isGroupUnstable(groupId)).thenReturn(false);
            mockedEnv.when(() -> Env.getCurrentColocateIndex()).thenReturn(colocateIndex);

            DistributionSpecHash left = new DistributionSpecHash(Lists.newArrayList(new ExprId(1)),
                    ShuffleType.NATURAL, 1L, 1L, Collections.emptySet());
            DistributionSpecHash right = new DistributionSpecHash(Lists.newArrayList(new ExprId(2)),
                    ShuffleType.EXECUTION_BUCKETED, 2L, 2L, Collections.emptySet());

            Expression leftKey1 = new SlotReference(new ExprId(1), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey1 = new SlotReference(new ExprId(2), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression leftKey2 = new SlotReference(new ExprId(3), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey2 = new SlotReference(new ExprId(4), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());

            List<Expression> conjuncts;

            // key same with distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1), new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and NOT have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey2), new EqualTo(leftKey2, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key not contains distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));
        }
    }

    @Test
    public void testCouldColocateJoinForDiffTableInSameGroupAndGroupIsUnstable() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        // same group and group is statble
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            GroupId groupId = new GroupId(1L, 1L);
            ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
            Mockito.when(colocateIndex.isSameGroup(1L, 2L)).thenReturn(true);
            Mockito.when(colocateIndex.getGroup(1L)).thenReturn(groupId);
            Mockito.when(colocateIndex.isGroupUnstable(groupId)).thenReturn(true);
            mockedEnv.when(() -> Env.getCurrentColocateIndex()).thenReturn(colocateIndex);

            DistributionSpecHash left = new DistributionSpecHash(Lists.newArrayList(new ExprId(1)),
                    ShuffleType.NATURAL, 1L, 1L, Collections.emptySet());
            DistributionSpecHash right = new DistributionSpecHash(Lists.newArrayList(new ExprId(2)),
                    ShuffleType.NATURAL, 2L, 2L, Collections.emptySet());

            Expression leftKey1 = new SlotReference(new ExprId(1), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey1 = new SlotReference(new ExprId(2), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression leftKey2 = new SlotReference(new ExprId(3), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey2 = new SlotReference(new ExprId(4), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());

            List<Expression> conjuncts;

            // key same with distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1), new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and NOT have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey2), new EqualTo(leftKey2, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key not contains distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));
        }
    }

    @Test
    public void testCouldColocateJoinForDiffTableNotInSameGroup() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        // same group and group is statble
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            GroupId groupId = new GroupId(1L, 1L);
            ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
            Mockito.when(colocateIndex.isSameGroup(1L, 2L)).thenReturn(true);
            Mockito.when(colocateIndex.getGroup(1L)).thenReturn(groupId);
            Mockito.when(colocateIndex.isGroupUnstable(groupId)).thenReturn(true);
            mockedEnv.when(() -> Env.getCurrentColocateIndex()).thenReturn(colocateIndex);

            DistributionSpecHash left = new DistributionSpecHash(Lists.newArrayList(new ExprId(1)), ShuffleType.NATURAL,
                    1L, 1L, Collections.emptySet());
            DistributionSpecHash right = new DistributionSpecHash(Lists.newArrayList(new ExprId(2)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet());

            Expression leftKey1 = new SlotReference(new ExprId(1), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey1 = new SlotReference(new ExprId(2), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression leftKey2 = new SlotReference(new ExprId(3), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());
            Expression rightKey2 = new SlotReference(new ExprId(4), "c1",
                    TinyIntType.INSTANCE, false, Lists.newArrayList());

            List<Expression> conjuncts;

            // key same with distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey1), new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key contains distribute key, and NOT have distribute key = distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey1, rightKey2), new EqualTo(leftKey2, rightKey1));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));

            // key not contains distribute key
            conjuncts = Lists.newArrayList(new EqualTo(leftKey2, rightKey2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, conjuncts));
        }
    }

    @Test
    public void testCheckBroadcastJoinStatsRowCountBoundary() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        double broadcastRowLimit = ctx.getSessionVariable().getBroadcastRowCountLimit();

        // rowCount strictly above broadcast_row_count_limit: reject.
        PhysicalHashJoin<?, ?> joinTooMany = Mockito.mock(PhysicalHashJoin.class);
        GroupExpression geTooMany = Mockito.mock(GroupExpression.class);
        Statistics statsTooMany = new StatisticsBuilder().setRowCount(broadcastRowLimit + 1).build();
        Mockito.when(joinTooMany.getGroupExpression()).thenReturn(Optional.of(geTooMany));
        Mockito.when(geTooMany.child(1)).thenReturn(Mockito.mock(Group.class));
        Mockito.when(geTooMany.child(1).getStatistics()).thenReturn(statsTooMany);
        Mockito.when(joinTooMany.right()).thenReturn(Mockito.mock(Plan.class));
        Mockito.when(joinTooMany.right().getOutput()).thenReturn(Lists.newArrayList());

        Assertions.assertFalse(JoinUtils.checkBroadcastJoinStats(joinTooMany),
                "Should reject broadcast when rowCount exceeds broadcast_row_count_limit");

        // rowCount exactly at limit: accept (the check is rowCount <= limit).
        PhysicalHashJoin<?, ?> joinAtLimit = Mockito.mock(PhysicalHashJoin.class);
        GroupExpression geAtLimit = Mockito.mock(GroupExpression.class);
        Statistics statsAtLimit = new StatisticsBuilder().setRowCount(broadcastRowLimit).build();
        Mockito.when(joinAtLimit.getGroupExpression()).thenReturn(Optional.of(geAtLimit));
        Mockito.when(geAtLimit.child(1)).thenReturn(Mockito.mock(Group.class));
        Mockito.when(geAtLimit.child(1).getStatistics()).thenReturn(statsAtLimit);
        Mockito.when(joinAtLimit.right()).thenReturn(Mockito.mock(Plan.class));
        Mockito.when(joinAtLimit.right().getOutput()).thenReturn(Lists.newArrayList());

        Assertions.assertTrue(JoinUtils.checkBroadcastJoinStats(joinAtLimit),
                "Should accept broadcast when rowCount equals broadcast_row_count_limit "
                        + "(check is inclusive, datasize is 0 with no output cols)");
    }

    @Test
    public void testCheckBroadcastJoinStatsHappyPath() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        GroupExpression ge = Mockito.mock(GroupExpression.class);
        Statistics stats = new StatisticsBuilder().setRowCount(100).build();
        Mockito.when(join.getGroupExpression()).thenReturn(Optional.of(ge));
        Mockito.when(ge.child(1)).thenReturn(Mockito.mock(Group.class));
        Mockito.when(ge.child(1).getStatistics()).thenReturn(stats);
        Mockito.when(join.right()).thenReturn(Mockito.mock(Plan.class));
        Mockito.when(join.right().getOutput()).thenReturn(Lists.newArrayList());

        Assertions.assertTrue(JoinUtils.checkBroadcastJoinStats(join),
                "Small build side under both gates should be accepted as broadcast candidate");
    }

    @Test
    public void testCheckBroadcastJoinStatsRejectsClampedRowCount() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        GroupExpression ge = Mockito.mock(GroupExpression.class);
        // Mirror what StatsCalculator.computeOlapScan does for an un-ANALYZE'd
        // Olap table: setRowCountWasUnknown(true) + setRowCount(1).
        Statistics stats = new StatisticsBuilder()
                .setRowCount(1)
                .setRowCountWasUnknown(true)
                .build();
        Mockito.when(join.getGroupExpression()).thenReturn(Optional.of(ge));
        Mockito.when(ge.child(1)).thenReturn(Mockito.mock(Group.class));
        Mockito.when(ge.child(1).getStatistics()).thenReturn(stats);
        Mockito.when(join.right()).thenReturn(Mockito.mock(Plan.class));
        Mockito.when(join.right().getOutput()).thenReturn(Lists.newArrayList());

        Assertions.assertFalse(JoinUtils.checkBroadcastJoinStats(join),
                "Clamped rowCount = 1 (rowCountWasUnknown=true) must NOT be accepted "
                        + "as a broadcast candidate; otherwise un-ANALYZE'd large tables "
                        + "would be wrongly broadcast.");
    }
}
