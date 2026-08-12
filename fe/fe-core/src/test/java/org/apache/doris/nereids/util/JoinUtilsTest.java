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
import org.apache.doris.nereids.properties.DistributionMapping;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.NaturalDistributionMappingSpec;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

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
    public void testCouldColocateJoinByDistributionMappings() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            GroupId groupId = new GroupId(1L, 1L);
            ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
            Mockito.when(colocateIndex.isSameGroup(1L, 2L)).thenReturn(true);
            Mockito.when(colocateIndex.getGroup(1L)).thenReturn(groupId);
            Mockito.when(colocateIndex.isGroupUnstable(groupId)).thenReturn(false);
            mockedEnv.when(Env::getCurrentColocateIndex).thenReturn(colocateIndex);

            DistributionMapping leftMapping1 = new DistributionMapping(
                    "mapping_1", ImmutableList.of(new ExprId(5)), ImmutableList.of(0));
            DistributionMapping rightMapping1 = new DistributionMapping(
                    "mapping_1", ImmutableList.of(new ExprId(6)), ImmutableList.of(0));
            DistributionMapping leftMapping2 = new DistributionMapping(
                    "mapping_2", ImmutableList.of(new ExprId(9)), ImmutableList.of(1));
            DistributionMapping rightMapping2 = new DistributionMapping(
                    "mapping_2", ImmutableList.of(new ExprId(10)), ImmutableList.of(1));
            DistributionSpecHash left = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(1), new ExprId(2)), ShuffleType.NATURAL,
                    1L, 1L, Collections.emptySet(), ImmutableList.of(leftMapping1, leftMapping2));
            DistributionSpecHash right = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(3), new ExprId(4)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet(), ImmutableList.of(rightMapping1, rightMapping2));

            SlotReference leftK2 = slot(2);
            SlotReference rightK2 = slot(4);
            SlotReference leftD1 = slot(5);
            SlotReference rightD1 = slot(6);
            SlotReference leftExtra = slot(7);
            SlotReference rightExtra = slot(8);
            SlotReference leftD2 = slot(9);
            SlotReference rightD2 = slot(10);

            List<Expression> directAndMapping = ImmutableList.of(
                    new EqualTo(leftD1, rightD1), new EqualTo(leftK2, rightK2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(left, right, directAndMapping));

            ctx.getSessionVariable().enableColocateMappingConstraint = true;
            Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, directAndMapping));
            Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, ImmutableList.of(
                    new EqualTo(leftD1, rightD1), new EqualTo(leftK2, rightK2),
                    new EqualTo(leftExtra, rightExtra))));
            DistributionMapping rightMappingWithDifferentId = new DistributionMapping(
                    "different_mapping", ImmutableList.of(new ExprId(6)), ImmutableList.of(0));
            DistributionSpecHash rightWithDifferentMappingId = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(3), new ExprId(4)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet(),
                    ImmutableList.of(rightMappingWithDifferentId, rightMapping2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(
                    left, rightWithDifferentMappingId, directAndMapping));
            DistributionMapping rightMappingWithWrongDeterminant = new DistributionMapping(
                    "mapping_1", ImmutableList.of(new ExprId(16)), ImmutableList.of(0));
            DistributionMapping rightMappingWithWrongTarget = new DistributionMapping(
                    "mapping_1", ImmutableList.of(new ExprId(6)), ImmutableList.of(1));
            DistributionSpecHash rightWithCompatibleCandidates = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(3), new ExprId(4)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet(),
                    ImmutableList.of(
                            rightMappingWithWrongDeterminant,
                            rightMappingWithWrongTarget,
                            rightMapping1,
                            rightMapping2));
            Assertions.assertTrue(JoinUtils.couldColocateJoin(
                    left, rightWithCompatibleCandidates, directAndMapping));
            DistributionSpecHash rightWithoutMatchingCandidate = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(3), new ExprId(4)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet(),
                    ImmutableList.of(
                            rightMappingWithWrongDeterminant,
                            rightMappingWithWrongTarget,
                            rightMapping2));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(
                    left, rightWithoutMatchingCandidate, directAndMapping));

            List<DistributionMapping> mostlyUnrelatedMappings = Lists.newArrayList();
            for (int i = 0; i < 128; i++) {
                mostlyUnrelatedMappings.add(new DistributionMapping(
                        "unrelated_" + i,
                        ImmutableList.of(new ExprId(1000 + i)),
                        ImmutableList.of(i % 2)));
            }
            mostlyUnrelatedMappings.add(rightMapping1);
            mostlyUnrelatedMappings.add(rightMapping2);
            DistributionSpecHash rightWithMostlyUnrelatedMappings = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(3), new ExprId(4)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet(), mostlyUnrelatedMappings);
            Assertions.assertTrue(JoinUtils.couldColocateJoin(
                    left, rightWithMostlyUnrelatedMappings, directAndMapping));

            DistributionMapping leftOrderedMapping = new DistributionMapping(
                    "ordered_mapping",
                    ImmutableList.of(new ExprId(5), new ExprId(9)),
                    ImmutableList.of(0, 1));
            DistributionMapping rightReorderedDeterminants = new DistributionMapping(
                    "ordered_mapping",
                    ImmutableList.of(new ExprId(10), new ExprId(6)),
                    ImmutableList.of(0, 1));
            DistributionSpecHash leftWithOrderedMapping = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(1), new ExprId(2)), ShuffleType.NATURAL,
                    1L, 1L, Collections.emptySet(), ImmutableList.of(leftOrderedMapping));
            DistributionSpecHash rightWithReorderedDeterminants = new DistributionSpecHash(
                    ImmutableList.of(new ExprId(3), new ExprId(4)), ShuffleType.NATURAL,
                    2L, 2L, Collections.emptySet(),
                    ImmutableList.of(rightReorderedDeterminants));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(
                    leftWithOrderedMapping, rightWithReorderedDeterminants,
                    ImmutableList.of(
                            new EqualTo(leftD1, rightD1),
                            new EqualTo(leftD2, rightD2))));
            Assertions.assertFalse(JoinUtils.couldColocateJoin(
                    left, right, ImmutableList.of(new EqualTo(leftD1, rightD1))));
            Assertions.assertTrue(JoinUtils.couldColocateJoin(left, right, ImmutableList.of(
                    new EqualTo(leftD1, rightD1), new EqualTo(leftD2, rightD2))));

            NaturalDistributionMappingSpec leftWithHiddenK1 =
                    NaturalDistributionMappingSpec.fromHashSpec(left).get().project(ImmutableMap.of(
                            new ExprId(2), new ExprId(2),
                            new ExprId(5), new ExprId(5))).get();
            NaturalDistributionMappingSpec rightWithHiddenK1 =
                    NaturalDistributionMappingSpec.fromHashSpec(right).get().project(ImmutableMap.of(
                            new ExprId(4), new ExprId(4),
                            new ExprId(6), new ExprId(6))).get();
            Assertions.assertTrue(JoinUtils.couldColocateJoinByMapping(
                    leftWithHiddenK1, rightWithHiddenK1, directAndMapping));
            Assertions.assertFalse(JoinUtils.couldColocateJoinByMapping(
                    leftWithHiddenK1, rightWithHiddenK1,
                    ImmutableList.of(new EqualTo(leftD1, rightD1))));
        }
    }

    private SlotReference slot(int exprId) {
        return new SlotReference(new ExprId(exprId), "c" + exprId,
                TinyIntType.INSTANCE, false, Lists.newArrayList());
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
}
