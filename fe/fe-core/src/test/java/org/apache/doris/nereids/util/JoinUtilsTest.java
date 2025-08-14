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
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class JoinUtilsTest {

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
}
