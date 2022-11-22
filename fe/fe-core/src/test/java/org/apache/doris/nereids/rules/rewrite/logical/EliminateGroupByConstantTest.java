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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EliminateGroupByConstantTest {

    @Test
    public void testIntegerLiteral() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new IntegerLiteral(1),
                        new SlotReference("k2", IntegerType.INSTANCE)),
                ImmutableList.of(
                        new SlotReference("k1", IntegerType.INSTANCE),
                        new SlotReference("k2", IntegerType.INSTANCE)),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(),
                        new OlapTable())
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(ImmutableList.of(new EliminateGroupByConstant().build()));

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 2);
        Assertions.assertTrue(aggregate1.getGroupByExpressions().get(0) instanceof Slot
                && aggregate1.getGroupByExpressions().get(1) instanceof Slot);
    }

    @Test
    public void testOtherLiteral() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new StringLiteral("str"),
                        new SlotReference("k2", IntegerType.INSTANCE)),
                ImmutableList.of(
                        new Alias(new StringLiteral("str"), "str"),
                        new SlotReference("k1", IntegerType.INSTANCE),
                        new SlotReference("k2", IntegerType.INSTANCE)),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(),
                        new OlapTable())
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(ImmutableList.of(new EliminateGroupByConstant().build()));

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 1);
        Assertions.assertTrue(aggregate1.getGroupByExpressions().get(0) instanceof Slot);
    }

    @Test
    public void testMixedLiteral() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new StringLiteral("str"),
                        new SlotReference("k2", IntegerType.INSTANCE),
                        new IntegerLiteral(1),
                        new IntegerLiteral(2),
                        new IntegerLiteral(3),
                        new IntegerLiteral(5)),
                ImmutableList.of(
                        new Alias(new StringLiteral("str"), "str"),
                        new SlotReference("k1", IntegerType.INSTANCE),
                        new SlotReference("k2", IntegerType.INSTANCE),
                        new Alias(new IntegerLiteral(1), "integer")),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(),
                        new OlapTable())
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(ImmutableList.of(new EliminateGroupByConstant().build()));

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 2);
        Assertions.assertTrue(aggregate1.getGroupByExpressions().get(0) instanceof Slot
                && aggregate1.getGroupByExpressions().get(1) instanceof Slot);
    }

    @Test
    public void testEliminateUnusedGroupBy() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new IntegerLiteral(1),
                        new IntegerLiteral(2)),
                ImmutableList.of(
                        new Alias(new Max(new SlotReference("k1", IntegerType.INSTANCE)), "max"),
                        new Alias(new Min(new SlotReference("k2", IntegerType.INSTANCE)), "min")),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(),
                        new OlapTable())
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(ImmutableList.of(new EliminateGroupByConstant().build()));

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 0);
    }
}
