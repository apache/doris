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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.trees.expressions.Add;
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
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class EliminateGroupByConstantTest {
    private static final OlapTable table = new OlapTable(0L, "student",
            ImmutableList.of(new Column("k1", Type.INT, true, AggregateType.NONE, "0", ""),
                    new Column("k2", Type.INT, false, AggregateType.NONE, "0", ""),
                    new Column("k3", Type.INT, true, AggregateType.NONE, "", "")),
            KeysType.PRIMARY_KEYS, new PartitionInfo(), null);
    private static final SlotReference k1 = new SlotReference("k1", IntegerType.INSTANCE);
    private static final SlotReference k2 = new SlotReference("k2", IntegerType.INSTANCE);

    static {
        table.setIndexMeta(-1,
                "t1",
                table.getFullSchema(),
                0, 0, (short) 0,
                TStorageType.COLUMN,
                KeysType.PRIMARY_KEYS);
    }

    @Test
    public void testIntegerLiteral() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(new IntegerLiteral(1), k2),
                ImmutableList.of(k1, k2),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(), table)
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(new EliminateGroupByConstant().build());
        context.bottomUpRewrite(new CheckAfterRewrite().build());

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 1);
        Assertions.assertTrue(aggregate1.getGroupByExpressions().get(0) instanceof Slot);
    }

    @Test
    public void testOtherLiteral() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new StringLiteral("str"), k2),
                ImmutableList.of(
                        new Alias(new StringLiteral("str"), "str"), k1, k2),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(), table)
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(new EliminateGroupByConstant().build());
        context.bottomUpRewrite(new CheckAfterRewrite().build());

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 1);
        Assertions.assertTrue(aggregate1.getGroupByExpressions().get(0) instanceof Slot);
    }

    @Test
    public void testMixedLiteral() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new StringLiteral("str"), k2,
                        new IntegerLiteral(1),
                        new IntegerLiteral(2),
                        new IntegerLiteral(3),
                        new Add(k1, k2)),
                ImmutableList.of(
                        new Alias(new StringLiteral("str"), "str"),
                        k2, k1, new Alias(new IntegerLiteral(1), "integer")),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(), table)
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(new EliminateGroupByConstant().build());
        context.bottomUpRewrite(new CheckAfterRewrite().build());

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 2);
        List groupByExprs = aggregate1.getGroupByExpressions();
        Assertions.assertTrue(groupByExprs.get(0) instanceof Slot
                && groupByExprs.get(1) instanceof Add);
    }

    @Test
    public void testComplexGroupBy() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new IntegerLiteral(1),
                        new IntegerLiteral(2),
                        new Add(k1, k2)),
                ImmutableList.of(
                        new Alias(new Max(k1), "max"),
                        new Alias(new Min(k2), "min"),
                        new Alias(new Add(k1, k2), "add")),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(), table)
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(new EliminateGroupByConstant().build());
        context.bottomUpRewrite(new CheckAfterRewrite().build());

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 1);
    }

    @Test
    public void testOutOfRange() {
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(
                        new StringLiteral("str"), k2,
                        new IntegerLiteral(1),
                        new IntegerLiteral(2),
                        new IntegerLiteral(3),
                        new IntegerLiteral(5),
                        new Add(k1, k2)),
                ImmutableList.of(
                        new Alias(new StringLiteral("str"), "str"),
                        k2, k1, new Alias(new IntegerLiteral(1), "integer")),
                new LogicalOlapScan(RelationId.createGenerator().getNextId(), table)
        );

        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.topDownRewrite(new EliminateGroupByConstant().build());
        context.bottomUpRewrite(new CheckAfterRewrite().build());

        LogicalAggregate aggregate1 = ((LogicalAggregate) context.getMemo().copyOut());
        Assertions.assertEquals(aggregate1.getGroupByExpressions().size(), 2);
    }
}
