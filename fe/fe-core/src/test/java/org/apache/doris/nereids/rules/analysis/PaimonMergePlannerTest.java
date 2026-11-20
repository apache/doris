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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.info.PaimonRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class PaimonMergePlannerTest {
    @Test
    void matchedMergeAcceptsNullPrimaryKeyForUpdateAndDelete() {
        for (boolean delete : new boolean[] {false, true}) {
            LogicalPlan child = targetRow(true);
            MergeMatchedClause clause = new MergeMatchedClause(Optional.empty(), delete ? ImmutableList.of()
                    : ImmutableList.of(new EqualTo(new UnboundSlot("value"), new IntegerLiteral(99))), delete);
            PaimonRowChangeSpec.Merge spec = new PaimonRowChangeSpec.Merge(
                    ImmutableList.of("target"), ImmutableList.of(clause), ImmutableList.of());
            LogicalPlan result = PaimonMergePlanner.build(
                    writeTarget(), spec, child, context(child));
            Map<Expression, Expression> nullRow = new HashMap<>();
            child.getOutput().forEach(slot -> nullRow.put(slot, new NullLiteral(slot.getDataType())));

            Assertions.assertEquals(new IntegerLiteral(0), fold(
                    ExpressionUtils.replace(branchLabel(result), nullRow)));
        }
    }

    @Test
    void mergeDistinguishesNullTargetValuesFromNullExtendedRows() {
        LogicalPlan target = targetRow(true);
        LogicalPlan source = new LogicalEmptyRelation(new RelationId(2), ImmutableList.of(
                new SlotReference("id", IntegerType.INSTANCE, true, ImmutableList.of("source")),
                new SlotReference("__DORIS_CHANGELOG_TARGET_PRESENT__", IntegerType.INSTANCE, true,
                        ImmutableList.of("source"))));
        LogicalPlan child = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN, ImmutableList.of(),
                ImmutableList.of(new EqualTo(target.getOutput().get(0), source.getOutput().get(0))),
                target, source, JoinReorderContext.EMPTY);
        MergeMatchedClause update = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot("value"),
                        new UnboundSlot("__DORIS_CHANGELOG_TARGET_PRESENT__"))), false);
        MergeNotMatchedClause insert = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.copyOf(source.getOutput()));
        PaimonRowChangeSpec.Merge spec = new PaimonRowChangeSpec.Merge(
                ImmutableList.of("target"), ImmutableList.of(update), ImmutableList.of(insert));
        LogicalPlan result = PaimonMergePlanner.build(
                writeTarget(), spec, child, context(child));
        LogicalJoin<?, ?> join = result.<LogicalJoin<?, ?>>collectToList(LogicalJoin.class::isInstance).get(0);
        Slot marker = join.getOutput().get(target.getOutput().size());
        Assertions.assertTrue(marker.nullable(), "the marker must inherit outer-join null-extension");
        Map<Expression, Expression> nullRow = new HashMap<>();
        join.getOutput().forEach(slot -> nullRow.put(slot, new NullLiteral(slot.getDataType())));
        Expression label = branchLabel(result);

        Assertions.assertEquals(new IntegerLiteral(1), fold(
                ExpressionUtils.replace(label, nullRow)));
        nullRow.put(marker, new TinyIntLiteral((byte) 1));
        Assertions.assertEquals(new IntegerLiteral(0), fold(
                ExpressionUtils.replace(label, nullRow)));
        // A source column sharing the private marker's name must still supply the UPDATE value.
        nullRow.put(source.getOutput().get(1), new IntegerLiteral(555));
        Expression value = result.<LogicalProject<?>>collectToList(LogicalProject.class::isInstance).stream()
                .flatMap(project -> project.getProjects().stream())
                .filter(project -> project instanceof Alias && project.getName().equals("value"))
                .findFirst().get().child(0);
        value.getInputSlots().stream().filter(slot -> slot.getName().equals("__DORIS_PAIMON_MERGE_BRANCH__"))
                .forEach(slot -> nullRow.put(slot, new IntegerLiteral(0)));
        Assertions.assertEquals(new IntegerLiteral(555), fold(
                ExpressionUtils.replace(value, nullRow)));
    }

    private Expression branchLabel(LogicalPlan plan) {
        return plan.<LogicalProject<?>>collectToList(LogicalProject.class::isInstance).stream()
                .flatMap(project -> project.getProjects().stream())
                .filter(project -> project.getName().equals("__DORIS_PAIMON_MERGE_BRANCH__"))
                .findFirst().get().child(0);
    }

    private Expression fold(Expression expression) {
        return FoldConstantRuleOnFE.evaluate(expression, null);
    }

    private PaimonWriteTarget writeTarget() {
        PaimonWriteTarget target = Mockito.mock(PaimonWriteTarget.class);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(target.getTable()).thenReturn(table);
        Mockito.when(table.primaryKeys()).thenReturn(ImmutableList.of("id"));
        Mockito.when(target.getSchema()).thenReturn(ImmutableList.of(
                new Column("id", ScalarType.createType(PrimitiveType.INT)),
                new Column("value", ScalarType.createType(PrimitiveType.INT))));
        return target;
    }

    private LogicalPlan targetRow(boolean nullableKey) {
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE, nullableKey,
                ImmutableList.of("target"));
        SlotReference value = new SlotReference("value", IntegerType.INSTANCE, true,
                ImmutableList.of("target"));
        return new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(id, value));
    }

    private CascadesContext context(LogicalPlan child) {
        CascadesContext context = MemoTestUtils.createCascadesContext(child);
        context.getConnectContext().setDatabase("test");
        return context;
    }
}
