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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class VariantSubPathPruningTest {
    @Test
    void testUnionSubPathsStayWithOriginalColumns() {
        SlotReference a = new SlotReference("a", VariantType.INSTANCE);
        SlotReference b = new SlotReference("b", VariantType.INSTANCE);
        SlotReference c = new SlotReference("c", VariantType.INSTANCE);
        ImmutableList.Builder<Plan> children = ImmutableList.builder();
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (int i = 0; i < 2; i++) {
            Alias childA = new Alias(new Cast(new StringLiteral("{\"x\":11}"), VariantType.INSTANCE), "a");
            Alias childB = new Alias(new Cast(new StringLiteral("{\"x\":22}"), VariantType.INSTANCE), "b");
            children.add(new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                    ImmutableList.of(childA, childB)));
            // The first and third UNION columns read the same child slot.
            childrenOutputs.add(ImmutableList.of(
                    (SlotReference) childA.toSlot(), (SlotReference) childB.toSlot(), (SlotReference) childA.toSlot()));
        }
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(a, b, c),
                childrenOutputs.build(), ImmutableList.of(), false, children.build());
        LogicalProject<LogicalUnion> project = new LogicalProject<>(ImmutableList.of(
                new Alias(new ElementAt(a, new VarcharLiteral("x"))),
                new Alias(new ElementAt(b, new VarcharLiteral("x"))),
                new Alias(new ElementAt(c, new VarcharLiteral("x")))), union);

        LogicalProject<?> rewritten = (LogicalProject<?>) new VariantSubPathPruning().rewriteRoot(project, null);
        LogicalUnion newUnion = (LogicalUnion) rewritten.child();
        Assertions.assertEquals(6, newUnion.getOutputs().size());
        for (int i = 0; i < 3; i++) {
            Assertions.assertEquals(union.getOutputs().get(i), newUnion.getOutputs().get(i * 2));
            Assertions.assertEquals(newUnion.getOutputs().get(i * 2 + 1), rewritten.getProjects().get(i).child(0));
        }
        for (List<SlotReference> outputs : newUnion.getRegularChildrenOutputs()) {
            Assertions.assertEquals(6, outputs.size());
            Assertions.assertEquals(outputs.get(1), outputs.get(5));
            Assertions.assertNotEquals(outputs.get(1), outputs.get(3));
        }
    }

    @Test
    void testConstantUnionWithWholeColumnAndSubPath() {
        SlotReference a = new SlotReference("a", VariantType.INSTANCE);
        SlotReference b = new SlotReference("b", VariantType.INSTANCE);
        List<NamedExpression> row = ImmutableList.of(
                new Alias(new Cast(new StringLiteral("{\"x\":11}"), VariantType.INSTANCE), "a"),
                new Alias(new Cast(new StringLiteral("{\"x\":22}"), VariantType.INSTANCE), "b"));
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(a, b),
                ImmutableList.of(), ImmutableList.of(row), false, ImmutableList.of());
        LogicalProject<LogicalUnion> project = new LogicalProject<>(
                ImmutableList.of(a, new Alias(new ElementAt(b, new VarcharLiteral("x")))), union);

        LogicalProject<?> rewritten = (LogicalProject<?>) new VariantSubPathPruning().rewriteRoot(project, null);
        LogicalUnion newUnion = (LogicalUnion) rewritten.child();
        Assertions.assertEquals(3, newUnion.getOutputs().size());
        Assertions.assertEquals(a, rewritten.getProjects().get(0));
        Assertions.assertEquals(newUnion.getOutputs().get(2), rewritten.getProjects().get(1).child(0));
        List<NamedExpression> newRow = newUnion.getConstantExprsList().get(0);
        Assertions.assertEquals(row, newRow.subList(0, 2));
        Assertions.assertEquals(new ElementAt(row.get(1).child(0), new VarcharLiteral("x")), newRow.get(2).child(0));
    }
}
