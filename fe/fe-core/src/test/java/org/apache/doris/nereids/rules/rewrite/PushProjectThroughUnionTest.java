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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Plain unit test for {@link PushProjectThroughUnion}: directly builds a
 * Project(LogicalUnion(...)) where the union holds a non-empty
 * {@code constantExprsList} (the mixed-union shape produced after
 * {@code MergeOneRowRelationIntoUnion} folds a {@code LogicalOneRowRelation}
 * into a union sibling) and asserts that after the rule rewrites the constant
 * rows, no constant cell's ExprId collides with the new UNION output ExprId.
 */
public class PushProjectThroughUnionTest {

    @Test
    public void testConstantExprIdsDistinctFromUnionOutput() {
        // Build a LogicalUnion that owns a single constant row and zero regular
        // children. The arity does not affect the constant-row rewrite branch
        // we want to cover; what matters is that constantExprsList is non-empty
        // and the parent project carries a non-Slot expression that triggers
        // the else branch of doPushProject.
        SlotReference unionOutput = new SlotReference(new ExprId(10), "s",
                IntegerType.INSTANCE, true, ImmutableList.of());
        NamedExpression row0 = new Alias(new ExprId(1), new IntegerLiteral(99), "s");
        LogicalUnion union = new LogicalUnion(Qualifier.ALL,
                ImmutableList.of(unionOutput),
                ImmutableList.of(),
                ImmutableList.of(ImmutableList.of(row0)),
                false,
                ImmutableList.of());

        // Parent project: CAST(s AS BIGINT) AS n. canPushProject accepts this
        // because the inner expression covered by the cast is a SlotReference.
        // The Alias's ExprId (100) becomes the new UNION output ExprId via
        // project.toSlot(). Without the fix, the constant row would also keep
        // ExprId 100 and collide.
        Alias parentAlias = new Alias(new ExprId(100),
                new Cast(unionOutput, BigIntType.INSTANCE), "n");
        LogicalProject<LogicalUnion> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(parentAlias), union);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushProjectThroughUnion())
                .getPlan();

        LogicalUnion newUnion = findUnion(rewritten);
        Assertions.assertNotNull(newUnion, "expected a LogicalUnion at/under the root after rewrite");

        List<? extends NamedExpression> outputs = newUnion.getOutputs();
        Assertions.assertEquals(1, outputs.size());
        // The new UNION output reuses the parent project's output ExprId.
        Assertions.assertEquals(parentAlias.getExprId(), outputs.get(0).getExprId());

        Set<ExprId> outputIds = new HashSet<>();
        for (NamedExpression out : outputs) {
            outputIds.add(out.getExprId());
        }

        List<List<NamedExpression>> consts = newUnion.getConstantExprsList();
        Assertions.assertEquals(1, consts.size(), "expected one constant row");

        for (int row = 0; row < consts.size(); row++) {
            for (int col = 0; col < consts.get(row).size(); col++) {
                ExprId cid = consts.get(row).get(col).getExprId();
                Assertions.assertFalse(outputIds.contains(cid),
                        "constant ExprId must not collide with UNION output; row="
                                + row + ", col=" + col + ", id=" + cid);
            }
        }
    }

    @Test
    public void testCastProjectCanOnlyPushThroughUnionAll() {
        SlotReference unionOutput = new SlotReference(new ExprId(10), "s",
                IntegerType.INSTANCE, true, ImmutableList.of());
        Alias castProject = new Alias(new ExprId(100),
                new Cast(unionOutput, BigIntType.INSTANCE), "n");
        ImmutableList<NamedExpression> projects = ImmutableList.of(castProject);

        LogicalUnion unionAll = new LogicalUnion(Qualifier.ALL,
                ImmutableList.of(unionOutput), ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of());
        Assertions.assertTrue(PushProjectThroughUnion.canPushProject(projects, unionAll));

        LogicalUnion unionDistinct = new LogicalUnion(Qualifier.DISTINCT,
                ImmutableList.of(unionOutput), ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of());
        Assertions.assertFalse(PushProjectThroughUnion.canPushProject(projects, unionDistinct));
    }

    private LogicalUnion findUnion(Plan p) {
        if (p instanceof LogicalUnion) {
            return (LogicalUnion) p;
        }
        for (Plan c : p.children()) {
            LogicalUnion u = findUnion(c);
            if (u != null) {
                return u;
            }
        }
        return null;
    }
}
