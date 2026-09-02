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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
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
 * Plain unit test for {@link PushProjectIntoUnion}: builds a Project(LogicalUnion)
 * tree directly and applies only the rule, so the assertions target the rule's
 * output without interference from any other rewrite.
 *
 * <p>Regression contract: after the rule fires, every constant row must hold
 * NamedExpressions whose ExprIds are (a) different from the new UNION output
 * ExprIds and (b) different from the corresponding constants in any other row
 * of the same column.
 */
public class PushProjectIntoUnionTest {

    @Test
    public void testConstantExprIdsDistinctFromUnionOutputAndAcrossRows() {
        // Original UNION: outputs=[s#10], no children, three constant rows: 1, 3, NULL
        // (already merged from OneRowRelations by an earlier rule). Each row's Alias
        // owns its own ExprId.
        SlotReference unionOutput = new SlotReference(new ExprId(10), "s",
                IntegerType.INSTANCE, true, ImmutableList.of());
        NamedExpression row0 = new Alias(new ExprId(1), new IntegerLiteral(1), "1");
        NamedExpression row1 = new Alias(new ExprId(2), new IntegerLiteral(3), "3");
        NamedExpression row2 = new Alias(new ExprId(3), new IntegerLiteral(7), "7");
        LogicalUnion union = new LogicalUnion(Qualifier.ALL,
                ImmutableList.of(unionOutput),
                ImmutableList.of(),
                ImmutableList.of(ImmutableList.of(row0), ImmutableList.of(row1), ImmutableList.of(row2)),
                false,
                ImmutableList.of());

        // Parent project: (s * 2) AS n. The Alias here owns ExprId 100, which will
        // become the new UNION output ExprId after the rule fires. Without the fix,
        // this same ExprId would be reused in every constant row.
        Alias parentAlias = new Alias(new ExprId(100),
                new Multiply(unionOutput, new IntegerLiteral(2)), "n");
        LogicalProject<LogicalUnion> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(parentAlias), union);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushProjectIntoUnion())
                .getPlan();

        // After the rule fires, the project is absorbed and the root becomes a UNION.
        LogicalUnion newUnion = findUnion(rewritten);
        Assertions.assertNotNull(newUnion, "expected a LogicalUnion at/under the root after rewrite");

        List<? extends NamedExpression> outputs = newUnion.getOutputs();
        Assertions.assertEquals(1, outputs.size());
        // The new UNION output keeps the parent project's output ExprId (100).
        Assertions.assertEquals(parentAlias.getExprId(), outputs.get(0).getExprId());

        Set<ExprId> outputIds = new HashSet<>();
        for (NamedExpression out : outputs) {
            outputIds.add(out.getExprId());
        }

        List<List<NamedExpression>> consts = newUnion.getConstantExprsList();
        Assertions.assertEquals(3, consts.size(), "expected three constant rows");

        // (a) constant ExprIds must NOT collide with any UNION output ExprId.
        for (int row = 0; row < consts.size(); row++) {
            for (int col = 0; col < consts.get(row).size(); col++) {
                ExprId cid = consts.get(row).get(col).getExprId();
                Assertions.assertFalse(outputIds.contains(cid),
                        "constant ExprId must not collide with UNION output; row="
                                + row + ", col=" + col + ", id=" + cid);
            }
        }

        // (b) for each column, all rows must have distinct constant ExprIds.
        for (int col = 0; col < outputs.size(); col++) {
            Set<ExprId> seen = new HashSet<>();
            for (int row = 0; row < consts.size(); row++) {
                ExprId cid = consts.get(row).get(col).getExprId();
                Assertions.assertTrue(seen.add(cid),
                        "constant ExprId duplicated across rows for column " + col + ": " + cid);
            }
        }
    }

    @Test
    public void testProjectReferencingOuterSlotIsNotPushedIntoUnion() {
        // A correlated subquery may carry a project that references an outer correlated slot
        // (e.g. an aliased outer column `o.flag AS f` resolved back to its producer). Such a
        // slot has no constant producer in the union, so pushing the project into the union
        // would leave a dangling slot reference; the rule must refuse to fire for this shape.
        SlotReference unionOutput = new SlotReference(new ExprId(10), "s",
                IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference outerSlot = new SlotReference(new ExprId(20), "flag",
                IntegerType.INSTANCE, true, ImmutableList.of("o"));
        LogicalUnion union = new LogicalUnion(Qualifier.ALL,
                ImmutableList.of(unionOutput), ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of());

        Alias outerAlias = new Alias(new ExprId(100), outerSlot, "f");
        LogicalProject<LogicalUnion> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(outerAlias), union);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushProjectIntoUnion())
                .getPlan();
        // The rule must not fire: the project stays above the union.
        Assertions.assertTrue(rewritten instanceof LogicalProject,
                "project referencing an outer slot must not be pushed into the union");
        Assertions.assertTrue(rewritten.child(0) instanceof LogicalUnion);
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
