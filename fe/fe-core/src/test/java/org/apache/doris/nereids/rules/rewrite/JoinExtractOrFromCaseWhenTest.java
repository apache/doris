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

import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JoinExtractOrFromCaseWhen}.
 */
class JoinExtractOrFromCaseWhenTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    private RuleFactory joinExtractOrFromCaseWhenRule() {
        return new JoinExtractOrFromCaseWhen();
    }

    /**
     * A join condition mixing both sides with a case-when-like expression is normally rewritten
     * into an OR-expansion condition; but when the condition also contains a NoneMovableFunction
     * (assert_true), the rewrite must be skipped so the join is left untouched.
     */
    @Test
    void testNoneMovableFunctionSkipsRewrite() {
        Slot leftA = scan1.getOutput().get(0);
        Slot leftB = scan1.getOutput().get(1);
        Slot rightA = scan2.getOutput().get(0);
        Slot rightB = scan2.getOutput().get(1);
        // (case when leftA > 0 then rightA else rightB end) = leftA + leftB
        Expression caseWhen = new If(new GreaterThan(leftA, new IntegerLiteral(0)), rightA, rightB);
        Expression extractable = new EqualTo(caseWhen, new Add(leftA, leftB));

        // control: without assert_true the rewrite fires and adds an OR-expansion condition.
        // (the condition must be an other join conjunct: OrExpansion.needRewriteJoin only
        // accepts a join with empty hash conjuncts, i.e. a nested loop join)
        LogicalPlan control = new LogicalPlanBuilder(scan1).join(scan2, JoinType.INNER_JOIN,
                ImmutableList.of(), ImmutableList.of(extractable)).build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), control)
                .applyTopDown(joinExtractOrFromCaseWhenRule())
                .matches(
                        logicalJoin().when(join -> !join.getOtherJoinConjuncts().isEmpty())
                );

        // guard: with assert_true the join condition must be left untouched.
        Expression guarded = new And(extractable,
                new AssertTrue(new EqualTo(leftA, rightA), new StringLiteral("msg")));
        LogicalPlan plan = new LogicalPlanBuilder(scan1).join(scan2, JoinType.INNER_JOIN,
                ImmutableList.of(), ImmutableList.of(guarded)).build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(joinExtractOrFromCaseWhenRule())
                .matches(
                        logicalJoin().when(join -> join.getOtherJoinConjuncts().equals(ImmutableList.of(guarded))
                                && join.getHashJoinConjuncts().isEmpty())
                );
    }
}
