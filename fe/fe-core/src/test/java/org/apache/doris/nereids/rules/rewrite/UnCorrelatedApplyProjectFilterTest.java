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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * The rule pulls the correlated predicates of the filter below the projection of an IN subquery into
 * the apply, and the projection which is kept above the filter has to expose the columns which the
 * pulled predicates read. The predicates which were already pulled into the apply have to stay in its
 * correlation filter: every one of them is a condition of the join which unnests the apply.
 */
class UnCorrelatedApplyProjectFilterTest {

    @Test
    public void testPredicateWhichWasAlreadyPulledIsKept() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        Slot r2 = right.getOutput().get(1); // t2.name

        Expression pulledPredicate = new EqualTo(x, r2);
        Expression correlatedPredicate = new EqualTo(r1, x);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(correlatedPredicate, new GreaterThan(r1, new BigIntLiteral(0))), right);
        LogicalProject<LogicalFilter<LogicalOlapScan>> project =
                new LogicalProject<>(ImmutableList.of(r2), filter);
        LogicalApply<LogicalOlapScan, LogicalProject<LogicalFilter<LogicalOlapScan>>> apply =
                new LogicalApply<>(ImmutableList.of(x), LogicalApply.SubQueryType.IN_SUBQUERY, false,
                        Optional.<Expression>of(x), Optional.empty(), Optional.of(pulledPredicate),
                        Optional.empty(), false, false, left, project);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyProjectFilter().build();
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        LogicalApply<?, ?> rewritten = (LogicalApply<?, ?>) transformed.get(0);
        Assertions.assertTrue(rewritten.getCorrelationFilter().isPresent());
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(rewritten.getCorrelationFilter().get());
        Assertions.assertTrue(conjuncts.contains(pulledPredicate),
                "the predicate which was already pulled into the apply must not be dropped");
        Assertions.assertTrue(conjuncts.contains(correlatedPredicate),
                "the predicate which this rule pulls has to be kept as well");
        Assertions.assertTrue(rewritten.right().getOutput().contains(r1),
                "the column which the pulled predicate reads has to be exposed by the projection");
    }
}
