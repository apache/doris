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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PullUpPredicatesTest {

    /**
     * A predicate containing a NoneMovableFunction (assert_true) must not be pulled up from its
     * filter: consumers would relocate it into another subtree and evaluate it on a different
     * row domain, changing its error behavior.
     */
    @Test
    void testDoNotPullUpNoneMovableFunction() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Slot k = scan.getOutput().get(0);
        Expression assertTrueExpr = new AssertTrue(
                new GreaterThan(k, new IntegerLiteral(0)), new StringLiteral("msg"));
        Expression normal = new GreaterThan(k, new IntegerLiteral(1));
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(assertTrueExpr, normal), scan);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, filter);
        PullUpPredicates pullUpPredicates = new PullUpPredicates(false, cascadesContext);
        ImmutableSet<Expression> predicates = filter.accept(pullUpPredicates, null);

        // the NoneMovableFunction predicate must not be pulled up; the deterministic one still is.
        Assertions.assertTrue(predicates.stream().noneMatch(Expression::containsNoneMovableOrVolatile),
                "NoneMovableFunction predicate must not be pulled up: " + predicates);
        Assertions.assertTrue(predicates.contains(normal),
                "deterministic predicate should be pulled up: " + predicates);
    }
}
