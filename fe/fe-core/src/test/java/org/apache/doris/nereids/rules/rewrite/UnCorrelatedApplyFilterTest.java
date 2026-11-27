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

import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

class UnCorrelatedApplyFilterTest extends TestWithFeService implements GeneratedPlanPatterns {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("testApplyFilter");
        connectContext.setDatabase("testApplyFilter");
        createTables(
                "CREATE TABLE t1 (col1 int, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");",
                "CREATE TABLE t2 (col1 int, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");"
        );
    }

    @Test
    void testCorrelatedFilter() {
        String sql = "select * from t1 where exists (select * from t2 where t2.col2 > t1.col2)";
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .matches(logicalApply()
                        .when(e -> e.left() instanceof LogicalOlapScan && e.right() instanceof LogicalOlapScan));
    }

    @Test
    void testComplexFilter() {
        String sql = "select * from t1 where exists "
                + "(select * from t2 where t2.col2 > t1.col2 and t2.col1 > 0)";
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .matches(logicalApply(any(), logicalFilter())
                        .when(e -> e.right().getPredicate().toSql().equals("(col1 > 0)")));
    }

    @Test
    void testPredicateWhichWasAlreadyPulledIsKept() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        Slot r2 = right.getOutput().get(1); // t2.name

        Expression pulledPredicate = new EqualTo(x, r2);
        Expression correlatedPredicate = new GreaterThan(r1, x);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(correlatedPredicate, new GreaterThan(r1, new BigIntLiteral(0))), right);
        LogicalApply<LogicalOlapScan, LogicalFilter<LogicalOlapScan>> apply =
                new LogicalApply<>(ImmutableList.of(x), LogicalApply.SubQueryType.EXITS_SUBQUERY, false,
                        Optional.empty(), Optional.empty(), Optional.of(pulledPredicate), Optional.empty(),
                        false, false, left, filter);

        Rule rule = new UnCorrelatedApplyFilter().build();
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        LogicalApply<?, ?> rewritten = (LogicalApply<?, ?>) transformed.get(0);
        Assertions.assertTrue(rewritten.getCorrelationFilter().isPresent());
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(rewritten.getCorrelationFilter().get());
        Assertions.assertTrue(conjuncts.contains(pulledPredicate),
                "the predicate which was already pulled into the apply must not be dropped");
        Assertions.assertTrue(conjuncts.contains(correlatedPredicate),
                "the predicate which this rule pulls has to be kept as well");
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }
}
