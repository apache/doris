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

package org.apache.doris.nereids.util;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.load.NereidsLoadUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PlanUtilsTest {
    @Test
    void filterMergesConjuncts() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Expression aNotNull = new Not(new IsNull(scan.getOutput().get(0)), true);
        Expression bNotNull = new Not(new IsNull(scan.getOutput().get(1)));
        Expression generatedBNotNull = new Not(new IsNull(scan.getOutput().get(1)), true);
        Expression existingPredicate = new GreaterThan(scan.getOutput().get(0), new IntegerLiteral(0));
        ImmutableSet<Expression> existingPredicates = ImmutableSet.of(bNotNull, existingPredicate);
        LogicalFilter<?> original = new LogicalFilter<>(existingPredicates, scan);

        LogicalFilter<?> merged = PlanUtils.filter(ImmutableSet.of(aNotNull, generatedBNotNull), original).get();

        Assertions.assertSame(scan, merged.child());
        Assertions.assertEquals(ImmutableSet.of(aNotNull, bNotNull, generatedBNotNull, existingPredicate),
                merged.getConjuncts());
        Assertions.assertEquals(existingPredicates, original.getConjuncts());
    }

    @Test
    void filterReusesExistingPredicates() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Expression aNotNull = new Not(new IsNull(scan.getOutput().get(0)), true);
        Expression bNotNull = new Not(new IsNull(scan.getOutput().get(1)), true);
        ImmutableSet<Expression> predicates = ImmutableSet.of(aNotNull, bNotNull);
        LogicalFilter<?> filter = new LogicalFilter<>(predicates, scan);

        Assertions.assertSame(filter, PlanUtils.filter(predicates, filter).get());
        Assertions.assertSame(filter, PlanUtils.filter(ImmutableSet.of(aNotNull), filter).get());
        Assertions.assertSame(filter, PlanUtils.filterOrSelf(ImmutableSet.of(bNotNull), filter));
        Assertions.assertSame(filter, PlanUtils.filterOrSelf(ImmutableSet.of(), filter));
        Assertions.assertTrue(PlanUtils.filter(ImmutableSet.of(), filter).isEmpty());
    }

    @Test
    void filterPreservesOriginalPredicate() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Expression originalPredicate = new GreaterThan(scan.getOutput().get(0), new IntegerLiteral(0));
        Expression inferredPredicate = originalPredicate.withInferred(true);
        Assertions.assertEquals(originalPredicate, inferredPredicate);

        LogicalFilter<?> inferredFilter = new LogicalFilter<>(ImmutableSet.of(inferredPredicate), scan);
        LogicalFilter<?> merged = PlanUtils.filter(ImmutableSet.of(originalPredicate), inferredFilter).get();
        Assertions.assertSame(scan, merged.child());
        Assertions.assertSame(originalPredicate, merged.getConjuncts().iterator().next());
        Assertions.assertTrue(inferredFilter.getConjuncts().iterator().next().isInferred());

        LogicalFilter<?> originalFilter = new LogicalFilter<>(ImmutableSet.of(originalPredicate), scan);
        Assertions.assertSame(originalFilter, PlanUtils.filter(ImmutableSet.of(inferredPredicate), originalFilter).get());
    }

    @Test
    void filterDoesNotMergeThroughProject() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Expression aNotNull = new Not(new IsNull(scan.getOutput().get(0)), true);
        Expression bNotNull = new Not(new IsNull(scan.getOutput().get(1)), true);
        LogicalFilter<?> original = new LogicalFilter<>(ImmutableSet.of(bNotNull), scan);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), original);

        LogicalFilter<?> filter = PlanUtils.filter(ImmutableSet.of(aNotNull), project).get();

        Assertions.assertSame(project, filter.child());
        Assertions.assertEquals(ImmutableSet.of(aNotNull), filter.getConjuncts());
        Assertions.assertSame(original, project.child());
    }

    @Test
    void filterOrSelf() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Plan filterOrSelf = PlanUtils.filterOrSelf(ImmutableSet.of(), scan);
        Assertions.assertSame(scan, filterOrSelf);

        Plan filter = PlanUtils.filterOrSelf(ImmutableSet.of(BooleanLiteral.TRUE), scan);
        Assertions.assertTrue(filter instanceof LogicalFilter);
    }

    @Test
    void translateToLegacyExprShouldQuoteReservedColumnForRoutineLoadReparse() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("", 0)));
        Expression expr = new NereidsParser().parseExpression("`group` is not null");

        Expr legacyExpr = PlanUtils.translateToLegacyExpr(expr, null, ctx);
        String exprSql = legacyExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);

        Assertions.assertTrue(exprSql.contains("`group`"));
        Assertions.assertDoesNotThrow(() -> NereidsLoadUtils.parseExpressionSeq(exprSql));
    }
}
