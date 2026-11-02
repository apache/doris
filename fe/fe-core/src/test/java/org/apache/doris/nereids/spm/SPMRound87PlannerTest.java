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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Round-87: a plan tree handed to {@link SPMOptimizer#optimize} was PARSED under the caller's
 * statement context, and parsing already allocates ExprIds - {@code LogicalPlanBuilder} builds a
 * BOUND {@link Alias} for a literal select item ({@code SELECT 7 AS b}). The capture then plans
 * that tree on a FRESH nested context. A fresh context restarts its generators at 0, and because
 * {@code SlotReference#equals} / {@code hashCode} compare ONLY the exprId, the table columns bound
 * below got the same small ids as those parsed aliases and every {@code Map<Slot, Expression>}
 * substitution conflated them: {@code SELECT k, k > 0 OR b = 7 AS c FROM (SELECT k, 7 AS b FROM t)
 * x} froze as {@code SELECT _spm_const_var(1) AS k, ...} and replayed the constant for every row.
 *
 * <p>The fix is that the nested capture context CONTINUES the outer generators (see
 * StatementContext#continueIdGeneratorsFrom), so the ids it allocates start above every id the
 * swapped-in tree already carries. These tests pin both halves: the parse really does bake an id
 * into the tree, and a continued context never hands that id out again.
 */
public class SPMRound87PlannerTest {

    private static final String DERIVED_LITERAL =
            "SELECT k, k > 0 OR b = 7 AS c FROM (SELECT k, 7 AS b FROM t) x ORDER BY k";

    /**
     * The parse of a literal select item inside a derived table produces a BOUND alias, not an
     * {@code UnboundAlias}: this is what carries an OUTER-statement ExprId into the tree SPM
     * re-plans, so the test would silently stop protecting anything if the parser changed that.
     */
    @Test
    public void testParseBakesAnExprIdIntoALiteralDerivedItem() {
        context();
        try {
            LogicalPlan parsed = parse(DERIVED_LITERAL);
            Assertions.assertFalse(parseTimeExprIds(parsed).isEmpty(),
                    "the parsed tree must already carry ExprIds: " + parsed.treeString());
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * A FRESH nested context (the pre-fix behaviour) hands out exactly the ids the parsed tree
     * already uses - the collision that made the derived constant shadow the real column.
     */
    @Test
    public void testFreshNestedContextReusesTheParseTimeIds() {
        context();
        try {
            Set<Integer> parseTimeIds = parseTimeExprIds(parse(DERIVED_LITERAL));
            StatementContext nested = new StatementContext();
            boolean collided = false;
            for (int i = 0; i <= parseTimeIds.size() + 1; i++) {
                collided |= parseTimeIds.contains(nested.getNextExprId().asInt());
            }
            Assertions.assertTrue(collided,
                    "a fresh context restarts at 0 and must reach a parse-time id");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The capture's nested context continues the outer generators, so no id it allocates can ever
     * repeat an id the parsed tree already carries - for every generator kind, not just ExprId
     * (a RelationId / CTEId / TableId collision is equally unwanted).
     */
    @Test
    public void testContinuedNestedContextNeverReusesAParseTimeId() {
        ConnectContext ctx = context();
        try {
            Set<Integer> parseTimeIds = parseTimeExprIds(parse(DERIVED_LITERAL));
            Assertions.assertFalse(parseTimeIds.isEmpty(), "precondition: parse-time ids exist");
            StatementContext nested = new StatementContext();
            nested.continueIdGeneratorsFrom(ctx.getStatementContext());
            for (int i = 0; i < 64; i++) {
                int exprId = nested.getNextExprId().asInt();
                Assertions.assertFalse(parseTimeIds.contains(exprId),
                        "the continued context must never hand out the parse-time id " + exprId);
            }
            // every OTHER generator kind continues as well, not just ExprId
            StatementContext outer = ctx.getStatementContext();
            int parseTimeRelationId = outer.getNextRelationId().asInt();
            StatementContext later = new StatementContext();
            later.continueIdGeneratorsFrom(outer);
            Assertions.assertTrue(later.getNextRelationId().asInt() > parseTimeRelationId,
                    "relation ids must continue above the ids the parsed tree already carries");
        } finally {
            ConnectContext.remove();
        }
    }

    /** The ids of every bound named expression inside the parsed tree's projections. */
    private static Set<Integer> parseTimeExprIds(Plan plan) {
        Set<Integer> ids = new HashSet<>();
        plan.foreach(node -> {
            if (node instanceof LogicalProject) {
                for (NamedExpression item : ((LogicalProject<?>) node).getProjects()) {
                    try {
                        ids.add(item.getExprId().asInt());
                    } catch (Throwable unbound) {
                        // an unbound item allocates its id during analysis: nothing to collide
                    }
                }
            }
        });
        return ids;
    }

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static ConnectContext context() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("db1");
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
