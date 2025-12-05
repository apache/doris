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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NeedSessionVarGuard;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * The purpose of this class is to add session var guards to all expressions that require guarding
 * The purpose of the `rewritePlanTree()` method is to add session variable guards to all expressions
 * in a plan tree that require guarding
 * If you need to traverse and add to an expression, use AddSessionVarGuardRewriter
 * If you need to add a guard to the plan tree, use rewritePlanTree()
 * */
public class SessionVarGuardRewriter extends ExpressionRewrite {
    private final List<Rule> rules;
    private final CascadesContext cascadesContext;

    public SessionVarGuardRewriter(Map<String, String> var, CascadesContext ctx) {
        super(new ExpressionRuleExecutor(ImmutableList.of(bottomUp(
                new ReplaceRule(new AddSessionVarGuardRewriter(var))))));
        rules = buildRules();
        cascadesContext = ctx;
    }

    /**rewrite all exprs in one plan node */
    private Plan rewritePlanNode(Plan plan) {
        for (Rule rule : rules) {
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, cascadesContext);
                Plan newPlan = newPlans.get(0);
                if (!newPlan.deepEquals(plan)) {
                    return newPlan;
                }
            }
        }
        return plan;
    }

    private static class ReplaceRule implements ExpressionPatternRuleFactory {
        private final AddSessionVarGuardRewriter addGuardRewriter;

        private ReplaceRule(AddSessionVarGuardRewriter guard) {
            this.addGuardRewriter = guard;
        }

        @Override
        public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
            return ImmutableList.of(
                    matchesType(Alias.class).thenApply(ctx -> {
                        Alias alias = ctx.expr;
                        Expression aliasChild = alias.child().accept(addGuardRewriter, Boolean.FALSE);
                        return alias.withChildren(ImmutableList.of(aliasChild));
                    }).toRule(ExpressionRuleType.ADD_SESSION_VAR_GUARD)
            );
        }
    }

    /** This ensures that all expressions implementing NeedSessionVarGuard are
     * wrapped in a SessionVarGuardExpr layer.
     * e.g. (a+b)*c -> guard(guard(a+b)*c)
     * */
    public static class AddSessionVarGuardRewriter extends DefaultExpressionRewriter<Boolean> {
        private final Map<String, String> sessionVar;

        public AddSessionVarGuardRewriter(Map<String, String> var) {
            sessionVar = var;
        }

        @Override
        public Expression visit(Expression expr, Boolean insideGuard) {
            Expression rewritten = rewriteChildren(this, expr, Boolean.FALSE);
            if (rewritten instanceof NeedSessionVarGuard && !Boolean.TRUE.equals(insideGuard)) {
                if (sessionVar == null) {
                    return expr;
                }
                return new SessionVarGuardExpr(rewritten, sessionVar);
            }
            return rewritten;
        }

        @Override
        public Expression visitSessionVarGuardExpr(SessionVarGuardExpr expr, Boolean context) {
            Expression child = expr.child().accept(this, Boolean.TRUE);
            if (child != expr.child()) {
                return expr.withChildren(ImmutableList.of(child));
            }
            return expr;
        }
    }

    /** rewrite plan tree */
    public static Plan rewritePlanTree(SessionVarGuardRewriter exprRewriter, Plan plan) {
        return plan.accept(new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visit(Plan plan, Void ctx) {
                plan = super.visit(plan, ctx);
                return exprRewriter.rewritePlanNode(plan);
            }
        }, null);
    }

    /**
     * Check if current query session variables match MV creation session variables.
     * Only compares variables that affect query results.
     */
    public static boolean checkSessionVariablesMatch(Map<String, String> currentSessionVars,
            Map<String, String> persistSessionVars) {
        if (persistSessionVars == null || persistSessionVars.isEmpty()) {
            // If no session variables saved, consider them matched
            return true;
        }
        return currentSessionVars.equals(persistSessionVars);
    }
}
