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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Check search expression usage before rewrites can eliminate invalid expressions.
 * This rule validates that search() expressions only appear in supported contexts.
 * Must run in analysis phase before search() gets optimized away.
 */
public class CheckSearchUsage implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                any().thenApply(ctx -> {
                    Plan plan = ctx.root;
                    checkPlan(plan);
                    return plan;
                }).toRule(RuleType.CHECK_SEARCH_USAGE)
        );
    }

    private void checkPlan(Plan plan) {
        if (containsSearchInPlanExpressions(plan)
                && !(plan instanceof LogicalFilter)) {
            throw new AnalysisException(
                    "search() is only supported in WHERE filters (filter predicates)");
        }
    }

    private boolean containsSearchInPlanExpressions(Plan plan) {
        for (Expression expr : plan.getExpressions()) {
            if (containsSearchExpression(expr)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsSearchExpression(Expression expression) {
        if (expression instanceof Search || expression instanceof SearchExpression) {
            return true;
        }
        for (Expression child : expression.children()) {
            if (containsSearchExpression(child)) {
                return true;
            }
        }
        return false;
    }
}
