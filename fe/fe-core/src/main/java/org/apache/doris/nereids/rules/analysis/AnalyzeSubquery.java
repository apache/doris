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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * AnalyzeSubquery. translate from subquery to LogicalApply.
 * In two steps
 * The first step is to replace the predicate corresponding to the filter where the subquery is located.
 * The second step converts the subquery into an apply node.
 */
public class AnalyzeSubquery implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.ANALYZE_FILTER_SUBQUERY.build(
                        logicalFilter().thenApply(ctx -> {
                            LogicalFilter filter = ctx.root;
                            Set<SubqueryExpr> subqueryExprs = filter.getPredicates()
                                    .collect(SubqueryExpr.class::isInstance);
                            if (subqueryExprs.isEmpty()) {
                                return filter;
                            }

                            // first step: Replace the subquery of predicate in LogicalFilter
                            // second step: Replace subquery with LogicalApply
                            return new LogicalFilter<>(new ReplaceSubquery().replace(filter.getPredicates()),
                                    analyzedSubquery(
                                    subqueryExprs, (LogicalPlan) filter.child(), ctx.cascadesContext
                            ));
                        })
                )
        );
    }

    private LogicalPlan analyzedSubquery(Set<SubqueryExpr> subqueryExprs,
            LogicalPlan childPlan, CascadesContext ctx) {
        LogicalPlan tmpPlan = childPlan;
        for (SubqueryExpr subqueryExpr : subqueryExprs) {
            if (!ctx.subqueryIsAnalyzed(subqueryExpr)) {
                tmpPlan = addApply(subqueryExpr, tmpPlan, ctx);
            }
        }
        return tmpPlan;
    }

    private LogicalPlan addApply(SubqueryExpr subquery,
            LogicalPlan childPlan, CascadesContext ctx) {
        ctx.setSubqueryExprIsAnalyzed(subquery, true);
        LogicalApply newApply = new LogicalApply(
                subquery.getCorrelateSlots(),
                subquery, Optional.empty(), childPlan, subquery.getQueryPlan());
        List<Slot> projects = new ArrayList<>(childPlan.getOutput());
        if (subquery instanceof ScalarSubquery) {
            projects.add(subquery.getQueryPlan().getOutput().get(0));
        }
        return new LogicalProject(projects, newApply);
    }

    /**
     * The Subquery in the LogicalFilter will change to LogicalApply, so we must replace the origin Subquery.
     * LogicalFilter(predicate(contain subquery)) -> LogicalFilter(predicate(not contain subquery)
     * Replace the subquery in logical with the relevant expression.
     *
     * The replacement rules are as follows:
     * before:
     *      1.filter(t1.a = scalarSubquery(output b));
     *      2.filter(inSubquery);   inSubquery = (t1.a in select ***);
     *      3.filter(exists);   exists = (select ***);
     *
     * after:
     *      1.filter(t1.a = b);
     *      2.filter(True);
     *      3.filter(True);
     */
    private static class ReplaceSubquery extends DefaultExpressionRewriter {
        public Expression replace(Expression expression) {
            return (Expression) expression.accept(this, null);
        }

        @Override
        public Object visitExistsSubquery(Exists exists, Object context) {
            return BooleanLiteral.TRUE;
        }

        @Override
        public Object visitInSubquery(InSubquery in, Object context) {
            return BooleanLiteral.TRUE;
        }

        @Override
        public Object visitScalarSubquery(ScalarSubquery scalar, Object context) {
            return scalar.getQueryPlan().getOutput().get(0);
        }
    }
}
