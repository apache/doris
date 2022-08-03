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
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AnalyzeSubquery. translate from subquery to correlatedJoin.
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
                            List<SubqueryExpr> subqueryExprs = filter.getPredicates()
                                    .collect(SubqueryExpr.class::isInstance);
                            if (subqueryExprs.isEmpty()) {
                                return filter;
                            }

                            Optional<Expression> newPredicates =
                                    recursiveReplacePredicate(filter.getPredicates());
                            if (newPredicates.isPresent()) {
                                return new LogicalFilter<>(
                                        newPredicates.get(),
                                        analyzedSubquery(subqueryExprs,
                                                (LogicalPlan) filter.child(), ctx.cascadesContext));
                            }
                            return analyzedSubquery(subqueryExprs,
                                    (LogicalPlan) filter.child(), ctx.cascadesContext);
                        })
                )
        );
    }

    /**
     * Convert expressions with subqueries in filter.
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
    private Optional<Expression> replaceSubquery(SubqueryExpr expr) {
        if (expr instanceof InSubquery || expr instanceof Exists) {
            return Optional.of(BooleanLiteral.TRUE);
        } else if (expr instanceof ScalarSubquery) {
            return Optional.of(expr.getQueryPlan().getOutput().get(0));
        }
        return Optional.empty();
    }

    private Optional<Expression> recursiveReplacePredicate(Expression oldPredicate) {
        List<Optional<Expression>> newChildren = oldPredicate.children()
                .stream()
                .map(this::recursiveReplacePredicate)
                .filter(Optional::isPresent)
                .collect(Collectors.toList());

        if (oldPredicate instanceof SubqueryExpr) {
            return replaceSubquery((SubqueryExpr) oldPredicate);
        }
        return newChildren.isEmpty() ? Optional.of(oldPredicate) : Optional.of(oldPredicate.withChildren(
                newChildren.stream().map(expr -> expr.get()).collect(Collectors.toList())));
    }

    private LogicalPlan analyzedSubquery(List<SubqueryExpr> subqueryExprs,
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
        LogicalApply newApply = new LogicalApply(childPlan, subquery.getQueryPlan(),
                subquery.getCorrelateSlots(),
                subquery, Optional.empty());
        List<Slot> projects = new ArrayList<>(childPlan.getOutput());
        if (subquery instanceof ScalarSubquery) {
            projects.add(subquery.getQueryPlan().getOutput().get(0));
        }
        return new LogicalProject(projects, newApply);
    }
}
