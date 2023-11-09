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

import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SubqueryToApply. translate from subquery to LogicalApply.
 * In two steps
 * The first step is to replace the predicate corresponding to the filter where the subquery is located.
 * The second step converts the subquery into an apply node.
 */
public class CheckLeadingJoin implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.FILTER_SUBQUERY_TO_APPLY.build(
                logicalFilter().thenApply(ctx -> {
                    LogicalFilter<Plan> filter = ctx.root;

                    ImmutableList<Set<SubqueryExpr>> subqueryExprsList = filter.getConjuncts().stream()
                            .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                            .collect(ImmutableList.toImmutableList());
                    if (!subqueryExprsList.isEmpty()) {
                        if (ctx.cascadesContext.isLeadingJoin()) {
                            Hint leading = ctx.cascadesContext.getHintMap().get("Leading");
                            leading.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                            leading.setErrorMessage("leading can not deal with subquery right now");
                            ctx.cascadesContext.setLeadingJoin(false);
                        }
                    }
                    return ctx.root;
                })
            ),
            RuleType.PROJECT_SUBQUERY_TO_APPLY.build(logicalProject().thenApply(ctx -> {
                LogicalProject<Plan> project = ctx.root;
                ImmutableList<Set<SubqueryExpr>> subqueryExprsList = project.getProjects().stream()
                        .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                        .collect(ImmutableList.toImmutableList());
                if (subqueryExprsList.stream().flatMap(Collection::stream).count() > 0) {
                    if (ctx.cascadesContext.isLeadingJoin()) {
                        Hint leading = ctx.cascadesContext.getHintMap().get("Leading");
                        leading.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                        leading.setErrorMessage("leading can not deal with subquery right now");
                        ctx.cascadesContext.setLeadingJoin(false);
                    }
                }
                return ctx.root;
            })),
            RuleType.ONE_ROW_RELATION_SUBQUERY_TO_APPLY.build(logicalOneRowRelation()
                .when(ctx -> ctx.getProjects().stream()
                    .anyMatch(project -> project.containsType(SubqueryExpr.class)))
                .thenApply(ctx -> {
                    if (ctx.cascadesContext.isLeadingJoin()) {
                        Hint leading = ctx.cascadesContext.getHintMap().get("Leading");
                        leading.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                        leading.setErrorMessage("leading can not deal with subquery right now");
                        ctx.cascadesContext.setLeadingJoin(false);
                    }
                    LogicalOneRowRelation oneRowRelation = ctx.root;
                    // create a LogicalProject node with the same project lists above LogicalOneRowRelation
                    // create a LogicalOneRowRelation with a dummy output column
                    // so PROJECT_SUBQUERY_TO_APPLY rule can handle the subquery unnest thing
                    return new LogicalProject<Plan>(oneRowRelation.getProjects(),
                        oneRowRelation.withProjects(
                            ImmutableList.of(new Alias(BooleanLiteral.of(true),
                                ctx.statementContext.generateColumnName()))));
                })),
            RuleType.JOIN_SUBQUERY_TO_APPLY
                .build(logicalJoin()
                    .when(join -> join.getHashJoinConjuncts().isEmpty() && !join.getOtherJoinConjuncts().isEmpty())
                    .thenApply(ctx -> {
                        LogicalJoin<Plan, Plan> join = ctx.root;
                        Map<Boolean, List<Expression>> joinConjuncts = join.getOtherJoinConjuncts().stream()
                                .collect(Collectors.groupingBy(conjunct -> conjunct.containsType(SubqueryExpr.class),
                                Collectors.toList()));
                        List<Expression> subqueryConjuncts = joinConjuncts.get(true);
                        if (subqueryConjuncts != null && !subqueryConjuncts.isEmpty()) {
                            if (ctx.cascadesContext.isLeadingJoin()) {
                                Hint leading = ctx.cascadesContext.getHintMap().get("Leading");
                                leading.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                                leading.setErrorMessage("leading can not deal with subquery right now");
                                ctx.cascadesContext.setLeadingJoin(false);
                            }
                        }
                        return join;
                    }))
        );
    }

}
