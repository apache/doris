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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnary;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

/** extract non-foldable expression which exist multiple times, and add them to a new project child.
 * for example:
 * before rewrite:  filter(random() >= 5 and random() <= 10), suppose the two random have the same unique expr id.
 * after rewrite: filter(k >= 5 and k <= 10) -> project(random() as k)
 */
public class AddProjectForNonfoldable implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                new GenerateRewrite().build(),
                new OneRowRelationRewrite().build(),
                new ProjectRewrite().build(),
                new FilterRewrite().build(),
                new HavingRewrite().build()
        );
    }

    private class GenerateRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalGenerate().thenApply(ctx -> {
                LogicalGenerate<Plan> generate = ctx.root;
                Optional<Pair<List<Function>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(generate, generate.getGenerators());
                if (rewrittenOpt.isPresent()) {
                    return generate.withGenerators(rewrittenOpt.get().first)
                            .withChildren(rewrittenOpt.get().second);
                } else {
                    return generate;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_NON_FOLDABLE_GENERATE);
        }
    }

    private class OneRowRelationRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalOneRowRelation().thenApply(ctx -> {
                LogicalOneRowRelation oneRowRelation = ctx.root;
                List<NamedExpression> nonfoldableAlias = getMultiNonfoldableAlias(oneRowRelation.getProjects());
                if (nonfoldableAlias.isEmpty()) {
                    return oneRowRelation;
                }

                Map<Expression, Slot> replaceMap = nonfoldableAlias.stream()
                        .collect(Collectors.toMap(alias -> alias.child(0), NamedExpression::toSlot));

                List<NamedExpression> newProjects = oneRowRelation.getProjects().stream()
                        .map(expr -> (NamedExpression) ExpressionUtils.replace(expr, replaceMap))
                        .collect(ImmutableList.toImmutableList());

                return new LogicalProject<>(newProjects, oneRowRelation.withProjects(nonfoldableAlias));
            }).toRule(RuleType.ADD_PROJECT_FOR_NON_FOLDABLE_ONE_ROW_RELATION);
        }
    }

    private class ProjectRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalProject().thenApply(ctx -> {
                LogicalProject<Plan> project = ctx.root;
                Optional<Pair<List<NamedExpression>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(project, project.getProjects());
                if (rewrittenOpt.isPresent()) {
                    return project.withProjectsAndChild(rewrittenOpt.get().first, rewrittenOpt.get().second);
                } else {
                    return project;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_NON_FOLDABLE_PROJECT);
        }
    }

    private class FilterRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalFilter().thenApply(ctx -> {
                LogicalFilter<Plan> filter = ctx.root;
                Optional<Pair<List<Expression>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(filter, filter.getConjuncts());
                if (rewrittenOpt.isPresent()) {
                    return filter.withConjunctsAndChild(
                            ImmutableSet.copyOf(rewrittenOpt.get().first),
                            rewrittenOpt.get().second);
                } else {
                    return filter;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_NON_FOLDABLE_FILTER);
        }
    }

    private class HavingRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalHaving().thenApply(ctx -> {
                LogicalHaving<Plan> having = ctx.root;
                Optional<Pair<List<Expression>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(having, having.getConjuncts());
                if (rewrittenOpt.isPresent()) {
                    return having.withExpressions(ImmutableSet.copyOf(rewrittenOpt.get().first))
                            .withChildren(rewrittenOpt.get().second);
                } else {
                    return having;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_NON_FOLDABLE_HAVING);
        }
    }

    // extract non-foldable expressions which exist multiple times from targets,
    // then alias the non-foldable expressions and put them into a child project,
    // then rewrite targets with the alias names.
    private <T extends Expression> Optional<Pair<List<T>, LogicalProject<Plan>>> rewriteExpressions(
            LogicalUnary<Plan> plan, Collection<T> targets) {
        List<NamedExpression> nonfoldableAlias = getMultiNonfoldableAlias(targets);
        if (nonfoldableAlias.isEmpty()) {
            return Optional.empty();
        }

        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                .addAll(plan.child().getOutputSet())
                .addAll(nonfoldableAlias)
                .build();

        Map<Expression, Slot> replaceMap = nonfoldableAlias.stream()
                .collect(Collectors.toMap(alias -> alias.child(0), NamedExpression::toSlot));
        List<T> replaceTargets = targets.stream()
                .map(expr -> (T) ExpressionUtils.replace(expr, replaceMap))
                .collect(Collectors.toList());

        return Optional.of(Pair.of(replaceTargets, new LogicalProject<>(projects, plan.child())));
    }

    private List<NamedExpression> getMultiNonfoldableAlias(Collection<? extends Expression> targets) {
        Map<Expression, Integer> nonfoldableCounter = Maps.newHashMap();
        targets.forEach(target -> target.foreach(e -> {
            Expression expr = (Expression) e;
            if (!expr.foldable()) {
                nonfoldableCounter.merge(expr, 1, Integer::sum);
            }
        }));

        return nonfoldableCounter.entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .map(Entry::getKey)
                .map(expr -> {
                    ExprId exprId = StatementScopeIdGenerator.newExprId();
                    return new Alias(exprId, expr, "$_expr_" + exprId.asInt() + "_$");
                })
                .collect(ImmutableList.toImmutableList());
    }
}
