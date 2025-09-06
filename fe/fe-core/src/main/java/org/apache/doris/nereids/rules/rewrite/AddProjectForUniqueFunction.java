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
import org.apache.doris.nereids.trees.expressions.functions.scalar.UniqueFunction;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/** extract unique function expression which exist multiple times, and add them to a new project child.
 * for example:
 * before rewrite:  filter(random() >= 5 and random() <= 10), suppose the two random have the same unique expr id.
 * after rewrite: filter(k >= 5 and k <= 10) -> project(random() as k)
 */
public class AddProjectForUniqueFunction implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                new GenerateRewrite().build(),
                new OneRowRelationRewrite().build(),
                new ProjectRewrite().build(),
                new FilterRewrite().build(),
                new HavingRewrite().build(),
                new AggregateRewrite().build(),
                new JoinRewrite().build()
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
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
        }
    }

    private class OneRowRelationRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalOneRowRelation().thenApply(ctx -> {
                LogicalOneRowRelation oneRowRelation = ctx.root;
                List<NamedExpression> uniqueFunctionAlias = tryGenUniqueFunctionAlias(oneRowRelation.getProjects());
                if (uniqueFunctionAlias.isEmpty()) {
                    return oneRowRelation;
                }

                Map<Expression, Slot> replaceMap = Maps.newHashMap();
                for (NamedExpression alias : uniqueFunctionAlias) {
                    replaceMap.put(alias.child(0), alias.toSlot());
                }
                ImmutableList.Builder<NamedExpression> newProjectBuilder
                        = ImmutableList.builderWithExpectedSize(oneRowRelation.getProjects().size());
                for (NamedExpression expr : oneRowRelation.getProjects()) {
                    newProjectBuilder.add((NamedExpression) ExpressionUtils.replace(expr, replaceMap));
                }
                return new LogicalProject<>(
                        newProjectBuilder.build(),
                        oneRowRelation.withProjects(uniqueFunctionAlias));
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
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
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
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
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
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
                    return having.withConjuncts(ImmutableSet.copyOf(rewrittenOpt.get().first))
                            .withChildren(rewrittenOpt.get().second);
                } else {
                    return having;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
        }
    }

    private class AggregateRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalAggregate().thenApply(ctx -> {
                LogicalAggregate<Plan> aggregate = ctx.root;
                List<Expression> targets = Lists.newArrayList();
                targets.addAll(aggregate.getGroupByExpressions());
                targets.addAll(aggregate.getOutputExpressions());
                Optional<Pair<List<Expression>, LogicalProject<Plan>>> rewrittenOpt
                        = rewriteExpressions(aggregate, targets);
                if (!rewrittenOpt.isPresent()) {
                    return aggregate;
                }

                LogicalProject<Plan> newChild = rewrittenOpt.get().second;
                List<Expression> newTargets = rewrittenOpt.get().first;
                int groupBySize = aggregate.getGroupByExpressions().size();
                ImmutableList<Expression> newGroupBy = ImmutableList.copyOf(
                        newTargets.subList(0, groupBySize));
                ImmutableList.Builder<NamedExpression> newOutputBuilder
                        = ImmutableList.builderWithExpectedSize(aggregate.getOutputExpressions().size());
                for (int i = groupBySize; i < newTargets.size(); i++) {
                    newOutputBuilder.add((NamedExpression) newTargets.get(i));
                }
                return aggregate.withChildGroupByAndOutput(newGroupBy, newOutputBuilder.build(), newChild);
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
        }
    }

    private class JoinRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalJoin().thenApply(ctx -> {
                LogicalJoin<Plan, Plan> join = ctx.root;
                int hashOtherConjunctsSize = join.getHashJoinConjuncts().size() + join.getOtherJoinConjuncts().size();
                int totalConjunctsSize = hashOtherConjunctsSize + join.getMarkJoinConjuncts().size();
                List<Expression> allConjuncts = Lists.newArrayListWithExpectedSize(totalConjunctsSize);
                allConjuncts.addAll(join.getHashJoinConjuncts());
                allConjuncts.addAll(join.getOtherJoinConjuncts());
                allConjuncts.addAll(join.getMarkJoinConjuncts());
                Optional<Pair<List<Expression>, LogicalProject<Plan>>> rewrittenOpt
                        = rewriteExpressions(join, allConjuncts);
                if (!rewrittenOpt.isPresent()) {
                    return join;
                }

                LogicalProject<Plan> newLeftChild = rewrittenOpt.get().second;
                List<Expression> newAllConjuncts = rewrittenOpt.get().first;
                List<Expression> newHashOtherConjuncts = newAllConjuncts.subList(0, hashOtherConjunctsSize);
                List<Expression> newMarkJoinConjuncts = ImmutableList.copyOf(
                        newAllConjuncts.subList(hashOtherConjunctsSize, totalConjunctsSize));
                // TODO: code from FindHashConditionForJoin
                Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                        newLeftChild.getOutput(), join.right().getOutput(), newHashOtherConjuncts);
                List<Expression> newHashJoinConjuncts = pair.first;
                List<Expression> newOtherJoinConjuncts = pair.second;
                JoinType joinType = join.getJoinType();
                if (joinType == JoinType.CROSS_JOIN && !newHashJoinConjuncts.isEmpty()) {
                    joinType = JoinType.INNER_JOIN;
                }
                return new LogicalJoin<>(joinType,
                        newHashJoinConjuncts,
                        newOtherJoinConjuncts,
                        newMarkJoinConjuncts,
                        join.getDistributeHint(),
                        join.getMarkJoinSlotReference(),
                        ImmutableList.of(newLeftChild, join.right()),
                        join.getJoinReorderContext());
            }).toRule(RuleType.ADD_PROJECT_FOR_UNIQUE_FUNCTION);
        }
    }

    /**
     * extract unique function which exist multiple times from targets,
     * then alias the unique function and put them into a child project,
     * then rewrite targets with the alias names.
     */
    @VisibleForTesting
    public <T extends Expression> Optional<Pair<List<T>, LogicalProject<Plan>>> rewriteExpressions(
            LogicalPlan plan, Collection<T> targets) {
        List<NamedExpression> uniqueFunctionAlias = tryGenUniqueFunctionAlias(targets);
        if (uniqueFunctionAlias.isEmpty()) {
            return Optional.empty();
        }

        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                .addAll(plan.child(0).getOutputSet())
                .addAll(uniqueFunctionAlias)
                .build();

        Map<Expression, Slot> replaceMap = Maps.newHashMap();
        for (NamedExpression alias : uniqueFunctionAlias) {
            replaceMap.put(alias.child(0), alias.toSlot());
        }
        ImmutableList.Builder<T> newTargetsBuilder = ImmutableList.builderWithExpectedSize(targets.size());
        for (T target : targets) {
            newTargetsBuilder.add((T) ExpressionUtils.replace(target, replaceMap));
        }

        return Optional.of(Pair.of(newTargetsBuilder.build(), new LogicalProject<>(projects, plan.child(0))));
    }

    /**
     * if a unique function exists multiple times in the targets, then add a project to alias it.
     */
    @VisibleForTesting
    public List<NamedExpression> tryGenUniqueFunctionAlias(Collection<? extends Expression> targets) {
        Map<UniqueFunction, Integer> unqiueFunctionCounter = Maps.newLinkedHashMap();
        for (Expression target : targets) {
            target.foreach(e -> {
                Expression expr = (Expression) e;
                if (expr instanceof UniqueFunction) {
                    unqiueFunctionCounter.merge((UniqueFunction) expr, 1, Integer::sum);
                }
            });
        }

        ImmutableList.Builder<NamedExpression> builder
                = ImmutableList.builderWithExpectedSize(unqiueFunctionCounter.size());
        for (Entry<UniqueFunction, Integer> entry : unqiueFunctionCounter.entrySet()) {
            if (entry.getValue() > 1) {
                ExprId exprId = StatementScopeIdGenerator.newExprId();
                String name = "$_" + entry.getKey().getName() + "_" + exprId.asInt() + "_$";
                builder.add(new Alias(exprId, entry.getKey(), name));
            }
        }

        return builder.build();
    }
}
