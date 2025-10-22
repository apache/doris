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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** PushDownNestedColumnThroughJoin */
public class PushDownNestedColumn implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.PUSH_DOWN_NESTED_COLUMN_THROUGH_JOIN.build(
                logicalProject(logicalJoin()).thenApply(this::pushThroughJoin)
            ),
            RuleType.PUSH_DOWN_NESTED_COLUMN_THROUGH_WINDOW.build(
                logicalProject(logicalWindow()).thenApply(this::pushThroughWindow)
            ),
            RuleType.PUSH_DOWN_NESTED_COLUMN_THROUGH_UNION.build(
                logicalProject(
                        logicalUnion().when(u -> u.getQualifier() == Qualifier.ALL)
                ).thenApply(this::pushThroughUnion)
            )
        );
    }

    private Plan pushThroughWindow(MatchingContext<LogicalProject<LogicalWindow<Plan>>> ctx) {
        LogicalProject<LogicalWindow<Plan>> project = ctx.root;
        LogicalWindow<Plan> window = project.child();
        PushdownProjectHelper pushdownProjectHelper
                = new PushdownProjectHelper(ctx.statementContext, window);

        Pair<Boolean, List<NamedExpression>> pushProjects
                = pushdownProjectHelper.pushDownExpressions(project.getProjects());

        if (pushProjects.first) {
            List<Plan> newWindowChildren = pushdownProjectHelper.buildNewChildren();
            return new LogicalProject<>(
                    pushProjects.second,
                    window.withChildren(newWindowChildren)
            );
        }
        return project;
    }

    private Plan pushThroughJoin(MatchingContext<LogicalProject<LogicalJoin<Plan, Plan>>> ctx) {
        LogicalProject<LogicalJoin<Plan, Plan>> project = ctx.root;
        LogicalJoin<Plan, Plan> join = project.child();
        PushdownProjectHelper pushdownProjectHelper
                = new PushdownProjectHelper(ctx.statementContext, join);

        Pair<Boolean, List<NamedExpression>> pushProjects
                = pushdownProjectHelper.pushDownExpressions(project.getProjects());

        if (pushProjects.first) {
            List<Plan> newJoinChildren = pushdownProjectHelper.buildNewChildren();
            return new LogicalProject<>(
                    pushProjects.second,
                    join.withChildren(newJoinChildren)
            );
        }
        return project;
    }

    private Plan pushThroughUnion(MatchingContext<LogicalProject<LogicalUnion>> ctx) {
        LogicalProject<LogicalUnion> project = ctx.root;
        LogicalUnion union = project.child();
        PushdownProjectHelper pushdownProjectHelper
                = new PushdownProjectHelper(ctx.statementContext, project);

        Pair<Boolean, List<NamedExpression>> pushProjects
                = pushdownProjectHelper.pushDownExpressions(project.getProjects());
        if (pushProjects.first) {
            List<NamedExpression> unionOutputs = union.getOutputs();
            Map<Slot, Integer> slotToColumnIndex = new LinkedHashMap<>();
            for (int i = 0; i < unionOutputs.size(); i++) {
                NamedExpression output = unionOutputs.get(i);
                slotToColumnIndex.put(output.toSlot(), i);
            }

            Collection<NamedExpression> pushDownProjections
                    = pushdownProjectHelper.childToPushDownProjects.values();
            List<Plan> newChildren = new ArrayList<>();
            List<List<SlotReference>> newChildrenOutputs = new ArrayList<>();
            for (Plan child : union.children()) {
                List<NamedExpression> pushedOutput = replaceSlot(
                        ctx.statementContext,
                        pushDownProjections,
                        slot -> {
                            Integer sourceColumnIndex = slotToColumnIndex.get(slot);
                            if (sourceColumnIndex != null) {
                                return child.getOutput().get(sourceColumnIndex).toSlot();
                            }
                            return slot;
                        }
                );

                LogicalProject<Plan> newChild = new LogicalProject<>(
                        ImmutableList.<NamedExpression>builder()
                                .addAll(child.getOutput())
                                .addAll(pushedOutput)
                                .build(),
                        child
                );

                newChildrenOutputs.add((List) newChild.getOutput());
                newChildren.add(newChild);
            }

            for (List<NamedExpression> originConstantExprs : union.getConstantExprsList()) {
                List<NamedExpression> pushedOutput = replaceSlot(
                        ctx.statementContext,
                        pushDownProjections,
                        slot -> {
                            Integer sourceColumnIndex = slotToColumnIndex.get(slot);
                            if (sourceColumnIndex != null) {
                                return originConstantExprs.get(sourceColumnIndex).toSlot();
                            }
                            return slot;
                        }
                );

                LogicalOneRowRelation originOneRowRelation = new LogicalOneRowRelation(
                        ctx.statementContext.getNextRelationId(),
                        originConstantExprs
                );

                LogicalProject<Plan> newChild = new LogicalProject<>(
                        ImmutableList.<NamedExpression>builder()
                                .addAll(originOneRowRelation.getOutput())
                                .addAll(pushedOutput)
                                .build(),
                        originOneRowRelation
                );

                newChildrenOutputs.add((List) newChild.getOutput());
                newChildren.add(newChild);
            }

            List<NamedExpression> newUnionOutputs = new ArrayList<>(union.getOutputs());
            for (NamedExpression projection : pushDownProjections) {
                newUnionOutputs.add(projection.toSlot());
            }

            return new LogicalProject<>(
                    pushProjects.second,
                    new LogicalUnion(
                            union.getQualifier(),
                            newUnionOutputs,
                            newChildrenOutputs,
                            ImmutableList.of(),
                            union.hasPushedFilter(),
                            newChildren
                    )
            );
        }
        return project;
    }

    private List<NamedExpression> replaceSlot(
            StatementContext statementContext,
            Collection<NamedExpression> pushDownProjections,
            Function<Slot, Slot> slotReplace) {
        List<NamedExpression> pushedOutput = new ArrayList<>();
        for (NamedExpression projection : pushDownProjections) {
            NamedExpression newOutput = (NamedExpression) projection.rewriteUp(e -> {
                if (e instanceof Slot) {
                    Slot newSlot = slotReplace.apply((Slot) e);
                    if (newSlot != null) {
                        return newSlot;
                    }
                }
                return e;
            });
            if (newOutput instanceof Alias) {
                pushedOutput.add(new Alias(statementContext.getNextExprId(), newOutput.child(0)));
            } else {
                pushedOutput.add(new Alias(statementContext.getNextExprId(), newOutput));
            }
        }
        return pushedOutput;
    }

    private static class PushdownProjectHelper {
        private final Plan plan;
        private final StatementContext statementContext;
        private final Map<Expression, Pair<Slot, Plan>> exprToChildAndSlot;
        private final Multimap<Plan, NamedExpression> childToPushDownProjects;

        public PushdownProjectHelper(StatementContext statementContext, Plan plan) {
            this.statementContext = statementContext;
            this.plan = plan;
            this.exprToChildAndSlot = new LinkedHashMap<>();
            this.childToPushDownProjects = ArrayListMultimap.create();
        }

        public <C extends Collection<E>, E extends Expression> Pair<Boolean, C> pushDownExpressions(C expressions) {
            ImmutableCollection.Builder<E> builder;
            if (expressions instanceof List) {
                builder = ImmutableList.builderWithExpectedSize(expressions.size());
            } else {
                builder = ImmutableSet.builderWithExpectedSize(expressions.size());
            }

            boolean extracted = false;
            for (E expression : expressions) {
                Optional<E> result = pushDownExpression(expression);
                if (!result.isPresent()) {
                    builder.add(expression);
                } else {
                    extracted = true;
                    builder.add(result.get());
                }
            }

            if (extracted) {
                return Pair.of(true, (C) builder.build());
            } else {
                return Pair.of(false, expressions);
            }
        }

        public <E extends Expression> Optional<E> pushDownExpression(E expression) {
            if (!(expression instanceof PreferPushDownProject
                    || (expression instanceof Alias && expression.child(0) instanceof PreferPushDownProject))) {
                return Optional.empty();
            }
            Pair<Slot, Plan> existPushdown = exprToChildAndSlot.get(expression);
            if (existPushdown != null) {
                return Optional.of((E) existPushdown.first);
            }

            Alias pushDownAlias = null;
            if (expression instanceof Alias) {
                pushDownAlias = (Alias) expression;
            } else {
                pushDownAlias = new Alias(statementContext.getNextExprId(), expression);
            }

            Set<Slot> inputSlots = expression.getInputSlots();
            for (Plan child : plan.children()) {
                if (child.getOutputSet().containsAll(inputSlots)) {
                    Slot remaimSlot = pushDownAlias.toSlot();
                    exprToChildAndSlot.put(expression, Pair.of(remaimSlot, child));
                    childToPushDownProjects.put(child, pushDownAlias);
                    return Optional.of((E) remaimSlot);
                }
            }
            return Optional.empty();
        }

        public List<Plan> buildNewChildren() {
            if (childToPushDownProjects.isEmpty()) {
                return plan.children();
            }
            ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
            for (Plan child : plan.children()) {
                Collection<NamedExpression> newProject = childToPushDownProjects.get(child);
                if (newProject.isEmpty()) {
                    newChildren.add(child);
                } else {
                    newChildren.add(
                            new LogicalProject<>(
                                    ImmutableList.<NamedExpression>builder()
                                            .addAll(child.getOutput())
                                            .addAll(newProject)
                                            .build(),
                                    child
                            )
                    );
                }
            }
            return newChildren.build();
        }
    }
}
