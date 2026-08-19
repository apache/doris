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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntryArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapLambdaValidator;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Materialize computed Map inputs used by {@link MapEntryArrayMap}.
 *
 * <p>A Map entry lambda takes {@code map_keys(computedMap)} and
 * {@code map_values(computedMap)} as its two input arrays.  rule evaThisluates
 * {@code computedMap} in a child Project and replaces all its occurrences with the same Slot:
 *
 * <pre>
 * before:
 *   Project[map_from_arrays(
 *     map_keys(computedMap),
 *     MapEntryArrayMap(
 *       (mapKey, mapValue) -> valueExpression,
 *       map_keys(computedMap), map_values(computedMap)))]
 *     child
 *
 * after:
 *   Project[map_from_arrays(
 *     map_keys(materializedMapSlot),
 *     MapEntryArrayMap(
 *       (mapKey, mapValue) -> valueExpression,
 *       map_keys(materializedMapSlot), map_values(materializedMapSlot)))]
 *     Project[child.*, computedMap AS materializedMapSlot]
 *       child
 * </pre>
 *
 * <p> Besides the basic rewrite above, this rule handles
 * repeated entry arrays, nested lambdas, and Join children through dedicated helper methods below.
 */
public class AddProjectForMapLambdaInput implements RewriteRuleFactory {

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
                List<Function> generators = materializeNestedMapInputs(generate.getGenerators());
                Optional<Pair<List<Function>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(generate, generators);
                if (rewrittenOpt.isPresent()) {
                    return generate.withGenerators(rewrittenOpt.get().first)
                            .withChildren(rewrittenOpt.get().second);
                } else if (!generators.equals(generate.getGenerators())) {
                    return generate.withGenerators(generators);
                } else {
                    return generate;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    private class OneRowRelationRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalOneRowRelation().thenApply(ctx -> {
                LogicalOneRowRelation oneRowRelation = ctx.root;
                List<NamedExpression> projects = materializeNestedMapInputs(oneRowRelation.getProjects());
                List<NamedExpression> mapInputAliases = tryGenMapInputAliases(projects);
                List<NamedExpression> rewrittenProjects = replaceExpressions(projects, mapInputAliases);
                List<NamedExpression> entryArrayAliases = tryGenSharedEntryArrayAliases(rewrittenProjects);
                if (mapInputAliases.isEmpty() && entryArrayAliases.isEmpty()) {
                    return projects.equals(oneRowRelation.getProjects())
                            ? oneRowRelation : oneRowRelation.withProjects(projects);
                }

                // A OneRowRelation has no child on which to install the usual materialization
                // Project. Use the relation itself as the lowest projection, then stack the shared
                // entry-array Project and the original output Project above it.
                Plan child;
                if (mapInputAliases.isEmpty()) {
                    child = oneRowRelation.withProjects(entryArrayAliases);
                } else {
                    child = oneRowRelation.withProjects(mapInputAliases);
                    if (!entryArrayAliases.isEmpty()) {
                        child = appendProject(child, entryArrayAliases);
                    }
                }
                rewrittenProjects = replaceExpressions(rewrittenProjects, entryArrayAliases);
                return new LogicalProject<>(rewrittenProjects, child);
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    private class ProjectRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalProject().thenApply(ctx -> {
                LogicalProject<Plan> project = ctx.root;
                List<NamedExpression> projects = materializeNestedMapInputs(project.getProjects());
                Optional<Pair<List<NamedExpression>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(project, projects);
                if (rewrittenOpt.isPresent()) {
                    return project.withProjectsAndChild(rewrittenOpt.get().first, rewrittenOpt.get().second);
                } else if (!projects.equals(project.getProjects())) {
                    return project.withProjects(projects);
                } else {
                    return project;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    private class FilterRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalFilter().thenApply(ctx -> {
                LogicalFilter<Plan> filter = ctx.root;
                List<Expression> conjuncts = materializeNestedMapInputs(filter.getConjuncts());
                Optional<Pair<List<Expression>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(filter, conjuncts);
                if (rewrittenOpt.isPresent()) {
                    return filter.withConjunctsAndChild(
                            ImmutableSet.copyOf(rewrittenOpt.get().first),
                            rewrittenOpt.get().second);
                } else if (!ImmutableSet.copyOf(conjuncts).equals(filter.getConjuncts())) {
                    return filter.withConjuncts(ImmutableSet.copyOf(conjuncts));
                } else {
                    return filter;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    private class HavingRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalHaving().thenApply(ctx -> {
                LogicalHaving<Plan> having = ctx.root;
                List<Expression> conjuncts = materializeNestedMapInputs(having.getConjuncts());
                Optional<Pair<List<Expression>, LogicalProject<Plan>>>
                        rewrittenOpt = rewriteExpressions(having, conjuncts);
                if (rewrittenOpt.isPresent()) {
                    return having.withConjuncts(ImmutableSet.copyOf(rewrittenOpt.get().first))
                            .withChildren(rewrittenOpt.get().second);
                } else if (!ImmutableSet.copyOf(conjuncts).equals(having.getConjuncts())) {
                    return having.withConjuncts(ImmutableSet.copyOf(conjuncts));
                } else {
                    return having;
                }
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    private class AggregateRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalAggregate().thenApply(ctx -> {
                LogicalAggregate<Plan> aggregate = ctx.root;
                List<Expression> originalTargets = Lists.newArrayList();
                originalTargets.addAll(aggregate.getGroupByExpressions());
                originalTargets.addAll(aggregate.getOutputExpressions());
                List<Expression> targets = materializeNestedMapInputs(originalTargets);
                Optional<Pair<List<Expression>, LogicalProject<Plan>>> rewrittenOpt
                        = rewriteExpressions(aggregate, targets);
                Plan newChild = rewrittenOpt.isPresent()
                        ? rewrittenOpt.get().second : aggregate.child();
                List<Expression> newTargets = rewrittenOpt.isPresent()
                        ? rewrittenOpt.get().first : targets;
                if (!rewrittenOpt.isPresent() && newTargets.equals(originalTargets)) {
                    return aggregate;
                }
                // rewriteExpressions treats group-by expressions and outputs as one ordered list
                // so a common Map input is materialized only once. Restore the two original lists
                // after replacement.
                int groupBySize = aggregate.getGroupByExpressions().size();
                ImmutableList<Expression> newGroupBy = ImmutableList.copyOf(
                        newTargets.subList(0, groupBySize));
                ImmutableList.Builder<NamedExpression> newOutputBuilder
                        = ImmutableList.builderWithExpectedSize(aggregate.getOutputExpressions().size());
                for (int i = groupBySize; i < newTargets.size(); i++) {
                    newOutputBuilder.add((NamedExpression) newTargets.get(i));
                }
                return aggregate.withChildGroupByAndOutput(newGroupBy, newOutputBuilder.build(), newChild);
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    private class JoinRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalJoin().thenApply(ctx -> {
                LogicalJoin<Plan, Plan> join = ctx.root;
                int hashOtherConjunctsSize = join.getHashJoinConjuncts().size()
                        + join.getOtherJoinConjuncts().size();
                int totalConjunctsSize = hashOtherConjunctsSize + join.getMarkJoinConjuncts().size();
                List<Expression> allConjuncts = Lists.newArrayListWithExpectedSize(totalConjunctsSize);
                allConjuncts.addAll(join.getHashJoinConjuncts());
                allConjuncts.addAll(join.getOtherJoinConjuncts());
                allConjuncts.addAll(join.getMarkJoinConjuncts());
                List<Expression> originalAllConjuncts = ImmutableList.copyOf(allConjuncts);
                allConjuncts = materializeNestedMapInputs(allConjuncts);
                Optional<JoinRewriteResult> rewrittenOpt = rewriteJoinExpressions(join, allConjuncts);
                if (!rewrittenOpt.isPresent() && allConjuncts.equals(originalAllConjuncts)) {
                    return join;
                }

                Plan newLeftChild = rewrittenOpt.map(result -> result.left).orElse(join.left());
                Plan newRightChild = rewrittenOpt.map(result -> result.right).orElse(join.right());
                List<Expression> newAllConjuncts = rewrittenOpt
                        .map(result -> result.newConjuncts).orElse(allConjuncts);
                List<Expression> newHashOtherConjuncts = newAllConjuncts.subList(0, hashOtherConjunctsSize);
                List<Expression> newMarkJoinConjuncts = ImmutableList.copyOf(
                        newAllConjuncts.subList(hashOtherConjunctsSize, totalConjunctsSize));

                Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                        newLeftChild.getOutput(), newRightChild.getOutput(), newHashOtherConjuncts);
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
                        ImmutableList.of(newLeftChild, newRightChild),
                        join.getJoinReorderContext());
            }).toRule(RuleType.ADD_PROJECT_FOR_MAP_LAMBDA_INPUT);
        }
    }

    /**
     * Rewrite expressions owned by a single-child plan and install their materialization Projects.
     *
     * <p>It first materializes computed Map inputs and replaces them in {@code targets}. It then
     * materializes any {@link MapEntryArrayMap} still used more than once. These are separate
     * Project layers because the second expression can depend on a Map Slot created by the first.
     * The returned pair contains the rewritten targets and the top materialization Project.
     */
    private <T extends Expression> Optional<Pair<List<T>, LogicalProject<Plan>>> rewriteExpressions(
            LogicalPlan plan, Collection<T> targets) {
        // computed map materialized
        List<NamedExpression> mapInputAliases = tryGenMapInputAliases(targets);
        List<T> rewrittenTargets = replaceExpressions(targets, mapInputAliases);
        // MapEntryArrayMap merteialized
        List<NamedExpression> entryArrayAliases = tryGenSharedEntryArrayAliases(rewrittenTargets);
        if (mapInputAliases.isEmpty() && entryArrayAliases.isEmpty()) {
            return Optional.empty();
        }

        Plan child = plan.child(0);
        if (!mapInputAliases.isEmpty()) {
            child = appendProject(child, mapInputAliases);
        }
        if (!entryArrayAliases.isEmpty()) {
            child = appendProject(child, entryArrayAliases);
            rewrittenTargets = replaceExpressions(rewrittenTargets, entryArrayAliases);
        }

        return Optional.of(Pair.of(rewrittenTargets, (LogicalProject<Plan>) child));
    }

    /** Add aliases without hiding any output already produced by {@code child}. */
    private LogicalProject<Plan> appendProject(Plan child, List<NamedExpression> aliases) {
        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                .addAll(child.getOutput())
                .addAll(aliases)
                .build();
        return new LogicalProject<>(projects, child);
    }

    /** Replace each aliased expression by its Slot in all target expression trees. */
    private <T extends Expression> List<T> replaceExpressions(
            Collection<T> expressions, List<NamedExpression> aliases) {
        if (aliases.isEmpty()) {
            return ImmutableList.copyOf(expressions);
        }
        Map<Expression, Slot> replaceMap = Maps.newHashMap();
        for (NamedExpression alias : aliases) {
            replaceMap.put(alias.child(0), alias.toSlot());
        }
        ImmutableList.Builder<T> builder = ImmutableList.builderWithExpectedSize(expressions.size());
        for (T expression : expressions) {
            builder.add((T) ExpressionUtils.replace(expression, replaceMap));
        }
        return builder.build();
    }

    /**
     * Rewrite Join conjuncts using the same two materialization stages as
     * {@link #rewriteExpressions(LogicalPlan, Collection)}.
     *
     * <p>Unlike a single-child plan, each generated alias must be attached to the Join child that
     * contains all its input Slots. An expression referencing both children cannot be evaluated in
     * either child Project, so a deterministic expression is left unchanged and a volatile one is
     * rejected. Entry-array aliases are assigned after Map aliases because they may use new Slots.
     */
    private Optional<JoinRewriteResult> rewriteJoinExpressions(LogicalJoin<Plan, Plan> join,
            Collection<Expression> targets) {
        List<Expression> rewrittenTargets = ImmutableList.copyOf(targets);
        Plan left = join.left();
        Plan right = join.right();

        Map<Expression, Set<Slot>> mapInputSlots = Maps.newLinkedHashMap();
        for (Expression target : rewrittenTargets) {
            Set<Expression> mapInputs = Sets.newLinkedHashSet();
            collectMapInputs(target, mapInputs);
            for (Expression mapInput : mapInputs) {
                Set<Slot> inputSlots = mapInput.getInputSlots();
                mapInputSlots.computeIfAbsent(mapInput, ignored -> Sets.newLinkedHashSet())
                        .addAll(inputSlots.isEmpty() ? target.getInputSlots() : inputSlots);
            }
        }

        ImmutableList.Builder<NamedExpression> leftAliases = ImmutableList.builder();
        ImmutableList.Builder<NamedExpression> rightAliases = ImmutableList.builder();
        Map<Expression, Slot> replaceMap = Maps.newHashMap();
        Set<Slot> leftOutputSet = left.getOutputSet();
        Set<Slot> rightOutputSet = right.getOutputSet();
        for (Entry<Expression, Set<Slot>> entry : mapInputSlots.entrySet()) {
            Set<Slot> inputSlots = entry.getValue();
            Set<Slot> mapInputExpressionSlots = entry.getKey().getInputSlots();
            if (!mapInputExpressionSlots.isEmpty()
                    && !leftOutputSet.containsAll(inputSlots)
                    && !rightOutputSet.containsAll(inputSlots)) {
                // No child Project can reference Slots from both sides. Recalculation is safe for
                // a deterministic expression, but a volatile Map would no longer have one stable
                // value shared by map_keys and map_values.
                if (entry.getKey().containsVolatileExpression()) {
                    throw new AnalysisException(
                            "A computed Map input containing a volatile expression cannot "
                                    + "reference both sides of a join");
                }
                continue;
            }
            ExprId exprId = StatementScopeIdGenerator.newExprId();
            Alias alias = new Alias(
                    exprId, entry.getKey(), "$_map_input_" + exprId.asInt() + "_$");
            replaceMap.put(alias.child(0), alias.toSlot());
            if (!inputSlots.isEmpty() && rightOutputSet.containsAll(inputSlots)) {
                rightAliases.add(alias);
            } else {
                leftAliases.add(alias);
            }
        }
        if (!replaceMap.isEmpty()) {
            List<NamedExpression> leftAliasList = leftAliases.build();
            List<NamedExpression> rightAliasList = rightAliases.build();
            left = appendProjectIfNeeded(left, leftAliasList);
            right = appendProjectIfNeeded(right, rightAliasList);
            rewrittenTargets = replaceExpressions(rewrittenTargets,
                    ImmutableList.<NamedExpression>builder()
                            .addAll(leftAliasList)
                            .addAll(rightAliasList)
                            .build());
        }

        List<NamedExpression> entryArrayAliases = tryGenSharedEntryArrayAliases(rewrittenTargets);
        ImmutableList.Builder<NamedExpression> leftEntryAliases = ImmutableList.builder();
        ImmutableList.Builder<NamedExpression> rightEntryAliases = ImmutableList.builder();
        leftOutputSet = left.getOutputSet();
        rightOutputSet = right.getOutputSet();
        for (NamedExpression alias : entryArrayAliases) {
            Expression entryArray = alias.child(0);
            Set<Slot> inputSlots = Sets.newLinkedHashSet(entryArray.getInputSlots());
            if (inputSlots.isEmpty()) {
                // As with a slot-free Map, inherit the containing conjunct's scope only to choose
                // a child. The expression itself remains valid on either side.
                for (Expression target : rewrittenTargets) {
                    if (target.anyMatch(entryArray::equals)) {
                        inputSlots.addAll(target.getInputSlots());
                    }
                }
            }
            Set<Slot> expressionSlots = entryArray.getInputSlots();
            if (!expressionSlots.isEmpty()
                    && !leftOutputSet.containsAll(inputSlots)
                    && !rightOutputSet.containsAll(inputSlots)) {
                if (entryArray.containsVolatileExpression()) {
                    throw new AnalysisException(
                            "A shared Map entry array containing a volatile expression cannot "
                                    + "reference both sides of a join");
                }
                continue;
            }
            if (!inputSlots.isEmpty() && rightOutputSet.containsAll(inputSlots)) {
                rightEntryAliases.add(alias);
            } else {
                leftEntryAliases.add(alias);
            }
        }
        List<NamedExpression> leftEntryAliasList = leftEntryAliases.build();
        List<NamedExpression> rightEntryAliasList = rightEntryAliases.build();
        if (!leftEntryAliasList.isEmpty() || !rightEntryAliasList.isEmpty()) {
            left = appendProjectIfNeeded(left, leftEntryAliasList);
            right = appendProjectIfNeeded(right, rightEntryAliasList);
            rewrittenTargets = replaceExpressions(rewrittenTargets,
                    ImmutableList.<NamedExpression>builder()
                            .addAll(leftEntryAliasList)
                            .addAll(rightEntryAliasList)
                            .build());
        }

        if (replaceMap.isEmpty() && leftEntryAliasList.isEmpty() && rightEntryAliasList.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new JoinRewriteResult(rewrittenTargets, left, right));
    }

    /** Avoid creating an identity Project when one side of a Join has no aliases. */
    private Plan appendProjectIfNeeded(Plan child, List<NamedExpression> aliases) {
        if (aliases.isEmpty()) {
            return child;
        }
        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                .addAll(child.getOutput())
                .addAll(aliases)
                .build();
        return new LogicalProject<>(projects, child);
    }

    /**
     * Find and alias each distinct computed Map consumed by a {@link MapEntryArrayMap}.
     *
     * <p>This method turns the expressions found by {@link #collectMapInputs(Expression, Set)} into
     * aliases. Slots and Map literals are excluded because they need no materialization.
     */
    private List<NamedExpression> tryGenMapInputAliases(
            Collection<? extends Expression> targets) {
        Set<Expression> mapInputs = Sets.newLinkedHashSet();
        for (Expression target : targets) {
            collectMapInputs(target, mapInputs);
        }

        ImmutableList.Builder<NamedExpression> aliases
                = ImmutableList.builderWithExpectedSize(mapInputs.size());
        for (Expression mapInput : mapInputs) {
            ExprId exprId = StatementScopeIdGenerator.newExprId();
            aliases.add(new Alias(exprId, mapInput, "$_map_input_" + exprId.asInt() + "_$"));
        }
        return aliases.build();
    }

    /**
     * Find repeated {@link MapEntryArrayMap} expressions and create one shared alias for each.
     *
     * <p>This is used by the current safe lowering of {@code map_apply}; the implementation does
     * not use the optional fast lowering into two independent two-parameter ArrayMaps. The original
     * two-parameter lambda is evaluated first and produces {@code ARRAY&lt;STRUCT&gt;}:
     *
     * <pre>
     * mappedEntries = MapEntryArrayMap(
     *   (mapKey, mapValue) -> struct(newKey, newValue),
     *   map_keys(inputMap), map_values(inputMap))
     * map_from_arrays(
     *   array_map(mappedEntry -> mappedEntry[1], mappedEntries),
     *   array_map(mappedEntry -> mappedEntry[2], mappedEntries))
     * </pre>
     *
     * <p>The two extraction ArrayMaps have one parameter because they iterate the resulting Struct
     * array, not the original Map. They do not copy or reevaluate the original lambda body.
     */
    private List<NamedExpression> tryGenSharedEntryArrayAliases(
            Collection<? extends Expression> targets) {
        Map<Expression, Integer> entryArrayCounts = Maps.newLinkedHashMap();
        for (Expression target : targets) {
            collectEntryArrayCounts(target, entryArrayCounts);
        }

        ImmutableList.Builder<NamedExpression> aliases = ImmutableList.builder();
        for (Entry<Expression, Integer> entry : entryArrayCounts.entrySet()) {
            if (entry.getValue() > 1) {
                ExprId exprId = StatementScopeIdGenerator.newExprId();
                aliases.add(new Alias(
                        exprId, entry.getKey(), "$_map_entries_" + exprId.asInt() + "_$"));
            }
        }
        return aliases.build();
    }

    /** Apply nested-lambda materialization independently to every target expression. */
    private <T extends Expression> List<T> materializeNestedMapInputs(Collection<T> expressions) {
        ImmutableList.Builder<T> builder = ImmutableList.builderWithExpectedSize(expressions.size());
        for (T expression : expressions) {
            builder.add((T) materializeNestedMapInputs(expression));
        }
        return builder.build();
    }

    /**
     * Materialize computed Maps that depend on lambda item Slots inside the owning ArrayMap.
     *
     * <p>Consider:
     *
     * <pre>
     * select transform_values(
     *   (outer_k, outer_v) -> transform_values((inner_k, inner_v) -> inner_k, map(outer_k + random(), outer_v)),
     *   map(1, 10));
     * </pre>
     *
     * A relation Project cannot evaluate {@code map(outer_k + random(), outer_v)} because {@code outer_k} and
     * {@code outer_v} exist only while the outer lambda is running. The outer ArrayMap is rewritten to
     * carry a hidden array whose item is that Map:
     *
     * <pre>
     * outer inputs before:
     *   outer_k <- map_keys(outerMap)
     *   outer_v <- map_values(outerMap)
     *
     * outer inputs after:
     *   outer_k <- map_keys(outerMap)
     *   outer_v <- map_values(outerMap)
     *   materializedInnerMap
     *      - array_map((outerKey, outerValue) -> map(outerKey + random(), outerValue),
     *                   map_keys(outerMap), map_values(outerMap))
     *
     * outer body after:
     *   transform_values((innerKey, innerValue) -> innerKey, materializedInnerMap)
     * </pre>
     *
     * <p>Traversal is bottom-up. For each ArrayMap, computed Maps in its body become hidden input
     * arrays; repeated entry arrays are handled afterward because they may use those hidden inputs.
     */
    private Expression materializeNestedMapInputs(Expression expression) {
        ImmutableList.Builder<Expression> children
                = ImmutableList.builderWithExpectedSize(expression.arity());
        boolean changed = false;
        for (Expression child : expression.children()) {
            Expression rewrittenChild = materializeNestedMapInputs(child);
            children.add(rewrittenChild);
            changed |= rewrittenChild != child;
        }
        Expression rewritten = changed ? expression.withChildren(children.build()) : expression;
        if (!(rewritten instanceof ArrayMap)) {
            return rewritten;
        }

        Lambda lambda = (Lambda) rewritten.child(0);
        Set<Expression> mapInputs = Sets.newLinkedHashSet();
        collectMapInputs(lambda.getLambdaFunction(), mapInputs);

        List<ArrayItemReference> sourceArguments = lambda.getLambdaArguments();
        List<String> argumentNames = Lists.newArrayList(lambda.getLambdaArgumentNames());
        List<ArrayItemReference> arguments = Lists.newArrayList(sourceArguments);
        Expression lambdaBody = lambda.getLambdaFunction();
        for (Expression mapInput : mapInputs) {
            Pair<ArrayMap, String> materialized = buildLambdaMaterializer(mapInput, sourceArguments);
            ArrayItemReference hiddenArgument = new ArrayItemReference(materialized.second, materialized.first);
            argumentNames.add(materialized.second);
            arguments.add(hiddenArgument);
            Map<Expression, Slot> replaceMap = Maps.newHashMap();
            replaceMap.put(mapInput, hiddenArgument.toSlot());
            lambdaBody = ExpressionUtils.replace(lambdaBody, replaceMap);
        }

        List<NamedExpression> entryArrayAliases = tryGenSharedEntryArrayAliases(
                ImmutableList.of(lambdaBody));
        for (NamedExpression entryArrayAlias : entryArrayAliases) {
            Expression entryArray = entryArrayAlias.child(0);
            Pair<ArrayMap, String> materialized = buildLambdaMaterializer(entryArray, arguments);
            ArrayItemReference hiddenArgument = new ArrayItemReference(materialized.second, materialized.first);
            argumentNames.add(materialized.second);
            arguments.add(hiddenArgument);
            Map<Expression, Slot> replaceMap = Maps.newHashMap();
            replaceMap.put(entryArray, hiddenArgument.toSlot());
            lambdaBody = ExpressionUtils.replace(lambdaBody, replaceMap);
        }
        if (arguments.size() == sourceArguments.size()) {
            return rewritten;
        }

        // A shared entry-array materializer can embed an earlier Map materializer. In that case the
        // final body references only the entry-array argument. Keep all user arguments, but remove
        // optimizer-added arguments no longer referenced by the final body to avoid evaluating the
        // embedded Map expression a second time.
        Set<ExprId> referencedArgumentIds = collectReferencedArgumentIds(lambdaBody);
        ImmutableList.Builder<String> retainedNames = ImmutableList.builder();
        ImmutableList.Builder<ArrayItemReference> retainedArguments = ImmutableList.builder();
        for (int i = 0; i < arguments.size(); i++) {
            ArrayItemReference argument = arguments.get(i);
            if (i < sourceArguments.size()
                    || referencedArgumentIds.contains(argument.getExprId())) {
                retainedNames.add(argumentNames.get(i));
                retainedArguments.add(argument);
            }
        }
        return rewritten.withChildren(ImmutableList.of(
                new Lambda(retainedNames.build(), lambdaBody, retainedArguments.build())));
    }

    /**
     * Build an ArrayMap that evaluates {@code expression} once per entry of the enclosing lambda.
     *
     * <p>Only enclosing arguments referenced by the expression are forwarded. For
     * {@code map(ok + random(), ov)}, the generated lambda receives copies of {@code ok} and
     * {@code ov}, with fresh ExprIds, and its body is rebound to those copies. If the expression
     * only captures relation Slots, one enclosing array is still forwarded as a row-count and
     * offset driver; all arrays of one ArrayMap have identical entry offsets.
     *
     * @return the materializing ArrayMap and the name of the hidden item argument that will expose
     *         each materialized result to the original lambda body
     */
    private Pair<ArrayMap, String> buildLambdaMaterializer(
            Expression expression, List<ArrayItemReference> sourceArguments) {
        Set<ExprId> referencedArgumentIds = collectReferencedArgumentIds(expression);

        List<ArrayItemReference> selectedArguments = sourceArguments.stream()
                .filter(argument -> referencedArgumentIds.contains(argument.getExprId()))
                .collect(ImmutableList.toImmutableList());
        if (selectedArguments.isEmpty()) {
            // ArrayMap needs an array to define the entry count even when the expression only
            // captures relation slots. Any current lambda input has the same entry offsets.
            selectedArguments = ImmutableList.of(sourceArguments.get(0));
        }

        Map<Expression, Slot> replaceMap = Maps.newHashMap();
        ImmutableList.Builder<String> materializerNames
                = ImmutableList.builderWithExpectedSize(selectedArguments.size());
        ImmutableList.Builder<ArrayItemReference> materializerArguments
                = ImmutableList.builderWithExpectedSize(selectedArguments.size());
        for (ArrayItemReference sourceArgument : selectedArguments) {
            ExprId exprId = StatementScopeIdGenerator.newExprId();
            String name = "$_map_materialize_arg_" + exprId.asInt() + "_$";
            ArrayItemReference materializerArgument = new ArrayItemReference(
                    exprId, name, sourceArgument.getArrayExpression());
            materializerNames.add(name);
            materializerArguments.add(materializerArgument);
            replaceMap.put(sourceArgument.toSlot(), materializerArgument.toSlot());
        }

        Expression materializerBody = ExpressionUtils.replace(expression, replaceMap);
        Lambda materializerLambda = new Lambda(
                materializerNames.build(), materializerBody, materializerArguments.build());
        ExprId hiddenExprId = StatementScopeIdGenerator.newExprId();
        String hiddenName = "$_map_input_" + hiddenExprId.asInt() + "_$";
        return Pair.of(new ArrayMap(materializerLambda), hiddenName);
    }

    /** Return ExprIds of lambda item Slots referenced by an expression. */
    private Set<ExprId> collectReferencedArgumentIds(Expression expression) {
        Set<ExprId> referencedArgumentIds = Sets.newHashSet();
        expression.foreach(node -> {
            if (node instanceof ArrayItemSlot) {
                referencedArgumentIds.add(((ArrayItemSlot) node).getExprId());
            }
        });
        return referencedArgumentIds;
    }

    /** Traverse an expression and collect the Map input of every {@link MapEntryArrayMap} marker. */
    private void collectMapInputs(Expression expression, Set<Expression> mapInputs) {
        MapEntryArrayMap marker = unwrapMarker(expression);
        if (marker != null) {
            Lambda lambda = (Lambda) marker.child(0);
            addMapInput(MapLambdaValidator.extractMapExpression("map lambda", lambda), mapInputs);
            return;
        }

        if (expression instanceof Lambda) {
            for (ArrayItemReference argument : ((Lambda) expression).getLambdaArguments()) {
                collectMapInputs(argument.getArrayExpression(), mapInputs);
            }
            return;
        }
        for (Expression child : expression.children()) {
            collectMapInputs(child, mapInputs);
        }
    }

    /** Add only Maps whose key/value expansion would otherwise repeat computation. */
    private void addMapInput(Expression mapInput, Set<Expression> mapInputs) {
        if (MapLambdaValidator.requiresSingleEvaluation(mapInput)) {
            mapInputs.add(mapInput);
        }
    }

    /**
     * Count each complete {@link MapEntryArrayMap} expression for
     * {@link #tryGenSharedEntryArrayAliases(Collection)}. As in {@code collectMapInputs}, only
     * lambda argument arrays are traversed across a Lambda boundary.
     */
    private void collectEntryArrayCounts(Expression expression, Map<Expression, Integer> counts) {
        if (unwrapMarker(expression) != null) {
            counts.merge(expression, 1, Integer::sum);
            return;
        }
        if (expression instanceof Lambda) {
            for (ArrayItemReference argument : ((Lambda) expression).getLambdaArguments()) {
                collectEntryArrayCounts(argument.getArrayExpression(), counts);
            }
            return;
        }
        for (Expression child : expression.children()) {
            collectEntryArrayCounts(child, counts);
        }
    }

    /** Find the Map entry marker through analyzer-inserted Cast wrappers. */
    private MapEntryArrayMap unwrapMarker(Expression expression) {
        while (expression instanceof Cast) {
            expression = expression.child(0);
        }
        return expression instanceof MapEntryArrayMap ? (MapEntryArrayMap) expression : null;
    }

    private static class JoinRewriteResult {
        private final List<Expression> newConjuncts;
        private final Plan left;
        private final Plan right;

        private JoinRewriteResult(List<Expression> newConjuncts, Plan left, Plan right) {
            this.newConjuncts = newConjuncts;
            this.left = left;
            this.right = right;
        }
    }
}
