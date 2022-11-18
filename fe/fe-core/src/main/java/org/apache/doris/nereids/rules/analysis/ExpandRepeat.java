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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.GroupingSetShape;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * eg: select sum(k2 + 1), grouping(k1) from t1 group by grouping sets ((k1));
 * Original Plan:
 *     +-- GroupingSets(
 *         keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *         outputs:sum(k2#2 + 1) as `sum(k2 + 1)`#3, group(grouping_prefix(k1#1)#7) as `grouping(k1 + 1)`#4
 *
 * After:
 * Project(sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, grouping(GROUPING_PREFIX_(k1#1)#7)) as `grouping(k1)`#10)
 *   +-- Aggregate(
 *          keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *          outputs:[(K2 + 1)#8), grouping_prefix(k1#1)#7]
 *         +-- GropingSets(
 *             keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *             outputs:k1#1, (k2 + 1)#8, grouping_id()#0, grouping_prefix(k1#1)#7
 *             +-- Project(k1#1, (K2#2 + 1) as `(k2 + 1)`#8)
 */
public class ExpandRepeat extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalRepeat().whenNot(LogicalRepeat::isExpand).thenApply(ctx -> {
            LogicalRepeat<GroupPlan> repeat = ctx.root;
            List<List<Expression>> groupingSets = repeat.getGroupingSets();
            List<Expression> groupByExpressions = repeat.getGroupByExpressions();
            List<NamedExpression> output = repeat.getOutputExpressions();
            List<GroupingSetShape> groupingSetShapes = repeat.getGroupingSetShapes();

            return buildNewAggWithNormalizeRepeat(
                    groupByExpressions, output, groupingSets, groupingSetShapes, repeat);
        }).toRule(RuleType.EXPAND_REPEAT);
    }

    private LogicalPlan buildNewAggWithNormalizeRepeat(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<List<Expression>> groupingSets,
            List<GroupingSetShape> groupingSetShapes,
            LogicalRepeat<GroupPlan> repeat) {
        Set<AggregateFunction> aggregateFunctions = collectAggregateFunctions(outputExpressions);
        Set<GroupingScalarFunction> groupingFunctions = collectGroupingFunctions(outputExpressions);

        // 1. generate new substituteMap
        // substitution map used to substitute expression in repeat's output to use it as top projections
        Map<Expression, Expression> substitutionMap =
                buildSubstitutionMap(groupByExpressions, aggregateFunctions, groupingFunctions);

        // 3. generate new groupByExpression
        List<Expression> newGroupByExpressions = generateNewGroupByExpressions(
                groupByExpressions, substitutionMap);

        // 4. generate new agg outputs
        List<NamedExpression> aggOutputs =
                buildAggOutputs(groupByExpressions, outputExpressions,
                        substitutionMap, aggregateFunctions, groupingFunctions);

        // 5. generate new repeat outputs
        Set<NamedExpression> repeatOutputs = buildRepeatOutputs(groupByExpressions,
                substitutionMap, aggregateFunctions, groupingFunctions);

        // 6. build bottom Projections
        List<NamedExpression> bottomProjects = buildBottomProjections(
                groupByExpressions, aggregateFunctions, substitutionMap);

        // 7. update substituteMap for aggFunc
        Map<Expression, Expression> updatedSubstitutionMap =
                updateSubstitutionMap(substitutionMap, aggregateFunctions);

        // assemble
        List<NamedExpression> newRepeatOutputs =
                reorderProjections(new ArrayList<>(repeatOutputs));
        List<NamedExpression> newAggOutputs =
                reorderProjections(aggOutputs);
        LogicalAggregate<GroupPlan> agg = new LogicalAggregate(newGroupByExpressions, newAggOutputs,
                false, true, true, AggPhase.LOCAL,
                repeat.replaceWithChild(groupingSets, newGroupByExpressions, newRepeatOutputs, groupingSetShapes,
                        true, new LogicalProject<>(bottomProjects, repeat.child())));
        List<NamedExpression> projections = outputExpressions.stream()
                .map(e -> ExpressionUtils.replace(e, updatedSubstitutionMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        return new LogicalProject<>(projections, agg);
    }

    private Set<AggregateFunction> collectAggregateFunctions(List<NamedExpression> outputs) {
        Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)));
        return partitionedOutputs.containsKey(true)
                ? partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet())
                : Sets.newHashSet();
    }

    /**
     * get groupingFunc from outputExpressions
     */
    private Set<GroupingScalarFunction> collectGroupingFunctions(List<NamedExpression> outputs) {
        Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                .collect(Collectors.groupingBy(e -> e.anyMatch(GroupingScalarFunction.class::isInstance)));
        return partitionedOutputs.containsKey(true)
                ? partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<GroupingScalarFunction>>collect(
                        GroupingScalarFunction.class::isInstance).stream())
                .collect(Collectors.toSet())
                : Sets.newHashSet();
    }

    /**
     * generate new groupByExpressions.
     */
    private List<Expression> generateNewGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        Map<Boolean, List<Expression>> whetherIsVirtualSlots = groupByExpressions.stream()
                .collect(Collectors.groupingBy(VirtualSlotReference.class::isInstance));
        List<Expression> newGroupByExpressions = new ImmutableList.Builder<Expression>()
                .addAll(buildNewGroupByWithNonVirtualSlot(whetherIsVirtualSlots, substitutionMap))
                .addAll(buildVirtualSlotForNewGroupBy(whetherIsVirtualSlots, substitutionMap))
                .build();

        return newGroupByExpressions;
    }

    private List<Expression> buildNewGroupByWithNonVirtualSlot(
            Map<Boolean, List<Expression>> whetherIsVirtualSlots,
            Map<Expression, Expression> substitutionMap) {
        List<Expression> newGroupByExpressions = new ArrayList<>();
        if (whetherIsVirtualSlots.containsKey(false)) {
            for (Expression groupByExpression : whetherIsVirtualSlots.get(false)) {
                if (groupByExpression instanceof SlotReference) {
                    newGroupByExpressions.add(groupByExpression);
                } else {
                    newGroupByExpressions.add(substitutionMap.get(groupByExpression));
                }
            }
        }
        return newGroupByExpressions;
    }

    private List<Expression> buildVirtualSlotForNewGroupBy(
            Map<Boolean, List<Expression>> whetherIsVirtualSlots,
            Map<Expression, Expression> substitutionMap) {
        List<Expression> newGroupByExpressions = new ArrayList<>();
        for (Expression virtualSLot : whetherIsVirtualSlots.get(true)) {
            if (((VirtualSlotReference) virtualSLot).getRealSlots().isEmpty()) {
                newGroupByExpressions.add(virtualSLot);
            } else {
                newGroupByExpressions.add((substitutionMap.get(virtualSLot)));
            }
        }
        return newGroupByExpressions;
    }

    private Map<Expression, Expression> buildSubstitutionMap(
            List<Expression> groupByExpressions,
            Set<AggregateFunction> aggregateFunctions,
            Set<GroupingScalarFunction> groupingFunctions) {
        Map<Expression, Expression> nonVirtualSlotMap = buildSubstitutionMapByNonVirtualSlot(groupByExpressions);
        Map<Expression, Expression> virtualSlotMap = buildSubstitutionMapByGroupingFunc(
                groupingFunctions, nonVirtualSlotMap);
        Map<Expression, Expression> aggFuncMap =
                buildSubstitutionMapByAggFunc(aggregateFunctions, nonVirtualSlotMap, virtualSlotMap);
        return new ImmutableMap.Builder<Expression, Expression>()
                .putAll(nonVirtualSlotMap)
                .putAll(virtualSlotMap)
                .putAll(aggFuncMap)
                .build();
    }

    private Map<Expression, Expression> buildSubstitutionMapByNonVirtualSlot(
            List<Expression> groupByExpressions) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        // build with nonVirtual slot
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof SlotReference) {
                // skip groupingFunction
                if (groupByExpression instanceof VirtualSlotReference
                        && !((VirtualSlotReference) groupByExpression).getRealSlots().isEmpty()) {
                    continue;
                }
                substitutionMap.put(groupByExpression, groupByExpression);
            } else {
                Alias alias = new Alias(groupByExpression, groupByExpression.toSql());
                substitutionMap.put(groupByExpression, alias.toSlot());
                substitutionMap.put(alias.toSlot(), alias);
            }
        }
        return substitutionMap;
    }

    private Map<Expression, Expression> buildSubstitutionMapByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> nonVirtualSlotMap,
            Map<Expression, Expression> virtualSlotMap) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (!(child instanceof SlotReference || child instanceof Literal)) {
                    if (nonVirtualSlotMap.containsKey(child)) {
                        newChildren.add(((Alias) nonVirtualSlotMap.get(child)).toSlot());
                    } else if (virtualSlotMap.containsKey(child)) {
                        newChildren.add(((Alias) virtualSlotMap.get(child)).toSlot());
                    } else {
                        Alias alias = new Alias(child, child.toSql());
                        newChildren.add(alias.toSlot());
                        substitutionMap.put(child, alias);
                    }
                } else {
                    newChildren.add(child);
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            Alias alias = new Alias(newFunction, newFunction.toSql());
            substitutionMap.put(aggregateFunction, alias.toSlot());
            substitutionMap.put(newFunction, alias);
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    /**
     * Generate a new virtualSlotReference for each groupingFunc by
     * replacing non-slotReference internal expressions with alisa.
     *
     * eg:
     *      old: GROUPING_PREFIX_k1(k1#0 + 1)
     *      new: GROUPING_PREFIX_k1((k1 + 1)#2)
     * @return List(Pair(old, new))
     */
    private Map<Expression, Expression> buildSubstitutionMapByGroupingFunc(
            Set<GroupingScalarFunction> groupingSetsFunctions,
            Map<Expression, Expression> nonVirtualSlotMap) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        for (GroupingScalarFunction groupingSetsFunction : groupingSetsFunctions) {
            List<Expression> outerChildren = Lists.newArrayList();
            for (Expression child : groupingSetsFunction.getArguments()) {
                List<Expression> innerChildren = Lists.newArrayList();
                for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                    if (realChild instanceof SlotReference || realChild instanceof Literal) {
                        innerChildren.add(realChild);
                    } else {
                        if (!nonVirtualSlotMap.containsKey(realChild)) {
                            Alias alias = new Alias(realChild, realChild.toSql());
                            innerChildren.add(alias.toSlot());
                            substitutionMap.put(realChild, alias);
                        } else {
                            innerChildren.add(nonVirtualSlotMap.get(realChild));
                        }
                    }
                }
                VirtualSlotReference newVirtual =
                        new VirtualSlotReference(((VirtualSlotReference) child).getExprId(),
                                ((VirtualSlotReference) child).getName(),
                                child.getDataType(), child.nullable(),
                                ((VirtualSlotReference) child).getQualifier(),
                                innerChildren, ((VirtualSlotReference) child).hasCast());
                substitutionMap.put(child, newVirtual);
                outerChildren.add(newVirtual);
            }
            GroupingScalarFunction newFunction =
                    (GroupingScalarFunction) groupingSetsFunction.withChildren(outerChildren);
            substitutionMap.put(groupingSetsFunction, newFunction);
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    /**
     * build AggOutputs order by outputExpressions.
     * Use oldOutputsToNewOutputs to represent the mapping from the original output to the new output.
     */
    private List<NamedExpression> buildAggOutputs(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Map<Expression, Expression> substitutionMap,
            Set<AggregateFunction> aggregateFunctions,
            Set<GroupingScalarFunction> groupingScalarFunctions) {
        Map<Expression, NamedExpression> oldOutputsToNewOutputs =
                new ImmutableMap.Builder<Expression, NamedExpression>()
                        // 1. Extract slotReference and additionally generated dummy columns in GroupByExpressions
                        .putAll(buildAggOutputsByGroupByExpressions(
                                groupByExpressions, outputExpressions, substitutionMap))
                        // 2. Generate new Outputs in GroupingFunction.
                        .putAll(buildAggOutputsByGroupingFunc(
                                groupingScalarFunctions, substitutionMap))
                        // 3. Generate new Outputs in AggFunction.
                        .putAll(buildAggOutputsByAggFunc(aggregateFunctions, substitutionMap))
                        .build();
        List<NamedExpression> newOutputs = new ArrayList<>();
        for (NamedExpression expression : outputExpressions) {
            if (expression instanceof Alias) {
                if (expression.anyMatch(AggregateFunction.class::isInstance)
                        || expression.anyMatch(GroupingScalarFunction.class::isInstance)) {
                    newOutputs.add(oldOutputsToNewOutputs.get(((Alias) expression).child()));
                } else {
                    Expression oldOutput = ((Alias) expression).child();
                    if (oldOutputsToNewOutputs.containsKey(oldOutput)) {
                        newOutputs.add(oldOutputsToNewOutputs.get(oldOutput).toSlot());
                    } else {
                        newOutputs.addAll(((ImmutableSet) oldOutput.collect(Slot.class::isInstance)).asList());
                    }
                }
            }
            if (expression instanceof SlotReference) {
                newOutputs.add(oldOutputsToNewOutputs.get(expression));
            }
        }
        return ImmutableList.copyOf(newOutputs);
    }

    private Map<Expression, NamedExpression> buildAggOutputsByGroupByExpressions(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputs,
            Map<Expression, Expression> substitutionMap) {
        Map<Expression, NamedExpression> newOutputs = new HashMap<>();
        groupByExpressions.stream().filter(k -> !(k instanceof VirtualSlotReference))
                .filter(e -> isInOutputExpressions(outputs, e))
                .forEach(e -> newOutputs.put(e,
                        (NamedExpression) substitutionMap.get(e)));
        return newOutputs;
    }

    private Map<Expression, NamedExpression> buildAggOutputsByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        Map<Expression, NamedExpression> newOutputs = new HashMap<>();
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    newChildren.add(child);
                } else {
                    newChildren.add(((Alias) substitutionMap.get(child)).toSlot());
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            newOutputs.put(aggregateFunction, (Alias) substitutionMap.get(newFunction));
        }
        return newOutputs;
    }

    private Map<Expression, NamedExpression> buildAggOutputsByGroupingFunc(
            Set<GroupingScalarFunction> groupingScalarFunctions,
            Map<Expression, Expression> substitutionMap) {
        Map<Expression, NamedExpression> newOutputs = new HashMap<>();
        for (GroupingScalarFunction groupingScalarFunction : groupingScalarFunctions) {
            List<Expression> outerChildren = Lists.newArrayList();
            for (Expression child : groupingScalarFunction.getArguments()) {
                outerChildren.add(substitutionMap.get(child));
            }
            newOutputs.put(groupingScalarFunction, (VirtualSlotReference) outerChildren.get(0));
        }
        return newOutputs;
    }

    private Set<NamedExpression> buildRepeatOutputs(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap,
            Set<AggregateFunction> aggregateFunctions,
            Set<GroupingScalarFunction> groupingScalarFunctions) {
        return new ImmutableSet.Builder<NamedExpression>()
                // 1. generate in groupByExpressions
                .addAll(buildRepeatOutputsByGroupByExpressions(groupByExpressions, substitutionMap))
                // 2. generate in AggregateFunction
                .addAll(buildRepeatOutputsByAggFunc(aggregateFunctions, substitutionMap))
                // 3. generate in GroupingFunc
                .addAll(buildRepeatOutputsByGroupingFunc(groupingScalarFunctions, substitutionMap))
                .build();
    }

    private List<NamedExpression> buildRepeatOutputsByGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> repeatOutputs = new ArrayList<>();
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof SlotReference) {
                // skip VirtualSlotReference generate with GroupingFunc
                if (isGroupingFuncVirtualSlot(groupByExpression)) {
                    continue;
                }
                repeatOutputs.add((NamedExpression) substitutionMap.get(groupByExpression));
            } else {
                repeatOutputs.add(getAlias(groupByExpression, substitutionMap).toSlot());
            }
        }
        return ImmutableList.copyOf(repeatOutputs);
    }

    private NamedExpression getAlias(Expression expression, Map<Expression, Expression> substitutionMap) {
        return (NamedExpression) substitutionMap.get(substitutionMap.get(expression));
    }

    private boolean isGroupingFuncVirtualSlot(Expression groupByExpression) {
        return groupByExpression instanceof VirtualSlotReference
                && !((VirtualSlotReference) groupByExpression).getRealSlots().isEmpty();
    }

    private boolean isInOutputExpressions(
            List<NamedExpression> outputExpressions,
            Expression expression) {
        return outputExpressions.stream().anyMatch(e -> e.anyMatch(expression::equals));
    }

    private List<NamedExpression> buildRepeatOutputsByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> repeatOutputs = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    if (child instanceof SlotReference) {
                        repeatOutputs.add((SlotReference) child);
                    }
                } else {
                    repeatOutputs.add(((Alias) substitutionMap.get(child)).toSlot());
                }
            }
        }
        return ImmutableList.copyOf(repeatOutputs);
    }

    private List<NamedExpression> buildRepeatOutputsByGroupingFunc(
            Set<GroupingScalarFunction> groupingScalarFunctions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> repeatOutputs = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (GroupingScalarFunction groupingScalarFunction : groupingScalarFunctions) {
            for (Expression child : groupingScalarFunction.getArguments()) {
                // add virtualSlot
                repeatOutputs.add((VirtualSlotReference) substitutionMap.get(child));
            }
        }
        return ImmutableList.copyOf(repeatOutputs);
    }

    private List<NamedExpression> buildBottomProjections(
            List<Expression> groupByExpressions,
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        return new ImmutableList.Builder<NamedExpression>()
                // 1. generate in groupByExpressions
                .addAll(buildBottomProjectionsByGroupByExpressions(groupByExpressions, substitutionMap))
                // 2. generate in AggregateFunction
                .addAll(buildBottomProjectionsByAggFunc(aggregateFunctions, substitutionMap))
                .build();
    }

    List<NamedExpression> buildBottomProjectionsByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> bottomProjections = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    if (child instanceof SlotReference) {
                        bottomProjections.add((SlotReference) child);
                    }
                } else {
                    bottomProjections.add((Alias) substitutionMap.get(child));
                }
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    /**
     * generate bottom projections with groupByExpressions.
     * eg:
     * groupByExpressions: k1#0, k2#1 + 1;
     * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
     */
    private List<NamedExpression> buildBottomProjectionsByGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> bottomProjections = new ArrayList<>();
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof VirtualSlotReference) {
                continue;
            } else if (groupByExpression instanceof SlotReference) {
                bottomProjections.add((NamedExpression) groupByExpression);
            } else {
                bottomProjections.add(getAlias(groupByExpression, substitutionMap));
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    /**
     * Convert the alias corresponding to agg to the corresponding slot.
     */
    private Map<Expression, Expression> updateSubstitutionMap(
            Map<Expression, Expression> oldSubstitution,
            Set<AggregateFunction> aggregateFunctions) {
        Map<Expression, Expression> newSubstitution = new HashMap<>();
        oldSubstitution.entrySet().stream()
                .forEach(e -> {
                    if (aggregateFunctions.contains(e.getKey())) {
                        newSubstitution.put(e.getKey(), ((NamedExpression) e.getValue()).toSlot());
                    } else {
                        newSubstitution.put(e.getKey(), e.getValue());
                    }
                });
        return ImmutableMap.copyOf(newSubstitution);
    }

    /**
     * Rearrange the order of the projects to ensure that
     * slotReference is in the front and virtualSlotReference is in the back.
     */
    private List<NamedExpression> reorderProjections(List<NamedExpression> projections) {
        Map<Boolean, List<NamedExpression>> partitionProjections = projections.stream()
                .collect(Collectors.groupingBy(VirtualSlotReference.class::isInstance,
                        LinkedHashMap::new, Collectors.toList()));
        List<NamedExpression> newProjections = partitionProjections.containsKey(false)
                ? partitionProjections.get(false) : new ArrayList<NamedExpression>();
        if (partitionProjections.containsKey(true)) {
            newProjections.addAll(partitionProjections.get(true));
        }
        return newProjections;
    }
}
