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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.grouping.GroupingSetsFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalGroupBy;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * normalize output, aggFunc and groupingFunc in grouping sets.
 * This rule is executed after NormalizeAggregate.
 *
 * eg: select sum(k2 + 1), grouping(k1) from t1 group by grouping sets ((k1));
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7],
 *   outputs:[sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, GROUPING_PREFIX_(k1#1)#7)
 * +-- Project(projections: k1#1, (k2#2 + 1) as (k2 + 1)#8), grouping_prefix(k1#1)#7
 *     +-- GroupingSets(
 *         keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *         outputs:sum(k2#2 + 1) as `sum(k2 + 1)`#3, group(grouping_prefix(k1#1)#7) as `grouping(k1 + 1)`#4
 *
 * After rule:
 * Aggregate(
 *   keys:[k1#1, SR#9]
 *   outputs:[sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, GROUPING_PREFIX_(k1#1)#7))
 *     +-- Project(k1#1, (K2 + 1)#10 as `(k2 + 1)`#8, grouping_id()#0, grouping_prefix(k1#1)#7)
 *         +-- GropingSets(
 *             keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *             outputs:k1#1, (k2 + 1)#10, grouping_prefix(k1#1)#7
 */
public class NormalizeGroupBy extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalProject(logicalGroupBy().whenNot(LogicalGroupBy::isNormalized)))
                .then(agg -> {
                    LogicalProject<LogicalGroupBy<GroupPlan>> project = agg.child();
                    LogicalGroupBy<GroupPlan> groupBy = project.child();

                    // substitution map used to substitute expression in groupBy's output to use it as top projections
                    final Map<Expression, Expression> substitutionMap = Maps.newHashMap();
                    // substitution map used to substitute expression in aggreate's groupBy
                    final Map<Expression, Expression> projectMap = Maps.newHashMap();
                    List<Expression> keys = groupBy.getGroupByExpressions();
                    List<NamedExpression> newOutputs = Lists.newArrayList();

                    // keys
                    Map<Boolean, List<Expression>> partitionedKeys = keys.stream()
                            .collect(Collectors.groupingBy(SlotReference.class::isInstance));
                    List<Expression> newKeys = Lists.newArrayList();
                    Set<NamedExpression> bottomProjections = new LinkedHashSet<>();

                    if (partitionedKeys.containsKey(true)) {
                        // process SlotReference keys
                        partitionedKeys.get(true).stream()
                                .filter(e -> !(e instanceof VirtualSlotReference))
                                .map(SlotReference.class::cast)
                                .peek(s -> substitutionMap.put(s, s))
                                .peek(bottomProjections::add)
                                .forEach(newKeys::add);
                    }
                    if (partitionedKeys.containsKey(false)) {
                        // process non-SlotReference keys
                        newKeys.addAll(partitionedKeys.get(false).stream()
                                .map(e -> new Alias(e, e.toSql()))
                                .peek(a -> substitutionMap.put(a.child(), a.toSlot()))
                                .peek(bottomProjections::add)
                                .map(Alias::toSlot)
                                .collect(Collectors.toList()));
                    }

                    project.getProjects().forEach(p -> {
                        if (p instanceof SlotReference) {
                            if (!(p instanceof VirtualSlotReference)) {
                                projectMap.put(p, p);
                            }
                        } else {
                            projectMap.put(((Alias) p).child(), p.toSlot());
                        }
                    });

                    substitutionMap.entrySet().stream()
                            .filter(kv -> groupBy.getOutputExpressions().stream()
                                    .anyMatch(e -> e.anyMatch(kv.getKey()::equals)))
                            .map(Entry::getValue)
                            .map(NamedExpression.class::cast)
                            .forEach(newOutputs::add);

                    // if we generate bottom, we need to generate to project too.
                    // output
                    List<NamedExpression> outputs = groupBy.getOutputExpressions();
                    Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                            .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)
                                    || e.anyMatch(GroupingSetsFunction.class::isInstance)));

                    boolean needBottomProjects = partitionedKeys.containsKey(false);
                    if (partitionedOutputs.containsKey(true)) {
                        // process expressions that contain groupBy function
                        Set<GroupingSetsFunction> groupingSetsFunctions = partitionedOutputs.get(true).stream()
                                .flatMap(e -> e.<Set<GroupingSetsFunction>>collect(
                                        GroupingSetsFunction.class::isInstance).stream())
                                .collect(Collectors.toSet());

                        // replace all non-slot expression in groupBy functions children.
                        for (GroupingSetsFunction groupingSetsFunction : groupingSetsFunctions) {
                            for (Expression child : groupingSetsFunction.getArguments()) {
                                List<Expression> innerChildren = Lists.newArrayList();
                                for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                                    if (realChild instanceof SlotReference || realChild instanceof Literal) {
                                        innerChildren.add(realChild);
                                        if (realChild instanceof SlotReference) {
                                            bottomProjections.add((SlotReference) realChild);
                                        }
                                    } else {
                                        needBottomProjects = true;
                                        innerChildren.add(substitutionMap.get(realChild));
                                    }
                                }
                                VirtualSlotReference newVirtual =
                                        new VirtualSlotReference(((VirtualSlotReference) child).getExprId(),
                                                ((VirtualSlotReference) child).getName(),
                                        child.getDataType(), child.nullable(),
                                                ((VirtualSlotReference) child).getQualifier(),
                                                innerChildren, ((VirtualSlotReference) child).isHasCast());
                                substitutionMap.put(child, newVirtual);
                                newOutputs.add(newVirtual);
                            }
                        }

                        // process expressions that contain agg function
                        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                                        AggregateFunction.class::isInstance).stream())
                                .collect(Collectors.toSet());

                        // replace all non-slot expression in agg functions children.
                        for (AggregateFunction aggregateFunction : aggregateFunctions) {
                            List<Expression> newChildren = Lists.newArrayList();
                            for (Expression child : aggregateFunction.getArguments()) {
                                if (child instanceof SlotReference || child instanceof Literal) {
                                    newChildren.add(child);
                                    if (child instanceof SlotReference) {
                                        bottomProjections.add((SlotReference) child);
                                        newOutputs.add((NamedExpression) child);
                                    }
                                } else {
                                    needBottomProjects = true;
                                    Alias alias = new Alias(child, child.toSql());
                                    bottomProjections.add(alias);
                                    newChildren.add(alias.toSlot());
                                    substitutionMap.put(child, alias.toSlot());
                                    newOutputs.add(alias.toSlot());
                                }
                            }
                        }
                    }

                    List<NamedExpression> projects = project.getProjects();
                    // assemble
                    // 1.Ensure that the columns are in order,
                    //   first slotRefrance and then virtualSlotRefrance
                    List<NamedExpression> newProjects = Utils.reorderProjections(projects);
                    List<NamedExpression> newBottomProjections =
                            Utils.reorderProjections(new ArrayList<>(bottomProjections));
                    List<NamedExpression> finalOutputs = Utils.reorderProjections(newOutputs);

                    LogicalPlan root = groupBy.child();
                    if (needBottomProjects) {
                        root = new LogicalProject<>(new ArrayList<>(newBottomProjections), root);
                    }

                    // 2. replace the alisa column
                    //    include: grouping Sets/project/aggregate
                    // replace grouping Sets output and groupBy
                    List<Expression> newVirtualSlot =
                            groupBy.getVirtualGroupingExprs().stream()
                                            .map(e -> replaceVirtualSlot(e, substitutionMap))
                                                    .collect(Collectors.toList());
                    root = groupBy.replaceWithChild(groupBy.getGroupingSets(), newKeys,
                            finalOutputs, groupBy.getGroupingIdList(),
                            groupBy.getVirtualSlotRefs(), newVirtualSlot,
                            groupBy.getGroupingList(), groupBy.isResolved(), groupBy.hasChangedOutput(),
                            true, root);

                    // replace project
                    List<NamedExpression> replacedProjects = newProjects.stream()
                            .map(e -> replaceVirtualSlot(e, projectMap))
                            .map(NamedExpression.class::cast)
                            .collect(Collectors.toList());
                    List<NamedExpression> finalProjects = replacedProjects.stream()
                            .map(e -> ExpressionUtils.replace(e, substitutionMap))
                            .map(NamedExpression.class::cast)
                            .collect(Collectors.toList());
                    root = new LogicalProject<>(finalProjects, root);

                    // replace agg groupBy
                    List<Expression> newAggGroupBy =
                            agg.getGroupByExpressions().stream()
                                            .map(e -> replaceVirtualSlot(e, projectMap))
                                                    .collect(Collectors.toList());
                    return new LogicalAggregate<>(newAggGroupBy, agg.getOutputExpressions(),
                            agg.isDisassembled(), agg.isNormalized(),
                            agg.isFinalPhase(), agg.getAggPhase(), root);
                }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }

    /**
     * Replace children in virtualSlotReference.
     */
    private Expression replaceVirtualSlot(
            Expression groupBy, Map<Expression, Expression> substitutionMap) {
        if (groupBy instanceof VirtualSlotReference) {
            if (!((VirtualSlotReference) groupBy).getRealSlots().isEmpty()) {
                List<Expression> newChildren = ((VirtualSlotReference) groupBy).getRealSlots().stream()
                        .map(child -> {
                            if (substitutionMap.containsKey(child)) {
                                return substitutionMap.get(child);
                            } else {
                                return child;
                            }
                        })
                        .collect(Collectors.toList());
                return new VirtualSlotReference(((VirtualSlotReference) groupBy).getExprId(),
                        ((VirtualSlotReference) groupBy).getName(), groupBy.getDataType(),
                        groupBy.nullable(), ((VirtualSlotReference) groupBy).getQualifier(),
                        newChildren, ((VirtualSlotReference) groupBy).isHasCast());
            }
        }
        return groupBy;
    }
}
