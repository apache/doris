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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SumAggWriter
 */
public class SumAggWriter extends DefaultPlanRewriter<SumAggContext> {
    private static final double LOWER_AGGREGATE_EFFECT_COEFFICIENT = 10000;
    private static final double LOW_AGGREGATE_EFFECT_COEFFICIENT = 1000;
    private static final double MEDIUM_AGGREGATE_EFFECT_COEFFICIENT = 100;
    private final StatsDerive derive = new StatsDerive(true);

    @Override
    public Plan visit(Plan plan, SumAggContext context) {
        return plan;
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, SumAggContext context) {
        if (project.getProjects().stream().allMatch(proj -> proj instanceof SlotReference
                || (proj instanceof Alias && proj.child(0) instanceof SlotReference))) {
            List<SlotReference> slotToPush = new ArrayList<>();
            for (SlotReference slot : context.ifThenSlots) {
                slotToPush.add((SlotReference) project.pushDownExpressionPastProject(slot));
            }
            List<SlotReference> groupBySlots = new ArrayList<>();
            for (SlotReference slot : context.groupKeys) {
                groupBySlots.add((SlotReference) project.pushDownExpressionPastProject(slot));
            }
            SumAggContext contextForChild = new SumAggContext(
                    context.aliasToBePushDown,
                    context.ifConditions,
                    slotToPush,
                    groupBySlots);
            Plan child = project.child().accept(this, contextForChild);
            if (child != project.child()) {
                List<NamedExpression> newProjects = Lists.newArrayList();
                for (NamedExpression ne : project.getProjects()) {
                    newProjects.add((NamedExpression) replaceBySlots(ne, child.getOutput()));
                }
                return project.withProjects(newProjects).withChildren(child);
            }
        }
        return project;
    }

    private static Expression replaceBySlots(Expression expression, List<Slot> slots) {
        Map<Slot, Slot> replaceMap = new HashMap<>();
        for (Slot slot1 : expression.getInputSlots()) {
            for (Slot slot2 : slots) {
                if (slot1.getExprId().asInt() == slot2.getExprId().asInt()) {
                    replaceMap.put(slot1, slot2);
                }
            }
        }
        Expression result = ExpressionUtils.replace(expression, replaceMap);
        return result;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, SumAggContext context) {
        Set<Slot> leftOutput = join.left().getOutputSet();
        Set<SlotReference> conditionSlots = join.getConditionSlot().stream()
                .map(slot -> (SlotReference) slot).collect(Collectors.toSet());
        for (Slot slot : context.ifThenSlots) {
            if (conditionSlots.contains(slot)) {
                return join;
            }
        }
        Set<SlotReference> conditionSlotsFromLeft = Sets.newHashSet(conditionSlots);
        conditionSlotsFromLeft.retainAll(leftOutput);
        for (SlotReference slot : context.groupKeys) {
            if (leftOutput.contains(slot)) {
                conditionSlotsFromLeft.add(slot);
            }
        }
        if (leftOutput.containsAll(context.ifThenSlots)) {
            SumAggContext contextForChild = new SumAggContext(
                    context.aliasToBePushDown,
                    context.ifConditions,
                    context.ifThenSlots,
                    Lists.newArrayList(conditionSlotsFromLeft)
            );
            Plan left = join.left().accept(this, contextForChild);
            if (join.left() != left) {
                return join.withChildren(left, join.right());
            }
        }
        return join;
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, SumAggContext context) {
        if (!union.getOutputSet().containsAll(context.ifThenSlots)) {
            return union;
        }
        if (!union.getConstantExprsList().isEmpty()) {
            return union;
        }

        if (!union.getOutputs().stream().allMatch(e -> e instanceof SlotReference)) {
            return union;
        }
        List<Plan> newChildren = Lists.newArrayList();

        boolean changed = false;
        for (int i = 0; i < union.children().size(); i++) {
            Plan child = union.children().get(i);
            List<SlotReference> ifThenSlotsForChild = new ArrayList<>();
            // List<SlotReference> groupByForChild = new ArrayList<>();
            for (SlotReference slot : context.ifThenSlots) {
                Expression pushed = union.pushDownExpressionPastSetOperator(slot, i);
                if (pushed instanceof SlotReference) {
                    ifThenSlotsForChild.add((SlotReference) pushed);
                } else {
                    return union;
                }
            }
            int childIdx = i;
            SumAggContext contextForChild = new SumAggContext(
                    context.aliasToBePushDown,
                    context.ifConditions,
                    ifThenSlotsForChild,
                    context.groupKeys.stream().map(slot
                            -> (SlotReference) union.pushDownExpressionPastSetOperator(slot, childIdx))
                            .collect(Collectors.toList())
                    );
            Plan newChild = child.accept(this, contextForChild);
            if (newChild != child) {
                changed = true;
            }
            newChildren.add(newChild);
        }
        if (changed) {
            List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayList();
            for (int i = 0; i < newChildren.size(); i++) {
                List<SlotReference> childOutput = new ArrayList<>();
                for (SlotReference slot : union.getRegularChildOutput(i)) {
                    for (Slot c : newChildren.get(i).getOutput()) {
                        if (slot.equals(c)) {
                            childOutput.add((SlotReference) c);
                            break;
                        }
                    }
                }
                newRegularChildrenOutputs.add(childOutput);
            }
            List<NamedExpression> newOutputs = new ArrayList<>();
            for (int i = 0; i < union.getOutput().size(); i++) {
                SlotReference originSlot = (SlotReference) union.getOutput().get(i);
                DataType dataType = newRegularChildrenOutputs.get(0).get(i).getDataType();
                newOutputs.add(originSlot.withNullableAndDataType(originSlot.nullable(), dataType));
            }
            return union.withChildrenAndOutputs(newChildren, newOutputs, newRegularChildrenOutputs);
        } else {
            return union;
        }
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, SumAggContext context) {
        return genAggregate(relation, context);
    }

    private Plan genAggregate(Plan child, SumAggContext context) {
        if (checkStats(child, context)) {
            List<NamedExpression> aggOutputExpressions = new ArrayList<>();
            for (SlotReference slot : context.ifThenSlots) {
                Alias alias = new Alias(slot.getExprId(), new Sum(slot));
                aggOutputExpressions.add(alias);
            }
            aggOutputExpressions.addAll(context.groupKeys);

            LogicalAggregate genAgg = new LogicalAggregate(context.groupKeys, aggOutputExpressions, child);
            return genAgg;
        } else {
            return child;
        }

    }

    private boolean checkStats(Plan plan, SumAggContext context) {
        if (ConnectContext.get() == null) {
            return false;
        }
        int mode = ConnectContext.get().getSessionVariable().eagerAggregationMode;
        if (mode < 0) {
            return false;
        }
        if (mode > 0) {
            return true;
        }
        Statistics stats = plan.getStats();
        if (stats == null) {
            stats = plan.accept(derive, new StatsDerive.DeriveContext());
        }
        if (stats.getRowCount() == 0) {
            return false;
        }

        List<ColumnStatistic> groupKeysStats = new ArrayList<>();

        List<ColumnStatistic> lower = Lists.newArrayList();
        List<ColumnStatistic> medium = Lists.newArrayList();
        List<ColumnStatistic> high = Lists.newArrayList();

        List<ColumnStatistic>[] cards = new List[] {lower, medium, high};

        for (NamedExpression key : context.groupKeys) {
            ColumnStatistic colStats = ExpressionEstimation.INSTANCE.estimate(key, stats);
            if (colStats.isUnKnown) {
                return false;
            }
            groupKeysStats.add(colStats);
            cards[groupByCardinality(colStats, stats.getRowCount())].add(colStats);
        }

        double lowerCartesian = 1.0;
        for (ColumnStatistic colStats : lower) {
            lowerCartesian = lowerCartesian * colStats.ndv;
        }

        // pow(row_count/20, a half of lower column size)
        double lowerUpper = Math.max(stats.getRowCount() / 20, 1);
        lowerUpper = Math.pow(lowerUpper, Math.max(lower.size() / 2, 1));

        if (high.isEmpty() && (lower.size() + medium.size()) == 1) {
            return true;
        }

        if (high.isEmpty() && medium.isEmpty()) {
            if (lower.size() == 1 && lowerCartesian * 20 <= stats.getRowCount()) {
                return true;
            } else if (lower.size() == 2 && lowerCartesian * 7 <= stats.getRowCount()) {
                return true;
            } else if (lower.size() <= 3 && lowerCartesian * 20 <= stats.getRowCount() && lowerCartesian < lowerUpper) {
                return true;
            } else {
                return false;
            }
        }

        if (high.size() >= 2 || medium.size() > 2 || (high.size() == 1 && !medium.isEmpty())) {
            return false;
        }

        // 3. Extremely low cardinality for lower with at most one medium or high.
        double lowerCartesianLowerBound =
                stats.getRowCount() / LOWER_AGGREGATE_EFFECT_COEFFICIENT;
        if (high.size() + medium.size() == 1 && lower.size() <= 2 && lowerCartesian <= lowerCartesianLowerBound) {
            StatsCalculator statsCalculator = new StatsCalculator(null);
            double estAggRowCount = statsCalculator.estimateGroupByRowCount(
                    context.groupKeys.stream().map(s -> (Expression) s).collect(Collectors.toList()),
                    stats);
            return estAggRowCount < lowerCartesianLowerBound;
        }

        return false;
    }

    // high(2): row_count / cardinality < MEDIUM_AGGREGATE_EFFECT_COEFFICIENT
    // medium(1): row_count / cardinality >= MEDIUM_AGGREGATE_EFFECT_COEFFICIENT and < LOW_AGGREGATE_EFFECT_COEFFICIENT
    // lower(0): row_count / cardinality >= LOW_AGGREGATE_EFFECT_COEFFICIENT
    private int groupByCardinality(ColumnStatistic colStats, double rowCount) {
        if (rowCount == 0 || colStats.ndv * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT > rowCount) {
            return 2;
        } else if (colStats.ndv * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT <= rowCount
                && colStats.ndv * LOW_AGGREGATE_EFFECT_COEFFICIENT > rowCount) {
            return 1;
        } else if (colStats.ndv * LOW_AGGREGATE_EFFECT_COEFFICIENT <= rowCount) {
            return 0;
        }
        return 2;
    }
}
