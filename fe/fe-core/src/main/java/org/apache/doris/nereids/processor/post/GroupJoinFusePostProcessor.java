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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGroupJoin;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.doris.nereids.util.ExpressionUtils;

/**
 * Post-Process processor that fuses PhysicalHashAggregate(PhysicalHashJoin)
 * into PhysicalGroupJoin.
 *
 * <p>This fusion runs AFTER CBO has chosen the optimal plan and join reorder is complete,
 * but BEFORE RuntimeFilter generation.</p>
 */
public class GroupJoinFusePostProcessor extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, CascadesContext ctx) {
        agg = (PhysicalHashAggregate<? extends Plan>) super.visit(agg, ctx);
        if (!canFuse(agg, ctx)) {
            return agg;
        }
        PhysicalHashJoin<? extends Plan, ? extends Plan> join =
                (PhysicalHashJoin<? extends Plan, ? extends Plan>) agg.child();

        PhysicalGroupJoin<Plan, Plan> groupJoin = new PhysicalGroupJoin<>(
                join.getJoinType(),
                join.getHashJoinConjuncts(),
                join.getOtherJoinConjuncts(),
                join.getDistributeHint(),
                agg.getGroupByExpressions(),
                agg.getOutputExpressions(),
                agg.getPartitionExpressions().orElse(ImmutableList.of()),
                agg.getLogicalProperties(),
                join.child(0),
                join.child(1));

        Statistics newStats = deriveGroupJoinStats(groupJoin, agg, join);
        if (newStats != null) {
            groupJoin = groupJoin.withPhysicalPropertiesAndStats(
                    groupJoin.getPhysicalProperties(), newStats);
        }
        groupJoin = (PhysicalGroupJoin<Plan, Plan>) groupJoin.copyStatsAndGroupIdFrom(
                (AbstractPhysicalPlan) agg);
        groupJoin = groupJoin.resetLogicalProperties();
        return groupJoin;
    }

    private boolean canFuse(PhysicalHashAggregate<? extends Plan> agg, CascadesContext ctx) {
        if (!ctx.getConnectContext().getSessionVariable().enableGroupJoin) {
            return false;
        }
        Plan child = agg.child();
        if (!(child instanceof PhysicalHashJoin)) {
            return false;
        }
        PhysicalHashJoin<? extends Plan, ? extends Plan> join =
                (PhysicalHashJoin<? extends Plan, ? extends Plan>) child;
        JoinType joinType = join.getJoinType();
        if (joinType != JoinType.INNER_JOIN && joinType != JoinType.LEFT_OUTER_JOIN) {
            return false;
        }
        Set<Expression> joinEqKeys = new HashSet<>();
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            joinEqKeys.addAll(conjunct.getInputSlots());
        }
        for (Expression groupBy : agg.getGroupByExpressions()) {
            if (!joinEqKeys.containsAll(groupBy.getInputSlots())) {
                return false;
            }
        }
        for (NamedExpression expr : agg.getOutputExpressions()) {
            if (containsDistinctAggregation(expr)) {
                return false;
            }
            if (!isSupportedAggregateFunction(expr)) {
                return false;
            }
        }
        AggMode aggMode = agg.getAggMode();
        if (aggMode != AggMode.INPUT_TO_BUFFER) {
            return false;
        }
        return true;
    }

    private boolean containsDistinctAggregation(Expression expr) {
        return ExpressionUtils.collect(ImmutableList.of(expr),
                e -> e instanceof AggregateFunction).stream()
                .anyMatch(af -> ((AggregateFunction) af).isDistinct());
    }

    private boolean isSupportedAggregateFunction(Expression expr) {
        return ExpressionUtils.collect(ImmutableList.of(expr),
                e -> e instanceof AggregateFunction).stream().allMatch(af -> {
            String name = ((AggregateFunction) af).getName().toLowerCase();
            return name.equals("sum") || name.equals("count")
                    || name.equals("min") || name.equals("max")
                    || name.equals("avg");
        });
    }

    private Statistics deriveGroupJoinStats(PhysicalGroupJoin<Plan, Plan> groupJoin,
                                            PhysicalHashAggregate<? extends Plan> agg,
                                            PhysicalHashJoin<? extends Plan, ? extends Plan> join) {
        Statistics buildStats = join.child(1).getStats();
        if (buildStats == null) {
            return null;
        }
        double estimatedGroupCount = buildStats.getRowCount();
        for (Expression groupBy : agg.getGroupByExpressions()) {
            if (groupBy instanceof SlotReference) {
                SlotReference slot = (SlotReference) groupBy;
                ColumnStatistic colStats = buildStats.columnStatistics().get(slot);
                if (colStats != null && colStats.ndv > 0) {
                    estimatedGroupCount = Math.min(estimatedGroupCount, colStats.ndv);
                }
            }
        }
        double outputRowCount;
        Statistics probeStats = join.child(0).getStats();
        if (join.getJoinType() == JoinType.LEFT_OUTER_JOIN && probeStats != null) {
            outputRowCount = probeStats.getRowCount();
        } else if (probeStats != null) {
            double matchRate = estimatedGroupCount / Math.max(1.0, probeStats.getRowCount());
            matchRate = Math.min(1.0, matchRate);
            outputRowCount = probeStats.getRowCount() * matchRate;
        } else {
            outputRowCount = estimatedGroupCount;
        }
        return new Statistics(outputRowCount, buildStats.columnStatistics());
    }
}
