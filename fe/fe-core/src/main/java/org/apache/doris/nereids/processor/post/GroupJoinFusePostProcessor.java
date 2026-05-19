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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGroupJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        Optional<FusionContext> fusionContext = tryCreateFusionContext(agg, ctx);
        if (!fusionContext.isPresent()) {
            return agg;
        }
        FusionContext context = fusionContext.get();
        PhysicalHashJoin<? extends Plan, ? extends Plan> join = context.join;

        PhysicalGroupJoin<Plan, Plan> groupJoin = new PhysicalGroupJoin<>(
                join.getJoinType(),
                join.getHashJoinConjuncts(),
                join.getOtherJoinConjuncts(),
                join.getDistributeHint(),
                context.groupByExpressions,
                context.outputExpressions,
                context.partitionExpressions,
                agg.getLogicalProperties(),
                context.probeChild,
                context.buildChild);

        Statistics newStats = deriveGroupJoinStats(groupJoin);
        if (newStats != null) {
            groupJoin = groupJoin.withPhysicalPropertiesAndStats(
                    groupJoin.getPhysicalProperties(), newStats);
        }
        groupJoin = (PhysicalGroupJoin<Plan, Plan>) groupJoin.copyStatsAndGroupIdFrom(
                (AbstractPhysicalPlan) agg);
        groupJoin = groupJoin.resetLogicalProperties();
        return groupJoin;
    }

    private Optional<FusionContext> tryCreateFusionContext(
            PhysicalHashAggregate<? extends Plan> agg, CascadesContext ctx) {
        if (!ctx.getConnectContext().getSessionVariable().enableGroupJoin) {
            return Optional.empty();
        }
        Plan child = agg.child();
        Map<Slot, Expression> projectReplaceMap = Collections.emptyMap();
        if (child instanceof PhysicalProject) {
            PhysicalProject<? extends Plan> project = (PhysicalProject<? extends Plan>) child;
            projectReplaceMap = ExpressionUtils.generateReplaceMap(project.getProjects());
            child = project.child();
        }
        if (!(child instanceof PhysicalHashJoin)) {
            return Optional.empty();
        }
        PhysicalHashJoin<? extends Plan, ? extends Plan> join =
                (PhysicalHashJoin<? extends Plan, ? extends Plan>) child;
        JoinType joinType = join.getJoinType();
        if (joinType != JoinType.INNER_JOIN && joinType != JoinType.LEFT_OUTER_JOIN) {
            return Optional.empty();
        }
        List<Expression> groupByExpressions = ExpressionUtils.replace(
                agg.getGroupByExpressions(), projectReplaceMap);
        List<NamedExpression> outputExpressions = ExpressionUtils.replaceNamedExpressions(
                agg.getOutputExpressions(), projectReplaceMap);
        List<Expression> partitionExpressions = ExpressionUtils.replace(
                agg.getPartitionExpressions().orElse(ImmutableList.of()), projectReplaceMap);
        for (Expression groupBy : groupByExpressions) {
            if (!join.getOutputSet().containsAll(groupBy.getInputSlots())) {
                return Optional.empty();
            }
        }
        for (NamedExpression expr : outputExpressions) {
            if (containsDistinctAggregation(expr)) {
                return Optional.empty();
            }
            if (!isSupportedAggregateFunction(expr)) {
                return Optional.empty();
            }
        }
        return chooseChildren(join, outputExpressions)
                .map(children -> new FusionContext(join, groupByExpressions, outputExpressions,
                        partitionExpressions, children.probeChild, children.buildChild));
    }

    private Optional<GroupJoinChildren> chooseChildren(
            PhysicalHashJoin<? extends Plan, ? extends Plan> join, List<NamedExpression> outputExpressions) {
        Set<Slot> aggregateInputSlots = collectAggregateInputSlots(outputExpressions);
        if (aggregateInputSlots.isEmpty()) {
            return Optional.of(new GroupJoinChildren(join.child(0), join.child(1)));
        }
        boolean fromLeft = join.left().getOutputSet().containsAll(aggregateInputSlots);
        boolean fromRight = join.right().getOutputSet().containsAll(aggregateInputSlots);
        if (fromRight) {
            return Optional.of(new GroupJoinChildren(join.child(0), join.child(1)));
        }
        if (fromLeft && join.getJoinType() == JoinType.INNER_JOIN) {
            return Optional.of(new GroupJoinChildren(join.child(1), join.child(0)));
        }
        return Optional.empty();
    }

    private Set<Slot> collectAggregateInputSlots(List<NamedExpression> outputExpressions) {
        Set<Slot> inputSlots = new HashSet<>();
        for (NamedExpression expr : outputExpressions) {
            ExpressionUtils.collect(ImmutableList.of(expr),
                    e -> e instanceof AggregateFunction)
                    .forEach(agg -> inputSlots.addAll(((Expression) agg).getInputSlots()));
        }
        return inputSlots;
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

    private Statistics deriveGroupJoinStats(PhysicalGroupJoin<Plan, Plan> groupJoin) {
        Statistics buildStats = groupJoin.right().getStats();
        if (buildStats == null) {
            return null;
        }
        double estimatedGroupCount = buildStats.getRowCount();
        for (Expression groupBy : groupJoin.getGroupByExpressions()) {
            if (groupBy instanceof SlotReference) {
                SlotReference slot = (SlotReference) groupBy;
                ColumnStatistic colStats = buildStats.columnStatistics().get(slot);
                if (colStats != null && colStats.ndv > 0) {
                    estimatedGroupCount = Math.min(estimatedGroupCount, colStats.ndv);
                }
            }
        }
        double outputRowCount;
        Statistics probeStats = groupJoin.left().getStats();
        if (groupJoin.getJoinType() == JoinType.LEFT_OUTER_JOIN && probeStats != null) {
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

    private static class FusionContext {
        private final PhysicalHashJoin<? extends Plan, ? extends Plan> join;
        private final List<Expression> groupByExpressions;
        private final List<NamedExpression> outputExpressions;
        private final List<Expression> partitionExpressions;
        private final Plan probeChild;
        private final Plan buildChild;

        private FusionContext(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
                List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
                List<Expression> partitionExpressions, Plan probeChild, Plan buildChild) {
            this.join = join;
            this.groupByExpressions = groupByExpressions;
            this.outputExpressions = outputExpressions;
            this.partitionExpressions = partitionExpressions;
            this.probeChild = probeChild;
            this.buildChild = buildChild;
        }
    }

    private static class GroupJoinChildren {
        private final Plan probeChild;
        private final Plan buildChild;

        private GroupJoinChildren(Plan probeChild, Plan buildChild) {
            this.probeChild = probeChild;
            this.buildChild = buildChild;
        }
    }
}
