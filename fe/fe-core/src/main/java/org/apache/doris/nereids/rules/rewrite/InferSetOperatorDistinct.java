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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Infer Distinct from SetOperator;
 * Example:
 * <pre>
 *                    Intersect
 *   Intersect ->        |
 *                  Agg for Distinct
 * </pre>
 */
public class InferSetOperatorDistinct extends OneRewriteRuleFactory {
    private static final double LOWER_AGGREGATE_EFFECT_COEFFICIENT = 10000;
    private static final double LOW_AGGREGATE_EFFECT_COEFFICIENT = 1000;
    private static final double MEDIUM_AGGREGATE_EFFECT_COEFFICIENT = 100;

    private final StatsDerive derive = new StatsDerive(false);

    @Override
    public Rule build() {
        return logicalSetOperation()
                .when(operation -> operation.getQualifier() == Qualifier.DISTINCT)
                .then(setOperation -> {
                    ImmutableList.Builder<Plan> newChildren =
                            ImmutableList.builderWithExpectedSize(setOperation.arity());
                    boolean hasNewChildren = false;
                    for (Plan child : setOperation.children()) {
                        if (shouldInferDistinct(child)) {
                            newChildren.add(new LogicalAggregate<>(
                                    ImmutableList.copyOf(child.getOutput()), true, child));
                            hasNewChildren = true;
                        } else {
                            newChildren.add(child);
                        }
                    }
                    if (!hasNewChildren) {
                        return null;
                    }
                    return setOperation.withChildren(newChildren.build());
                }).toRule(RuleType.INFER_SET_OPERATOR_DISTINCT);
    }

    private boolean shouldInferDistinct(Plan child) {
        return !isAgg(child) && rejectNLJ(child)
                && shouldGenerateAggregateByNdv(child, child.getOutput());
    }

    private boolean isAgg(Plan plan) {
        return plan instanceof LogicalAggregate || (plan instanceof LogicalProject && plan.child(
                0) instanceof LogicalAggregate);
    }

    // if children exist NLJ, we can't infer distinct
    // because NLJ could generate bitmap runtime filter. and it will execute failed when we do infer distinct.
    private boolean rejectNLJ(Plan plan) {
        if (plan instanceof LogicalProject) {
            plan = plan.child(0);
        }
        if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            return join.getOtherJoinConjuncts().isEmpty();
        }
        return true;
    }

    private boolean shouldGenerateAggregateByNdv(Plan plan, List<? extends NamedExpression> groupKeys) {
        Statistics stats = plan.getStats();
        if (stats == null) {
            stats = plan.accept(derive, new StatsDerive.DeriveContext());
            if (stats == null) {
                return false;
            }
        }
        if (stats.getRowCount() <= 0) {
            return false;
        }

        List<ColumnStatistic> lower = new ArrayList<>();
        List<ColumnStatistic> medium = new ArrayList<>();
        List<ColumnStatistic> high = new ArrayList<>();

        List<ColumnStatistic>[] cards = new List[] { lower, medium, high };

        for (NamedExpression key : groupKeys) {
            ColumnStatistic colStats = ExpressionEstimation.INSTANCE.estimate(key, stats);
            if (colStats.isUnKnown) {
                return false;
            }
            if (stats.getRowCount() * 0.9 <= colStats.ndv) {
                return false;
            }
            cards[groupByCardinality(colStats, stats.getRowCount())].add(colStats);
        }

        double lowerCartesian = 1.0;
        for (ColumnStatistic colStats : lower) {
            lowerCartesian = lowerCartesian * colStats.ndv;
        }

        // Same NDV heuristic as EagerAggRewriter#checkStats, but kept local because set-op
        // local distinct and eager aggregation have different optimization boundaries.
        double lowerUpper = Math.max(stats.getRowCount() / 20, 1);
        lowerUpper = Math.pow(lowerUpper, Math.max(lower.size() / 2, 1));

        if (high.isEmpty() && (lower.size() + medium.size()) <= 2) {
            return true;
        }

        if (high.isEmpty() && medium.isEmpty()) {
            if (lower.size() == 1 && lowerCartesian * 20 <= stats.getRowCount()) {
                return true;
            } else if (lower.size() == 2 && lowerCartesian * 7 <= stats.getRowCount()) {
                return true;
            } else if (lower.size() <= 3 && lowerCartesian * 20 <= stats.getRowCount()
                    && lowerCartesian < lowerUpper) {
                return true;
            } else {
                return false;
            }
        }

        if (high.size() >= 2 || medium.size() > 2 || (high.size() == 1 && !medium.isEmpty())) {
            return false;
        }

        double lowerCartesianLowerBound = stats.getRowCount() / LOWER_AGGREGATE_EFFECT_COEFFICIENT;
        if (high.size() + medium.size() == 1 && lower.size() <= 2
                && lowerCartesian <= lowerCartesianLowerBound) {
            return true;
        }

        return false;
    }

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
