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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.stats.FilterEstimation.EstimationContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.statistics.ColumnStat;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * Calculate selectivity of expression that produces boolean value.
 * TODO: Should consider the distribution of data.
 */
public class FilterEstimation extends ExpressionVisitor<StatsDeriveResult, EstimationContext> {

    private static final double DEFAULT_SELECTIVITY = 0.1;

    private static final double DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY = 1.0 / 3.0;

    private static final double DEFAULT_EQUALITY_COMPARISON_SELECTIVITY = 0.1;

    private final StatsDeriveResult stats;

    public FilterEstimation(StatsDeriveResult stats) {
        Preconditions.checkNotNull(stats);
        this.stats = stats;
    }

    /**
     * This method will update the stats according to the selectivity.
     */
    public StatsDeriveResult estimate(Expression expression) {
        // For a comparison predicate, only when it's left side is a slot and right side is a literal, we would
        // consider is a valid predicate.
        StatsDeriveResult stats = calculate(expression);
        double expectedRowCount = stats.getRowCount();
        for (ColumnStat columnStat : stats.getSlotToColumnStats().values()) {
            if (columnStat.getNdv() > expectedRowCount) {
                columnStat.setNdv(expectedRowCount);
            }
        }
        return stats;
    }

    private StatsDeriveResult calculate(Expression expression) {
        return expression.accept(this, null);
    }

    @Override
    public StatsDeriveResult visit(Expression expr, EstimationContext context) {
        return new StatsDeriveResult(stats).updateRowCountBySelectivity(DEFAULT_SELECTIVITY);
    }

    @Override
    public StatsDeriveResult visitCompoundPredicate(CompoundPredicate predicate, EstimationContext context) {
        Expression leftExpr = predicate.child(0);
        Expression rightExpr = predicate.child(1);
        StatsDeriveResult leftStats = leftExpr.accept(this, null);
        if (predicate instanceof And) {
            return rightExpr.accept(new FilterEstimation(leftStats), null);
        } else if (predicate instanceof Or) {
            StatsDeriveResult rightStats = rightExpr.accept(this, null);
            StatsDeriveResult andStats = rightExpr.accept(new FilterEstimation(leftStats), null);
            double rowCount = leftStats.getRowCount() + rightStats.getRowCount() - andStats.getRowCount();
            StatsDeriveResult orStats = new StatsDeriveResult(stats).setRowCount(rowCount);
            for (Map.Entry<Slot, ColumnStat> entry : leftStats.getSlotToColumnStats().entrySet()) {
                Slot keySlot = entry.getKey();
                ColumnStat leftColStats = entry.getValue();
                ColumnStat rightColStats = rightStats.getColumnStatsBySlot(keySlot);
                ColumnStat estimatedColStats = new ColumnStat(leftColStats);
                estimatedColStats.setMinValue(Math.min(leftColStats.getMinValue(), rightColStats.getMinValue()));
                estimatedColStats
                        .setMaxSizeByte(Math.max(leftColStats.getMaxSizeByte(), rightColStats.getMaxSizeByte()));
                orStats.addColumnStats(keySlot, estimatedColStats);
            }
            return orStats;
        }
        throw new RuntimeException(String.format("Unexpected predicate type: %s", predicate.toSql()));
    }

    @Override
    public StatsDeriveResult visitComparisonPredicate(ComparisonPredicate cp, EstimationContext context) {
        Expression left = cp.left();
        Expression right = cp.right();
        ColumnStat statsForLeft = ExpressionEstimation.estimate(left, stats);
        ColumnStat statsForRight = ExpressionEstimation.estimate(right, stats);

        double selectivity;
        if (!(left instanceof Literal) && !(right instanceof Literal)) {
            selectivity = calculateWhenBothChildIsColumn(cp, statsForLeft, statsForRight);
        } else {
            // For literal, it's max min is same value.
            selectivity = calculateWhenRightChildIsLiteral(cp, statsForLeft, statsForRight.getMaxValue());
        }
        return new StatsDeriveResult(stats).updateRowCountOnCopy(selectivity);
    }

    private double calculateWhenRightChildIsLiteral(ComparisonPredicate cp,
            ColumnStat statsForLeft, double val) {
        double ndv = statsForLeft.getNdv();
        double max = statsForLeft.getMaxValue();
        double min = statsForLeft.getMinValue();
        if (cp instanceof EqualTo) {
            if (val > max || val < min) {
                return 0.0;
            }
            return 1.0 / ndv;
        }
        if (cp instanceof LessThan) {
            if (val <= min) {
                return 0.0;
            }
            if (val > max) {
                return 1.0;
            }
            if (val == max) {
                return 1.0 - 1.0 / ndv;
            }
            return (val - min) / (max - min);
        }
        if (cp instanceof LessThanEqual) {
            if (val < min) {
                return 0.0;
            }
            if (val == min) {
                return 1.0 / ndv;
            }
            if (val >= max) {
                return 1.0;
            }
            return (val - min) / (max - min);
        }
        if (cp instanceof GreaterThan) {
            if (val >= max) {
                return 0.0;
            }
            if (val == min) {
                return 1.0 - 1.0 / ndv;
            }
            if (val < min) {
                return 1.0;
            }
            return (max - val) / (max - min);
        }
        if (cp instanceof GreaterThanEqual) {
            if (val > max) {
                return 0.0;
            }
            if (val == max) {
                return 1.0 / ndv;
            }
            if (val <= min) {
                return 1.0;
            }
            return (max - val) / (max - min);
        }
        throw new RuntimeException(String.format("Unexpected expression : %s", cp.toSql()));
    }

    private double calculateWhenBothChildIsColumn(ComparisonPredicate cp,
            ColumnStat statsForLeft, ColumnStat statsForRight) {
        double leftMin = statsForLeft.getMinValue();
        double rightMin = statsForRight.getMinValue();
        double leftMax = statsForLeft.getMaxValue();
        double rightMax = statsForRight.getMaxValue();
        if (cp instanceof EqualTo) {
            if (!statsForLeft.hasIntersect(statsForRight)) {
                return 0.0;
            }
            return DEFAULT_EQUALITY_COMPARISON_SELECTIVITY;
        }
        if (cp instanceof GreaterThan) {
            if (leftMax <= rightMin) {
                return 0.0;
            } else if (leftMin >= rightMax) {
                return 1.0;
            } else {
                return DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY;
            }
        }
        if (cp instanceof GreaterThanEqual) {
            if (leftMax < rightMin) {
                return 0.0;
            } else if (leftMin > rightMax) {
                return 1.0;
            } else {
                return DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY;
            }
        }
        if (cp instanceof LessThan) {
            if (leftMin >= rightMax) {
                return 0.0;
            } else if (leftMax <= rightMin) {
                return 1.0;
            } else {
                return DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY;
            }
        }
        if (cp instanceof LessThanEqual) {
            if (leftMin > rightMax) {
                return 0.0;
            } else if (leftMax < rightMin) {
                return 1.0;
            } else {
                return DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY;
            }
        }
        throw new RuntimeException(String.format("Unexpected expression : %s", cp.toSql()));
    }

    @Override
    public StatsDeriveResult visitInPredicate(InPredicate inPredicate, EstimationContext context) {
        boolean isNotIn = context != null && context.isNot;
        Expression compareExpr = inPredicate.getCompareExpr();
        ColumnStat compareExprStats = ExpressionEstimation.estimate(compareExpr, stats);
        if (ColumnStat.isInvalid(compareExprStats)) {
            return stats;
        }
        List<Expression> options = inPredicate.getOptions();
        double maxOption = 0;
        double minOption = Double.MAX_VALUE;
        double optionDistinctCount = 0;
        for (Expression option : options) {
            ColumnStat optionStats = ExpressionEstimation.estimate(option, stats);
            if (ColumnStat.isInvalid(optionStats)) {
                return stats;
            }
            optionDistinctCount += optionStats.getNdv();
            maxOption = Math.max(optionStats.getMaxValue(), maxOption);
            minOption = Math.min(optionStats.getMinValue(), minOption);
        }
        double selectivity = DEFAULT_SELECTIVITY;
        double cmpExprMax = compareExprStats.getMaxValue();
        double cmpExprMin = compareExprStats.getMinValue();
        boolean hasOverlap = Math.max(cmpExprMin, minOption) <= Math.min(cmpExprMax, maxOption);
        if (!hasOverlap) {
            selectivity = 0.0;
        }
        double cmpDistinctCount = compareExprStats.getNdv();
        selectivity = Math.min(1.0, optionDistinctCount / cmpDistinctCount);
        double expectedMax = Math.min(cmpExprMax, maxOption);
        double expectedMin = Math.max(cmpExprMin, minOption);
        compareExprStats.setMaxValue(expectedMax);
        compareExprStats.setMinValue(expectedMin);
        StatsDeriveResult estimated = new StatsDeriveResult(stats);
        if (compareExpr instanceof SlotReference && !isNotIn) {
            estimated.addColumnStats((SlotReference) compareExpr, compareExprStats);
        }
        return estimated.updateRowCountOnCopy(isNotIn ? 1.0 - selectivity : selectivity);
    }

    @Override
    public StatsDeriveResult visitNot(Not not, EstimationContext none) {
        Preconditions.checkState(!(not.child() instanceof Not),
                "Consecutive Not statement should be merged previously");
        EstimationContext context = new EstimationContext();
        context.isNot = true;
        return not.child().accept(this, context);
    }

    static class EstimationContext {
        private boolean isNot;
    }

}
