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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.stats.FilterEstimation.EstimationContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.coercion.RangeScalable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.StatisticRange;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Calculate selectivity of expression that produces boolean value.
 * TODO: Should consider the distribution of data.
 */
public class FilterEstimation extends ExpressionVisitor<Statistics, EstimationContext> {
    public static final double DEFAULT_INEQUALITY_COEFFICIENT = 0.5;
    // "Range selectivity is prone to producing outliers, so we add this threshold limit.
    // The threshold estimation is calculated based on selecting one month out of fifty years."
    public static final double RANGE_SELECTIVITY_THRESHOLD = 0.0016;
    public static final double DEFAULT_IN_COEFFICIENT = 1.0 / 3.0;

    public static final double DEFAULT_LIKE_COMPARISON_SELECTIVITY = 0.2;
    public static final double DEFAULT_ISNULL_SELECTIVITY = 0.005;
    private Set<Slot> aggSlots;

    private boolean isOnBaseTable = false;

    public FilterEstimation() {
    }

    public FilterEstimation(Set<Slot> aggSlots) {
        this.aggSlots = aggSlots;
    }

    public FilterEstimation(boolean isOnBaseTable) {
        this.isOnBaseTable = isOnBaseTable;
    }

    /**
     * This method will update the stats according to the selectivity.
     */
    public Statistics estimate(Expression expression, Statistics inputStats) {
        Statistics outputStats = expression.accept(this, new EstimationContext(inputStats));
        if (outputStats.getRowCount() == 0 && inputStats.getDeltaRowCount() > 0) {
            StatisticsBuilder deltaStats = new StatisticsBuilder();
            deltaStats.setDeltaRowCount(0);
            deltaStats.setRowCount(inputStats.getDeltaRowCount());
            for (Expression expr : inputStats.columnStatistics().keySet()) {
                deltaStats.putColumnStatistics(expr, ColumnStatistic.UNKNOWN);
            }
            outputStats = expression.accept(this, new EstimationContext(deltaStats.build()));
        }
        outputStats.enforceValid();
        return outputStats;
    }

    @Override
    public Statistics visit(Expression expr, EstimationContext context) {
        return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
    }

    @Override
    public Statistics visitCompoundPredicate(CompoundPredicate predicate, EstimationContext context) {
        Expression leftExpr = predicate.child(0);
        Expression rightExpr = predicate.child(1);
        Statistics leftStats = leftExpr.accept(this, context);
        leftStats = leftStats.normalizeByRatio(context.statistics.getRowCount());
        Statistics andStats = rightExpr.accept(this,
                new EstimationContext(leftStats));
        if (predicate instanceof And) {
            return andStats;
        } else if (predicate instanceof Or) {
            Statistics rightStats = rightExpr.accept(this, context);
            double rowCount = leftStats.getRowCount() + rightStats.getRowCount() - andStats.getRowCount();
            Statistics orStats = context.statistics.withRowCount(rowCount);
            Set<Slot> leftInputSlots = leftExpr.getInputSlots();
            Set<Slot> rightInputSlots = rightExpr.getInputSlots();
            for (Slot slot : context.keyColumns) {
                if (leftInputSlots.contains(slot) && rightInputSlots.contains(slot)) {
                    ColumnStatistic leftColStats = leftStats.findColumnStatistics(slot);
                    ColumnStatistic rightColStats = rightStats.findColumnStatistics(slot);
                    StatisticRange leftRange = StatisticRange.from(leftColStats, slot.getDataType());
                    StatisticRange rightRange = StatisticRange.from(rightColStats, slot.getDataType());
                    StatisticRange union = leftRange.union(rightRange);
                    ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder(
                            context.statistics.findColumnStatistics(slot));
                    colBuilder.setMinValue(union.getLow()).setMinExpr(union.getLowExpr())
                            .setMaxValue(union.getHigh()).setMaxExpr(union.getHighExpr())
                            .setNdv(union.getDistinctValues());
                    double maxNumNulls = Math.max(leftColStats.numNulls, rightColStats.numNulls);
                    colBuilder.setNumNulls(Math.min(colBuilder.getCount(), maxNumNulls));
                    orStats.addColumnStats(slot, colBuilder.build());
                }
            }
            return orStats;
        }
        // should not come here
        Preconditions.checkArgument(false,
                "unsupported compound operator: %s in %s",
                predicate.getClass().getName(), predicate.toSql());
        return context.statistics;
    }

    @Override
    public Statistics visitComparisonPredicate(ComparisonPredicate cp, EstimationContext context) {
        Expression left = cp.left();
        if (left instanceof SlotReference && ((SlotReference) left).getColumn().isPresent()) {
            if ("__DORIS_DELETE_SIGN__".equals(((SlotReference) left).getColumn().get().getName())) {
                return context.statistics;
            }
        }
        Expression right = cp.right();
        if (right instanceof SlotReference && ((SlotReference) right).getColumn().isPresent()) {
            if ("__DORIS_DELETE_SIGN__".equals(((SlotReference) right).getColumn().get().getName())) {
                return context.statistics;
            }
        }
        ColumnStatistic statsForLeft = ExpressionEstimation.estimate(left, context.statistics);
        ColumnStatistic statsForRight = ExpressionEstimation.estimate(right, context.statistics);
        if (!left.isConstant() && !right.isConstant()) {
            return calculateWhenBothColumn(cp, context, statsForLeft, statsForRight);
        } else {
            // For literal, it's max min is same value.
            return calculateWhenLiteralRight(cp,
                    statsForLeft,
                    statsForRight,
                    context);
        }
    }

    private Statistics updateLessThanLiteral(Expression leftExpr, DataType dataType, ColumnStatistic statsForLeft,
            ColumnStatistic statsForRight, EstimationContext context) {
        StatisticRange rightRange = new StatisticRange(statsForLeft.minValue, statsForLeft.minExpr,
                statsForRight.maxValue, statsForRight.maxExpr,
                statsForLeft.ndv, dataType);
        return estimateBinaryComparisonFilter(leftExpr, dataType,
                statsForLeft,
                rightRange, context);
    }

    private Statistics updateGreaterThanLiteral(Expression leftExpr, DataType dataType, ColumnStatistic statsForLeft,
            ColumnStatistic statsForRight, EstimationContext context) {
        StatisticRange rightRange = new StatisticRange(statsForRight.minValue, statsForRight.minExpr,
                statsForLeft.maxValue, statsForLeft.maxExpr,
                statsForLeft.ndv, dataType);
        return estimateBinaryComparisonFilter(leftExpr, dataType, statsForLeft, rightRange, context);
    }

    private Statistics calculateWhenLiteralRight(ComparisonPredicate cp,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight, EstimationContext context) {
        if (statsForLeft.isUnKnown) {
            return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
        }

        if (cp instanceof EqualPredicate) {
            return estimateEqualTo(cp, statsForLeft, statsForRight, context);
        } else {
            // literal Map used to covert dateLiteral back to stringLiteral
            Map<DateLiteral, StringLiteral> literalMap = new HashMap<>();
            DataType compareType = cp.left().getDataType();
            Optional<ColumnStatistic> statsForLeftMayConvertedOpt =
                    tryConvertStringColStatsToDateColStats(statsForLeft, literalMap);
            Optional<ColumnStatistic> statsForRightMayConvertedOpt = (statsForLeftMayConvertedOpt.isPresent())
                    ? tryConvertStringColStatsToDateColStats(statsForRight, literalMap)
                    : Optional.empty();

            boolean converted = false;
            ColumnStatistic statsForLeftMayConverted = statsForLeft;
            ColumnStatistic statsForRightMayConverted = statsForRight;
            if (statsForLeftMayConvertedOpt.isPresent() && statsForRightMayConvertedOpt.isPresent()
                    && statsForRightMayConvertedOpt.get().minExpr.getType()
                    == statsForLeftMayConvertedOpt.get().minExpr.getType()) {
                // string type is converted to date type
                converted = true;
                compareType = DateTimeType.INSTANCE;
                statsForLeftMayConverted = statsForLeftMayConvertedOpt.get();
                statsForRightMayConverted = statsForRightMayConvertedOpt.get();
            }
            Statistics result = null;
            if (cp instanceof LessThan || cp instanceof LessThanEqual) {
                result = updateLessThanLiteral(cp.left(), compareType, statsForLeftMayConverted,
                        statsForRightMayConverted, context);
            } else if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
                result = updateGreaterThanLiteral(cp.left(), compareType, statsForLeftMayConverted,
                        statsForRightMayConverted, context);
            } else {
                throw new RuntimeException(String.format("Unexpected expression : %s", cp.toSql()));
            }
            if (converted) {
                // convert min/max of left.colStats back to string type
                ColumnStatistic newLeftStats = result.findColumnStatistics(cp.left());
                result.addColumnStats(cp.left(), convertDateColStatsToStringColStats(newLeftStats, literalMap));
            }
            return result;
        }
    }

    private ColumnStatistic convertDateColStatsToStringColStats(ColumnStatistic colStats,
            Map<DateLiteral, StringLiteral> literalMap) {
        if (colStats.minExpr == null && colStats.maxExpr == null) {
            // when sel=0, minExpr and maxExpr are both null
            return colStats;
        }
        Preconditions.checkArgument(colStats.minExpr instanceof DateLiteral
                        && colStats.maxExpr instanceof DateLiteral,
                "cannot convert colStats back to stringType %s", colStats.toString());
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder(colStats);
        StringLiteral newMinLiteral = new StringLiteral(colStats.maxExpr.toString());
        return builder.setMaxExpr(newMinLiteral)
                .setMaxExpr(literalMap.get(colStats.maxExpr))
                .setMaxValue(StringLikeLiteral.getDouble(colStats.maxExpr.toString()))
                .setMinExpr(literalMap.get(colStats.minExpr))
                .setMinValue(StringLikeLiteral.getDouble(colStats.minExpr.getStringValue()))
                .build();
    }

    private Optional<ColumnStatistic> tryConvertStringColStatsToDateColStats(ColumnStatistic colStats,
            Map<DateLiteral, StringLiteral> literalMap) {
        if (colStats.minExpr == null || colStats.maxExpr == null) {
            return Optional.empty();
        }
        if (!(colStats.minExpr instanceof StringLiteral) || !(colStats.maxExpr instanceof StringLiteral)) {
            return Optional.empty();
        }
        Optional<DateLiteral> newMinExpr = tryConvertStrLiteralToDateLiteral(colStats.minExpr);
        if (!newMinExpr.isPresent()) {
            return Optional.empty();
        }
        Optional<DateLiteral> newMaxExpr = tryConvertStrLiteralToDateLiteral(colStats.maxExpr);
        if (!newMaxExpr.isPresent()) {
            return Optional.empty();
        }
        if (newMaxExpr.get().getType() != newMinExpr.get().getType()) {
            return Optional.empty();
        }
        literalMap.put(newMinExpr.get(), (StringLiteral) colStats.minExpr);
        literalMap.put(newMaxExpr.get(), (StringLiteral) colStats.maxExpr);

        ColumnStatisticBuilder builder = new ColumnStatisticBuilder(colStats);
        return Optional.of(builder.setMinValue(newMinExpr.get().getDoubleValueAsDateTime())
                .setMinExpr(newMinExpr.get())
                .setMaxValue(newMaxExpr.get().getDoubleValueAsDateTime())
                .setMaxExpr(newMaxExpr.get())
                .build());
    }

    private Optional<DateLiteral> tryConvertStrLiteralToDateLiteral(LiteralExpr literal) {
        if (literal == null) {
            return Optional.empty();
        }
        if (!(literal instanceof StringLiteral)) {
            return Optional.empty();
        }

        DateLiteral dt = null;
        try {
            dt = new DateLiteral(literal.getStringValue());
            dt.checkValueValid();
        } catch (Exception e) {
            // ignore
        }
        return dt == null ? Optional.empty() : Optional.of(dt);
    }

    private Statistics estimateEqualTo(ComparisonPredicate cp, ColumnStatistic statsForLeft,
            ColumnStatistic statsForRight,
            EstimationContext context) {
        double selectivity;
        if (statsForLeft.isUnKnown) {
            selectivity = DEFAULT_INEQUALITY_COEFFICIENT;
        } else {
            double ndv = statsForLeft.ndv;
            if (statsForRight.isUnKnown) {
                if (ndv >= 1.0) {
                    selectivity = 1.0 / ndv;
                } else {
                    selectivity = DEFAULT_INEQUALITY_COEFFICIENT;
                }
            } else {
                double val = statsForRight.maxValue;
                if (val > statsForLeft.maxValue || val < statsForLeft.minValue) {
                    selectivity = 0.0;
                } else if (ndv >= 1.0) {
                    selectivity = StatsMathUtil.minNonNaN(1.0, 1.0 / ndv);
                } else {
                    selectivity = DEFAULT_INEQUALITY_COEFFICIENT;
                }
                selectivity = getNotNullSelectivity(statsForLeft, selectivity);
            }
        }
        Statistics equalStats = context.statistics.withSel(selectivity);
        Expression left = cp.left();
        equalStats.addColumnStats(left, statsForRight);
        context.addKeyIfSlot(left);
        if (!(left instanceof SlotReference)) {
            left.accept(new ColumnStatsAdjustVisitor(), equalStats);
        }
        return equalStats;
    }

    private Statistics calculateWhenBothColumn(ComparisonPredicate cp, EstimationContext context,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight) {
        Expression left = cp.left();
        Expression right = cp.right();
        if (cp instanceof EqualPredicate) {
            return estimateColumnEqualToColumn(left, statsForLeft, right, statsForRight,
                    cp instanceof NullSafeEqual, context);
        }
        if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
            return estimateColumnLessThanColumn(right, statsForRight, left, statsForLeft, context);
        }
        if (cp instanceof LessThan || cp instanceof LessThanEqual) {
            return estimateColumnLessThanColumn(left, statsForLeft, right, statsForRight, context);
        }
        return context.statistics;
    }

    @Override
    public Statistics visitInPredicate(InPredicate inPredicate, EstimationContext context) {
        Expression compareExpr = inPredicate.getCompareExpr();
        ColumnStatistic compareExprStats = ExpressionEstimation.estimate(compareExpr, context.statistics);
        if (compareExprStats.isUnKnown || compareExpr instanceof Function) {
            return context.statistics.withSel(DEFAULT_IN_COEFFICIENT);
        }
        List<Expression> options = inPredicate.getOptions();
        // init minOption and maxOption by compareExpr.max and compareExpr.min respectively,
        // and then adjust min/max by options
        double minOptionValue = compareExprStats.maxValue;
        double maxOptionValue = compareExprStats.minValue;
        LiteralExpr minOptionLiteral = compareExprStats.maxExpr;
        LiteralExpr maxOptionLiteral = compareExprStats.minExpr;
        /* suppose A.(min, max) = (0, 10), A.ndv=10
         A in ( 1, 2, 5, 100):
              validInOptCount = 3, that is (1, 2, 5)
              table selectivity = 3/10
              A.min = 1, A.max=5
              A.selectivity = 3/5
              A.ndv = 3
         A not in (1, 2, 3, 100):
              validInOptCount = 10 - 3
              we assume that 1, 2, 3 exist in A
              A.ndv = 10 - 3 = 7
              table selectivity = 7/10
              A.(min, max) not changed
              A.selectivity = 7/10
        */
        int validInOptCount = 0;
        double selectivity = 1.0;
        ColumnStatisticBuilder compareExprStatsBuilder = new ColumnStatisticBuilder(compareExprStats);
        int nonLiteralOptionCount = 0;
        for (Expression option : options) {
            ColumnStatistic optionStats = ExpressionEstimation.estimate(option, context.statistics);
            if (option instanceof Literal) {
                // remove the options which is out of compareExpr.range
                if (compareExprStats.minValue <= optionStats.maxValue
                        && optionStats.minValue <= compareExprStats.maxValue) {
                    validInOptCount++;
                    LiteralExpr optionLiteralExpr = ((Literal) option).toLegacyLiteral();
                    if (maxOptionLiteral == null || optionLiteralExpr.compareTo(maxOptionLiteral) >= 0) {
                        maxOptionLiteral = optionLiteralExpr;
                        maxOptionValue = optionStats.maxValue;
                    }

                    if (minOptionLiteral == null || optionLiteralExpr.compareTo(minOptionLiteral) <= 0) {
                        minOptionLiteral = optionLiteralExpr;
                        minOptionValue = optionStats.minValue;
                    }
                }
            } else {
                nonLiteralOptionCount++;
            }
        }
        if (nonLiteralOptionCount > 0) {
            // A in (x+1, ...)
            // "x+1" is not literal, and if const-fold can not handle it, it blocks estimation of min/max value.
            // and hence, we do not adjust compareExpr.stats.range.
            int newNdv = nonLiteralOptionCount + validInOptCount;
            if (newNdv < compareExprStats.ndv) {
                compareExprStatsBuilder.setNdv(newNdv);
                selectivity = StatsMathUtil.divide(newNdv, compareExprStats.ndv);
            } else {
                selectivity = 1.0;
            }
        } else {
            maxOptionValue = Math.min(maxOptionValue, compareExprStats.maxValue);
            minOptionValue = Math.max(minOptionValue, compareExprStats.minValue);
            compareExprStatsBuilder.setMaxValue(maxOptionValue);
            compareExprStatsBuilder.setMaxExpr(maxOptionLiteral);
            compareExprStatsBuilder.setMinValue(minOptionValue);
            compareExprStatsBuilder.setMinExpr(minOptionLiteral);
            if (validInOptCount < compareExprStats.ndv) {
                compareExprStatsBuilder.setNdv(validInOptCount);
                selectivity = StatsMathUtil.divide(validInOptCount, compareExprStats.ndv);
            } else {
                selectivity = 1.0;
            }
        }
        compareExprStatsBuilder.setNumNulls(0);
        Statistics estimated = new StatisticsBuilder(context.statistics).build();
        ColumnStatistic stats = compareExprStatsBuilder.build();
        selectivity = getNotNullSelectivity(stats, selectivity);
        estimated = estimated.withSel(selectivity);
        estimated.addColumnStats(compareExpr, stats);
        context.addKeyIfSlot(compareExpr);
        return estimated;
    }

    // Right Now, we just assume the selectivity is 1 when stats is Unknown
    private Statistics handleUnknownCase(EstimationContext context) {
        return context.statistics;
    }

    @Override
    public Statistics visitNot(Not not, EstimationContext context) {
        if (context.statistics.isInputSlotsUnknown(not.getInputSlots())) {
            return handleUnknownCase(context);
        }
        Expression child = not.child();
        Statistics childStats = child.accept(this, context);
        //if estimated rowCount is 0, adjust to 1 to make upper join reorder reasonable.
        double rowCount = Math.max(context.statistics.getRowCount() - childStats.getRowCount(), 1);
        StatisticsBuilder statisticsBuilder = new StatisticsBuilder(context.statistics).setRowCount(rowCount);
        // update key col stats
        for (Slot slot : not.child().getInputSlots()) {
            ColumnStatistic originColStats = context.statistics.findColumnStatistics(slot);
            ColumnStatistic childColStats = childStats.findColumnStatistics(slot);
            if (context.isKeySlot(slot)) {
                ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder(childColStats);
                // update column stats for
                // 1. not (A=B)
                // 2. not A in (...)
                // 3. not A is null
                // 4. not A like XXX
                colBuilder.setNumNulls(0);
                Preconditions.checkArgument(
                        child instanceof EqualPredicate
                                || child instanceof InPredicate
                                || child instanceof IsNull
                                || child instanceof Like
                                || child instanceof Match,
                        "Not-predicate meet unexpected child: %s", child.toSql());
                if (child instanceof Like) {
                    rowCount = context.statistics.getRowCount() - childStats.getRowCount();
                    colBuilder.setNdv(Math.max(1.0, originColStats.ndv - childColStats.ndv));
                } else if (child instanceof InPredicate) {
                    colBuilder.setNdv(Math.max(1.0, originColStats.ndv - childColStats.ndv));
                    colBuilder.setMinValue(originColStats.minValue)
                            .setMinExpr(originColStats.minExpr)
                            .setMaxValue(originColStats.maxValue)
                            .setMaxExpr(originColStats.maxExpr);
                } else if (child instanceof IsNull) {
                    colBuilder.setNdv(originColStats.ndv);
                    colBuilder.setMinValue(originColStats.minValue)
                            .setMinExpr(originColStats.minExpr)
                            .setMaxValue(originColStats.maxValue)
                            .setMaxExpr(originColStats.maxExpr);
                } else if (child instanceof EqualPredicate) {
                    colBuilder.setNdv(Math.max(1.0, originColStats.ndv - childColStats.ndv));
                    colBuilder.setMinValue(originColStats.minValue)
                            .setMinExpr(originColStats.minExpr)
                            .setMaxValue(originColStats.maxValue)
                            .setMaxExpr(originColStats.maxExpr);
                } else if (child instanceof Match) {
                    rowCount = context.statistics.getRowCount() - childStats.getRowCount();
                    colBuilder.setNdv(Math.max(1.0, originColStats.ndv - childColStats.ndv));
                }
                if (not.child().getInputSlots().size() == 1 && !(child instanceof IsNull)) {
                    // only consider the single column numNull, otherwise, ignore
                    rowCount = Math.max(rowCount - originColStats.numNulls, 1);
                    statisticsBuilder.setRowCount(rowCount);
                }
                statisticsBuilder.putColumnStatistics(slot, colBuilder.build());
            }
        }

        return statisticsBuilder.build();
    }

    @Override
    public Statistics visitIsNull(IsNull isNull, EstimationContext context) {
        ColumnStatistic childColStats = ExpressionEstimation.estimate(isNull.child(), context.statistics);
        if (childColStats.isUnKnown()) {
            double row = context.statistics.getRowCount() * DEFAULT_ISNULL_SELECTIVITY;
            return new StatisticsBuilder(context.statistics).setRowCount(row).build();
        }
        double outputRowCount = childColStats.numNulls;
        if (!isOnBaseTable) {
            // for is null on base table, use the numNulls, otherwise
            // nulls will be generated such as outer join and then we do a protection
            Expression child = isNull.child();
            Statistics childStats = child.accept(this, context);
            outputRowCount = Math.max(childStats.getRowCount() * DEFAULT_ISNULL_SELECTIVITY, outputRowCount);
            outputRowCount = Math.max(outputRowCount, 1);
        }
        ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder(childColStats);
        colBuilder.setCount(outputRowCount).setNumNulls(outputRowCount)
                .setMaxValue(Double.POSITIVE_INFINITY)
                .setMinValue(Double.NEGATIVE_INFINITY)
                .setNdv(0);
        StatisticsBuilder builder = new StatisticsBuilder(context.statistics);
        builder.setRowCount(outputRowCount);
        builder.putColumnStatistics(isNull, colBuilder.build());
        context.addKeyIfSlot(isNull.child());
        return builder.build();
    }

    static class EstimationContext {
        private final Statistics statistics;

        private final Set<Slot> keyColumns = Sets.newHashSet();

        public EstimationContext(Statistics statistics) {
            this.statistics = statistics;
        }

        public void addKeyIfSlot(Expression expr) {
            if (expr instanceof Slot) {
                keyColumns.add((Slot) expr);
            }
        }

        public boolean isKeySlot(Expression expr) {
            if (expr instanceof Slot) {
                return keyColumns.contains((Slot) expr);
            }
            return false;
        }
    }

    private Statistics estimateBinaryComparisonFilter(Expression leftExpr, DataType dataType, ColumnStatistic leftStats,
            StatisticRange rightRange, EstimationContext context) {
        StatisticRange leftRange =
                new StatisticRange(leftStats.minValue, leftStats.minExpr, leftStats.maxValue, leftStats.maxExpr,
                        leftStats.ndv, dataType);
        StatisticRange intersectRange = leftRange.cover(rightRange);

        ColumnStatisticBuilder leftColumnStatisticBuilder;
        Statistics updatedStatistics;
        if (intersectRange.isEmpty()) {
            updatedStatistics = context.statistics.withRowCount(0);
            leftColumnStatisticBuilder = new ColumnStatisticBuilder(leftStats)
                    .setMinValue(Double.NEGATIVE_INFINITY)
                    .setMinExpr(null)
                    .setMaxValue(Double.POSITIVE_INFINITY)
                    .setMaxExpr(null)
                    .setNdv(0)
                    .setCount(0)
                    .setNumNulls(0);
        } else {
            leftColumnStatisticBuilder = new ColumnStatisticBuilder(leftStats)
                    .setMinValue(intersectRange.getLow())
                    .setMinExpr(intersectRange.getLowExpr())
                    .setMaxValue(intersectRange.getHigh())
                    .setMaxExpr(intersectRange.getHighExpr())
                    .setNdv(intersectRange.getDistinctValues())
                    .setNumNulls(0);
            double sel = leftRange.getDistinctValues() == 0
                    ? 1.0
                    : intersectRange.getDistinctValues() / leftRange.getDistinctValues();
            if (!(dataType instanceof RangeScalable) && (sel != 0.0 && sel != 1.0)) {
                sel = DEFAULT_INEQUALITY_COEFFICIENT;
            } else {
                sel = Math.max(sel, RANGE_SELECTIVITY_THRESHOLD);
            }
            sel = getNotNullSelectivity(leftStats, sel);
            updatedStatistics = context.statistics.withSel(sel);
            leftColumnStatisticBuilder.setCount(updatedStatistics.getRowCount());
        }
        updatedStatistics.addColumnStats(leftExpr, leftColumnStatisticBuilder.build());
        context.addKeyIfSlot(leftExpr);
        leftExpr.accept(new ColumnStatsAdjustVisitor(), updatedStatistics);
        return updatedStatistics;
    }

    private Statistics estimateColumnEqualToColumn(Expression leftExpr, ColumnStatistic leftStats,
            Expression rightExpr, ColumnStatistic rightStats, boolean keepNull, EstimationContext context) {
        StatisticRange leftRange = StatisticRange.from(leftStats, leftExpr.getDataType());
        StatisticRange rightRange = StatisticRange.from(rightStats, rightExpr.getDataType());
        StatisticRange leftIntersectRight = leftRange.intersect(rightRange);
        StatisticRange intersect = rightRange.intersect(leftIntersectRight);
        ColumnStatisticBuilder intersectBuilder = new ColumnStatisticBuilder(leftStats);
        intersectBuilder.setNdv(intersect.getDistinctValues());
        intersectBuilder.setMinValue(intersect.getLow());
        intersectBuilder.setMaxValue(intersect.getHigh());
        double numNull = 0;
        if (keepNull) {
            numNull = Math.min(leftStats.numNulls, rightStats.numNulls);
        }
        intersectBuilder.setNumNulls(numNull);
        double sel = 1 / StatsMathUtil.nonZeroDivisor(Math.max(leftStats.ndv, rightStats.ndv));
        Statistics updatedStatistics = context.statistics.withSel(sel, numNull);
        updatedStatistics.addColumnStats(leftExpr, intersectBuilder.build());
        updatedStatistics.addColumnStats(rightExpr, intersectBuilder.build());

        context.addKeyIfSlot(leftExpr);
        context.addKeyIfSlot(rightExpr);
        return updatedStatistics;
    }

    private Statistics estimateColumnLessThanColumn(Expression leftExpr, ColumnStatistic leftStats,
            Expression rightExpr, ColumnStatistic rightStats, EstimationContext context) {
        StatisticRange leftRange = StatisticRange.from(leftStats, leftExpr.getDataType());
        StatisticRange rightRange = StatisticRange.from(rightStats, rightExpr.getDataType());
        Statistics statistics = null;
        // Left always less than Right
        if (leftRange.getHigh() < rightRange.getLow()) {
            statistics =
                    context.statistics.withRowCount(Math.min(context.statistics.getRowCount() - leftStats.numNulls,
                            context.statistics.getRowCount() - rightStats.numNulls));
            statistics.addColumnStats(leftExpr, new ColumnStatisticBuilder(leftStats).setNumNulls(0.0).build());
            statistics.addColumnStats(rightExpr, new ColumnStatisticBuilder(rightStats).setNumNulls(0.0).build());
            context.addKeyIfSlot(leftExpr);
            context.addKeyIfSlot(rightExpr);
            return statistics;
        }
        if (leftRange.isInfinite() || rightRange.isInfinite()) {
            return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
        }

        double leftOverlapPercent = leftRange.overlapPercentWith(rightRange);

        if (leftOverlapPercent == 0.0) {
            // Left always greater than right
            return context.statistics.withRowCount(0.0);
        }
        StatisticRange leftAlwaysLessThanRightRange = new StatisticRange(leftStats.minValue, leftStats.minExpr,
                rightStats.minValue, rightStats.minExpr, Double.NaN, leftExpr.getDataType());
        double leftAlwaysLessThanRightPercent = 0;
        if (leftRange.getLow() < rightRange.getLow()) {
            leftAlwaysLessThanRightPercent = leftRange.overlapPercentWith(leftAlwaysLessThanRightRange);
        }
        ColumnStatistic leftColumnStatistic = new ColumnStatisticBuilder(leftStats)
                .setMaxValue(Math.min(leftRange.getHigh(), rightRange.getHigh()))
                .setMinValue(leftRange.getLow())
                .setNdv(leftStats.ndv * (leftAlwaysLessThanRightPercent + leftOverlapPercent))
                .setNumNulls(0)
                .build();
        double rightOverlappingRangeFraction = rightRange.overlapPercentWith(leftRange);
        double rightAlwaysGreaterRangeFraction = 0;
        if (leftRange.getHigh() < rightRange.getHigh()) {
            rightAlwaysGreaterRangeFraction = rightRange.overlapPercentWith(new StatisticRange(
                    leftRange.getHigh(), leftRange.getHighExpr(),
                    rightRange.getHigh(), rightRange.getHighExpr(),
                    Double.NaN, rightExpr.getDataType()));
        }
        ColumnStatistic rightColumnStatistic = new ColumnStatisticBuilder(rightStats)
                .setMinValue(Math.max(leftRange.getLow(), rightRange.getLow()))
                .setMaxValue(rightRange.getHigh())
                .setNdv(rightStats.ndv * (rightAlwaysGreaterRangeFraction + rightOverlappingRangeFraction))
                .setNumNulls(0)
                .build();
        double sel = DEFAULT_INEQUALITY_COEFFICIENT;
        if (leftExpr.getDataType() instanceof RangeScalable) {
            sel = leftAlwaysLessThanRightPercent
                    + leftOverlapPercent * rightOverlappingRangeFraction * DEFAULT_INEQUALITY_COEFFICIENT
                    + leftOverlapPercent * rightAlwaysGreaterRangeFraction;
        } else if (leftOverlapPercent == 1.0) {
            sel = 1.0;
        }
        context.addKeyIfSlot(leftExpr);
        context.addKeyIfSlot(rightExpr);
        return context.statistics.withSel(sel)
                .addColumnStats(leftExpr, leftColumnStatistic)
                .addColumnStats(rightExpr, rightColumnStatistic);
    }

    @Override
    public Statistics visitLike(Like like, EstimationContext context) {
        StatisticsBuilder statsBuilder = new StatisticsBuilder(context.statistics);
        statsBuilder.setRowCount(context.statistics.getRowCount() * DEFAULT_LIKE_COMPARISON_SELECTIVITY);
        if (like.left() instanceof Slot) {
            ColumnStatistic origin = context.statistics.findColumnStatistics(like.left());
            Preconditions.checkArgument(origin != null,
                    "col stats not found. slot=%s in %s",
                    like.left().toSql(), like.toSql());
            ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder(origin);
            double selectivity = StatsMathUtil.divide(DEFAULT_LIKE_COMPARISON_SELECTIVITY, origin.ndv);
            double notNullSel = getNotNullSelectivity(origin, selectivity);
            colBuilder.setNdv(origin.ndv * DEFAULT_LIKE_COMPARISON_SELECTIVITY)
                    .setCount(notNullSel * context.statistics.getRowCount()).setNumNulls(0);
            statsBuilder.putColumnStatistics(like.left(), colBuilder.build());
            context.addKeyIfSlot(like.left());
        }
        return statsBuilder.build();
    }

    private double getNotNullSelectivity(ColumnStatistic stats, double origSel) {
        double rowCount = stats.count;
        double numNulls = stats.numNulls;

        // comment following check since current rowCount and ndv may be inconsistant
        // e.g, rowCount has been reduced by one filter but another filter column's
        // ndv and numNull remains originally, which will unexpectedly go into the following
        // normalization.

        //if (numNulls > rowCount - ndv) {
        //    numNulls = rowCount - ndv > 0 ? rowCount - ndv : 0;
        //}
        double notNullSel = rowCount <= 1.0 ? 1.0 : 1 - Statistics.getValidSelectivity(numNulls / rowCount);
        double validSel = origSel * notNullSel;
        return Statistics.getValidSelectivity(validSel);
    }
}
