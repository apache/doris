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
        outputStats.normalizeColumnStatistics();
        return outputStats;
    }

    @Override
    public Statistics visit(Expression expr, EstimationContext context) {
        return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
    }

    @Override
    public Statistics visitAnd(And and, EstimationContext context) {
        List<Expression> children = and.children();
        Statistics inputStats = context.statistics;
        Statistics outputStats = inputStats;
        Preconditions.checkArgument(children.size() > 1, "and expression abnormal: " + and);
        for (Expression child : children) {
            outputStats = child.accept(this, new EstimationContext(inputStats));
            outputStats.normalizeColumnStatistics(inputStats.getRowCount(), true);
            inputStats = outputStats;
        }
        return outputStats;
    }

    @Override
    public Statistics visitOr(Or or, EstimationContext context) {
        List<Expression> children = or.children();
        Set<Slot> leftInputSlots = Sets.newHashSet(children.get(0).getInputSlots());
        Statistics leftStats = children.get(0).accept(this, context);
        leftStats.normalizeColumnStatistics(context.statistics.getRowCount(), true);
        Statistics outputStats = leftStats;
        Preconditions.checkArgument(children.size() > 1, "and expression abnormal: " + or);
        for (int i = 1; i < children.size(); i++) {
            Expression child = children.get(i);
            Statistics andStats = child.accept(this, new EstimationContext(leftStats));
            Statistics rightStats = child.accept(this, context);
            rightStats.normalizeColumnStatistics(context.statistics.getRowCount(), true);
            double rowCount = leftStats.getRowCount() + rightStats.getRowCount() - andStats.getRowCount();
            Statistics orStats = context.statistics.withRowCount(rowCount);

            Set<Slot> rightInputSlots = child.getInputSlots();
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
            leftStats = orStats;
            outputStats = orStats;
            leftInputSlots.addAll(child.getInputSlots());
        }
        return outputStats;
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
            return estimateColumnToColumn(cp, context, statsForLeft, statsForRight);
        } else {
            return estimateColumnToConstant(cp, statsForLeft, statsForRight, context);
        }
    }

    private Statistics estimateColumnLessThanConstant(Expression leftExpr, DataType dataType,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight, EstimationContext context) {
        StatisticRange constantRange = new StatisticRange(statsForLeft.minValue, statsForLeft.minExpr,
                statsForRight.maxValue, statsForRight.maxExpr, statsForLeft.ndv, dataType);
        return estimateColumnToConstantRange(leftExpr, dataType, statsForLeft, constantRange, context);
    }

    private Statistics estimateColumnGreaterThanConstant(Expression leftExpr, DataType dataType,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight, EstimationContext context) {
        StatisticRange constantRange = new StatisticRange(statsForRight.minValue, statsForRight.minExpr,
                statsForLeft.maxValue, statsForLeft.maxExpr, statsForLeft.ndv, dataType);
        return estimateColumnToConstantRange(leftExpr, dataType, statsForLeft, constantRange, context);
    }

    private Statistics estimateColumnToConstant(ComparisonPredicate cp,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight, EstimationContext context) {
        if (statsForLeft.isUnKnown) {
            return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
        } else if (cp instanceof EqualPredicate) {
            return estimateColumnEqualToConstant(cp, statsForLeft, statsForRight, context);
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

            Statistics result;
            if (cp instanceof LessThan || cp instanceof LessThanEqual) {
                result = estimateColumnLessThanConstant(cp.left(), compareType, statsForLeftMayConverted,
                        statsForRightMayConverted, context);
            } else if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
                result = estimateColumnGreaterThanConstant(cp.left(), compareType, statsForLeftMayConverted,
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

    private Statistics estimateColumnEqualToConstant(ComparisonPredicate cp, ColumnStatistic statsForLeft,
            ColumnStatistic statsForRight, EstimationContext context) {
        double selectivity;
        if (statsForLeft.isUnKnown) {
            selectivity = DEFAULT_INEQUALITY_COEFFICIENT;
        } else {
            double ndv = statsForLeft.ndv;
            double numNulls = statsForLeft.numNulls;
            double rowCount = context.statistics.getRowCount();
            if (statsForRight.isUnKnown) {
                if (ndv >= 1.0) {
                    selectivity = 1.0 / ndv;
                } else {
                    selectivity = DEFAULT_INEQUALITY_COEFFICIENT;
                }
            } else {
                double val = statsForRight.maxValue;
                if (val > statsForLeft.maxValue || val < statsForLeft.minValue) {
                    // TODO: make sure left's stats is RangeScalable whose min/max is trustable.
                    // The equal to constant doesn't rely on the range, so maybe safe here.
                    selectivity = 0.0;
                } else if (ndv >= 1.0) {
                    selectivity = StatsMathUtil.minNonNaN(1.0, 1.0 / ndv);
                } else {
                    selectivity = DEFAULT_INEQUALITY_COEFFICIENT;
                }
                selectivity = getNotNullSelectivity(numNulls, rowCount, ndv, selectivity);
            }
        }
        Statistics equalStats = context.statistics.withSel(selectivity);
        Expression left = cp.left();
        equalStats.addColumnStats(left, statsForRight);
        context.addKeyIfSlot(left);
        if (!(left instanceof SlotReference)) {
            left.accept(new ColumnStatsAdjustVisitor(), equalStats);
        }
        // normalize column statistics here to sync numNulls by proportion and ndv by current row count
        equalStats.normalizeColumnStatistics(context.statistics.getRowCount(), true);
        return equalStats;
    }

    private Statistics estimateColumnToColumn(ComparisonPredicate cp, EstimationContext context,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight) {
        Expression left = cp.left();
        Expression right = cp.right();
        if (cp instanceof EqualPredicate) {
            return estimateColumnEqualToColumn(left, statsForLeft, right, statsForRight,
                    cp instanceof NullSafeEqual, context);
        } else if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
            return estimateColumnLessThanColumn(right, statsForRight, left, statsForLeft, context);
        } else if (cp instanceof LessThan || cp instanceof LessThanEqual) {
            return estimateColumnLessThanColumn(left, statsForLeft, right, statsForRight, context);
        } else {
            return context.statistics;
        }
    }

    private ColumnStatistic updateInPredicateColumnStatistics(InPredicate inPredicate, EstimationContext context,
            ColumnStatistic compareExprStats) {
        List<Expression> options = inPredicate.getOptions();
        ColumnStatisticBuilder compareExprStatsBuilder = new ColumnStatisticBuilder(compareExprStats);

        if (!compareExprStats.isMinMaxInvalid()) {
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
            int nonLiteralOptionCount = 0;
            for (Expression option : options) {
                ColumnStatistic optionStats = ExpressionEstimation.estimate(option, context.statistics);
                if (option instanceof Literal) {
                    // remove the options which is out of compareExpr.range
                    Preconditions.checkState(Math.abs(optionStats.maxValue - optionStats.minValue) < 1e-06,
                            "literal's min/max doesn't equal");
                    double constValue = optionStats.maxValue;
                    if (compareExprStats.minValue <= constValue && compareExprStats.maxValue >= constValue) {
                        validInOptCount++;
                        LiteralExpr optionLiteralExpr = ((Literal) option).toLegacyLiteral();
                        if (maxOptionLiteral == null || optionLiteralExpr.compareTo(maxOptionLiteral) >= 0) {
                            maxOptionLiteral = optionLiteralExpr;
                            maxOptionValue = constValue;
                        }

                        if (minOptionLiteral == null || optionLiteralExpr.compareTo(minOptionLiteral) <= 0) {
                            minOptionLiteral = optionLiteralExpr;
                            minOptionValue = constValue;
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
                }
            }
        } else {
            // other types, such as string type, using option's size to estimate
            // min/max will not be updated
            compareExprStatsBuilder.setNdv(Math.min(options.size(), compareExprStats.getOriginalNdv()));
        }
        compareExprStatsBuilder.setNumNulls(0);
        return compareExprStatsBuilder.build();
    }

    @Override
    public Statistics visitInPredicate(InPredicate inPredicate, EstimationContext context) {
        Expression compareExpr = inPredicate.getCompareExpr();
        ColumnStatistic compareExprStats = ExpressionEstimation.estimate(compareExpr, context.statistics);
        if (compareExprStats.isUnKnown || compareExpr instanceof Function) {
            return context.statistics.withSel(DEFAULT_IN_COEFFICIENT);
        }

        List<Expression> options = inPredicate.getOptions();
        ColumnStatistic newCompareExprStats = updateInPredicateColumnStatistics(inPredicate, context, compareExprStats);
        double selectivity;
        if (!newCompareExprStats.isMinMaxInvalid()) {
            selectivity = Statistics.getValidSelectivity(
                    Math.min(StatsMathUtil.divide(newCompareExprStats.ndv, compareExprStats.ndv), 1));
        } else {
            selectivity = Statistics.getValidSelectivity(
                    Math.min(options.size() / compareExprStats.getOriginalNdv(), 1));
        }

        Statistics estimated = new StatisticsBuilder(context.statistics).build();
        selectivity = getNotNullSelectivity(compareExprStats.numNulls, estimated.getRowCount(),
                compareExprStats.ndv, selectivity);
        estimated = estimated.withSel(selectivity);
        estimated.addColumnStats(compareExpr, newCompareExprStats);
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
        childStats.normalizeColumnStatistics();
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
                // 5. not array_contains([xx, xx], xx)
                colBuilder.setNumNulls(0);
                Preconditions.checkArgument(
                        child instanceof EqualPredicate
                                || child instanceof InPredicate
                                || child instanceof IsNull
                                || child instanceof Like
                                || child instanceof Match
                                || child instanceof Function,
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
        double outputRowCount = Math.min(childColStats.numNulls, context.statistics.getRowCount());
        if (!isOnBaseTable) {
            // for is null on base table, use the numNulls, otherwise
            // nulls will be generated such as outer join and then we do a protection
            Expression child = isNull.child();
            Statistics childStats = child.accept(this, context);
            childStats.normalizeColumnStatistics();
            outputRowCount = Math.max(childStats.getRowCount() * DEFAULT_ISNULL_SELECTIVITY, outputRowCount);
            outputRowCount = Math.max(outputRowCount, 1);
        }

        ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder(childColStats);
        colBuilder.setNumNulls(outputRowCount)
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

    private Statistics estimateColumnToConstantRange(Expression leftExpr, DataType dataType, ColumnStatistic leftStats,
            StatisticRange rightRange, EstimationContext context) {
        StatisticRange leftRange = new StatisticRange(leftStats.minValue, leftStats.minExpr,
                leftStats.maxValue, leftStats.maxExpr, leftStats.ndv, dataType);
        ColumnStatisticBuilder leftColumnStatisticBuilder;
        Statistics updatedStatistics;

        StatisticRange intersectRange = leftRange.intersect(rightRange, true);
        double sel = leftRange.getDistinctValues() == 0
                ? 1.0
                : intersectRange.getDistinctValues() / leftRange.getDistinctValues();
        if (intersectRange.isEmpty()) {
            leftColumnStatisticBuilder = new ColumnStatisticBuilder(leftStats)
                    .setMinValue(Double.NEGATIVE_INFINITY)
                    .setMinExpr(null)
                    .setMaxValue(Double.POSITIVE_INFINITY)
                    .setMaxExpr(null)
                    .setNdv(0)
                    .setNumNulls(0);
            updatedStatistics = context.statistics.withRowCount(0);
        } else if (dataType instanceof RangeScalable || sel == 0 || sel == 1) {
            leftColumnStatisticBuilder = new ColumnStatisticBuilder(leftStats)
                    .setMinValue(intersectRange.getLow())
                    .setMinExpr(intersectRange.getLowExpr())
                    .setMaxValue(intersectRange.getHigh())
                    .setMaxExpr(intersectRange.getHighExpr())
                    .setNdv(intersectRange.getDistinctValues())
                    .setNumNulls(0);
            sel = Math.max(sel, RANGE_SELECTIVITY_THRESHOLD);
            sel = getNotNullSelectivity(leftStats.numNulls, context.statistics.getRowCount(), leftStats.ndv, sel);
            updatedStatistics = context.statistics.withSel(sel);
        } else {
            sel = DEFAULT_INEQUALITY_COEFFICIENT;
            sel = getNotNullSelectivity(leftStats.numNulls, context.statistics.getRowCount(), leftStats.ndv, sel);
            leftColumnStatisticBuilder = new ColumnStatisticBuilder(leftStats)
                    .setMinValue(intersectRange.getLow())
                    .setMinExpr(intersectRange.getLowExpr())
                    .setMaxValue(intersectRange.getHigh())
                    .setMaxExpr(intersectRange.getHighExpr())
                    .setNdv(Math.max(1, Math.min(leftStats.ndv * sel, intersectRange.getDistinctValues())))
                    .setNumNulls(0);
            updatedStatistics = context.statistics.withSel(sel);
        }
        updatedStatistics.addColumnStats(leftExpr, leftColumnStatisticBuilder.build());
        context.addKeyIfSlot(leftExpr);
        leftExpr.accept(new ColumnStatsAdjustVisitor(), updatedStatistics);
        // normalize column statistics here to sync numNulls by proportion and ndv by current row count
        updatedStatistics.normalizeColumnStatistics(context.statistics.getRowCount(), true);

        return updatedStatistics;
    }

    private Statistics estimateColumnEqualToColumn(Expression leftExpr, ColumnStatistic leftStats,
            Expression rightExpr, ColumnStatistic rightStats, boolean keepNull, EstimationContext context) {
        ColumnStatisticBuilder intersectBuilder = new ColumnStatisticBuilder(leftStats);
        StatisticRange leftRange = StatisticRange.from(leftStats, leftExpr.getDataType());
        StatisticRange rightRange = StatisticRange.from(rightStats, rightExpr.getDataType());
        StatisticRange intersect = leftRange.intersect(rightRange);
        intersectBuilder.setMinValue(intersect.getLow());
        intersectBuilder.setMaxValue(intersect.getHigh());

        if (leftExpr.getDataType() instanceof RangeScalable && rightExpr.getDataType() instanceof RangeScalable
                && !leftStats.isMinMaxInvalid() && !rightStats.isMinMaxInvalid()) {
            intersectBuilder.setNdv(intersect.getDistinctValues());
        } else {
            // intersect ndv uses min ndv but selectivity computing use the max
            intersectBuilder.setNdv(Math.min(leftStats.ndv, rightStats.ndv));
        }
        double numNull = keepNull ? Math.min(leftStats.numNulls, rightStats.numNulls) : 0;
        intersectBuilder.setNumNulls(numNull);

        // TODO: consider notNullSelectivity
        //double origRowCount = context.statistics.getRowCount();
        double leftNotNullSel = 1.0; //Statistics.getValidSelectivity(1 - (leftStats.numNulls / origRowCount));
        double rightNotNullSel = 1.0; //Statistics.getValidSelectivity(1 - (rightStats.numNulls / origRowCount));
        double notNullSel = 1 / StatsMathUtil.nonZeroDivisor(Math.max(leftStats.ndv, rightStats.ndv))
                * (keepNull ? 1 : leftNotNullSel * rightNotNullSel);

        Statistics updatedStatistics = context.statistics.withSel(notNullSel, numNull);
        ColumnStatistic newLeftStatistics = intersectBuilder
                .setAvgSizeByte(leftStats.avgSizeByte).build();
        ColumnStatistic newRightStatistics = intersectBuilder
                .setAvgSizeByte(rightStats.avgSizeByte).build();
        updatedStatistics.addColumnStats(leftExpr, newLeftStatistics);
        updatedStatistics.addColumnStats(rightExpr, newRightStatistics);

        context.addKeyIfSlot(leftExpr);
        context.addKeyIfSlot(rightExpr);
        return updatedStatistics;
    }

    private Statistics estimateColumnLessThanColumn(Expression leftExpr, ColumnStatistic leftStats,
            Expression rightExpr, ColumnStatistic rightStats, EstimationContext context) {
        StatisticRange leftRange = StatisticRange.from(leftStats, leftExpr.getDataType());
        StatisticRange rightRange = StatisticRange.from(rightStats, rightExpr.getDataType());
        StatisticRange intersect = leftRange.intersect(rightRange);

        if (leftExpr.getDataType() instanceof RangeScalable && rightExpr.getDataType() instanceof RangeScalable
                    && !leftStats.isMinMaxInvalid() && !rightStats.isMinMaxInvalid()) {
            // TODO: use intersect interface to refine this to avoid this kind of left-dominating style
            Statistics statistics;
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
            double sel;
            if (leftExpr.getDataType() instanceof RangeScalable) {
                sel = leftAlwaysLessThanRightPercent
                        + leftOverlapPercent * rightOverlappingRangeFraction * DEFAULT_INEQUALITY_COEFFICIENT
                        + leftOverlapPercent * rightAlwaysGreaterRangeFraction;
            } else if (leftOverlapPercent == 1.0) {
                sel = 1.0;
            } else {
                sel = DEFAULT_INEQUALITY_COEFFICIENT;
            }
            context.addKeyIfSlot(leftExpr);
            context.addKeyIfSlot(rightExpr);
            return context.statistics.withSel(sel)
                    .addColumnStats(leftExpr, leftColumnStatistic)
                    .addColumnStats(rightExpr, rightColumnStatistic);
        } else {
            ColumnStatistic leftColumnStatistic = new ColumnStatisticBuilder(leftStats)
                    .setMaxValue(intersect.getHigh())
                    .setMinValue(intersect.getLow())
                    .setNumNulls(0)
                    .setNdv(Math.max(leftStats.ndv * DEFAULT_INEQUALITY_COEFFICIENT, 1))
                    .build();
            ColumnStatistic rightColumnStatistic = new ColumnStatisticBuilder(rightStats)
                    .setMaxValue(intersect.getHigh())
                    .setMinValue(intersect.getLow())
                    .setNumNulls(0)
                    .setNdv(Math.max(rightStats.ndv * DEFAULT_INEQUALITY_COEFFICIENT, 1))
                    .build();
            context.addKeyIfSlot(leftExpr);
            context.addKeyIfSlot(rightExpr);
            return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT)
                    .addColumnStats(leftExpr, leftColumnStatistic)
                    .addColumnStats(rightExpr, rightColumnStatistic);
        }
    }

    @Override
    public Statistics visitLike(Like like, EstimationContext context) {
        StatisticsBuilder statsBuilder = new StatisticsBuilder(context.statistics);
        double rowCount = context.statistics.getRowCount() * DEFAULT_LIKE_COMPARISON_SELECTIVITY;
        statsBuilder.setRowCount(rowCount);
        if (like.left() instanceof Slot) {
            ColumnStatistic origin = context.statistics.findColumnStatistics(like.left());
            Preconditions.checkArgument(origin != null,
                    "col stats not found. slot=%s in %s",
                    like.left().toSql(), like.toSql());
            ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder(origin);
            colBuilder.setNdv(origin.ndv * DEFAULT_LIKE_COMPARISON_SELECTIVITY).setNumNulls(0);
            statsBuilder.putColumnStatistics(like.left(), colBuilder.build());
            context.addKeyIfSlot(like.left());
        }
        return statsBuilder.build();
    }

    private double getNotNullSelectivity(double origNumNulls, double origRowCount, double origNdv, double origSel) {
        if (origNumNulls > origRowCount - origNdv) {
            origNumNulls = origRowCount - origNdv > 0 ? origRowCount - origNdv : 0;
        }
        double notNullSel = origRowCount <= 1.0 ? 1.0 : 1 - Statistics
                .getValidSelectivity(origNumNulls / origRowCount);
        double validSel = origSel * notNullSel;
        return Statistics.getValidSelectivity(validSel);
    }
}
