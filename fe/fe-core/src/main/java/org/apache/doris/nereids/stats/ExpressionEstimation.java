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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Acos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ascii;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Asin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Atan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfMonth;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfWeek;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfYear;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromDays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Hour;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Minute;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Negative;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Quarter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Radians;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Second;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sqrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekOfYear;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsSub;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * Used to estimate for expressions that not producing boolean value.
 */
public class ExpressionEstimation extends ExpressionVisitor<ColumnStatistic, Statistics> {

    public static final long DAYS_FROM_0_TO_1970 = 719528;
    public static final long DAYS_FROM_0_TO_9999 = 3652424;
    private static final ExpressionEstimation INSTANCE = new ExpressionEstimation();

    /**
     * returned columnStat is newly created or a copy of stats
     */
    public static ColumnStatistic estimate(Expression expression, Statistics stats) {
        ColumnStatistic columnStatistic = expression.accept(INSTANCE, stats);
        if (columnStatistic == null) {
            return ColumnStatistic.UNKNOWN;
        }
        return columnStatistic;
    }

    @Override
    public ColumnStatistic visit(Expression expr, Statistics context) {
        List<Expression> childrenExpr = expr.children();
        if (CollectionUtils.isEmpty(childrenExpr)) {
            return ColumnStatistic.UNKNOWN;
        }
        return expr.child(0).accept(this, context);
    }

    //TODO: case-when need to re-implemented
    @Override
    public ColumnStatistic visitCaseWhen(CaseWhen caseWhen, Statistics context) {
        return new ColumnStatisticBuilder()
                .setNdv(caseWhen.getWhenClauses().size() + 1)
                .setMinValue(0)
                .setMaxValue(Double.MAX_VALUE)
                .setAvgSizeByte(8)
                .setNumNulls(0)
                .build();
    }

    @Override
    public ColumnStatistic visitIf(If function, Statistics context) {
        // TODO: copy from visitCaseWhen, polish them.
        return new ColumnStatisticBuilder()
                .setNdv(2)
                .setMinValue(0)
                .setMaxValue(Double.POSITIVE_INFINITY)
                .setAvgSizeByte(8)
                .setNumNulls(0)
                .build();
    }

    @Override
    public ColumnStatistic visitCast(Cast cast, Statistics context) {
        ColumnStatistic stats = context.findColumnStatistics(cast);
        if (stats != null) {
            return stats;
        }
        ColumnStatistic childColStats = cast.child().accept(this, context);
        Preconditions.checkNotNull(childColStats, "childColStats is null");
        return castMinMax(childColStats, cast.getDataType());
    }

    private ColumnStatistic castMinMax(ColumnStatistic colStats, DataType targetType) {
        if (colStats.minExpr instanceof StringLiteral || colStats.maxExpr instanceof StringLiteral) {
            if (targetType.isDateLikeType()) {
                ColumnStatisticBuilder builder = new ColumnStatisticBuilder(colStats);
                if (colStats.minExpr != null) {
                    try {
                        String strMin = colStats.minExpr.getStringValue();
                        DateLiteral dateMinLiteral = new DateLiteral(strMin);
                        long min = dateMinLiteral.getValue();
                        builder.setMinValue(min);
                        builder.setMinExpr(dateMinLiteral.toLegacyLiteral());
                    } catch (AnalysisException e) {
                        // ignore exception. do not convert min
                    }
                }
                if (colStats.maxExpr != null) {
                    try {
                        String strMax = colStats.maxExpr.getStringValue();
                        DateLiteral dateMaxLiteral = new DateLiteral(strMax);
                        long max = dateMaxLiteral.getValue();
                        builder.setMaxValue(max);
                        builder.setMaxExpr(dateMaxLiteral.toLegacyLiteral());
                    } catch (AnalysisException e) {
                        // ignore exception. do not convert max
                    }
                }
                return builder.build();
            }
        }
        return colStats;
    }

    @Override
    public ColumnStatistic visitLiteral(Literal literal, Statistics context) {
        if (ColumnStatistic.UNSUPPORTED_TYPE.contains(literal.getDataType().toCatalogDataType())) {
            return ColumnStatistic.UNKNOWN;
        }
        double literalVal = literal.getDouble();
        return new ColumnStatisticBuilder()
                .setMaxValue(literalVal)
                .setMinValue(literalVal)
                .setNdv(1)
                .setNumNulls(1)
                .setAvgSizeByte(1)
                .setMinExpr(literal.toLegacyLiteral())
                .setMaxExpr(literal.toLegacyLiteral())
                .build();
    }

    @Override
    public ColumnStatistic visitSlotReference(SlotReference slotReference, Statistics context) {
        return context.findColumnStatistics(slotReference);
    }

    @Override
    public ColumnStatistic visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, Statistics context) {
        ColumnStatistic leftColStats = binaryArithmetic.left().accept(this, context);
        ColumnStatistic rightColStats = binaryArithmetic.right().accept(this, context);
        double leftNdv = leftColStats.ndv;
        double rightNdv = rightColStats.ndv;
        double ndv = Math.max(leftNdv, rightNdv);
        double leftNullCount = leftColStats.numNulls;
        double rightNullCount = rightColStats.numNulls;
        double rowCount = context.getRowCount();
        double numNulls = context.getRowCount()
                * (1 - (1 - (leftNullCount / rowCount) * (1 - rightNullCount / rowCount)));
        double leftMax = leftColStats.maxValue;
        double rightMax = rightColStats.maxValue;
        double leftMin = leftColStats.minValue;
        double rightMin = rightColStats.minValue;
        int exprResultTypeWidth = binaryArithmetic.getDataType().width();
        double dataSize = exprResultTypeWidth * rowCount;
        if (binaryArithmetic instanceof Add) {
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv).setAvgSizeByte(leftColStats.avgSizeByte)
                    .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(leftMin + rightMin)
                    .setMaxValue(leftMax + rightMax)
                    .setMinExpr(null).setMaxExpr(null).build();
        }
        if (binaryArithmetic instanceof Subtract) {
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv).setAvgSizeByte(leftColStats.avgSizeByte)
                    .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(leftMin - rightMax)
                    .setMaxValue(leftMax - rightMin).setMinExpr(null)
                    .setMaxExpr(null).build();
        }
        // TODO: stat for multiply and divide produced by below algorithm may have huge deviation with reality.
        if (binaryArithmetic instanceof Multiply) {
            double min = Math.min(
                    Math.min(
                            Math.min(leftMin * rightMin, leftMin * rightMax),
                            leftMax * rightMin),
                    leftMax * rightMax);
            double max = Math.max(
                    Math.max(
                            Math.max(leftMin * rightMin, leftMin * rightMax),
                            leftMax * rightMin),
                    leftMax * rightMax);
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv).setAvgSizeByte(leftColStats.avgSizeByte)
                    .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(min).setMaxValue(max)
                    .setMaxExpr(null).setMinExpr(null).build();
        }
        if (binaryArithmetic instanceof Divide || binaryArithmetic instanceof IntegralDivide) {
            double min = Math.min(
                    Math.min(
                            Math.min(leftMin / noneZeroDivisor(rightMin), leftMin / noneZeroDivisor(rightMax)),
                            leftMax / noneZeroDivisor(rightMin)),
                    leftMax / noneZeroDivisor(rightMax));
            double max = Math.max(
                    Math.max(
                            Math.max(leftMin / noneZeroDivisor(rightMin), leftMin / noneZeroDivisor(rightMax)),
                            leftMax / noneZeroDivisor(rightMin)),
                    leftMax / noneZeroDivisor(rightMax));
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv).setAvgSizeByte(leftColStats.avgSizeByte)
                    .setNumNulls(numNulls).setDataSize(binaryArithmetic.getDataType().width()).setMinValue(min)
                    .setMaxValue(max).build();
        }
        if (binaryArithmetic instanceof Mod) {
            double min = -Math.max(Math.abs(rightMin), Math.abs(rightMax));
            double max = -min;
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv)
                    .setAvgSizeByte(exprResultTypeWidth)
                    .setDataSize(dataSize)
                    .setNumNulls(numNulls)
                    .setMaxValue(max)
                    .setMinValue(min)
                    .build();
        }

        return ColumnStatistic.UNKNOWN;
    }

    private double noneZeroDivisor(double d) {
        return d == 0.0 ? 1.0 : d;
    }

    @Override
    public ColumnStatistic visitMin(Min min, Statistics context) {
        Expression child = min.child();
        ColumnStatistic columnStat = child.accept(this, context);
        if (columnStat.isUnKnown) {
            return ColumnStatistic.UNKNOWN;
        }
        /*
        we keep columnStat.min and columnStat.max, but set ndv=1.
        if there is group-by keys, we will update count when visiting group clause
        */
        double width = min.child().getDataType().width();
        return new ColumnStatisticBuilder().setCount(1).setNdv(1).setAvgSizeByte(width)
                .setMinValue(columnStat.minValue).setMinExpr(columnStat.minExpr)
                .setMaxValue(columnStat.maxValue).setMaxExpr(columnStat.maxExpr).build();
    }

    @Override
    public ColumnStatistic visitMax(Max max, Statistics context) {
        Expression child = max.child();
        ColumnStatistic columnStat = child.accept(this, context);
        if (columnStat.isUnKnown) {
            return ColumnStatistic.UNKNOWN;
        }
        /*
        we keep columnStat.min and columnStat.max, but set ndv=1.
        if there is group-by keys, we will update count when visiting group clause
        */
        int width = max.child().getDataType().width();
        return new ColumnStatisticBuilder().setCount(1D).setNdv(1D).setAvgSizeByte(width)
                .setMinValue(columnStat.minValue).setMinExpr(columnStat.minExpr)
                .setMaxValue(columnStat.maxValue).setMaxExpr(columnStat.maxExpr)
                .build();
    }

    @Override
    public ColumnStatistic visitCount(Count count, Statistics context) {
        double width = count.getDataType().width();
        return new ColumnStatisticBuilder().setCount(1D).setAvgSizeByte(width).setNumNulls(0)
                .setDataSize(width).setMinValue(0).setMaxValue(context.getRowCount())
                .setMaxExpr(null).setMinExpr(null).build();
    }

    // TODO: return a proper estimated stat after supports histogram
    @Override
    public ColumnStatistic visitSum(Sum sum, Statistics context) {
        return sum.child().accept(this, context);
    }

    // TODO: return a proper estimated stat after supports histogram
    @Override
    public ColumnStatistic visitAvg(Avg avg, Statistics context) {
        return avg.child().accept(this, context);
    }

    @Override
    public ColumnStatistic visitYear(Year year, Statistics context) {
        ColumnStatistic childStat = year.child().accept(this, context);
        long minYear = 1970;
        long maxYear = 2038;
        return new ColumnStatisticBuilder()
                .setCount(childStat.count)
                .setNdv(maxYear - minYear + 1)
                .setAvgSizeByte(4)
                .setNumNulls(childStat.numNulls)
                .setDataSize(4 * childStat.count)
                .setMinValue(minYear)
                .setMaxValue(maxYear).setMinExpr(null).build();
    }

    @Override
    public ColumnStatistic visitWeekOfYear(WeekOfYear weekOfYear, Statistics context) {
        ColumnStatistic childStat = weekOfYear.child().accept(this, context);
        double width = weekOfYear.getDataType().width();
        return new ColumnStatisticBuilder(childStat)
                .setNdv(54)
                .setAvgSizeByte(width)
                .setNumNulls(childStat.numNulls)
                .setDataSize(1).setMinValue(1).setMaxValue(53).setMinExpr(null)
                .build();
    }

    // TODO: find a proper way to predicate stat of substring
    @Override
    public ColumnStatistic visitSubstring(Substring substring, Statistics context) {
        return substring.child(0).accept(this, context);
    }

    @Override
    public ColumnStatistic visitAlias(Alias alias, Statistics context) {
        return alias.child().accept(this, context);
    }

    @Override
    public ColumnStatistic visitVirtualReference(VirtualSlotReference virtualSlotReference, Statistics context) {
        return ColumnStatistic.UNKNOWN;
    }

    @Override
    public ColumnStatistic visitBoundFunction(BoundFunction boundFunction, Statistics context) {
        return ColumnStatistic.UNKNOWN;
    }

    @Override
    public ColumnStatistic visitAggregateExpression(AggregateExpression aggregateExpression,
            Statistics context) {
        return aggregateExpression.child().accept(this, context);
    }

    @Override
    public ColumnStatistic visitComparisonPredicate(ComparisonPredicate cp, Statistics context) {
        ColumnStatistic leftStats = cp.left().accept(this, context);
        ColumnStatistic rightStats = cp.right().accept(this, context);
        return new ColumnStatisticBuilder(leftStats)
                .setNumNulls(StatsMathUtil.maxNonNaN(leftStats.numNulls, rightStats.numNulls))
                .setHistogram(null)
                .setNdv(2).build();
    }

    @Override
    public ColumnStatistic visitCompoundPredicate(CompoundPredicate compoundPredicate, Statistics context) {
        List<Expression> childExprs = compoundPredicate.children();
        ColumnStatistic firstChild = childExprs.get(0).accept(this, context);
        double maxNull = StatsMathUtil.maxNonNaN(firstChild.numNulls, 1);
        for (int i = 1; i < childExprs.size(); i++) {
            ColumnStatistic columnStatistic = childExprs.get(i).accept(this, context);
            maxNull = StatsMathUtil.maxNonNaN(maxNull, columnStatistic.numNulls);
        }
        return new ColumnStatisticBuilder(firstChild).setNumNulls(maxNull).setNdv(2).setHistogram(null).build();
    }

    @Override
    public ColumnStatistic visitTimestampArithmetic(TimestampArithmetic arithmetic, Statistics context) {
        Operator operator = arithmetic.getOp();
        switch (operator) {
            case ADD:
                return dateAdd(arithmetic, context);
            case SUBTRACT:
                return dateSub(arithmetic, context);
            default:
                return arithmetic.left().accept(this, context);
        }
    }

    @Override
    public ColumnStatistic visitMarkJoinReference(
            MarkJoinSlotReference markJoinSlotReference, Statistics context) {
        return ColumnStatistic.UNKNOWN;
    }

    public ColumnStatistic visitNullIf(NullIf nullIf, Statistics context) {
        Expression leftChild = nullIf.left();
        return leftChild.accept(this, context);
    }

    @Override
    public ColumnStatistic visitLeast(Least least, Statistics context) {
        return least.child(0).accept(this, context);
    }

    @Override
    public ColumnStatistic visitAscii(Ascii ascii, Statistics context) {
        DataType returnType = ascii.getDataType();
        ColumnStatistic childColumnStats = ascii.child().accept(this, context);
        return new ColumnStatisticBuilder()
                .setDataSize(returnType.width() * context.getRowCount())
                .setNdv(128)
                .setMinValue(0)
                .setMaxValue(127)
                .setNumNulls(childColumnStats.numNulls)
                .setAvgSizeByte(returnType.width()).build();
    }

    @Override
    public ColumnStatistic visitQuarter(Quarter quarter, Statistics context) {
        DataType returnType = quarter.getDataType();
        ColumnStatistic childColumnStats = quarter.child().accept(this, context);
        return new ColumnStatisticBuilder()
                .setNdv(4)
                .setMinValue(1)
                .setMaxValue(4)
                .setNumNulls(childColumnStats.numNulls)
                .setAvgSizeByte(returnType.width())
                .setDataSize(returnType.width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitDayOfMonth(DayOfMonth dayOfMonth, Statistics context) {
        DataType returnType = dayOfMonth.getDataType();
        ColumnStatistic childColumnStats = dayOfMonth.child().accept(this, context);
        return new ColumnStatisticBuilder(childColumnStats).setNdv(31)
                .setAvgSizeByte(returnType.width())
                .setDataSize(returnType.width() * context.getRowCount())
                .setMaxValue(1)
                .setMaxValue(31).build();
    }

    @Override
    public ColumnStatistic visitDayOfWeek(DayOfWeek dayOfWeek, Statistics context) {
        ColumnStatistic childColumnStats = dayOfWeek.child().accept(this, context);
        return new ColumnStatisticBuilder(childColumnStats)
                .setNdv(7)
                .setMinValue(1)
                .setMaxValue(7).build();
    }

    @Override
    public ColumnStatistic visitDayOfYear(DayOfYear dayOfYear, Statistics context) {
        ColumnStatistic childColumnStats = dayOfYear.child().accept(this, context);
        return new ColumnStatisticBuilder(childColumnStats)
                .setNdv(366)
                .setMaxValue(366)
                .setAvgSizeByte(dayOfYear.getDataType().width())
                .setDataSize(dayOfYear.getDataType().width() * context.getRowCount())
                .setMinValue(1)
                .build();
    }

    @Override
    public ColumnStatistic visitHour(Hour hour, Statistics context) {
        ColumnStatistic childColumnStats = hour.child().accept(this, context);
        return new ColumnStatisticBuilder(childColumnStats)
                .setNdv(24)
                .setMinValue(0)
                .setAvgSizeByte(hour.getDataType().width())
                .setDataSize(hour.getDataType().width() * context.getRowCount())
                .setMaxValue(23).build();
    }

    @Override
    public ColumnStatistic visitMinute(Minute minute, Statistics context) {
        ColumnStatistic childColumnStats = minute.child().accept(this, context);
        return new ColumnStatisticBuilder(childColumnStats)
                .setNdv(60)
                .setMinValue(0)
                .setAvgSizeByte(minute.getDataType().width())
                .setDataSize(minute.getDataType().width() * context.getRowCount())
                .setMaxValue(59).build();
    }

    @Override
    public ColumnStatistic visitSecond(Second second, Statistics context) {
        ColumnStatistic childColumnStats = second.child().accept(this, context);
        return new ColumnStatisticBuilder(childColumnStats)
                .setNdv(60)
                .setMinValue(0)
                .setAvgSizeByte(second.getDataType().width())
                .setDataSize(second.getDataType().width() * context.getRowCount())
                .setMaxValue(59).build();
    }

    @Override
    public ColumnStatistic visitToDate(ToDate toDate, Statistics context) {
        ColumnStatistic childColumnStats = toDate.child().accept(this, context);
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(childColumnStats)
                .setAvgSizeByte(toDate.getDataType().width())
                .setDataSize(toDate.getDataType().width() * context.getRowCount());
        if (childColumnStats.minOrMaxIsInf()) {
            return columnStatisticBuilder.build();
        }
        double minValue = getDatetimeFromLong((long) childColumnStats.minValue).toLocalDate()
                .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        double maxValue = getDatetimeFromLong((long) childColumnStats.maxValue).toLocalDate()
                .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        return columnStatisticBuilder.setMaxValue(maxValue)
                .setMinValue(minValue)
                .build();
    }

    private LocalDateTime getDatetimeFromLong(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(dateTime), ZoneId.systemDefault());
    }

    @Override
    public ColumnStatistic visitToDays(ToDays toDays, Statistics context) {
        ColumnStatistic childColumnStats = toDays.child().accept(this, context);
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(childColumnStats)
                .setAvgSizeByte(toDays.getDataType().width())
                .setDataSize(toDays.getDataType().width() * context.getRowCount());
        if (childColumnStats.minOrMaxIsInf()) {
            return columnStatisticBuilder.build();
        }
        double minValue = getDatetimeFromLong((long) childColumnStats.minValue).toLocalDate().toEpochDay()
                + (double) DAYS_FROM_0_TO_1970;
        double maxValue = getDatetimeFromLong((long) childColumnStats.maxValue).toLocalDate().toEpochDay()
                + (double) DAYS_FROM_0_TO_1970;
        return columnStatisticBuilder.setMaxValue(maxValue)
                .setMinValue(minValue)
                .build();
    }

    @Override
    public ColumnStatistic visitFromDays(FromDays fromDays, Statistics context) {
        ColumnStatistic childColumnStats = fromDays.child().accept(this, context);
        double minValue = childColumnStats.minValue;
        double maxValue = childColumnStats.maxValue;
        if (minValue < DAYS_FROM_0_TO_1970) {
            minValue = LocalDate.ofEpochDay(0).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        } else {
            if (minValue > DAYS_FROM_0_TO_9999) {
                minValue = LocalDate.ofEpochDay(DAYS_FROM_0_TO_9999 - DAYS_FROM_0_TO_1970)
                        .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
            } else {
                minValue = LocalDate.ofEpochDay((long) (minValue - DAYS_FROM_0_TO_1970))
                        .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
            }
        }

        if (maxValue < DAYS_FROM_0_TO_1970) {
            maxValue = LocalDate.ofEpochDay(0).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        } else {
            if (maxValue > DAYS_FROM_0_TO_9999) {
                maxValue = LocalDate.ofEpochDay(DAYS_FROM_0_TO_9999 - DAYS_FROM_0_TO_1970)
                        .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
            } else {
                maxValue = LocalDate.ofEpochDay((long) (maxValue - DAYS_FROM_0_TO_1970))
                        .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
            }
        }
        return new ColumnStatisticBuilder(childColumnStats)
                .setMinValue(minValue)
                .setMaxValue(maxValue)
                .setAvgSizeByte(fromDays.getDataType().width())
                .setDataSize(fromDays.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitAbs(Abs abs, Statistics context) {
        ColumnStatistic childColumnStats = abs.child().accept(this, context);
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(childColumnStats);
        double max = Math.max(Math.abs(childColumnStats.minValue), Math.abs(childColumnStats.maxValue));
        double min;
        if (childColumnStats.minValue < 0 && childColumnStats.maxValue < 0
                || childColumnStats.minValue >= 0 && childColumnStats.maxValue >= 0) {
            min = Math.min(childColumnStats.minValue, childColumnStats.maxValue);
        } else {
            min = 0;
        }
        return columnStatisticBuilder
                .setMinValue(min)
                .setMaxValue(max)
                .setNdv(max - min + 1)
                .setAvgSizeByte(abs.getDataType().width())
                .setDataSize(abs.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitAcos(Acos acos, Statistics context) {
        ColumnStatistic childColumnStats = acos.child().accept(this, context);
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(childColumnStats);
        return columnStatisticBuilder
                .setMinValue(0)
                .setAvgSizeByte(acos.getDataType().width())
                .setDataSize(acos.getDataType().width() * context.getRowCount())
                .setMaxValue(Math.PI).build();
    }

    @Override
    public ColumnStatistic visitAsin(Asin asin, Statistics context) {
        ColumnStatistic columnStatistic = asin.child().accept(this, context);
        return new ColumnStatisticBuilder(columnStatistic)
                .setMinValue(-Math.PI / 2)
                .setMaxValue(Math.PI / 2)
                .setAvgSizeByte(asin.getDataType().width())
                .setDataSize(asin.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitAtan(Atan atan, Statistics context) {
        ColumnStatistic columnStatistic = atan.child().accept(this, context);

        return new ColumnStatisticBuilder(columnStatistic)
                .setMinValue(-Math.PI / 2)
                .setMaxValue(Math.PI / 2)
                .setAvgSizeByte(atan.getDataType().width())
                .setDataSize(atan.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitSqrt(Sqrt sqrt, Statistics context) {
        ColumnStatistic columnStatistic = sqrt.child().accept(this, context);
        return new ColumnStatisticBuilder(columnStatistic)
                .setMinValue(0)
                .setMaxValue(Math.sqrt(columnStatistic.maxValue))
                .setAvgSizeByte(sqrt.getDataType().width())
                .setDataSize(sqrt.getDataType().width() * context.getRowCount()).build();

    }

    @Override
    public ColumnStatistic visitRadians(Radians radians, Statistics context) {
        ColumnStatistic columnStatistic = radians.child().accept(this, context);
        return new ColumnStatisticBuilder(columnStatistic)
                .setMinValue(Math.toRadians(columnStatistic.minValue))
                .setMaxValue(Math.toRadians(columnStatistic.maxValue))
                .setAvgSizeByte(radians.getDataType().width())
                .setDataSize(radians.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitRandom(Random random, Statistics context) {
        return new ColumnStatisticBuilder()
                .setMinValue(0)
                .setMaxValue(1)
                .setNumNulls(0)
                .setHistogram(null)
                .setAvgSizeByte(random.getDataType().width())
                .setDataSize(random.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitNegative(Negative negative, Statistics context) {
        ColumnStatistic columnStatistic = negative.child(0).accept(this, context);
        return new ColumnStatisticBuilder(columnStatistic)
                .setMinValue(Math.min(-columnStatistic.minValue, -columnStatistic.maxValue))
                .setMaxValue(Math.max(-columnStatistic.minValue, -columnStatistic.maxValue))
                .setAvgSizeByte(negative.getDataType().width())
                .setDataSize(negative.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitYearsAdd(YearsAdd yearsAdd, Statistics context) {
        return dateAdd(yearsAdd, context);
    }

    @Override
    public ColumnStatistic visitMonthsAdd(MonthsAdd monthsAdd, Statistics context) {
        return dateAdd(monthsAdd, context);
    }

    @Override
    public ColumnStatistic visitDaysAdd(DaysAdd daysAdd, Statistics context) {
        return dateAdd(daysAdd, context);
    }

    @Override
    public ColumnStatistic visitMinutesAdd(MinutesAdd minutesAdd, Statistics context) {
        return dateAdd(minutesAdd, context);
    }

    @Override
    public ColumnStatistic visitSecondsAdd(SecondsAdd secondsAdd, Statistics context) {
        return dateAdd(secondsAdd, context);
    }

    @Override
    public ColumnStatistic visitYearsSub(YearsSub yearsSub, Statistics context) {
        return dateSub(yearsSub, context);
    }

    @Override
    public ColumnStatistic visitMonthsSub(MonthsSub monthsSub, Statistics context) {
        return dateSub(monthsSub, context);
    }

    @Override
    public ColumnStatistic visitDaysSub(DaysSub daysSub, Statistics context) {
        return dateSub(daysSub, context);
    }

    @Override
    public ColumnStatistic visitHoursSub(HoursSub hoursSub, Statistics context) {
        return dateSub(hoursSub, context);
    }

    @Override
    public ColumnStatistic visitMinutesSub(MinutesSub minutesSub, Statistics context) {
        return dateSub(minutesSub, context);
    }

    @Override
    public ColumnStatistic visitSecondsSub(SecondsSub secondsSub, Statistics context) {
        return dateSub(secondsSub, context);
    }

    private ColumnStatistic dateAdd(Expression date, Statistics context) {
        ColumnStatistic leftChild = date.child(0).accept(this, context);
        ColumnStatistic rightChild = date.child(1).accept(this, context);
        return new ColumnStatisticBuilder(leftChild)
                .setMinValue(leftChild.minValue + rightChild.minValue)
                .setMaxValue(leftChild.maxValue + rightChild.maxValue)
                .setAvgSizeByte(date.getDataType().width())
                .setDataSize(date.getDataType().width() * context.getRowCount()).build();
    }

    private ColumnStatistic dateSub(Expression date, Statistics context) {
        ColumnStatistic leftChild = date.child(0).accept(this, context);
        ColumnStatistic rightChild = date.child(1).accept(this, context);
        return new ColumnStatisticBuilder(leftChild)
                .setMinValue(leftChild.minValue - rightChild.minValue)
                .setMaxValue(leftChild.maxValue - rightChild.maxValue)
                .setAvgSizeByte(date.getDataType().width())
                .setDataSize(date.getDataType().width() * context.getRowCount()).build();
    }

    private ColumnStatistic dateDiff(double interval, Expression date, Statistics context) {
        ColumnStatistic leftChild = date.child(0).accept(this, context);
        ColumnStatistic rightChild = date.child(1).accept(this, context);
        return new ColumnStatisticBuilder(leftChild)
                .setMinValue((leftChild.minValue - rightChild.maxValue) / interval)
                .setMaxValue((leftChild.maxValue - rightChild.minValue) / interval)
                .setAvgSizeByte(date.getDataType().width())
                .setDataSize(date.getDataType().width() * context.getRowCount()).build();
    }

    @Override
    public ColumnStatistic visitYearsDiff(YearsDiff yearsDiff, Statistics context) {
        return dateDiff(3600 * 24 * 365, yearsDiff, context);
    }

    @Override
    public ColumnStatistic visitMonthsDiff(MonthsDiff monthsDiff, Statistics context) {
        return dateDiff(3600 * 24 * 31, monthsDiff, context);

    }

    @Override
    public ColumnStatistic visitWeeksDiff(WeeksDiff weeksDiff, Statistics context) {
        return dateDiff(3600 * 24 * 7, weeksDiff, context);
    }

    @Override
    public ColumnStatistic visitDaysDiff(DaysDiff daysDiff, Statistics context) {
        return dateDiff(3600 * 24, daysDiff, context);
    }

    @Override
    public ColumnStatistic visitHoursDiff(HoursDiff hoursDiff, Statistics context) {
        return dateDiff(3600, hoursDiff, context);
    }

    @Override
    public ColumnStatistic visitMinutesDiff(MinutesDiff minutesDiff, Statistics context) {
        return dateDiff(60, minutesDiff, context);
    }

    @Override
    public ColumnStatistic visitSecondsDiff(SecondsDiff secondsDiff, Statistics context) {
        return dateDiff(1, secondsDiff, context);
    }
}

