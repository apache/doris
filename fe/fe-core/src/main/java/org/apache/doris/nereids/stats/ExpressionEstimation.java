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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekOfYear;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

/**
 * Used to estimate for expressions that not producing boolean value.
 */
public class ExpressionEstimation extends ExpressionVisitor<ColumnStatistic, StatsDeriveResult> {

    private static ExpressionEstimation INSTANCE = new ExpressionEstimation();

    /**
     * returned columnStat is newly created or a copy of stats
     */
    public static ColumnStatistic estimate(Expression expression, StatsDeriveResult stats) {
        return INSTANCE.visit(expression, stats);
    }

    @Override
    public ColumnStatistic visit(Expression expr, StatsDeriveResult context) {
        return expr.accept(this, context);
    }

    //TODO: case-when need to re-implemented
    @Override
    public ColumnStatistic visitCaseWhen(CaseWhen caseWhen, StatsDeriveResult context) {
        ColumnStatisticBuilder columnStat = new ColumnStatisticBuilder();
        columnStat.setNdv(caseWhen.getWhenClauses().size() + 1);
        columnStat.setMinValue(0);
        columnStat.setMaxValue(Double.MAX_VALUE);
        columnStat.setAvgSizeByte(8);
        columnStat.setNumNulls(0);
        return columnStat.build();
    }

    public ColumnStatistic visitCast(Cast cast, StatsDeriveResult context) {
        return cast.child().accept(this, context);
    }

    @Override
    public ColumnStatistic visitLiteral(Literal literal, StatsDeriveResult context) {
        if (ColumnStatistic.MAX_MIN_UNSUPPORTED_TYPE.contains(literal.getDataType().toCatalogDataType())) {
            return ColumnStatistic.DEFAULT;
        }
        double literalVal = literal.getDouble();
        ColumnStatisticBuilder columnStatBuilder = new ColumnStatisticBuilder();
        columnStatBuilder.setMaxValue(literalVal);
        columnStatBuilder.setMinValue(literalVal);
        columnStatBuilder.setNdv(1);
        columnStatBuilder.setNumNulls(1);
        columnStatBuilder.setAvgSizeByte(1);
        return columnStatBuilder.build();
    }

    @Override
    public ColumnStatistic visitSlotReference(SlotReference slotReference, StatsDeriveResult context) {
        ColumnStatistic columnStat = context.getColumnStatsBySlot(slotReference);
        Preconditions.checkState(columnStat != null);
        return columnStat.copy();
    }

    @Override
    public ColumnStatistic visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, StatsDeriveResult context) {
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
        double dataSize = binaryArithmetic.getDataType().width() * rowCount;
        if (binaryArithmetic instanceof Add) {
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv).setAvgSizeByte(leftColStats.avgSizeByte)
                    .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(leftMin + rightMin)
                    .setMaxValue(leftMax + rightMax).setSelectivity(1.0)
                    .setMinExpr(null).setMaxExpr(null).build();
        }
        if (binaryArithmetic instanceof Subtract) {
            return new ColumnStatisticBuilder().setCount(rowCount).setNdv(ndv).setAvgSizeByte(leftColStats.avgSizeByte)
                    .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(leftMin - rightMax)
                    .setMaxValue(leftMax - rightMin).setSelectivity(1.0).setMinExpr(null)
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
                    .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(min).setMaxValue(max).setSelectivity(1.0)
                    .setMaxExpr(null).setMinExpr(null).build();
        }
        if (binaryArithmetic instanceof Divide) {
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
                    .setMaxValue(max).setSelectivity(1.0).setMaxExpr(null).setMinExpr(null).build();
        }
        return ColumnStatistic.DEFAULT;
    }

    private double noneZeroDivisor(double d) {
        return d == 0.0 ? 1.0 : d;
    }

    @Override
    public ColumnStatistic visitMin(Min min, StatsDeriveResult context) {
        Expression child = min.child();
        ColumnStatistic columnStat = child.accept(this, context);
        if (columnStat == ColumnStatistic.DEFAULT) {
            return ColumnStatistic.DEFAULT;
        }
        /*
        we keep columnStat.min and columnStat.max, but set ndv=1.
        if there is group-by keys, we will update ndv when visiting group clause
        */
        double width = min.child().getDataType().width();
        return new ColumnStatisticBuilder().setCount(1).setNdv(1).setAvgSizeByte(width).setNumNulls(width)
                .setDataSize(child.getDataType().width()).setMinValue(columnStat.minValue)
                .setMaxValue(columnStat.maxValue).setSelectivity(1.0)
                .setMinExpr(null).build();
    }

    @Override
    public ColumnStatistic visitMax(Max max, StatsDeriveResult context) {
        Expression child = max.child();
        ColumnStatistic columnStat = child.accept(this, context);
        if (columnStat == ColumnStatistic.DEFAULT) {
            return ColumnStatistic.DEFAULT;
        }
        /*
        we keep columnStat.min and columnStat.max, but set ndv=1.
        if there is group-by keys, we will update ndv when visiting group clause
        */
        int width = max.child().getDataType().width();
        return new ColumnStatisticBuilder().setCount(1D).setNdv(1D).setAvgSizeByte(width).setNumNulls(0)
                .setDataSize(width).setMinValue(columnStat.minValue).setMaxValue(columnStat.maxValue)
                .setSelectivity(1.0).setMaxExpr(null).setMinExpr(null).build();
    }

    @Override
    public ColumnStatistic visitCount(Count count, StatsDeriveResult context) {
        if (count.isStar()) {
            return ColumnStatistic.DEFAULT;
        }
        Expression child = count.child(0);
        ColumnStatistic columnStat = child.accept(this, context);
        if (columnStat == ColumnStatistic.DEFAULT) {
            return ColumnStatistic.DEFAULT;
        }
        double expectedValue = context.getRowCount() - columnStat.numNulls;
        double width = (double) count.getDataType().width();
        return new ColumnStatisticBuilder().setCount(1D).setNdv(1D).setAvgSizeByte(width).setNumNulls(0)
                .setDataSize(width).setMinValue(expectedValue).setMaxValue(expectedValue).setSelectivity(1.0)
                .setMaxExpr(null).setMinExpr(null).build();
    }

    // TODO: return a proper estimated stat after supports histogram
    @Override
    public ColumnStatistic visitSum(Sum sum, StatsDeriveResult context) {
        return sum.child().accept(this, context);
    }

    // TODO: return a proper estimated stat after supports histogram
    @Override
    public ColumnStatistic visitAvg(Avg avg, StatsDeriveResult context) {
        return avg.child().accept(this, context);
    }

    @Override
    public ColumnStatistic visitYear(Year year, StatsDeriveResult context) {
        ColumnStatistic childStat = year.child().accept(this, context);
        double maxVal = childStat.maxValue;
        double minVal = childStat.minValue;
        long minYear = Utils.getLocalDatetimeFromLong((long) minVal).getYear();
        long maxYear = Utils.getLocalDatetimeFromLong((long) maxVal).getYear();
        return new ColumnStatisticBuilder().setCount(childStat.count).setNdv(childStat.ndv).setAvgSizeByte(4)
                .setNumNulls(childStat.numNulls).setDataSize(maxYear - minYear + 1).setMinValue(minYear)
                .setMaxValue(maxYear).setSelectivity(1.0).setMinExpr(null).build();
    }

    @Override
    public ColumnStatistic visitWeekOfYear(WeekOfYear weekOfYear, StatsDeriveResult context) {
        ColumnStatistic childStat = weekOfYear.child().accept(this, context);
        double width = weekOfYear.getDataType().width();
        return new ColumnStatisticBuilder().setCount(52).setNdv(2).setAvgSizeByte(width).setNumNulls(childStat.numNulls)
                .setDataSize(1).setMinValue(1).setMaxValue(52).setSelectivity(1.0).setMinExpr(null)
                .build();
    }

    // TODO: find a proper way to predicate stat of substring
    @Override
    public ColumnStatistic visitSubstring(Substring substring, StatsDeriveResult context) {
        return substring.child(0).accept(this, context);
    }

    @Override
    public ColumnStatistic visitAlias(Alias alias, StatsDeriveResult context) {
        return alias.child().accept(this, context);
    }

    @Override
    public ColumnStatistic visitVirtualReference(VirtualSlotReference virtualSlotReference, StatsDeriveResult context) {
        return ColumnStatistic.DEFAULT;
    }

    @Override
    public ColumnStatistic visitBoundFunction(BoundFunction boundFunction, StatsDeriveResult context) {
        return ColumnStatistic.DEFAULT;
    }
}
