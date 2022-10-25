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
import org.apache.doris.statistics.ColumnStat;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

/**
 * Used to estimate for expressions that not producing boolean value.
 */
public class ExpressionEstimation extends ExpressionVisitor<ColumnStat, StatsDeriveResult> {

    private static ExpressionEstimation INSTANCE = new ExpressionEstimation();

    /**
     * returned columnStat is newly created or a copy of stats
     */
    public static ColumnStat estimate(Expression expression, StatsDeriveResult stats) {
        return INSTANCE.visit(expression, stats);
    }

    @Override
    public ColumnStat visit(Expression expr, StatsDeriveResult context) {
        return expr.accept(this, context);
    }

    //TODO: case-when need to re-implemented
    @Override
    public ColumnStat visitCaseWhen(CaseWhen caseWhen, StatsDeriveResult context) {
        ColumnStat columnStat = new ColumnStat();
        columnStat.setNdv(caseWhen.getWhenClauses().size() + 1);
        columnStat.setSelectivity(1.0);
        columnStat.setMinValue(0);
        columnStat.setMaxValue(Double.MAX_VALUE);
        columnStat.setAvgSizeByte(8);
        columnStat.setNumNulls(0);
        columnStat.setMaxSizeByte(8);
        return columnStat;
    }

    public ColumnStat visitCast(Cast cast, StatsDeriveResult context) {
        return cast.child().accept(this, context);
    }

    @Override
    public ColumnStat visitLiteral(Literal literal, StatsDeriveResult context) {
        if (ColumnStat.MAX_MIN_UNSUPPORTED_TYPE.contains(literal.getDataType().toCatalogDataType())) {
            return ColumnStat.UNKNOWN;
        }
        double literalVal = literal.getDouble();
        ColumnStat columnStat = new ColumnStat();
        columnStat.setMaxValue(literalVal);
        columnStat.setMinValue(literalVal);
        columnStat.setNdv(1);
        columnStat.setNumNulls(1);
        columnStat.setAvgSizeByte(1);
        return columnStat;
    }

    @Override
    public ColumnStat visitSlotReference(SlotReference slotReference, StatsDeriveResult context) {
        ColumnStat columnStat = context.getColumnStatsBySlot(slotReference);
        Preconditions.checkState(columnStat != null);
        return columnStat.copy();
    }

    @Override
    public ColumnStat visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, StatsDeriveResult context) {
        ColumnStat leftColStats = binaryArithmetic.left().accept(this, context);
        ColumnStat rightColStats = binaryArithmetic.right().accept(this, context);
        double leftNdv = leftColStats.getNdv();
        double rightNdv = rightColStats.getNdv();
        double ndv = Math.max(leftNdv, rightNdv);
        double leftNullCount = leftColStats.getNumNulls();
        double rightNullCount = rightColStats.getNumNulls();
        double rowCount = context.getRowCount();
        double numNulls = context.getRowCount()
                * (1 - (1 - (leftNullCount / rowCount) * (1 - rightNullCount / rowCount)));
        double leftMax = leftColStats.getMaxValue();
        double rightMax = rightColStats.getMaxValue();
        double leftMin = leftColStats.getMinValue();
        double rightMin = rightColStats.getMinValue();

        if (binaryArithmetic instanceof Add) {
            return new ColumnStat(ndv, leftColStats.getAvgSizeByte(), leftColStats.getMaxSizeByte(),
                    numNulls, leftMin + rightMin, leftMax + rightMax);
        }
        if (binaryArithmetic instanceof Subtract) {
            return new ColumnStat(ndv, leftColStats.getAvgSizeByte(), leftColStats.getMaxSizeByte(),
                    numNulls, leftMin - rightMax, leftMax - rightMin);
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
            return new ColumnStat(ndv, leftColStats.getAvgSizeByte(), leftColStats.getMaxSizeByte(),
                    numNulls, min, max);
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
            return new ColumnStat(ndv, leftColStats.getAvgSizeByte(), leftColStats.getMaxSizeByte(),
                    numNulls, min, max);
        }
        return ColumnStat.UNKNOWN;
    }

    private double noneZeroDivisor(double d) {
        return d == 0.0 ? 1.0 : d;
    }

    @Override
    public ColumnStat visitMin(Min min, StatsDeriveResult context) {
        Expression child = min.child();
        ColumnStat columnStat = child.accept(this, context);
        if (columnStat == ColumnStat.UNKNOWN) {
            return ColumnStat.UNKNOWN;
        }
        return new ColumnStat(1, min.child().getDataType().width(),
                min.child().getDataType().width(), 1, columnStat.getMinValue(), columnStat.getMinValue());
    }

    @Override
    public ColumnStat visitMax(Max max, StatsDeriveResult context) {
        Expression child = max.child();
        ColumnStat columnStat = child.accept(this, context);
        if (columnStat == ColumnStat.UNKNOWN) {
            return ColumnStat.UNKNOWN;
        }
        return new ColumnStat(1, max.child().getDataType().width(),
                max.child().getDataType().width(), 0, columnStat.getMaxValue(), columnStat.getMaxValue());
    }

    @Override
    public ColumnStat visitCount(Count count, StatsDeriveResult context) {
        //count() returns long type
        return new ColumnStat(1.0, 8.0, 8.0, 0.0,
                Double.MIN_VALUE, Double.MAX_VALUE);
    }

    // TODO: return a proper estimated stat after supports histogram
    @Override
    public ColumnStat visitSum(Sum sum, StatsDeriveResult context) {
        return sum.child().accept(this, context);
    }

    // TODO: return a proper estimated stat after supports histogram
    @Override
    public ColumnStat visitAvg(Avg avg, StatsDeriveResult context) {
        return avg.child().accept(this, context);
    }

    @Override
    public ColumnStat visitYear(Year year, StatsDeriveResult context) {
        ColumnStat childStat = year.child().accept(this, context);
        double maxVal = childStat.getMaxValue();
        double minVal = childStat.getMinValue();
        long minYear = Utils.getLocalDatetimeFromLong((long) minVal).getYear();
        long maxYear = Utils.getLocalDatetimeFromLong((long) maxVal).getYear();
        return new ColumnStat(childStat.getNdv(), 4, 4,
                maxYear - minYear + 1, minYear, maxYear);
    }

    @Override
    public ColumnStat visitWeekOfYear(WeekOfYear weekOfYear, StatsDeriveResult context) {
        ColumnStat childStat = weekOfYear.child().accept(this, context);
        return new ColumnStat(52, 2, 2, childStat.getNumNulls(), 1, 52);
    }

    // TODO: find a proper way to predicate stat of substring
    @Override
    public ColumnStat visitSubstring(Substring substring, StatsDeriveResult context) {
        return substring.child(0).accept(this, context);
    }

    @Override
    public ColumnStat visitAlias(Alias alias, StatsDeriveResult context) {
        return alias.child().accept(this, context);
    }
}
