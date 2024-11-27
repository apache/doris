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

package org.apache.doris.statistics;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.types.DataType;

import java.util.Objects;

public class StatisticRange {
    private static final double INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.25;
    private static final double INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.5;

    /**
     * {@code NaN} represents empty range ({@code high} must be {@code NaN} too)
     */
    private final double low;

    private final LiteralExpr lowExpr;
    /**
     * {@code NaN} represents empty range ({@code low} must be {@code NaN} too)
     */
    private final double high;

    private final LiteralExpr highExpr;

    private final double distinctValues;

    private final DataType dataType;

    private final boolean isEmpty;

    public StatisticRange(double low, LiteralExpr lowExpr, double high, LiteralExpr highExpr,
                          double distinctValues, DataType dataType) {
        this(low, lowExpr, high, highExpr, distinctValues, dataType, false);
    }

    private StatisticRange(double low, LiteralExpr lowExpr, double high, LiteralExpr highExpr,
                          double distinctValues, DataType dataType, boolean isEmpty) {
        this.low = low;
        this.lowExpr = lowExpr;
        this.high = high;
        this.highExpr = highExpr;
        this.distinctValues = distinctValues;
        this.dataType = dataType;
        this.isEmpty = isEmpty;
    }

    public LiteralExpr getLowExpr() {
        return lowExpr;
    }

    public LiteralExpr getHighExpr() {
        return highExpr;
    }

    public DataType getDataType() {
        return dataType;
    }

    public double overlapPercentWith(StatisticRange other) {
        Objects.requireNonNull(other, "other is null");
        if (this.isEmpty() || other.isEmpty() || this.distinctValues == 0 || other.distinctValues == 0) {
            return 0.0; // zero is better than NaN as it will behave properly for calculating row count
        }

        if (this.equals(other) && !isBothInfinite()) {
            return 1.0;
        }

        double lengthOfIntersect = dataType.rangeLength(Math.min(this.high, other.high), Math.max(this.low, other.low));
        if (Double.isInfinite(lengthOfIntersect)) {
            if (Double.isFinite(this.distinctValues) && Double.isFinite(other.distinctValues)) {
                return Math.min(other.distinctValues / this.distinctValues, 1);
            }
            return INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
        }
        if (lengthOfIntersect == 0) {
            return 1 / Math.max(this.distinctValues, 1);
        }
        if (lengthOfIntersect < 0) {
            return 0;
        }
        double length = length();
        if (Double.isInfinite(length)) {
            return INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
        }
        if (lengthOfIntersect > 0) {
            return lengthOfIntersect / length;
        }
        return INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
    }

    public static StatisticRange empty(DataType dataType) {
        return new StatisticRange(Double.NEGATIVE_INFINITY, null, Double.POSITIVE_INFINITY,
                null, 0, dataType, true);
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public boolean isBothInfinite() {
        return Double.isInfinite(low) && Double.isInfinite(high);
    }

    public boolean isInfinite() {
        return Double.isInfinite(low) || Double.isInfinite(high);
    }

    public boolean isOneSideInfinite() {
        return isInfinite() && !isBothInfinite();
    }

    public boolean isFinite() {
        return Double.isFinite(low) && Double.isFinite(high);
    }

    public static StatisticRange from(ColumnStatistic colStats, DataType dataType) {
        return new StatisticRange(colStats.minValue, colStats.minExpr, colStats.maxValue, colStats.maxExpr,
                colStats.ndv, dataType);
    }

    public double getLow() {
        return low;
    }

    public double getHigh() {
        return high;
    }

    public double length() {
        return dataType.rangeLength(this.high, this.low);
    }

    public StatisticRange intersect(StatisticRange other) {
        Pair<Double, LiteralExpr> biggerLow = maxPair(low, lowExpr, other.low, other.lowExpr);
        double newLow = biggerLow.first;
        LiteralExpr newLowExpr = biggerLow.second;

        Pair<Double, LiteralExpr> smallerHigh = minPair(high, highExpr, other.high, other.highExpr);
        double newHigh = smallerHigh.first;
        LiteralExpr newHighExpr = smallerHigh.second;
        if (newLow <= newHigh) {
            return new StatisticRange(newLow, newLowExpr, newHigh, newHighExpr,
                    overlappingDistinctValues(other), dataType);
        }
        return empty(dataType);
    }

    public Pair<Double, LiteralExpr> minPair(double r1, LiteralExpr e1, double r2, LiteralExpr e2) {
        if (r1 < r2) {
            return Pair.of(r1, e1);
        }
        return Pair.of(r2, e2);
    }

    public Pair<Double, LiteralExpr> maxPair(double r1, LiteralExpr e1, double r2, LiteralExpr e2) {
        if (r1 > r2) {
            return Pair.of(r1, e1);
        }
        return Pair.of(r2, e2);
    }

    public StatisticRange cover(StatisticRange other) {
        StatisticRange resultRange;
        Pair<Double, LiteralExpr> biggerLow = maxPair(low, lowExpr, other.low, other.lowExpr);
        double newLow = biggerLow.first;
        LiteralExpr newLowExpr = biggerLow.second;
        Pair<Double, LiteralExpr> smallerHigh = minPair(high, highExpr, other.high, other.highExpr);
        double newHigh = smallerHigh.first;
        LiteralExpr newHighExpr = smallerHigh.second;

        if (newLow <= newHigh) {
            double overlapPercentOfLeft = overlapPercentWith(other);
            double overlapDistinctValuesLeft = overlapPercentOfLeft * distinctValues;
            double coveredDistinctValues = minExcludeNaN(distinctValues, overlapDistinctValuesLeft);
            if (this.isBothInfinite() && other.isOneSideInfinite()) {
                resultRange = new StatisticRange(newLow, newLowExpr, newHigh, newHighExpr,
                        distinctValues * INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR,
                        dataType);
            } else {
                resultRange = new StatisticRange(newLow, newLowExpr, newHigh, newHighExpr, coveredDistinctValues,
                        dataType);
            }
        } else {
            resultRange = empty(dataType);
        }
        return resultRange;
    }

    public StatisticRange union(StatisticRange other) {
        double overlapPercentThis = this.overlapPercentWith(other);
        double overlapPercentOther = other.overlapPercentWith(this);
        double overlapNDVThis = overlapPercentThis * distinctValues;
        double overlapNDVOther = overlapPercentOther * other.distinctValues;
        double maxOverlapNDV = Math.max(overlapNDVThis, overlapNDVOther);
        double newNDV = maxOverlapNDV + ((1 - overlapPercentThis) * distinctValues)
                + ((1 - overlapPercentOther) * other.distinctValues);
        Pair<Double, LiteralExpr> smallerMin = minPair(low, lowExpr, other.low, other.lowExpr);
        Pair<Double, LiteralExpr> biggerHigh = maxPair(high, highExpr, other.high, other.highExpr);
        return new StatisticRange(smallerMin.first, smallerMin.second,
                biggerHigh.first, biggerHigh.second, newNDV, dataType);
    }

    private double overlappingDistinctValues(StatisticRange other) {
        double overlapPercentOfLeft = overlapPercentWith(other);
        double overlapPercentOfRight = other.overlapPercentWith(this);
        double overlapDistinctValuesLeft = overlapPercentOfLeft * distinctValues;
        double overlapDistinctValuesRight = overlapPercentOfRight * other.distinctValues;
        return minExcludeNaN(overlapDistinctValuesLeft, overlapDistinctValuesRight);
    }

    public static double minExcludeNaN(double v1, double v2) {
        if (Double.isNaN(v1)) {
            return v2;
        }
        if (Double.isNaN(v2)) {
            return v1;
        }
        return Math.min(v1, v2);
    }

    public static double maxExcludeNaN(double v1, double v2) {
        if (Double.isNaN(v1)) {
            return v2;
        }
        if (Double.isNaN(v2)) {
            return v1;
        }
        return Math.max(v1, v2);
    }

    public double getDistinctValues() {
        return distinctValues;
    }

    @Override
    public String toString() {
        return "range=(" + lowExpr + "," + highExpr + "), ndv=" + distinctValues;
    }
}
