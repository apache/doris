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

import java.util.Objects;

public class StatisticRange {
    private static final double INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.25;
    private static final double INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.5;

    /**
     * {@code NaN} represents empty range ({@code high} must be {@code NaN} too)
     */
    private final double low;
    /**
     * {@code NaN} represents empty range ({@code low} must be {@code NaN} too)
     */
    private final double high;

    private final double distinctValues;

    public StatisticRange(double low, double high, double distinctValues) {
        this.low = low;
        this.high = high;
        this.distinctValues = distinctValues;
    }

    public double overlapPercentWith(StatisticRange other) {
        Objects.requireNonNull(other, "other is null");
        if (this.isEmpty() || other.isEmpty() || this.distinctValues == 0 || other.distinctValues == 0) {
            return 0.0; // zero is better than NaN as it will behave properly for calculating row count
        }

        if (this.equals(other) && !isBothInfinite()) {
            return 1.0;
        }

        double lengthOfIntersect = Math.min(this.high, other.high) - Math.max(this.low, other.low);
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

    public static StatisticRange empty() {
        return new StatisticRange(Double.NaN, Double.NaN, 0);
    }

    public boolean isEmpty() {
        return Double.isNaN(low) && Double.isNaN(high);
    }

    public boolean isBothInfinite() {
        return Double.isInfinite(low) && Double.isInfinite(high);
    }

    public static StatisticRange from(ColumnStatistic column) {
        return new StatisticRange(column.minValue, column.maxValue, column.ndv);
    }

    public double getLow() {
        return low;
    }

    public double getHigh() {
        return high;
    }

    public double length() {
        return this.high - this.low;
    }

    public StatisticRange intersect(StatisticRange other) {
        double newLow = Math.max(low, other.low);
        double newHigh = Math.min(high, other.high);
        if (newLow <= newHigh) {
            return new StatisticRange(newLow, newHigh, overlappingDistinctValues(other));
        }
        return empty();
    }

    public StatisticRange union(StatisticRange other) {
        double overlapPercentThis = this.overlapPercentWith(other);
        double overlapPercentOther = other.overlapPercentWith(this);
        double overlapNDVThis = overlapPercentThis * distinctValues;
        double overlapNDVOther = overlapPercentOther * other.distinctValues;
        double maxOverlapNDV = Math.max(overlapNDVThis, overlapNDVOther);
        double newNDV = maxOverlapNDV + ((1 - overlapPercentThis) * distinctValues)
                + ((1 - overlapPercentOther) * other.distinctValues);
        return new StatisticRange(Math.min(low, other.low), Math.max(high, other.high), newNDV);
    }

    private double overlappingDistinctValues(StatisticRange other) {
        double overlapPercentOfLeft = overlapPercentWith(other);
        double overlapPercentOfRight = other.overlapPercentWith(this);
        double overlapDistinctValuesLeft = overlapPercentOfLeft * distinctValues;
        double overlapDistinctValuesRight = overlapPercentOfRight * other.distinctValues;
        double minInputDistinctValues = minExcludeNaN(this.distinctValues, other.distinctValues);

        return minExcludeNaN(minInputDistinctValues,
                maxExcludeNaN(overlapDistinctValuesLeft, overlapDistinctValuesRight));
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

    public static StatisticRange fromColumnStatistics(ColumnStatistic columnStatistic) {
        return new StatisticRange(columnStatistic.minValue, columnStatistic.maxValue, columnStatistic.ndv);
    }
}
