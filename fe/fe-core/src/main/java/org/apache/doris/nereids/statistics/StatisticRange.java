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

package org.apache.doris.nereids.statistics;

import com.google.common.base.Preconditions;

/**
 * Calculate range with statistics.
 */
public class StatisticRange {
    private static final double INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.25;
    private static final double INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.5;

    private final double low;
    private final double high;
    private final double distinctValues;

    /**
     * Constructor.
     */
    public StatisticRange(double low, double high, double distinctValues) {
        Preconditions.checkArgument(low <= high || (Double.isNaN(low) && Double.isNaN(high)),
                "low value must be less than or equal to high value or both values have to be NaN,"
                        + " got %s and %s respectively", low, high);
        Preconditions.checkArgument(distinctValues >= 0 || Double.isNaN(distinctValues),
                "Distinct values count should be non-negative, got: %s", distinctValues);

        this.low = low;
        this.high = high;
        this.distinctValues = distinctValues;
    }

    public static StatisticRange empty() {
        return new StatisticRange(Double.NaN, Double.NaN, 0);
    }

    public double getLow() {
        return low;
    }

    public double getHigh() {
        return high;
    }

    public double getDistinctValues() {
        return distinctValues;
    }

    public double length() {
        return this.high - this.low;
    }

    public boolean isEmpty() {
        return Double.isNaN(low) && Double.isNaN(high);
    }
}
