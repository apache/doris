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
 * VariableStatsEstimate.
 */
public class VariableStatsEstimate {
    private final double lowValue;
    private final double highValue;
    private final double nullsFraction;
    private final double averageRowSize;
    private final double distinctValuesCount;

    /**
     * Constructor.
     */
    public VariableStatsEstimate(double lowValue, double highValue, double nullsFraction, double averageRowSize,
            double distinctValuesCount) {
        Preconditions.checkArgument(lowValue <= highValue || (Double.isNaN(lowValue) && Double.isNaN(highValue)),
                "low value must be less than or equal to high value or both values have to be NaN, got %s and %s respectively",
                lowValue, highValue);
        Preconditions.checkArgument((0 <= nullsFraction && nullsFraction <= 1.) || Double.isNaN(nullsFraction),
                "Nulls fraction should be within [0, 1] or NaN, got: %s", nullsFraction);
        Preconditions.checkArgument(averageRowSize >= 0 || Double.isNaN(averageRowSize),
                "Average row size should be non-negative or NaN, got: %s", averageRowSize);
        Preconditions.checkArgument(distinctValuesCount >= 0 || Double.isNaN(distinctValuesCount),
                "Distinct values count should be non-negative, got: %s", distinctValuesCount);

        this.lowValue = lowValue;
        this.highValue = highValue;
        boolean isEmptyRange = Double.isNaN(lowValue) && Double.isNaN(highValue);
        this.nullsFraction = isEmptyRange ? 1.0 : nullsFraction;
        this.averageRowSize = averageRowSize;
        this.distinctValuesCount = distinctValuesCount;
    }
}
