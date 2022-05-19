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

/**
 * Statistics.
 */
public class StatisticsEstimate {
    private final double outputRowCount;

    private final boolean confident;

    /**
     * Constructor.
     *
     * @param outputRowCount output RowCount.
     * @param confident      statistics is inaccurate.
     */
    private StatisticsEstimate(double outputRowCount, boolean confident) {
        this.outputRowCount = outputRowCount;
        this.confident = confident;
    }

    public double getOutputRowCount() {
        return outputRowCount;
    }

    public boolean isConfident() {
        return confident;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder buildFrom(StatisticsEstimate other) {
        return new Builder(other.getOutputRowCount(), other.isConfident());
    }

    /**
     * Builder.
     */
    public static final class Builder {
        private double outputRowCount;
        private boolean confident;

        public Builder() {
            this(Double.NaN, false);
        }

        private Builder(double outputRowCount, boolean confident) {
            this.outputRowCount = outputRowCount;
            this.confident = confident;
        }

        public Builder setOutputRowCount(double outputRowCount) {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setConfident(boolean confident) {
            this.confident = confident;
            return this;
        }

        public StatisticsEstimate build() {
            return new StatisticsEstimate(outputRowCount, confident);
        }
    }
}
