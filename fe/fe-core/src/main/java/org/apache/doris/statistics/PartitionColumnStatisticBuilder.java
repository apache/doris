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
import org.apache.doris.statistics.util.Hll128;

public class PartitionColumnStatisticBuilder {
    private double count;
    private Hll128 ndv;
    private double avgSizeByte;
    private double numNulls;
    private double dataSize;
    private double minValue = Double.NEGATIVE_INFINITY;
    private double maxValue = Double.POSITIVE_INFINITY;
    private LiteralExpr minExpr;
    private LiteralExpr maxExpr;
    private boolean isUnknown;
    private String updatedTime;

    public PartitionColumnStatisticBuilder() {
    }

    public PartitionColumnStatisticBuilder(PartitionColumnStatistic statistic) {
        this.count = statistic.count;
        this.ndv = statistic.ndv;
        this.avgSizeByte = statistic.avgSizeByte;
        this.numNulls = statistic.numNulls;
        this.dataSize = statistic.dataSize;
        this.minValue = statistic.minValue;
        this.maxValue = statistic.maxValue;
        this.minExpr = statistic.minExpr;
        this.maxExpr = statistic.maxExpr;
        this.isUnknown = statistic.isUnKnown;
        this.updatedTime = statistic.updatedTime;
    }

    public PartitionColumnStatisticBuilder setCount(double count) {
        this.count = count;
        return this;
    }

    public PartitionColumnStatisticBuilder setNdv(Hll128 ndv) {
        this.ndv = ndv;
        return this;
    }

    public PartitionColumnStatisticBuilder setAvgSizeByte(double avgSizeByte) {
        this.avgSizeByte = avgSizeByte;
        return this;
    }

    public PartitionColumnStatisticBuilder setNumNulls(double numNulls) {
        this.numNulls = numNulls;
        return this;
    }

    public PartitionColumnStatisticBuilder setDataSize(double dataSize) {
        this.dataSize = dataSize;
        return this;
    }

    public PartitionColumnStatisticBuilder setMinValue(double minValue) {
        this.minValue = minValue;
        return this;
    }

    public PartitionColumnStatisticBuilder setMaxValue(double maxValue) {
        this.maxValue = maxValue;
        return this;
    }

    public PartitionColumnStatisticBuilder setMinExpr(LiteralExpr minExpr) {
        this.minExpr = minExpr;
        return this;
    }

    public PartitionColumnStatisticBuilder setMaxExpr(LiteralExpr maxExpr) {
        this.maxExpr = maxExpr;
        return this;
    }

    public PartitionColumnStatisticBuilder setIsUnknown(boolean isUnknown) {
        this.isUnknown = isUnknown;
        return this;
    }

    public PartitionColumnStatisticBuilder setUpdatedTime(String updatedTime) {
        this.updatedTime = updatedTime;
        return this;
    }

    public double getCount() {
        return count;
    }

    public Hll128 getNdv() {
        return ndv;
    }

    public double getAvgSizeByte() {
        return avgSizeByte;
    }

    public double getNumNulls() {
        return numNulls;
    }

    public double getDataSize() {
        return dataSize;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public LiteralExpr getMinExpr() {
        return minExpr;
    }

    public LiteralExpr getMaxExpr() {
        return maxExpr;
    }

    public boolean isUnknown() {
        return isUnknown;
    }

    public String getUpdatedTime() {
        return updatedTime;
    }

    public PartitionColumnStatistic build() {
        dataSize = dataSize > 0 ? dataSize : Math.max((count - numNulls + 1) * avgSizeByte, 0);
        return new PartitionColumnStatistic(count, ndv, avgSizeByte, numNulls,
                dataSize, minValue, maxValue, minExpr, maxExpr,
                isUnknown, updatedTime);
    }

    public PartitionColumnStatisticBuilder merge(PartitionColumnStatistic other) {
        count += other.count;
        ndv.merge(other.ndv);
        numNulls += other.numNulls;
        if (minValue > other.minValue) {
            minValue = other.minValue;
            minExpr = other.minExpr;
        }
        if (maxValue < other.maxValue) {
            maxValue = other.maxValue;
            maxExpr = other.maxExpr;
        }
        isUnknown = isUnknown && other.isUnKnown;
        return this;
    }

    public ColumnStatistic toColumnStatistics() {
        return new ColumnStatistic(count, ndv.estimateCardinality(), null,
                avgSizeByte, numNulls, dataSize, minValue, maxValue, minExpr, maxExpr, isUnknown, updatedTime);
    }
}
