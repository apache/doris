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

public class ColumnStatisticBuilder {
    private double count;
    private double ndv;
    private double avgSizeByte;
    private double numNulls;
    private double dataSize;
    private double minValue;
    private double maxValue;
    private double selectivity = 1.0;
    private LiteralExpr minExpr;
    private LiteralExpr maxExpr;

    private boolean isUnknown;

    public ColumnStatisticBuilder() {
    }

    public ColumnStatisticBuilder(ColumnStatistic columnStatistic) {
        this.count = columnStatistic.count;
        this.ndv = columnStatistic.ndv;
        this.avgSizeByte = columnStatistic.avgSizeByte;
        this.numNulls = columnStatistic.numNulls;
        this.dataSize = columnStatistic.dataSize;
        this.minValue = columnStatistic.minValue;
        this.maxValue = columnStatistic.maxValue;
        this.selectivity = columnStatistic.selectivity;
        this.minExpr = columnStatistic.minExpr;
        this.maxExpr = columnStatistic.maxExpr;
        this.isUnknown = columnStatistic.isUnKnown;
    }

    public ColumnStatisticBuilder setCount(double count) {
        this.count = count;
        return this;
    }

    public ColumnStatisticBuilder setNdv(double ndv) {
        this.ndv = ndv;
        return this;
    }

    public ColumnStatisticBuilder setAvgSizeByte(double avgSizeByte) {
        this.avgSizeByte = avgSizeByte;
        return this;
    }

    public ColumnStatisticBuilder setNumNulls(double numNulls) {
        this.numNulls = numNulls;
        return this;
    }

    public ColumnStatisticBuilder setDataSize(double dataSize) {
        this.dataSize = dataSize;
        return this;
    }

    public ColumnStatisticBuilder setMinValue(double minValue) {
        this.minValue = minValue;
        return this;
    }

    public ColumnStatisticBuilder setMaxValue(double maxValue) {
        this.maxValue = maxValue;
        return this;
    }

    public ColumnStatisticBuilder setSelectivity(double selectivity) {
        this.selectivity = selectivity;
        return this;
    }

    public ColumnStatisticBuilder setMinExpr(LiteralExpr minExpr) {
        this.minExpr = minExpr;
        return this;
    }

    public ColumnStatisticBuilder setMaxExpr(LiteralExpr maxExpr) {
        this.maxExpr = maxExpr;
        return this;
    }

    public ColumnStatisticBuilder setIsUnknown(boolean isUnknown) {
        this.isUnknown = isUnknown;
        return this;
    }

    public double getCount() {
        return count;
    }

    public double getNdv() {
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

    public double getSelectivity() {
        return selectivity;
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

    public ColumnStatistic build() {
        return new ColumnStatistic(count, ndv, avgSizeByte, numNulls, dataSize, minValue, maxValue, selectivity,
                minExpr, maxExpr, isUnknown);
    }
}
