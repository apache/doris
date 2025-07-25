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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.coercion.CharacterType;

import java.util.Map;

public class ColumnStatisticBuilder {
    private double count;
    private double ndv;
    private double avgSizeByte;
    private double numNulls;
    private double dataSize;
    private double minValue = Double.NEGATIVE_INFINITY;
    private double maxValue = Double.POSITIVE_INFINITY;
    private LiteralExpr minExpr;
    private LiteralExpr maxExpr;

    private boolean isUnknown;

    private ColumnStatistic original;

    private String updatedTime;
    private Map<Literal, Float> hotValues;

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
        this.minExpr = columnStatistic.minExpr;
        this.maxExpr = columnStatistic.maxExpr;
        this.isUnknown = columnStatistic.isUnKnown;
        this.original = columnStatistic.original;
        this.updatedTime = columnStatistic.updatedTime;
        this.hotValues = columnStatistic.hotValues;
    }

    // ATTENTION: DON'T USE FOLLOWING TWO DURING STATS DERIVING EXCEPT FOR INITIALIZATION
    public ColumnStatisticBuilder(double count) {
        this.count = count;
    }

    public ColumnStatisticBuilder(ColumnStatistic columnStatistic, double count) {
        this.count = count;
        this.ndv = columnStatistic.ndv;
        this.avgSizeByte = columnStatistic.avgSizeByte;
        this.numNulls = columnStatistic.numNulls;
        this.dataSize = columnStatistic.dataSize;
        this.minValue = columnStatistic.minValue;
        this.maxValue = columnStatistic.maxValue;
        this.minExpr = columnStatistic.minExpr;
        this.maxExpr = columnStatistic.maxExpr;
        this.isUnknown = columnStatistic.isUnKnown;
        this.original = columnStatistic.original;
        this.updatedTime = columnStatistic.updatedTime;
        this.hotValues = columnStatistic.hotValues;
    }

    public ColumnStatisticBuilder setNdv(double ndv) {
        this.ndv = ndv;
        return this;
    }

    public ColumnStatisticBuilder setOriginal(ColumnStatistic original) {
        this.original = original;
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

    public ColumnStatisticBuilder setHotValues(Map<Literal, Float> hotValues) {
        this.hotValues = hotValues;
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

    public ColumnStatisticBuilder setUpdatedTime(String updatedTime) {
        this.updatedTime = updatedTime;
        return this;
    }

    public Map<Literal, Float> getHotValues() {
        return hotValues;
    }

    public ColumnStatistic build() {
        dataSize = dataSize > 0 ? dataSize : Math.max((count - numNulls + 1) * avgSizeByte, 0);
        if (original == null && !isUnknown) {
            original = new ColumnStatistic(count, ndv, null, avgSizeByte, numNulls,
                    dataSize, minValue, maxValue, minExpr, maxExpr,
                    isUnknown, updatedTime, hotValues);
        }
        ColumnStatistic colStats = new ColumnStatistic(count, ndv, original, avgSizeByte, numNulls,
                dataSize, minValue, maxValue, minExpr, maxExpr,
                isUnknown, updatedTime, hotValues);
        return colStats;
    }

    public void normalizeAvgSizeByte(SlotReference slot) {
        if (isUnknown) {
            return;
        }
        if (avgSizeByte > 0) {
            return;
        }
        avgSizeByte = slot.getDataType().toCatalogDataType().getSlotSize();
        // When defining SQL schemas, users often tend to set the length of string \
        // fields much longer than actually needed for storage.
        if (slot.getDataType() instanceof CharacterType) {
            avgSizeByte = Math.min(avgSizeByte, CharacterType.DEFAULT_WIDTH);
        }
    }
}
