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

import org.apache.doris.common.Id;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This structure is maintained in each operator to store the statistical information results obtained by the operator.
 */
public class StatsDeriveResult {
    private double rowCount = -1;
    // The data size of the corresponding column in the operator
    // The actual key is slotId
    private final Map<Id, Float> columnIdToDataSize = Maps.newHashMap();
    // The ndv of the corresponding column in the operator
    // The actual key is slotId
    private final Map<Id, Long> columnIdToNdv = Maps.newHashMap();

    private Map<Slot, ColumnStat> slotToColumnStats;
    private int width = 1;
    private double penalty = 0.0;
    //TODO: isReduced to be removed after remove StatsCalculatorV1
    public boolean isReduced = false;

    public StatsDeriveResult(double rowCount, Map<Slot, ColumnStat> slotToColumnStats) {
        this.rowCount = rowCount;
        this.slotToColumnStats = slotToColumnStats;
    }

    public StatsDeriveResult(double rowCount, Map<Id, Float> columnIdToDataSize, Map<Id, Long> columnIdToNdv) {
        this.rowCount = rowCount;
        this.columnIdToDataSize.putAll(columnIdToDataSize);
        this.columnIdToNdv.putAll(columnIdToNdv);
    }

    public StatsDeriveResult(StatsDeriveResult another) {
        this.rowCount = another.rowCount;
        this.columnIdToDataSize.putAll(another.columnIdToDataSize);
        this.columnIdToNdv.putAll(another.columnIdToNdv);
        slotToColumnStats = new HashMap<>();
        for (Entry<Slot, ColumnStat> entry : another.slotToColumnStats.entrySet()) {
            slotToColumnStats.put(entry.getKey(), entry.getValue().copy());
        }
        this.isReduced = another.isReduced;
        this.width = another.width;
        this.penalty = another.penalty;
    }

    public double computeSize() {
        return Math.max(1, columnIdToDataSize.values().stream().reduce(0F, Float::sum)) * rowCount;
    }

    /**
     * Compute the data size of all input columns.
     *
     * @param slotIds all input columns.
     * @return sum data size.
     */
    public double computeColumnSize(List<Id> slotIds) {
        float count = 0;
        boolean exist = false;

        for (Entry<Id, Float> entry : columnIdToDataSize.entrySet()) {
            if (slotIds.contains(entry.getKey())) {
                count += entry.getValue();
                exist = true;
            }
        }
        if (!exist) {
            count = (float) 1.0;
        }
        return count * rowCount;
    }

    public StatsDeriveResult setRowCount(double rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public double getRowCount() {
        return rowCount;
    }

    public Map<Id, Long> getColumnIdToNdv() {
        return columnIdToNdv;
    }

    public Map<Id, Float> getColumnIdToDataSize() {
        return columnIdToDataSize;
    }

    public Map<Slot, ColumnStat> getSlotToColumnStats() {
        return slotToColumnStats;
    }

    public void setSlotToColumnStats(Map<Slot, ColumnStat> slotToColumnStats) {
        this.slotToColumnStats = slotToColumnStats;
    }

    public void updateColumnStatsForSlot(Slot slot, ColumnStat columnStat) {
        slotToColumnStats.put(slot, columnStat);
    }

    public StatsDeriveResult updateBySelectivity(double selectivity, Set<Slot> exclude) {
        double originRowCount = rowCount;
        for (Entry<Slot, ColumnStat> entry : slotToColumnStats.entrySet()) {
            if (!exclude.contains(entry.getKey())) {
                entry.getValue().updateBySelectivity(selectivity, originRowCount);
            }
        }
        rowCount *= selectivity;
        return this;
    }

    public StatsDeriveResult updateBySelectivity(double selectivity) {
        double originRowCount = rowCount;
        for (Entry<Slot, ColumnStat> entry : slotToColumnStats.entrySet()) {
            entry.getValue().updateBySelectivity(selectivity, originRowCount);
        }
        rowCount *= selectivity;
        return this;
    }

    public StatsDeriveResult updateRowCountByLimit(long limit) {
        double originRowCount = rowCount;
        if (limit > 0 && rowCount > 0 && rowCount > limit) {
            double selectivity = ((double) limit) / rowCount;
            rowCount = limit;
            for (Entry<Slot, ColumnStat> entry : slotToColumnStats.entrySet()) {
                entry.getValue().updateBySelectivity(selectivity, originRowCount);
            }
        }
        return this;
    }

    public StatsDeriveResult merge(StatsDeriveResult other) {
        for (Entry<Slot, ColumnStat> entry : other.getSlotToColumnStats().entrySet()) {
            this.slotToColumnStats.put(entry.getKey(), entry.getValue().copy());
        }
        return this;
    }

    public StatsDeriveResult copy() {
        return new StatsDeriveResult(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(rows=").append((long) rowCount)
                .append(", isReduced=").append(isReduced)
                .append(", width=").append(width)
                .append(", penalty=").append(penalty).append(")");
        return builder.toString();
    }

    public static String toString(StatsDeriveResult stats) {
        if (stats == null) {
            return "null";
        } else {
            return stats.toString();
        }
    }

    public StatsDeriveResult updateRowCountOnCopy(double selectivity) {
        StatsDeriveResult copy = new StatsDeriveResult(this);
        copy.setRowCount(rowCount * selectivity);
        for (Entry<Slot, ColumnStat> entry : copy.slotToColumnStats.entrySet()) {
            entry.getValue().updateBySelectivity(selectivity, rowCount);
        }
        return copy;
    }

    public StatsDeriveResult addColumnStats(Slot slot, ColumnStat stats) {
        slotToColumnStats.put(slot, stats);
        return this;
    }

    public ColumnStat getColumnStatsBySlot(Slot slot) {
        return slotToColumnStats.get(slot);
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public double getPenalty() {
        return penalty;
    }

    public void setPenalty(double penalty) {
        this.penalty = penalty;
    }
}
