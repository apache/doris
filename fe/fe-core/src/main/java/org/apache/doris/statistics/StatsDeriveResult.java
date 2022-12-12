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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This structure is maintained in each operator to store the statistical information results obtained by the operator.
 */
public class StatsDeriveResult {
    private final double rowCount;
    private double computeSize = -1D;

    private int width = 1;
    private double penalty = 0.0;
    // TODO: Should we use immutable type for this field?
    private final Map<Id, ColumnStatistic> slotIdToColumnStats;

    public StatsDeriveResult(double rowCount, int width, double penalty,
            Map<Id, ColumnStatistic> slotIdToColumnStats) {
        this.rowCount = rowCount;
        this.width = width;
        this.penalty = penalty;
        this.slotIdToColumnStats = slotIdToColumnStats;
    }

    public StatsDeriveResult(double rowCount,
            Map<Id, ColumnStatistic> slotIdToColumnStats) {
        this.rowCount = rowCount;
        this.width = 1;
        this.penalty = 0;
        this.slotIdToColumnStats = slotIdToColumnStats;
    }

    public StatsDeriveResult(double rowCount, int width, double penalty) {
        this.rowCount = rowCount;
        this.width = width;
        this.penalty = penalty;
        slotIdToColumnStats = new HashMap<>();
    }

    public StatsDeriveResult(double rowCount) {
        this.rowCount = rowCount;
        this.width = 1;
        this.penalty = 0;
        slotIdToColumnStats = new HashMap<>();
    }

    public StatsDeriveResult(StatsDeriveResult another) {
        this.rowCount = another.rowCount;
        slotIdToColumnStats = new HashMap<>(another.slotIdToColumnStats);
        this.width = another.width;
        this.penalty = another.penalty;
    }

    public double computeSize() {
        if (computeSize < 0) {
            computeSize = Math.max(1, slotIdToColumnStats.values().stream()
                    .map(s -> s.dataSize).reduce(0D, Double::sum)
            ) * rowCount;
        }
        return computeSize;
    }

    /**
     * Compute the data size of all input columns.
     *
     * @param slotIds all input columns.
     * @return sum data size.
     */
    public double computeColumnSize(List<Id> slotIds) {
        double count = 0;
        boolean exist = false;

        for (Entry<Id, ColumnStatistic> entry : slotIdToColumnStats.entrySet()) {
            if (slotIds.contains(entry.getKey())) {
                count += entry.getValue().dataSize;
                exist = true;
            }
        }
        if (!exist) {
            count = (float) 1.0;
        }
        return count * rowCount;
    }

    public double getRowCount() {
        return rowCount;
    }

    public Map<Id, ColumnStatistic> getSlotIdToColumnStats() {
        return slotIdToColumnStats;
    }

    public StatsDeriveResult withSelectivity(double selectivity) {
        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(rowCount * selectivity, width, penalty);
        for (Entry<Id, ColumnStatistic> entry : slotIdToColumnStats.entrySet()) {
            statsDeriveResult.addColumnStats(entry.getKey(),
                        entry.getValue().updateBySelectivity(selectivity, rowCount));
        }
        return statsDeriveResult;
    }

    public StatsDeriveResult updateRowCountByLimit(long limit) {
        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(limit, width, penalty);
        if (limit > 0 && rowCount > 0 && rowCount > limit) {
            double selectivity = ((double) limit) / rowCount;
            for (Entry<Id, ColumnStatistic> entry : slotIdToColumnStats.entrySet()) {
                statsDeriveResult.addColumnStats(entry.getKey(), entry.getValue().multiply(selectivity));
            }
        }
        return statsDeriveResult;
    }

    public StatsDeriveResult merge(StatsDeriveResult other) {
        for (Entry<Id, ColumnStatistic> entry : other.getSlotIdToColumnStats().entrySet()) {
            this.slotIdToColumnStats.put(entry.getKey(), entry.getValue().copy());
        }
        return this;
    }

    public StatsDeriveResult copy() {
        return new StatsDeriveResult(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(rows=").append((long) Math.ceil(rowCount))
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
        StatsDeriveResult copy = new StatsDeriveResult(rowCount * selectivity, width, penalty);
        for (Entry<Id, ColumnStatistic> entry : slotIdToColumnStats.entrySet()) {
            copy.addColumnStats(entry.getKey(), entry.getValue().multiply(selectivity));
        }
        return copy;
    }

    public StatsDeriveResult updateRowCount(double rowCount) {
        return new StatsDeriveResult(rowCount, width, penalty, slotIdToColumnStats);
    }

    public StatsDeriveResult addColumnStats(Id id, ColumnStatistic stats) {
        slotIdToColumnStats.put(id, stats);
        return this;
    }

    public ColumnStatistic getColumnStatsBySlotId(Id slotId) {
        return slotIdToColumnStats.get(slotId);
    }

    public ColumnStatistic getColumnStatsBySlot(Slot slot) {
        return slotIdToColumnStats.get(slot.getExprId());
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
