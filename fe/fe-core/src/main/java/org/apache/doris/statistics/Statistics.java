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

import org.apache.doris.nereids.stats.StatsMathUtil;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Statistics {
    private static final int K_BYTES = 1024;

    private final double rowCount;

    private final Map<Expression, ColumnStatistic> expressionToColumnStats;

    // the byte size of one tuple
    private double tupleSize;

    private boolean unknown = true;

    public Statistics(Statistics another) {
        this.rowCount = another.rowCount;
        this.expressionToColumnStats = new HashMap<>(another.expressionToColumnStats);
        this.tupleSize = another.tupleSize;
    }

    public Statistics(double rowCount, Map<Expression, ColumnStatistic> expressionToColumnStats) {
        this.rowCount = rowCount;
        this.expressionToColumnStats = expressionToColumnStats;
    }

    public ColumnStatistic findColumnStatistics(Expression expression) {
        return expressionToColumnStats.get(expression);
    }

    public Map<Expression, ColumnStatistic> columnStatistics() {
        return expressionToColumnStats;
    }

    public double getRowCount() {
        return rowCount;
    }

    public Statistics withRowCount(double rowCount) {
        return new Statistics(rowCount, new HashMap<>(expressionToColumnStats));
    }

    /**
     * Update by count.
     */
    public Statistics withRowCountAndEnforceValid(double rowCount) {
        Statistics statistics = new Statistics(rowCount, expressionToColumnStats);
        statistics.enforceValid();
        return statistics;
    }

    public void enforceValid() {
        for (Entry<Expression, ColumnStatistic> entry : expressionToColumnStats.entrySet()) {
            ColumnStatistic columnStatistic = entry.getValue();
            if (!checkColumnStatsValid(columnStatistic)) {
                double ndv = Math.min(columnStatistic.ndv, rowCount);
                ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(columnStatistic);
                columnStatisticBuilder.setNdv(ndv);
                columnStatisticBuilder.setNumNulls(Math.min(columnStatistic.numNulls, rowCount - ndv));
                columnStatisticBuilder.setCount(rowCount);
                columnStatistic = columnStatisticBuilder.build();
                expressionToColumnStats.put(entry.getKey(), columnStatistic);
            }
        }
    }

    public boolean checkColumnStatsValid(ColumnStatistic columnStatistic) {
        return columnStatistic.ndv <= rowCount
                && columnStatistic.numNulls <= rowCount - columnStatistic.ndv;
    }

    public Statistics withSel(double sel) {
        sel = StatsMathUtil.minNonNaN(sel, 1);
        if (Double.isNaN(rowCount)) {
            return this;
        }
        double newCount = rowCount * sel;
        return new Statistics(newCount, new HashMap<>(expressionToColumnStats));
    }

    public Statistics addColumnStats(Expression expression, ColumnStatistic columnStatistic) {
        expressionToColumnStats.put(expression, columnStatistic);
        return this;
    }

    public boolean isInputSlotsUnknown(Set<Slot> inputs) {
        return inputs.stream()
                .allMatch(s -> expressionToColumnStats.containsKey(s)
                        && expressionToColumnStats.get(s).isUnKnown);
    }

    private double computeTupleSize() {
        if (tupleSize <= 0) {
            double tempSize = 0.0;
            for (ColumnStatistic s : expressionToColumnStats.values()) {
                tempSize += s.avgSizeByte;
            }
            tupleSize = Math.max(1, tempSize);
        }
        return tupleSize;
    }

    public double computeSize() {
        return computeTupleSize() * rowCount;
    }

    public double dataSizeFactor() {
        return computeTupleSize() / K_BYTES;
    }

    @Override
    public String toString() {
        if (Double.isNaN(rowCount)) {
            return "NaN";
        }
        if (Double.POSITIVE_INFINITY == rowCount) {
            return "Infinite";
        }
        if (Double.NEGATIVE_INFINITY == rowCount) {
            return "-Infinite";
        }
        DecimalFormat format = new DecimalFormat("#,###.##");
        return format.format(rowCount);
    }

    public int getBENumber() {
        return 1;
    }

    public static Statistics zero(Statistics statistics) {
        Statistics zero = new Statistics(0, new HashMap<>());
        for (Map.Entry<Expression, ColumnStatistic> entry : statistics.expressionToColumnStats.entrySet()) {
            zero.addColumnStats(entry.getKey(), ColumnStatistic.ZERO);
        }
        return zero;
    }

    /**
     * merge this and other colStats.ndv, choose min
     */
    public void updateNdv(Statistics other) {
        for (Expression expr : expressionToColumnStats.keySet()) {
            ColumnStatistic otherColStats = other.findColumnStatistics(expr);
            if (otherColStats != null) {
                ColumnStatistic thisColStats = expressionToColumnStats.get(expr);
                if (thisColStats.ndv > otherColStats.ndv) {
                    expressionToColumnStats.put(expr,
                            new ColumnStatisticBuilder(thisColStats).setNdv(otherColStats.ndv).build());
                }
            }
        }
    }

    public String detail(String prefix) {
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append("rows=").append(rowCount).append("\n");
        builder.append(prefix).append("tupleSize=").append(computeTupleSize()).append("\n");

        for (Entry<Expression, ColumnStatistic> entry : expressionToColumnStats.entrySet()) {
            builder.append(prefix).append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
        }
        return builder.toString();
    }
}
