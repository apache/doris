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

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Statistics {
    private final double rowCount;

    private final Map<Expression, ColumnStatistic> expressionToColumnStats;

    private double computeSize;

    @Deprecated
    private double width;

    @Deprecated
    private double penalty;

    /**
     * after filter, compute the new ndv of a column
     * @param ndv original ndv of column
     * @param newRowCount the row count of table after filter
     * @param oldRowCount the row count of table before filter
     * @return the new ndv after filter
     */
    public static double computeNdv(double ndv, double newRowCount, double oldRowCount) {
        double selectOneTuple = newRowCount / StatsMathUtil.nonZeroDivisor(oldRowCount);
        double allTuplesOfSameDistinctValueNotSelected = Math.pow((1 - selectOneTuple), oldRowCount / ndv);
        return Math.min(ndv * (1 - allTuplesOfSameDistinctValueNotSelected), newRowCount);
    }

    public Statistics(Statistics another) {
        this.rowCount = another.rowCount;
        this.expressionToColumnStats = new HashMap<>(another.expressionToColumnStats);
        this.width = another.width;
        this.penalty = another.penalty;
    }

    public Statistics(double rowCount, Map<Expression, ColumnStatistic> expressionToColumnStats) {
        this.rowCount = rowCount;
        this.expressionToColumnStats = expressionToColumnStats;
    }

    public Statistics(double rowCount, Map<Expression, ColumnStatistic> expressionToColumnStats, double width,
            double penalty) {
        this.rowCount = rowCount;
        this.expressionToColumnStats = expressionToColumnStats;
        this.width = width;
        this.penalty = penalty;
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

    /*
     * Return a stats with new rowCount and fix each column stats.
     */
    public Statistics withRowCount(double rowCount) {
        if (Double.isNaN(rowCount)) {
            return this;
        }
        Statistics statistics = new Statistics(rowCount, new HashMap<>(expressionToColumnStats), width, penalty);
        statistics.fix(rowCount, StatsMathUtil.nonZeroDivisor(this.rowCount));
        return statistics;
    }

    /**
     * Update by count.
     */
    public Statistics updateRowCountOnly(double rowCount) {
        Statistics statistics = new Statistics(rowCount, expressionToColumnStats);
        for (Entry<Expression, ColumnStatistic> entry : expressionToColumnStats.entrySet()) {
            ColumnStatistic columnStatistic = entry.getValue();
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(columnStatistic);
            columnStatisticBuilder.setNdv(Math.min(columnStatistic.ndv, rowCount));
            double nullFactor = (rowCount - columnStatistic.numNulls) / rowCount;
            columnStatisticBuilder.setNumNulls(nullFactor * rowCount);
            columnStatisticBuilder.setCount(rowCount);
            statistics.addColumnStats(entry.getKey(), columnStatisticBuilder.build());
        }
        return statistics;
    }

    /**
     * Fix by sel.
     */
    public void fix(double newRowCount, double originRowCount) {
        double sel = newRowCount / originRowCount;
        for (Entry<Expression, ColumnStatistic> entry : expressionToColumnStats.entrySet()) {
            ColumnStatistic columnStatistic = entry.getValue();
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(columnStatistic);
            columnStatisticBuilder.setNdv(computeNdv(columnStatistic.ndv, newRowCount, originRowCount));
            columnStatisticBuilder.setNumNulls(Math.min(columnStatistic.numNulls * sel, newRowCount));
            columnStatisticBuilder.setCount(newRowCount);
            expressionToColumnStats.put(entry.getKey(), columnStatisticBuilder.build());
        }
    }

    public Statistics withSel(double sel) {
        sel = StatsMathUtil.minNonNaN(sel, 1);
        return withRowCount(rowCount * sel);
    }

    public Statistics addColumnStats(Expression expression, ColumnStatistic columnStatistic) {
        expressionToColumnStats.put(expression, columnStatistic);
        return this;
    }

    public Statistics merge(Statistics statistics) {
        expressionToColumnStats.putAll(statistics.expressionToColumnStats);
        return this;
    }

    public double computeSize() {
        if (computeSize <= 0) {
            computeSize = Math.max(1, expressionToColumnStats.values().stream()
                    .map(s -> s.avgSizeByte).reduce(0D, Double::sum)
            ) * rowCount;
        }
        return computeSize;
    }

    @Override
    public String toString() {
        DecimalFormat format = new DecimalFormat("#,###.##");
        return format.format(rowCount);
    }

    public void setWidth(double width) {
        this.width = width;
    }

    public void setPenalty(double penalty) {
        this.penalty = penalty;
    }

    public double getWidth() {
        return width;
    }

    public double getPenalty() {
        return penalty;
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
}
