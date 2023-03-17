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

    public Statistics withRowCount(double rowCount) {
        Statistics statistics = new Statistics(rowCount, new HashMap<>(expressionToColumnStats), width, penalty);
        statistics.fix(rowCount / StatsMathUtil.nonZeroDivisor(this.rowCount));
        return statistics;
    }

    public void fix(double sel) {
        for (Entry<Expression, ColumnStatistic> entry : expressionToColumnStats.entrySet()) {
            ColumnStatistic columnStatistic = entry.getValue();
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(columnStatistic);
            columnStatisticBuilder.setNdv(Math.min(Math.ceil(columnStatistic.ndv * sel), rowCount));
            columnStatisticBuilder.setNumNulls(Math.min(Math.ceil(columnStatistic.numNulls * sel), rowCount));
            columnStatisticBuilder.setCount(Math.min(Math.ceil(columnStatistic.count * sel), rowCount));
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
                    .map(s -> s.dataSize).reduce(0D, Double::sum)
            ) * rowCount;
        }
        return computeSize;
    }

    @Override
    public String toString() {
        return String.format("rows=%.4f", rowCount);
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
}
