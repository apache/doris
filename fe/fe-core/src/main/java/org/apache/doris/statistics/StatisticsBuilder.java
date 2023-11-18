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

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.HashMap;
import java.util.Map;

public class StatisticsBuilder {

    private double rowCount;

    private final Map<Expression, ColumnStatistic> expressionToColumnStats;

    public StatisticsBuilder() {
        expressionToColumnStats = new HashMap<>();
    }

    public StatisticsBuilder(Statistics statistics) {
        this.rowCount = statistics.getRowCount();
        expressionToColumnStats = new HashMap<>();
        expressionToColumnStats.putAll(statistics.columnStatistics());
    }

    public StatisticsBuilder setRowCount(double rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public StatisticsBuilder putColumnStatistics(
            Map<Expression, ColumnStatistic> expressionToColumnStats) {
        this.expressionToColumnStats.putAll(expressionToColumnStats);
        return this;
    }

    public StatisticsBuilder putColumnStatistics(Expression expression, ColumnStatistic columnStatistic) {
        expressionToColumnStats.put(expression, columnStatistic);
        return this;
    }

    public Statistics build() {
        return new Statistics(rowCount, expressionToColumnStats);
    }
}
