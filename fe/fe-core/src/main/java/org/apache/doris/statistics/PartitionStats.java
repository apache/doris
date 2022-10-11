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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * There are the statistics of partition.
 * The partition stats are mainly used to provide input for the Optimizer's cost model.
 * The description of partition stats are following:
 *     - @rowCount: The row count of partition.
 *     - @dataSize: The data size of partition.
 *     - @nameToColumnStats: <@String columnName, @ColumnStats columnStats>
 * <p>
 * Each column in the Table will have corresponding @ColumnStats.
 * Those @ColumnStats are recorded in @nameToColumnStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding:
 *     - @ColumnStats: based on the column name.
 *     - @rowCount: The row count of partition.
 *     - @dataSize: The data size of partition.
 * <p>
 * The granularity of the statistics is whole partition.
 * For example: "@rowCount = 1000" means that the row count is 1000 in the whole partition.
 * <p>
 *  After the statistics task is successfully completed, update the PartitionStats,
 *  PartitionStats should not be updated in any other way.
 */
public class PartitionStats {
    public static final StatsType DATA_SIZE = StatsType.DATA_SIZE;
    public static final StatsType ROW_COUNT = StatsType.ROW_COUNT;

    private static final Predicate<Long> DESIRED_ROW_COUNT_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_DATA_SIZE_PRED = (v) -> v >= -1L;

    private long rowCount = -1;
    private long dataSize = -1;
    private final Map<String, ColumnStat> nameToColumnStats = Maps.newConcurrentMap();

    /**
     * Return a default partition statistic.
     */
    public static PartitionStats getDefaultPartitionStats() {
        return new PartitionStats();
    }

    public PartitionStats() {
    }

    public PartitionStats(long rowCount, long dataSize) {
        this.rowCount = rowCount;
        this.dataSize = dataSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public Map<String, ColumnStat> getNameToColumnStats() {
        return nameToColumnStats;
    }

    public ColumnStat getColumnStats(String columnName) {
        return nameToColumnStats.get(columnName);
    }

    /**
     * If the column statistics do not exist, the default statistics will be returned.
     */
    public ColumnStat getColumnStatsOrDefault(String columnName) {
        return nameToColumnStats.getOrDefault(columnName,
                ColumnStat.getDefaultColumnStats());
    }

    /**
     * Show the partition row count and data size.
     */
    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(rowCount));
        result.add(Long.toString(dataSize));
        return result;
    }

    /**
     * After the statistics task is successfully completed, update the statistics of the partition,
     * statistics should not be updated in any other way.
     */
    public void updatePartitionStats(Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            StatsType statsType = entry.getKey();
            String value = entry.getValue();
            if (statsType == ROW_COUNT) {
                rowCount = Util.getLongPropertyOrDefault(value, rowCount,
                        DESIRED_ROW_COUNT_PRED, ROW_COUNT + " should >= -1");
            } else if (statsType == DATA_SIZE) {
                dataSize = Util.getLongPropertyOrDefault(value, dataSize,
                        DESIRED_DATA_SIZE_PRED, DATA_SIZE + " should >= -1");
            }
        }
    }

    /**
     * After the statistics task is successfully completed, update the statistics of the column,
     * statistics should not be updated in any other way.
     */
    public void updateColumnStats(String columnName,
                                  Type columnType,
                                  Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        ColumnStat columnStat = getNotNullColumnStats(columnName);
        columnStat.updateStats(columnType, statsTypeToValue);
    }

    /**
     * If column stats is not exist, create a new one.
     *
     * @param columnName column name
     * @return @ColumnStats
     */
    public ColumnStat getNotNullColumnStats(String columnName) {
        ColumnStat columnStat = nameToColumnStats.get(columnName);
        if (columnStat == null) {
            columnStat = new ColumnStat();
            nameToColumnStats.put(columnName, columnStat);
        }
        return columnStat;
    }
}
