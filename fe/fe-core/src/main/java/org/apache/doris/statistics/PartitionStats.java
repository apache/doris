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
 *
 * <p>Each column in the Table will have corresponding @ColumnStats.
 * Those @ColumnStats are recorded in @nameToColumnStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding:
 *     - @ColumnStats: based on the column name.
 *     - @rowCount: The row count of partition.
 *     - @dataSize: The data size of partition.
 *
 * <p>The granularity of the statistics is whole partition.
 * For example: "@rowCount = 1000" means that the row count is 1000 in the whole partition.
 */
public class PartitionStats {

    public static final StatsType DATA_SIZE = StatsType.DATA_SIZE;
    public static final StatsType ROW_COUNT = StatsType.ROW_COUNT;

    private static final Predicate<Long> DESIRED_ROW_COUNT_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_DATA_SIZE_PRED = (v) -> v >= -1L;

    private long rowCount = -1;
    private long dataSize = -1;
    private final Map<String, ColumnStats> nameToColumnStats = Maps.newConcurrentMap();

    public Map<String, ColumnStats> getNameToColumnStats() {
        return nameToColumnStats;
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

    /**
     * Update the partition stats.
     *
     * @param statsTypeToValue the map of stats type to value
     * @throws AnalysisException if the stats value is not valid
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

    public void updateColumnStats(String columnName,
                                  Type columnType,
                                  Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        ColumnStats columnStats = getNotNullColumnStats(columnName);
        columnStats.updateStats(columnType, statsTypeToValue);
    }

    /**
     * If column stats is not exist, create a new one.
     *
     * @param columnName column name
     * @return @ColumnStats
     */
    public ColumnStats getNotNullColumnStats(String columnName) {
        ColumnStats columnStats = nameToColumnStats.get(columnName);
        if (columnStats == null) {
            columnStats = new ColumnStats();
            nameToColumnStats.put(columnName, columnStats);
        }
        return columnStats;
    }

    /**
     * show the partition row count and data size.
     */
    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(rowCount));
        result.add(Long.toString(dataSize));
        return result;
    }
}
