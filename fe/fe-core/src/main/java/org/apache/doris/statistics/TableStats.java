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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * There are the statistics of table.
 * The table stats are mainly used to provide input for the Optimizer's cost model.
 * The description of table stats are following:
 *     - @rowCount: The row count of table.
 *     - @dataSize: The data size of table.
 *     - @nameToColumnStats: <@String columnName, @ColumnStats columnStats>
 * <p>
 * Each column in the Table will have corresponding @ColumnStats.
 * Those @ColumnStats are recorded in @nameToColumnStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding:
 *     - @ColumnStats based on the column name.
 *     - @rowCount: The row count of table.
 *     - @dataSize: The data size of table.
 * <p>
 * The granularity of the statistics is whole table.
 * For example: "@rowCount = 1000" means that the row count is 1000 in the whole table.
 * <p>
 * After the statistics task is successfully completed, update the TableStats,
 * TableStats should not be updated in any other way.
 */
public class TableStats {
    public static final StatsType DATA_SIZE = StatsType.DATA_SIZE;
    public static final StatsType ROW_COUNT = StatsType.ROW_COUNT;

    private static final Predicate<Double> DESIRED_ROW_COUNT_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_DATA_SIZE_PRED = (v) -> v >= -1L;

    private double rowCount = -1;
    private long dataSize = -1;
    private final Map<String, PartitionStats> nameToPartitionStats = Maps.newConcurrentMap();
    private final Map<String, ColumnStat> nameToColumnStats = Maps.newConcurrentMap();

    /**
     * Return a default partition statistic.
     */
    public static TableStats getDefaultTableStats() {
        return new TableStats();
    }

    public TableStats() {
    }

    public TableStats(double rowCount, long dataSize) {
        this.rowCount = rowCount;
        this.dataSize = dataSize;
    }

    public double getRowCount() {
        // '!isEmpty()' is added mainly because the result returns 0
        // instead of the expected -1 when nameToPartitionStats is empty.
        if (rowCount == -1 && !nameToPartitionStats.isEmpty()) {
            return nameToPartitionStats.values().stream()
                    .filter(partitionStats -> partitionStats.getRowCount() != -1)
                    .mapToLong(PartitionStats::getRowCount).sum();
        }
        return rowCount;
    }

    public long getDataSize() {
        if (dataSize == -1 && !nameToPartitionStats.isEmpty()) {
            return nameToPartitionStats.values().stream()
                    .filter(partitionStats -> partitionStats.getDataSize() != -1)
                    .mapToLong(PartitionStats::getDataSize).sum();
        }
        return dataSize;
    }

    public Map<String, PartitionStats> getNameToPartitionStats() {
        return nameToPartitionStats;
    }

    public Map<String, ColumnStat> getNameToColumnStats() {
        if (nameToColumnStats.isEmpty()) {
            return getAggPartitionColStats();
        }
        return nameToColumnStats;
    }

    public PartitionStats getPartitionStats(String partitionName) {
        return nameToPartitionStats.get(partitionName);
    }

    /**
     * If the partition statistics do not exist, the default statistics will be returned.
     */
    public PartitionStats getPartitionStatsOrDefault(String columnName) {
        return nameToPartitionStats.getOrDefault(columnName,
                PartitionStats.getDefaultPartitionStats());
    }

    /**
     * If the column statistics do not exist, the default statistics will be returned.
     */
    public ColumnStat getColumnStatsOrDefault(String columnName) {
        return nameToColumnStats.getOrDefault(columnName,
                ColumnStat.getDefaultColumnStats());
    }

    /**
     * After the statistics task is successfully completed, update the statistics of the partition,
     * statistics should not be updated in any other way.
     */
    public void updateTableStats(Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            if (entry.getKey() == ROW_COUNT) {
                rowCount = Util.getDoublePropertyOrDefault(entry.getValue(), rowCount,
                        DESIRED_ROW_COUNT_PRED, ROW_COUNT + " should >= -1");
            } else if (entry.getKey() == DATA_SIZE) {
                dataSize = Util.getLongPropertyOrDefault(entry.getValue(), dataSize,
                        DESIRED_DATA_SIZE_PRED, DATA_SIZE + " should >= -1");
            }
        }
    }

    /**
     * After the statistics task is successfully completed, update the statistics of the partition,
     * statistics should not be updated in any other way.
     */
    public void updatePartitionStats(String partitionName, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        PartitionStats partitionStats = getNotNullPartitionStats(partitionName);
        partitionStats.updatePartitionStats(statsTypeToValue);
    }

    /**
     * After the statistics task is successfully completed, update the statistics of the column,
     * statistics should not be updated in any other way.
     */
    public void updateColumnStats(String columnName, Type columnType, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        ColumnStat columnStat = getColumnStats(columnName);
        columnStat.updateStats(columnType, statsTypeToValue);
    }

    /**
     * If partition stats is not exist, create a new one.
     *
     * @param partitionName partition name
     * @return @PartitionStats
     */
    private PartitionStats getNotNullPartitionStats(String partitionName) {
        PartitionStats partitionStat = nameToPartitionStats.get(partitionName);
        if (partitionStat == null) {
            partitionStat = new PartitionStats();
            nameToPartitionStats.put(partitionName, partitionStat);
        }
        return partitionStat;
    }

    /**
     * If column stats is not exist, create a new one.
     *
     * @param columnName column name
     * @return @ColumnStats
     */
    private ColumnStat getNotNullColumnStats(String columnName) {
        ColumnStat columnStat = nameToColumnStats.get(columnName);
        if (columnStat == null) {
            columnStat = new ColumnStat();
            nameToColumnStats.put(columnName, columnStat);
        }
        return columnStat;
    }

    public ColumnStat getColumnStats(String columnName) {
        ColumnStat columnStat = nameToColumnStats.get(columnName);
        if (columnStat == null) {
            columnStat = new ColumnStat();
            nameToColumnStats.put(columnName, columnStat);
        }
        return columnStat;
    }

    public ColumnStat getColumnStatCopy(String columnName) {
        ColumnStat columnStat = getColumnStats(columnName);
        return columnStat.copy();
    }

    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Double.toString(getRowCount()));
        result.add(Long.toString(getDataSize()));
        return result;
    }

    public List<String> getShowInfo(String partitionName) {
        PartitionStats partitionStats = nameToPartitionStats.get(partitionName);
        return partitionStats.getShowInfo();
    }

    private Map<String, ColumnStat> getAggPartitionColStats() {
        Map<String, ColumnStat> aggColumnStats = new HashMap<>();
        for (PartitionStats partitionStats : nameToPartitionStats.values()) {
            partitionStats.getNameToColumnStats().forEach((colName, columnStats) -> {
                if (!aggColumnStats.containsKey(colName)) {
                    aggColumnStats.put(colName, columnStats);
                } else {
                    ColumnStat tblColStats = aggColumnStats.get(colName);
                    mergePartitionColumnStats(tblColStats, columnStats);
                }
            });
        }

        return aggColumnStats;
    }

    private void mergePartitionColumnStats(ColumnStat leftStats, ColumnStat rightStats) {
        if (leftStats.getNdv() == -1) {
            if (rightStats.getNdv() != -1) {
                leftStats.setNdv(rightStats.getNdv());
            }
        } else {
            if (rightStats.getNdv() != -1) {
                double ndv = leftStats.getNdv() + rightStats.getNdv();
                leftStats.setNdv(ndv);
            }
        }

        if (leftStats.getAvgSizeByte() == -1) {
            if (rightStats.getAvgSizeByte() != -1) {
                leftStats.setAvgSizeByte(rightStats.getAvgSizeByte());
            }
        } else {
            if (rightStats.getAvgSizeByte() != -1) {
                double avgSize = (leftStats.getAvgSizeByte() + rightStats.getAvgSizeByte()) / 2;
                leftStats.setAvgSizeByte(avgSize);
            }
        }

        if (leftStats.getMaxSizeByte() == -1) {
            if (rightStats.getMaxSizeByte() != -1) {
                leftStats.setMaxSizeByte(rightStats.getMaxSizeByte());
            }
        } else {
            if (rightStats.getMaxSizeByte() != -1) {
                double maxSize = Math.max(leftStats.getMaxSizeByte(), rightStats.getMaxSizeByte());
                leftStats.setMaxSizeByte(maxSize);
            }
        }

        if (leftStats.getNumNulls() == -1) {
            if (rightStats.getNumNulls() != -1) {
                leftStats.setNumNulls(rightStats.getNumNulls());
            }
        } else {
            if (rightStats.getNumNulls() != -1) {
                double numNulls = leftStats.getNumNulls() + rightStats.getNumNulls();
                leftStats.setNumNulls(numNulls);
            }
        }

        if (Double.isNaN(leftStats.getMinValue())) {
            if (!Double.isNaN(rightStats.getMinValue())) {
                leftStats.setMinValue(rightStats.getMinValue());
            }
        } else if (!Double.isNaN(rightStats.getMinValue())) {
            double minValue = Math.max(leftStats.getMinValue(), rightStats.getMinValue());
            leftStats.setMinValue(minValue);
        }

        if (Double.isNaN(leftStats.getMaxValue())) {
            if (!Double.isNaN(rightStats.getMaxValue())) {
                leftStats.setMaxValue(rightStats.getMaxValue());
            }
        } else if (!Double.isNaN(rightStats.getMaxValue())) {
            double maxValue = Math.min(leftStats.getMaxValue(), rightStats.getMaxValue());
            leftStats.setMaxValue(maxValue);
        }
    }

    /**
     * This method is for unit test.
     */
    public void putColumnStats(String name, ColumnStat columnStat) {
        nameToColumnStats.put(name, columnStat);
    }
}
