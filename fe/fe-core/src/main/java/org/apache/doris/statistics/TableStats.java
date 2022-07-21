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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
 * <p>Each column in the Table will have corresponding @ColumnStats.
 * Those @ColumnStats are recorded in @nameToColumnStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding:
 *     - @ColumnStats based on the column name.
 *     - @rowCount: The row count of table.
 *     - @dataSize: The data size of table.
 * <p>The granularity of the statistics is whole table.
 * For example: "@rowCount = 1000" means that the row count is 1000 in the whole table.
 */
public class TableStats {

    public static final StatsType DATA_SIZE = StatsType.DATA_SIZE;
    public static final StatsType ROW_COUNT = StatsType.ROW_COUNT;

    private static final Predicate<Long> DESIRED_ROW_COUNT_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_DATA_SIZE_PRED = (v) -> v >= -1L;

    private long rowCount = -1;
    private long dataSize = -1;
    private final Map<String, PartitionStats> nameToPartitionStats = Maps.newConcurrentMap();
    private final Map<String, ColumnStats> nameToColumnStats = Maps.newConcurrentMap();

    public long getRowCount() {
        if  (rowCount == -1) {
            return nameToPartitionStats.values().stream()
                    .filter(partitionStats -> partitionStats.getRowCount() != -1)
                    .mapToLong(PartitionStats::getRowCount).sum();
        }
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getDataSize() {
        if (dataSize == -1) {
            return nameToPartitionStats.values().stream()
                    .filter(partitionStats -> partitionStats.getDataSize() != -1)
                    .mapToLong(PartitionStats::getDataSize).sum();
        }
        return dataSize;
    }

    public Map<String, PartitionStats> getNameToPartitionStats() {
        return nameToPartitionStats;
    }

    public Map<String, ColumnStats> getNameToColumnStats() {
        if (nameToColumnStats.isEmpty()) {
            return getAggPartitionColStats();
        }
        return nameToColumnStats;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public void updateTableStats(Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            if (entry.getKey() == ROW_COUNT) {
                rowCount = Util.getLongPropertyOrDefault(entry.getValue(), rowCount,
                        DESIRED_ROW_COUNT_PRED, ROW_COUNT + " should >= -1");
            } else if (entry.getKey() == DATA_SIZE) {
                dataSize = Util.getLongPropertyOrDefault(entry.getValue(), dataSize,
                        DESIRED_DATA_SIZE_PRED, DATA_SIZE + " should >= -1");
            }
        }
    }

    public void updatePartitionStats(String partitionName, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        PartitionStats partitionStats = getNotNullPartitionStats(partitionName);
        partitionStats.updatePartitionStats(statsTypeToValue);
    }

    public void updateColumnStats(String columnName, Type columnType, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        ColumnStats columnStats = getNotNullColumnStats(columnName);
        columnStats.updateStats(columnType, statsTypeToValue);
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
    public ColumnStats getNotNullColumnStats(String columnName) {
        ColumnStats columnStats = nameToColumnStats.get(columnName);
        if (columnStats == null) {
            columnStats = new ColumnStats();
            nameToColumnStats.put(columnName, columnStats);
        }
        return columnStats;
    }

    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(getRowCount()));
        result.add(Long.toString(getDataSize()));
        return result;
    }

    public List<String> getShowInfo(String partitionName) {
        PartitionStats partitionStats = nameToPartitionStats.get(partitionName);
        return partitionStats.getShowInfo();
    }

    private Map<String, ColumnStats> getAggPartitionColStats() {
        Map<String, ColumnStats> aggColumnStats = Maps.newConcurrentMap();
        for (PartitionStats partitionStats : nameToPartitionStats.values()) {
            partitionStats.getNameToColumnStats().forEach((colName, columnStats) -> {
                if (!aggColumnStats.containsKey(colName)) {
                    aggColumnStats.put(colName, columnStats);
                } else {
                    ColumnStats tblColStats = aggColumnStats.get(colName);
                    aggPartitionColumnStats(tblColStats, columnStats);
                }
            });
        }

        return aggColumnStats;
    }

    private void aggPartitionColumnStats(ColumnStats leftStats, ColumnStats rightStats) {
        if (leftStats.getNdv() == -1) {
            if (rightStats.getNdv() != -1) {
                leftStats.setNdv(rightStats.getNdv());
            }
        } else {
            if (rightStats.getNdv() != -1) {
                long ndv = leftStats.getNdv() + rightStats.getNdv();
                leftStats.setNdv(ndv);
            }
        }

        if (leftStats.getAvgSize() == -1) {
            if (rightStats.getAvgSize() != -1) {
                leftStats.setAvgSize(rightStats.getAvgSize());
            }
        } else {
            if (rightStats.getAvgSize() != -1) {
                float avgSize = (leftStats.getAvgSize() + rightStats.getAvgSize()) / 2;
                leftStats.setAvgSize(avgSize);
            }
        }

        if (leftStats.getMaxSize() == -1) {
            if (rightStats.getMaxSize() != -1) {
                leftStats.setMaxSize(rightStats.getMaxSize());
            }
        } else {
            if (rightStats.getMaxSize() != -1) {
                long maxSize = Math.max(leftStats.getMaxSize(), rightStats.getMaxSize());
                leftStats.setMaxSize(maxSize);
            }
        }

        if (leftStats.getNumNulls() == -1) {
            if (rightStats.getNumNulls() != -1) {
                leftStats.setNumNulls(rightStats.getNumNulls());
            }
        } else {
            if (rightStats.getNumNulls() != -1) {
                long numNulls = leftStats.getNumNulls() + rightStats.getNumNulls();
                leftStats.setNumNulls(numNulls);
            }
        }

        if (leftStats.getMinValue() == null) {
            if (rightStats.getMinValue() != null) {
                leftStats.setMinValue(rightStats.getMinValue());
            }
        } else {
            if (rightStats.getMinValue() != null) {
                LiteralExpr minValue = leftStats.getMinValue().compareTo(rightStats.getMinValue()) > 0
                        ? leftStats.getMinValue() : rightStats.getMinValue();
                leftStats.setMinValue(minValue);
            }
        }

        if (leftStats.getMaxValue() == null) {
            if (rightStats.getMaxValue() != null) {
                leftStats.setMaxValue(rightStats.getMaxValue());
            }
        } else {
            if (rightStats.getMaxValue() != null) {
                LiteralExpr maxValue = leftStats.getMaxValue().compareTo(rightStats.getMaxValue()) > 0
                        ? leftStats.getMaxValue() : rightStats.getMaxValue();
                leftStats.setMaxValue(maxValue);
            }
        }
    }
}
