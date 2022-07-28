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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * There are the statistics of all of tables.
 * The @Statistics are mainly used to provide input for the Optimizer's cost model.
 *
 * @idToTableStats: <@Long tableId, @TableStats tableStats>
 * Each table will have corresponding @TableStats.
 * Those @TableStats are recorded in @idToTableStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding
 * @TableStats based on the table id.
 */
public class Statistics {

    private final Map<Long, TableStats> idToTableStats = Maps.newConcurrentMap();

    public void updateTableStats(long tableId, Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        TableStats tableStats = getNotNullTableStats(tableId);
        tableStats.updateTableStats(statsTypeToValue);
    }

    public void updatePartitionStats(long tableId, String partitionName, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        TableStats tableStats = getNotNullTableStats(tableId);
        tableStats.updatePartitionStats(partitionName, statsTypeToValue);
    }

    public void updateColumnStats(long tableId, String columnName, Type columnType,
                                  Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        TableStats tableStats = getNotNullTableStats(tableId);
        tableStats.updateColumnStats(columnName, columnType, statsTypeToValue);
    }

    public void updateColumnStats(long tableId, String partitionName, String columnName, Type columnType,
                                  Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        TableStats tableStats = getNotNullTableStats(tableId);
        Map<String, PartitionStats> nameToPartitionStats = tableStats.getNameToPartitionStats();
        PartitionStats partitionStats = nameToPartitionStats.get(partitionName);
        partitionStats.updateColumnStats(columnName, columnType, statsTypeToValue);
    }

    /**
     * if the table stats is not exist, create a new one.
     *
     * @param tableId table id
     * @return @TableStats
     */
    public TableStats getNotNullTableStats(long tableId) {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            idToTableStats.put(tableId, tableStats);
        }
        return tableStats;
    }

    /**
     * Get the table stats for the given table id.
     *
     * @param tableId table id
     * @return @TableStats
     * @throws AnalysisException if table stats not exists
     */
    public TableStats getTableStats(long tableId) throws AnalysisException {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            throw new AnalysisException("Table " + tableId + " has no statistics");
        }
        return tableStats;
    }

    /**
     * Get the partitions stats for the given table id.
     *
     * @param tableId table id
     * @return partition name and @PartitionStats
     * @throws AnalysisException if partitions stats not exists
     */
    public Map<String, PartitionStats> getPartitionStats(long tableId) throws AnalysisException {
        TableStats tableStats = getTableStats(tableId);
        Map<String, PartitionStats> nameToPartitionStats = tableStats.getNameToPartitionStats();
        if (nameToPartitionStats == null) {
            throw new AnalysisException("Table " + tableId + " has no partition statistics");
        }
        return nameToPartitionStats;
    }

    /**
     * Get the partition stats for the given table id and partition name.
     *
     * @param tableId table id
     * @param partitionName partition name
     * @return partition name and @PartitionStats
     * @throws AnalysisException if partition stats not exists
     */
    public Map<String, PartitionStats> getPartitionStats(long tableId, String partitionName)
            throws AnalysisException {
        Map<String, PartitionStats> partitionStats = getPartitionStats(tableId);
        PartitionStats partitionStat = partitionStats.get(partitionName);
        if (partitionStat == null) {
            throw new AnalysisException("Partition " + partitionName + " of table " + tableId + " has no statistics");
        }
        Map<String, PartitionStats> statsMap = Maps.newHashMap();
        statsMap.put(partitionName, partitionStat);
        return statsMap;
    }

    /**
     * Get the columns stats for the given table id.
     *
     * @param tableId table id
     * @return column name and @ColumnStats
     * @throws AnalysisException if columns stats not exists
     */
    public Map<String, ColumnStats> getColumnStats(long tableId) throws AnalysisException {
        TableStats tableStats = getTableStats(tableId);
        Map<String, ColumnStats> nameToColumnStats = tableStats.getNameToColumnStats();
        if (nameToColumnStats == null) {
            throw new AnalysisException("Table " + tableId + " has no column statistics");
        }
        return nameToColumnStats;
    }

    /**
     * Get the columns stats for the given table id and partition name.
     *
     * @param tableId table id
     * @param partitionName partition name
     * @return column name and @ColumnStats
     * @throws AnalysisException if column stats not exists
     */
    public Map<String, ColumnStats> getColumnStats(long tableId, String partitionName) throws AnalysisException {
        Map<String, PartitionStats> partitionStats = getPartitionStats(tableId, partitionName);
        PartitionStats partitionStat = partitionStats.get(partitionName);
        if (partitionStat == null) {
            throw new AnalysisException("Partition " + partitionName + " of table " + tableId + " has no statistics");
        }
        return partitionStat.getNameToColumnStats();
    }

    // TODO: mock statistics need to be removed in the future
    public void mockTableStatsWithRowCount(long tableId, long rowCount) {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            idToTableStats.put(tableId, tableStats);
        }

        if (tableStats.getRowCount() != rowCount) {
            tableStats.setRowCount(rowCount);
        }
    }

    // Used for unit test
    public void putTableStats(long id, TableStats tableStats) {
        this.idToTableStats.put(id, tableStats);
    }
}
