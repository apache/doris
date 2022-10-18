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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * There are the statistics of all tables.
 * The @Statistics are mainly used to provide input for the Optimizer's cost model.
 *
 * @idToTableStats: <@Long tableId, @TableStats tableStats>
 *     - Each table will have corresponding @TableStats
 *     - Those @TableStats are recorded in @idToTableStats form of MAP.
 *     - This facilitates the optimizer to quickly find the corresponding
 * @TableStats based on the table id.
 */
public class Statistics {
    private static final Logger LOG = LogManager.getLogger(Statistics.class);

    private final Map<Long, TableStats> idToTableStats = Maps.newConcurrentMap();

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
     * If the table statistics do not exist, the default statistics will be returned.
     */
    public TableStats getTableStatsOrDefault(long tableId) throws AnalysisException {
        return idToTableStats.getOrDefault(tableId, TableStats.getDefaultTableStats());
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
    public Map<String, ColumnStat> getColumnStats(long tableId) throws AnalysisException {
        TableStats tableStats = getTableStats(tableId);
        Map<String, ColumnStat> nameToColumnStats = tableStats.getNameToColumnStats();
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
    public Map<String, ColumnStat> getColumnStats(long tableId, String partitionName) throws AnalysisException {
        Map<String, PartitionStats> partitionStats = getPartitionStats(tableId, partitionName);
        PartitionStats partitionStat = partitionStats.get(partitionName);
        if (partitionStat == null) {
            throw new AnalysisException("Partition " + partitionName + " of table " + tableId + " has no statistics");
        }
        return partitionStat.getNameToColumnStats();
    }

    public void updateTableStats(long tableId, Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        synchronized (this) {
            TableStats tableStats = getNotNullTableStats(tableId);
            tableStats.updateTableStats(statsTypeToValue);
        }
    }

    public void updatePartitionStats(long tableId, String partitionName, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        synchronized (this) {
            TableStats tableStats = getNotNullTableStats(tableId);
            tableStats.updatePartitionStats(partitionName, statsTypeToValue);
        }
    }

    public void updateColumnStats(long tableId, String columnName, Type columnType,
                                  Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        synchronized (this) {
            TableStats tableStats = getNotNullTableStats(tableId);
            tableStats.updateColumnStats(columnName, columnType, statsTypeToValue);
        }
    }

    public void updateColumnStats(long tableId, String partitionName, String columnName, Type columnType,
                                  Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        synchronized (this) {
            PartitionStats partitionStats = getNotNullPartitionStats(tableId, partitionName);
            partitionStats.updateColumnStats(columnName, columnType, statsTypeToValue);
        }
    }

    public void dropTableStats(long tableId) {
        dropPartitionStats(tableId, null);
    }

    public void dropPartitionStats(long tableId, String partitionName) {
        synchronized (this) {
            if (idToTableStats.containsKey(tableId)) {
                if (Strings.isNullOrEmpty(partitionName)) {
                    idToTableStats.remove(tableId);
                    LOG.info("Deleted table(id={}) statistics.", tableId);
                } else {
                    TableStats tableStats = idToTableStats.get(tableId);
                    tableStats.getNameToPartitionStats().remove(partitionName);
                    LOG.info("Deleted statistics for partition {} of table(id={}).",
                            partitionName, tableId);
                }
            }
        }
    }

    // TODO: mock statistics need to be removed in the future
    public void mockTableStatsWithRowCount(long tableId, double rowCount) {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats(rowCount, 1);
            idToTableStats.put(tableId, tableStats);
        }
    }

    /**
     * if the table stats is not exist, create a new one.
     *
     * @param tableId table id
     * @return @TableStats
     */
    private TableStats getNotNullTableStats(long tableId) {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            idToTableStats.put(tableId, tableStats);
        }
        return tableStats;
    }

    /**
     * if the partition stats is not exist, create a new one.
     *
     * @param tableId table id
     * @param partitionName partition name
     * @return @TableStats
     */
    private PartitionStats getNotNullPartitionStats(long tableId, String partitionName) {
        TableStats tableStats = getNotNullTableStats(tableId);
        Map<String, PartitionStats> nameToPartitionStats = tableStats.getNameToPartitionStats();
        PartitionStats partitionStats = nameToPartitionStats.get(partitionName);
        if (partitionStats == null) {
            partitionStats = new PartitionStats();
            nameToPartitionStats.put(partitionName, partitionStats);
        }
        return partitionStats;
    }
}
