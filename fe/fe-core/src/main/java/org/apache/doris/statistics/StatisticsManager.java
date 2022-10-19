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

import org.apache.doris.analysis.AlterColumnStatsStmt;
import org.apache.doris.analysis.AlterTableStatsStmt;
import org.apache.doris.analysis.DropTableStatsStmt;
import org.apache.doris.analysis.ShowColumnStatsStmt;
import org.apache.doris.analysis.ShowTableStatsStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;
import org.apache.doris.statistics.StatsGranularity.Granularity;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StatisticsManager {

    private final Statistics statistics;

    public StatisticsManager() {
        statistics = new Statistics();
    }

    public Statistics getStatistics() {
        return statistics;
    }

    /**
     * Support for deleting table or partition statistics.
     *
     * @param stmt get table name and partition name from it.
     */
    public void dropStats(DropTableStatsStmt stmt) {
        Map<Long, Set<String>> tblIdToPartition = stmt.getTblIdToPartition();

        if (tblIdToPartition != null && !tblIdToPartition.isEmpty()) {
            tblIdToPartition.forEach((tableId, partitions) -> {
                if (partitions == null || partitions.isEmpty()) {
                    statistics.dropTableStats(tableId);
                } else {
                    for (String partition : partitions) {
                        statistics.dropPartitionStats(tableId, partition);
                    }
                }
            });
        }
    }

    /**
     * Alter table or partition stats. if partition name is not null, update partition stats.
     *
     * @param stmt alter table stats stmt
     * @throws AnalysisException if table or partition not exist
     */
    public void alterTableStatistics(AlterTableStatsStmt stmt) throws AnalysisException {
        Table table = validateTableName(stmt.getTableName());
        List<String> partitionNames = stmt.getPartitionNames();
        Map<StatsType, String> statsTypeToValue = stmt.getStatsTypeToValue();

        if (partitionNames.isEmpty()) {
            statistics.updateTableStats(table.getId(), statsTypeToValue);
            return;
        }

        for (String partitionName : partitionNames) {
            partitionName = validatePartitionName(table, partitionName);
            statistics.updatePartitionStats(table.getId(), partitionName, statsTypeToValue);
        }
    }

    /**
     * Alter column stats. if partition name is not null, update column of partition stats.
     *
     * @param stmt alter column stats stmt
     * @throws AnalysisException if table, column or partition not exist
     */
    public void alterColumnStatistics(AlterColumnStatsStmt stmt) throws AnalysisException {
        Table table = validateTableName(stmt.getTableName());
        String colName = stmt.getColumnName();
        List<String> partitionNames = stmt.getPartitionNames();
        Map<StatsType, String> statsTypeToValue = stmt.getStatsTypeToValue();

        if ((partitionNames.isEmpty()) && table instanceof OlapTable
                && !((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.UNPARTITIONED)) {
            throw new AnalysisException("Partitioned table must specify partition name.");
        }

        if (partitionNames.isEmpty()) {
            Column column = validateColumn(table, colName);
            Type colType = column.getType();
            statistics.updateColumnStats(table.getId(), colName, colType, statsTypeToValue);
            return;
        }

        for (String partitionName : partitionNames) {
            validatePartitionName(table, partitionName);
            Column column = validateColumn(table, colName);
            Type colType = column.getType();
            statistics.updateColumnStats(table.getId(), partitionName, colName, colType, statsTypeToValue);
        }
    }

    /**
     * Update statistics. there are three types of statistics: column, table and column.
     *
     * @param statsTaskResults statistics task results
     * @throws AnalysisException if column, table or partition not exist
     */
    public void updateStatistics(List<StatisticsTaskResult> statsTaskResults) throws AnalysisException {
        // tablet granularity stats(row count, max value, min value, ndv)
        Map<StatsType, Map<TaskResult, List<String>>> tabletStats = Maps.newHashMap();

        for (StatisticsTaskResult statsTaskResult : statsTaskResults) {
            if (statsTaskResult != null) {
                List<TaskResult> taskResults = statsTaskResult.getTaskResults();

                for (TaskResult result : taskResults) {
                    validateResult(result);
                    long tblId = result.getTableId();
                    Map<StatsType, String> statsTypeToValue = result.getStatsTypeToValue();

                    if (result.getGranularity() == Granularity.TABLET) {
                        statsTypeToValue.forEach((statsType, value) -> {
                            if (tabletStats.containsKey(statsType)) {
                                Map<TaskResult, List<String>> resultToValue = tabletStats.get(statsType);
                                List<String> values = resultToValue.get(result);
                                values.add(value);
                            } else {
                                Map<TaskResult, List<String>> resultToValue = Maps.newHashMap();
                                List<String> values = Lists.newArrayList();
                                values.add(value);
                                resultToValue.put(result, values);
                                tabletStats.put(statsType, resultToValue);
                            }
                        });
                        continue;
                    }

                    switch (result.getCategory()) {
                        case TABLE:
                            statistics.updateTableStats(tblId, statsTypeToValue);
                            break;
                        case PARTITION:
                            String partitionName = result.getPartitionName();
                            statistics.updatePartitionStats(tblId, partitionName, statsTypeToValue);
                            break;
                        case COLUMN:
                            updateColumnStats(result, statsTypeToValue);
                            break;
                        default:
                            throw new AnalysisException("Unknown stats category: " + result.getCategory());
                    }
                }
            }
        }

        // update tablet granularity stats
        updateTabletStats(tabletStats);
    }

    private void updateColumnStats(TaskResult result, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        long dbId = result.getDbId();
        long tblId = result.getTableId();
        String partitionName = result.getPartitionName();
        String colName = result.getColumnName();

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
        OlapTable table = (OlapTable) db.getTableOrAnalysisException(tblId);
        Column column = table.getColumn(colName);
        Type colType = column.getType();

        switch (result.getGranularity()) {
            case TABLE:
                statistics.updateColumnStats(tblId, colName, colType, statsTypeToValue);
                break;
            case PARTITION:
                statistics.updateColumnStats(tblId, partitionName, colName, colType, statsTypeToValue);
                break;
            default:
                // The tablet granularity is handle separately
                throw new AnalysisException("Unknown granularity: " + result.getGranularity());
        }
    }

    private void updateTabletStats(Map<StatsType, Map<TaskResult, List<String>>> tabletStats)
            throws AnalysisException {
        for (Map.Entry<StatsType, Map<TaskResult, List<String>>> statsEntry : tabletStats.entrySet()) {
            StatsType statsType = statsEntry.getKey();
            Map<TaskResult, List<String>> resultToValue = statsEntry.getValue();

            for (Map.Entry<TaskResult, List<String>> resultEntry : resultToValue.entrySet()) {
                TaskResult result = resultEntry.getKey();
                List<String> values = resultEntry.getValue();

                switch (statsType) {
                    case ROW_COUNT:
                        updateTabletRowCount(result, values);
                        break;
                    case MAX_VALUE:
                        updateTabletMaxValue(result, values);
                        break;
                    case MIN_VALUE:
                        updateTabletMinValue(result, values);
                        break;
                    case NDV:
                        updateTabletNDV(result, values);
                        break;
                    default:
                        throw new AnalysisException("Unknown stats type: " + statsType);
                }
            }
        }
    }

    /**
     * Get the statistics of a table. if specified partition name, get the statistics of the partition.
     *
     * @param stmt statement
     * @return partition or table statistics
     * @throws AnalysisException statistics not exist
     */
    public List<List<String>> showTableStatsList(ShowTableStatsStmt stmt) throws AnalysisException {
        String dbName = stmt.getDbName();
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        String tableName = stmt.getTableName();
        List<List<String>> result = Lists.newArrayList();

        if (tableName != null) {
            Table table = db.getTableOrAnalysisException(tableName);
            // check priv
            if (!Env.getCurrentEnv().getAuth()
                    .checkTblPriv(ConnectContext.get(), dbName, tableName, PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tableName);
            }

            List<String> partitionNames = stmt.getPartitionNames();

            if (partitionNames.isEmpty()) {
                result.add(showTableStats(table));
            } else {
                for (String partitionName : partitionNames) {
                    validatePartitionName(table, partitionName);
                    result.add(showTableStats(table, partitionName));
                }
            }
        } else {
            for (Table table : db.getTables()) {
                if (!Env.getCurrentEnv().getAuth()
                        .checkTblPriv(ConnectContext.get(), dbName, table.getName(), PrivPredicate.SHOW)) {
                    continue;
                }
                try {
                    result.add(showTableStats(table));
                } catch (AnalysisException e) {
                    // ignore no stats table
                }
            }
        }
        return result;
    }

    /**
     * Get the column statistics of a table. if specified partition name,
     * get the column statistics of the partition.
     *
     * @param stmt statement
     * @return column statistics for  a partition or table
     * @throws AnalysisException statistics not exist
     */
    public List<List<String>> showColumnStatsList(ShowColumnStatsStmt stmt) throws AnalysisException {
        TableName tableName = stmt.getTableName();
        List<String> partitionNames = stmt.getPartitionNames();

        // check meta
        Table table = validateTableName(tableName);

        // check priv
        if (!Env.getCurrentEnv().getAuth()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }

        if (partitionNames.isEmpty()) {
            return showColumnStats(table.getId());
        }

        List<List<String>> result = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            validatePartitionName(table, partitionName);
            result.addAll(showColumnStats(table.getId(), partitionName));
        }
        return result;
    }

    private List<String> showTableStats(Table table) throws AnalysisException {
        TableStats tableStats = statistics.getTableStats(table.getId());
        if (tableStats == null) {
            throw new AnalysisException("There is no statistics in this table:" + table.getName());
        }
        List<String> row = Lists.newArrayList();
        row.add(table.getName());
        row.addAll(tableStats.getShowInfo());
        return row;
    }

    private List<String> showTableStats(Table table, String partitionName) throws AnalysisException {
        Map<String, PartitionStats> partitionStats = statistics.getPartitionStats(table.getId(), partitionName);
        PartitionStats partitionStat = partitionStats.get(partitionName);
        if (partitionStat == null) {
            throw new AnalysisException("There is no statistics in this partition:" + partitionName);
        }
        List<String> row = Lists.newArrayList();
        row.add(partitionName);
        row.addAll(partitionStat.getShowInfo());
        return row;
    }

    private List<List<String>> showColumnStats(long tableId) throws AnalysisException {
        List<List<String>> result = Lists.newArrayList();
        Map<String, ColumnStat> columnStats = statistics.getColumnStats(tableId);
        columnStats.forEach((key, stats) -> {
            List<String> row = Lists.newArrayList();
            row.add(key);
            row.addAll(stats.getShowInfo());
            result.add(row);
        });
        return result;
    }

    private List<List<String>> showColumnStats(long tableId, String partitionName) throws AnalysisException {
        List<List<String>> result = Lists.newArrayList();
        Map<String, ColumnStat> columnStats = statistics.getColumnStats(tableId, partitionName);
        columnStats.forEach((key, stats) -> {
            List<String> row = Lists.newArrayList();
            row.add(key);
            row.addAll(stats.getShowInfo());
            result.add(row);
        });
        return result;
    }

    private void updateTabletRowCount(TaskResult result, List<String> values) throws AnalysisException {
        long statsValue = values.stream().filter(NumberUtils::isCreatable)
                .mapToLong(Long::parseLong).sum();

        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();
        statsTypeToValue.put(StatsType.ROW_COUNT, String.valueOf(statsValue));

        if (result.getCategory() == StatsCategory.Category.TABLE) {
            statistics.updateTableStats(result.getTableId(), statsTypeToValue);
        } else if (result.getCategory() == StatsCategory.Category.PARTITION) {
            statistics.updatePartitionStats(result.getTableId(), result.getPartitionName(), statsTypeToValue);
        }
    }

    private void updateTabletMaxValue(TaskResult result, List<String> values) throws AnalysisException {
        Column column = getNotNullColumn(result);
        Type type = column.getType();
        String maxValue = getNumericMaxOrMinValue(values, type, true);

        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();
        statsTypeToValue.put(StatsType.MAX_VALUE, maxValue);

        updateTabletGranularityStats(result, type, statsTypeToValue);
    }

    private void updateTabletMinValue(TaskResult result, List<String> values) throws AnalysisException {
        Column column = getNotNullColumn(result);
        Type type = column.getType();
        String minValue = getNumericMaxOrMinValue(values, type, false);

        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();
        statsTypeToValue.put(StatsType.MIN_VALUE, minValue);

        updateTabletGranularityStats(result, type, statsTypeToValue);
    }

    private void updateTabletNDV(TaskResult result, List<String> values) throws AnalysisException {
        double statsValue = values.stream().filter(NumberUtils::isCreatable)
                .mapToLong(Long::parseLong).sum();

        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();
        statsTypeToValue.put(StatsType.NDV, String.valueOf(statsValue));

        Column column = getNotNullColumn(result);
        Type type = column.getType();
        updateTabletGranularityStats(result, type, statsTypeToValue);
    }

    private void updateTabletGranularityStats(TaskResult result, Type columnType,
            Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        if (result.getCategory() == StatsCategory.Category.TABLE) {
            statistics.updateColumnStats(result.getTableId(),
                    result.getColumnName(), columnType, statsTypeToValue);
        } else if (result.getCategory() == StatsCategory.Category.PARTITION) {
            statistics.updateColumnStats(result.getTableId(), result.getPartitionName(),
                    result.getColumnName(), columnType, statsTypeToValue);
        }
    }

    private Table validateTableName(TableName dbTableName) throws AnalysisException {
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        return db.getTableOrAnalysisException(tableName);
    }

    /**
     * Partition name is optional, if partition name is not null, it will be validated.
     */
    private String validatePartitionName(Table table, String partitionName) throws AnalysisException {
        if (!table.isPartitioned() && !Strings.isNullOrEmpty(partitionName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_ON_NONPARTITIONED,
                    partitionName, table.getName());
        }

        if (!Strings.isNullOrEmpty(partitionName) && table.getPartition(partitionName) == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION,
                    partitionName, table.getName());
        }

        return partitionName;
    }

    private Column validateColumn(Table table, String columnName) throws AnalysisException {
        Column column = table.getColumn(columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
        }
        return column;
    }

    private void validateResult(TaskResult result) throws AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(result.getDbId());
        Table table = db.getTableOrAnalysisException(result.getTableId());

        if (!Strings.isNullOrEmpty(result.getPartitionName())) {
            validatePartitionName(table, result.getPartitionName());
        }

        if (!Strings.isNullOrEmpty(result.getColumnName())) {
            validateColumn(table, result.getColumnName());
        }

        Map<StatsType, String> statsTypeToValue = result.getStatsTypeToValue();
        if (statsTypeToValue == null || statsTypeToValue.isEmpty()) {
            throw new AnalysisException("StatsTypeToValue is empty.");
        }
    }

    private Column getNotNullColumn(TaskResult result) throws AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(result.getDbId());
        Table table = db.getTableOrAnalysisException(result.getTableId());
        Column column = table.getColumn(result.getColumnName());
        if (column == null) {
            throw new AnalysisException("Column " + result.getColumnName() + " does not exist");
        }
        return column;
    }

    /**
     * Get the max/min value of the column.
     *
     * @param values String List of values
     * @param type column type
     * @param maxOrMin true for max, false for min
     * @return the max/min value of the column.
     */
    private String getNumericMaxOrMinValue(List<String> values, Type type, boolean maxOrMin) {
        if (type.isFixedPointType()) {
            long result = 0L;
            for (String value : values) {
                if (NumberUtils.isCreatable(value)) {
                    long temp = Long.parseLong(value);
                    if (maxOrMin) {
                        result = Math.max(result, temp);
                    } else {
                        result = Math.min(result, temp);
                    }
                }
            }
            return String.valueOf(result);
        }

        if (type.isFloatingPointType()) {
            double result = 0.0;
            for (String value : values) {
                if (NumberUtils.isCreatable(value)) {
                    double temp = Double.parseDouble(value);
                    if (maxOrMin) {
                        result = Math.max(result, temp);
                    } else {
                        result = Math.min(result, temp);
                    }
                }
            }
            return String.valueOf(result);
        }

        // is not numeric type
        values.sort(Comparator.naturalOrder());
        return values.size() > 0 ? values.get(values.size() - 1) : null;
    }
}
