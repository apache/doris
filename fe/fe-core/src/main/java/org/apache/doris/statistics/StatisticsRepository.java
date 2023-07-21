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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * All the logic that interacts with internal statistics table should be placed here.
 */
public class StatisticsRepository {

    private static final Logger LOG = LogManager.getLogger(StatisticsRepository.class);

    private static final String FULL_QUALIFIED_DB_NAME = "`" + SystemInfoService.DEFAULT_CLUSTER + ":"
            + FeConstants.INTERNAL_DB_NAME + "`";

    private static final String FULL_QUALIFIED_COLUMN_STATISTICS_NAME = FULL_QUALIFIED_DB_NAME + "."
            + "`" + StatisticConstants.STATISTIC_TBL_NAME + "`";

    private static final String FULL_QUALIFIED_COLUMN_HISTOGRAM_NAME = FULL_QUALIFIED_DB_NAME + "."
            + "`" + StatisticConstants.HISTOGRAM_TBL_NAME + "`";

    private static final String FETCH_COLUMN_STATISTIC_TEMPLATE = "SELECT * FROM "
            + FULL_QUALIFIED_COLUMN_STATISTICS_NAME
            + " WHERE `id` = '${id}'";

    private static final String FETCH_PARTITIONS_STATISTIC_TEMPLATE = "SELECT * FROM "
            + FULL_QUALIFIED_COLUMN_STATISTICS_NAME
            + " WHERE `id` IN (${idList})";

    private static final String FETCH_COLUMN_HISTOGRAM_TEMPLATE = "SELECT * FROM "
            + FULL_QUALIFIED_COLUMN_HISTOGRAM_NAME
            + " WHERE `id` = '${id}'";

    private static final String INSERT_INTO_COLUMN_STATISTICS = "INSERT INTO "
            + FULL_QUALIFIED_COLUMN_STATISTICS_NAME + " VALUES('${id}', ${catalogId}, ${dbId}, ${tblId}, '${idxId}',"
            + "'${colId}', ${partId}, ${count}, ${ndv}, ${nullCount}, '${min}', '${max}', ${dataSize}, NOW())";

    private static final String DROP_TABLE_STATISTICS_TEMPLATE = "DELETE FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + "${tblName}" + " WHERE ${condition}";

    private static final String FETCH_RECENT_STATS_UPDATED_COL =
            "SELECT * FROM "
                    + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.STATISTIC_TBL_NAME
                    + " WHERE part_id is NULL "
                    + " ORDER BY update_time DESC LIMIT "
                    + Config.stats_cache_size;

    private static final String FETCH_STATS_FULL_NAME =
            "SELECT id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id FROM "
                    + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.STATISTIC_TBL_NAME
                    + " ORDER BY update_time "
                    + "LIMIT ${limit} OFFSET ${offset}";

    private static final String FETCH_STATS_PART_ID = "SELECT col_id, part_id FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.STATISTIC_TBL_NAME
            + " WHERE tbl_id = ${tblId}"
            + " AND part_id IS NOT NULL";

    private static final String PERSIST_TABLE_STATS_TEMPLATE = "INSERT INTO "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_TBL_NAME
            + " VALUES('${id}', ${catalogId}, ${dbId}, ${tblId}, ${indexId}, ${partId}, ${rowCount},"
            + " ${lastAnalyzeTimeInMs}, NOW())";

    private static final String FETCH_TABLE_LEVEL_STATS_TEMPLATE = "SELECT * FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_TBL_NAME
            + " WHERE tbl_id = ${tblId}"
            + " AND part_id IS NULL";

    private static final String FETCH_TABLE_LEVEL_PART_STATS_TEMPLATE = "SELECT * FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_TBL_NAME
            + " WHERE part_id = ${partId}";


    private static final String FETCH_PART_TABLE_STATS_TEMPLATE = "SELECT * FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_TBL_NAME
            + " WHERE tbl_id = ${tblId}"
            + " AND part_id IS NOT NULL";

    private static final String QUERY_COLUMN_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.STATISTIC_TBL_NAME + " WHERE "
            + "tbl_id=${tblId} AND idx_id=${idxId} AND col_id='${colId}'";

    private static final String QUERY_PARTITION_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.STATISTIC_TBL_NAME + " WHERE "
            + " tbl_id=${tblId} AND idx_id=${idxId} AND col_id='${colId}' "
            + " AND part_id IS NOT NULL";

    public static ColumnStatistic queryColumnStatisticsByName(long tableId, String colName) {
        ResultRow resultRow = queryColumnStatisticById(tableId, colName);
        if (resultRow == null) {
            return ColumnStatistic.UNKNOWN;
        }
        return ColumnStatistic.fromResultRow(resultRow);
    }

    public static List<ColumnStatistic> queryColumnStatisticsByPartitions(TableName tableName, String colName,
            List<String> partitionNames) throws AnalysisException {
        DBObjects dbObjects = StatisticsUtil.convertTableNameToObjects(tableName);
        Set<Long> partitionIds = new HashSet<>();
        for (String partitionName : partitionNames) {
            Partition partition = dbObjects.table.getPartition(partitionName);
            if (partition == null) {
                throw new AnalysisException(String.format("partition:%s not exists", partitionName));
            }
            partitionIds.add(partition.getId());
        }
        return queryPartitionStatistics(dbObjects.table.getId(),
                colName, partitionIds).stream().map(ColumnStatistic::fromResultRow).collect(
                Collectors.toList());
    }

    public static ResultRow queryColumnStatisticById(long tblId, String colName) {
        return queryColumnStatisticById(tblId, colName, false);
    }

    public static ResultRow queryColumnHistogramById(long tblId, String colName) {
        return queryColumnStatisticById(tblId, colName, true);
    }

    private static ResultRow queryColumnStatisticById(long tblId, String colName, boolean isHistogram) {
        Map<String, String> map = new HashMap<>();
        String id = constructId(tblId, -1, colName);
        map.put("id", id);
        List<ResultRow> rows = isHistogram ? StatisticsUtil.executeQuery(FETCH_COLUMN_HISTOGRAM_TEMPLATE, map) :
                StatisticsUtil.executeQuery(FETCH_COLUMN_STATISTIC_TEMPLATE, map);
        int size = rows.size();
        if (size > 1) {
            throw new IllegalStateException(String.format("id: %s should be unique, but return more than one row", id));
        }
        return size == 0 ? null : rows.get(0);
    }

    public static List<ResultRow> queryPartitionStatistics(long tblId, String colName, Set<Long> partIds) {
        StringJoiner sj = new StringJoiner(",");
        for (Long partId : partIds) {
            sj.add("'" + constructId(tblId, -1, colName, partId) + "'");
        }
        Map<String, String> params = new HashMap<>();
        params.put("idList", sj.toString());
        List<ResultRow> rows = StatisticsUtil.executeQuery(FETCH_PARTITIONS_STATISTIC_TEMPLATE, params);
        return rows == null ? Collections.emptyList() : rows;
    }

    public static Histogram queryColumnHistogramByName(long tableId, String colName) {
        ResultRow resultRow = queryColumnHistogramById(tableId, colName);
        if (resultRow == null) {
            return Histogram.UNKNOWN;
        }
        return Histogram.fromResultRow(resultRow);
    }

    private static String constructId(Object... params) {
        StringJoiner stringJoiner = new StringJoiner("-");
        for (Object param : params) {
            stringJoiner.add(param.toString());
        }
        return stringJoiner.toString();
    }

    public static void dropStatistics(Set<Long> partIds) throws DdlException {
        dropStatisticsByPartId(partIds, StatisticConstants.ANALYSIS_TBL_NAME);
        dropStatisticsByPartId(partIds, StatisticConstants.STATISTIC_TBL_NAME);
    }

    public static void dropStatistics(long tblId, Set<String> colNames) throws DdlException {
        dropStatisticsByColName(tblId, colNames, StatisticConstants.STATISTIC_TBL_NAME);
        dropStatisticsByColName(tblId, colNames, StatisticConstants.HISTOGRAM_TBL_NAME);
    }

    public static void dropExternalTableStatistics(long tblId) throws DdlException {
        Map<String, String> params = new HashMap<>();
        String inPredicate = String.format("tbl_id = %s", tblId);
        params.put("tblName", StatisticConstants.ANALYSIS_TBL_NAME);
        params.put("condition", inPredicate);
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(DROP_TABLE_STATISTICS_TEMPLATE));
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    public static void dropStatisticsByColName(long tblId, Set<String> colNames, String statsTblName)
            throws DdlException {
        Map<String, String> params = new HashMap<>();
        String right = colNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        String inPredicate = String.format("tbl_id = %s AND %s IN (%s)", tblId, "col_id", right);
        params.put("tblName", statsTblName);
        params.put("condition", inPredicate);
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(DROP_TABLE_STATISTICS_TEMPLATE));
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    public static void dropStatisticsByPartId(Set<Long> partIds, String statsTblName) throws DdlException {
        Map<String, String> params = new HashMap<>();
        String right = StatisticsUtil.joinElementsToString(partIds, ",");
        String inPredicate = String.format(" part_id IN (%s)", right);
        params.put("tblName", statsTblName);
        params.put("condition", inPredicate);
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(DROP_TABLE_STATISTICS_TEMPLATE));
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    public static void persistTableStats(Map<String, String> params) throws Exception {
        StatisticsUtil.execUpdate(PERSIST_TABLE_STATS_TEMPLATE, params);
    }

    public static void alterTableStatistics(AlterTableStatsStmt alterTableStatsStmt) throws Exception {
        TableName tableName = alterTableStatsStmt.getTableName();
        DBObjects objects = StatisticsUtil.convertTableNameToObjects(tableName);
        String rowCount = alterTableStatsStmt.getValue(StatsType.ROW_COUNT);
        TableStatisticBuilder builder = new TableStatisticBuilder();
        builder.setRowCount(Long.parseLong(rowCount));
        builder.setLastAnalyzeTimeInMs(0);
        TableStatistic tableStatistic = builder.build();
        Map<String, String> params = new HashMap<>();
        String id = StatisticsUtil.constructId(objects.table.getId(), -1);
        params.put("id", id);
        params.put("catalogId", String.valueOf(objects.catalog.getId()));
        params.put("dbId", String.valueOf(objects.db.getId()));
        params.put("tblId", String.valueOf(objects.table.getId()));
        params.put("indexId", "-1");
        params.put("partId", "NULL");
        params.put("rowCount", String.valueOf(tableStatistic.rowCount));
        params.put("lastAnalyzeTimeInMs", "0");
        StatisticsUtil.execUpdate(PERSIST_TABLE_STATS_TEMPLATE, params);
        // TODO update statistics cache
        // Env.getCurrentEnv().getStatisticsCache()
        //         .updateColStatsCache(objects.table.getId(), -1, builder.build());
    }

    public static void alterColumnStatistics(AlterColumnStatsStmt alterColumnStatsStmt) throws Exception {
        TableName tableName = alterColumnStatsStmt.getTableName();
        List<Long> partitionIds = alterColumnStatsStmt.getPartitionIds();
        DBObjects objects = StatisticsUtil.convertTableNameToObjects(tableName);
        String rowCount = alterColumnStatsStmt.getValue(StatsType.ROW_COUNT);
        String ndv = alterColumnStatsStmt.getValue(StatsType.NDV);
        String nullCount = alterColumnStatsStmt.getValue(StatsType.NUM_NULLS);
        String min = alterColumnStatsStmt.getValue(StatsType.MIN_VALUE);
        String max = alterColumnStatsStmt.getValue(StatsType.MAX_VALUE);
        String dataSize = alterColumnStatsStmt.getValue(StatsType.DATA_SIZE);
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder();
        String colName = alterColumnStatsStmt.getColumnName();
        Column column = objects.table.getColumn(colName);
        if (rowCount != null) {
            builder.setCount(Double.parseDouble(rowCount));
        }
        if (ndv != null) {
            double dNdv = Double.parseDouble(ndv);
            builder.setNdv(dNdv);
            builder.setOriginal(null);
        }
        if (nullCount != null) {
            builder.setNumNulls(Double.parseDouble(nullCount));
        }
        if (min != null) {
            builder.setMinExpr(StatisticsUtil.readableValue(column.getType(), min));
            builder.setMinValue(StatisticsUtil.convertToDouble(column.getType(), min));
        }
        if (max != null) {
            builder.setMaxExpr(StatisticsUtil.readableValue(column.getType(), max));
            builder.setMaxValue(StatisticsUtil.convertToDouble(column.getType(), max));
        }
        if (dataSize != null) {
            builder.setDataSize(Double.parseDouble(dataSize));
        }

        ColumnStatistic columnStatistic = builder.build();
        Map<String, String> params = new HashMap<>();
        params.put("id", constructId(objects.table.getId(), -1, colName));
        params.put("catalogId", String.valueOf(objects.catalog.getId()));
        params.put("dbId", String.valueOf(objects.db.getId()));
        params.put("idxId", "-1");
        params.put("tblId", String.valueOf(objects.table.getId()));
        params.put("colId", String.valueOf(colName));
        params.put("count", String.valueOf(columnStatistic.count));
        params.put("ndv", String.valueOf(columnStatistic.ndv));
        params.put("nullCount", String.valueOf(columnStatistic.numNulls));
        params.put("min", min == null ? "NULL" : min);
        params.put("max", max == null ? "NULL" : max);
        params.put("dataSize", String.valueOf(columnStatistic.dataSize));

        if (partitionIds.isEmpty()) {
            // update table granularity statistics
            params.put("partId", "NULL");
            StatisticsUtil.execUpdate(INSERT_INTO_COLUMN_STATISTICS, params);
            Env.getCurrentEnv().getStatisticsCache()
                    .updateColStatsCache(objects.table.getId(), -1, colName, builder.build());
        } else {
            // update partition granularity statistics
            for (Long partitionId : partitionIds) {
                HashMap<String, String> partParams = Maps.newHashMap(params);
                partParams.put("partId", String.valueOf(partitionId));
                StatisticsUtil.execUpdate(INSERT_INTO_COLUMN_STATISTICS, partParams);
                // TODO cache partition granular statistics
                // Env.getCurrentEnv().getStatisticsCache()
                //         .updateColStatsCache(partitionId, -1, colName, builder.build());
            }
        }
    }

    public static List<ResultRow> fetchRecentStatsUpdatedCol() {
        return StatisticsUtil.execStatisticQuery(FETCH_RECENT_STATS_UPDATED_COL);
    }

    public static List<ResultRow> fetchStatsFullName(long limit, long offset) {
        Map<String, String> params = new HashMap<>();
        params.put("limit", String.valueOf(limit));
        params.put("offset", String.valueOf(offset));
        return StatisticsUtil.execStatisticQuery(new StringSubstitutor(params).replace(FETCH_STATS_FULL_NAME));
    }

    public static Map<String, Set<Long>> fetchColAndPartsForStats(long tblId) {
        Map<String, String> params = Maps.newHashMap();
        params.put("tblId", String.valueOf(tblId));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String partSql = stringSubstitutor.replace(FETCH_STATS_PART_ID);
        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(partSql);

        Map<String, Set<Long>> columnToPartitions = Maps.newHashMap();

        resultRows.forEach(row -> {
            try {
                String colId = row.getColumnValue("col_id");
                String partId = row.getColumnValue("part_id");
                if (partId == null) {
                    return;
                }
                columnToPartitions.computeIfAbsent(colId,
                        k -> new HashSet<>()).add(Long.valueOf(partId));
            } catch (NumberFormatException | DdlException e) {
                LOG.warn("Failed to obtain the column and partition for statistics.",
                        e);
            }
        });

        return columnToPartitions;
    }

    public static TableStatistic fetchTableLevelStats(long tblId) throws DdlException {
        ImmutableMap<String, String> params = ImmutableMap
                .of("tblId", String.valueOf(tblId));
        String sql = StatisticsUtil.replaceParams(FETCH_TABLE_LEVEL_STATS_TEMPLATE, params);
        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(sql);
        if (resultRows.size() == 1) {
            return TableStatistic.fromResultRow(resultRows.get(0));
        }
        throw new DdlException("Query result is not as expected: " + sql);
    }

    public static TableStatistic fetchTableLevelOfPartStats(long partId) throws DdlException {
        ImmutableMap<String, String> params = ImmutableMap
                .of("partId", String.valueOf(partId));
        String sql = StatisticsUtil.replaceParams(FETCH_TABLE_LEVEL_PART_STATS_TEMPLATE, params);
        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(sql);
        if (resultRows.size() == 1) {
            return TableStatistic.fromResultRow(resultRows.get(0));
        }
        throw new DdlException("Query result is not as expected: " + sql);
    }

    public static Map<Long, TableStatistic> fetchTableLevelOfIdPartStats(long tblId) throws DdlException {
        ImmutableMap<String, String> params = ImmutableMap
                .of("tblId", String.valueOf(tblId));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(FETCH_PART_TABLE_STATS_TEMPLATE);
        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(sql);

        if (resultRows.size() == 0) {
            return Collections.emptyMap();
        }

        Map<Long, TableStatistic> idToPartitionTableStats = Maps.newHashMap();

        for (ResultRow resultRow : resultRows) {
            long partId = Long.parseLong(resultRow.getColumnValue("part_id"));
            TableStatistic partStats = TableStatistic.fromResultRow(resultRow);
            idToPartitionTableStats.put(partId, partStats);
        }

        return idToPartitionTableStats;
    }

    public static List<ResultRow> loadColStats(long tableId, long idxId, String colName) {
        Map<String, String> params = new HashMap<>();
        params.put("tblId", String.valueOf(tableId));
        params.put("idxId", String.valueOf(idxId));
        params.put("colId", colName);

        return StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                .replace(QUERY_COLUMN_STATISTICS));
    }

    public static List<ResultRow> loadPartStats(long tableId, long idxId, String colName) {
        Map<String, String> params = new HashMap<>();
        params.put("tblId", String.valueOf(tableId));
        params.put("idxId", String.valueOf(idxId));
        params.put("colId", colName);

        return StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                .replace(QUERY_PARTITION_STATISTICS));
    }
}
