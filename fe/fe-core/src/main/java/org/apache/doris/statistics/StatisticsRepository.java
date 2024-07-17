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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * All the logic that interacts with internal statistics table should be placed here.
 */
public class StatisticsRepository {

    private static final Logger LOG = LogManager.getLogger(StatisticsRepository.class);

    private static final String FULL_QUALIFIED_DB_NAME = "`" + FeConstants.INTERNAL_DB_NAME + "`";

    private static final String FULL_QUALIFIED_COLUMN_STATISTICS_NAME = FULL_QUALIFIED_DB_NAME + "."
            + "`" + StatisticConstants.TABLE_STATISTIC_TBL_NAME + "`";

    private static final String FULL_QUALIFIED_PARTITION_STATISTICS_NAME = FULL_QUALIFIED_DB_NAME + "."
            + "`" + StatisticConstants.PARTITION_STATISTIC_TBL_NAME + "`";

    private static final String FULL_QUALIFIED_COLUMN_HISTOGRAM_NAME = FULL_QUALIFIED_DB_NAME + "."
            + "`" + StatisticConstants.HISTOGRAM_TBL_NAME + "`";

    private static final String FETCH_COLUMN_STATISTIC_TEMPLATE = "SELECT * FROM "
            + FULL_QUALIFIED_COLUMN_STATISTICS_NAME
            + " WHERE `id` = '${id}' AND `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}'";

    private static final String FETCH_PARTITION_STATISTIC_TEMPLATE = "SELECT `catalog_id`, `db_id`, `tbl_id`, `idx_id`,"
            + " `part_name`, `col_id`, `count`, hll_to_base64(`ndv`) as ndv, `null_count`, `min`, `max`, "
            + "`data_size_in_bytes`, `update_time` FROM " + FULL_QUALIFIED_PARTITION_STATISTICS_NAME
            + " WHERE `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}' AND `tbl_id` = ${tableId}"
            + " AND `idx_id` = '${indexId}' AND `part_name` IN (${partName}) AND `col_id` = '${columnId}'";

    private static final String FETCH_PARTITIONS_STATISTIC_TEMPLATE = "SELECT col_id, part_name, idx_id, count, "
            + "hll_cardinality(ndv) as ndv, null_count, min, max, data_size_in_bytes, update_time FROM "
            + FULL_QUALIFIED_PARTITION_STATISTICS_NAME
            + " WHERE `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}' AND `tbl_id` = ${tableId}"
            + " AND `part_name` in (${partitionInfo}) AND `col_id` in (${columnInfo})";

    private static final String FETCH_COLUMN_HISTOGRAM_TEMPLATE = "SELECT * FROM "
            + FULL_QUALIFIED_COLUMN_HISTOGRAM_NAME
            + " WHERE `id` = '${id}' AND `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}'";

    private static final String INSERT_INTO_COLUMN_STATISTICS_FOR_ALTER = "INSERT INTO "
            + FULL_QUALIFIED_COLUMN_STATISTICS_NAME + " VALUES('${id}', ${catalogId}, ${dbId}, ${tblId}, '${idxId}',"
            + "'${colId}', ${partId}, ${count}, ${ndv}, ${nullCount}, ${min}, ${max}, ${dataSize}, NOW())";

    private static final String DELETE_TABLE_STATISTICS_BY_COLUMN_TEMPLATE = "DELETE FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.TABLE_STATISTIC_TBL_NAME
            + " WHERE `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}' AND `tbl_id` = '${tblId}'"
            + "${columnCondition}";

    private static final String DELETE_TABLE_STATISTICS_ALL_COLUMN_TEMPLATE = "DELETE FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.TABLE_STATISTIC_TBL_NAME
            + " WHERE `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}' AND `tbl_id` = '${tblId}'";

    private static final String DELETE_PARTITION_STATISTICS_TEMPLATE = "DELETE FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.PARTITION_STATISTIC_TBL_NAME
            + " WHERE `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}' AND `tbl_id` = '${tblId}'"
            + " ${columnCondition} ${partitionCondition}";

    private static final String FETCH_RECENT_STATS_UPDATED_COL =
            "SELECT * FROM "
                    + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.TABLE_STATISTIC_TBL_NAME
                    + " WHERE part_id is NULL "
                    + " ORDER BY update_time DESC LIMIT "
                    + Config.stats_cache_size;

    private static final String FETCH_TABLE_STATS_FULL_NAME =
            "SELECT id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id FROM "
                    + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.TABLE_STATISTIC_TBL_NAME
                    + " ORDER BY update_time "
                    + "LIMIT ${limit} OFFSET ${offset}";

    private static final String FETCH_PARTITION_STATS_FULL_NAME =
            "SELECT \"\" as id, catalog_id, db_id, tbl_id, idx_id, col_id, part_name FROM "
                    + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.PARTITION_STATISTIC_TBL_NAME
                    + " ORDER BY update_time "
                    + "LIMIT ${limit} OFFSET ${offset}";

    private static final String FETCH_TABLE_STATISTICS = "SELECT * FROM "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.TABLE_STATISTIC_TBL_NAME
            + " WHERE tbl_id = ${tblId} AND `catalog_id` = '${catalogId}' AND `db_id` = '${dbId}'"
            + " AND part_id IS NULL";

    public static ColumnStatistic queryColumnStatisticsByName(
            long ctlId, long dbId, long tableId, long indexId, String colName) {
        ResultRow resultRow = queryColumnStatisticById(ctlId, dbId, tableId, indexId, colName);
        if (resultRow == null) {
            return ColumnStatistic.UNKNOWN;
        }
        return ColumnStatistic.fromResultRow(resultRow);
    }

    public static List<ResultRow> queryColumnStatisticsByPartitions(TableIf table, Set<String> columnNames,
            List<String> partitionNames) {
        if (!table.isPartitionedTable()) {
            return new ArrayList<>();
        }
        long ctlId = table.getDatabase().getCatalog().getId();
        long dbId = table.getDatabase().getId();
        Map<String, String> params = new HashMap<>();
        generateCtlDbIdParams(ctlId, dbId, params);
        params.put("tableId", String.valueOf(table.getId()));
        StringJoiner sj = new StringJoiner(",");
        for (String colName : columnNames) {
            sj.add("'" + StatisticsUtil.escapeSQL(colName) + "'");
        }
        params.put("columnInfo", sj.toString());
        sj = new StringJoiner(",");
        for (String part : partitionNames) {
            sj.add("'" + StatisticsUtil.escapeSQL(part) + "'");
        }
        params.put("partitionInfo", sj.toString());
        return StatisticsUtil.executeQuery(FETCH_PARTITIONS_STATISTIC_TEMPLATE, params);
    }

    public static List<ResultRow> queryColumnStatisticsForTable(long ctlId, long dbId, long tableId) {
        Map<String, String> params = new HashMap<>();
        params.put("tblId", String.valueOf(tableId));
        generateCtlDbIdParams(ctlId, dbId, params);
        List<ResultRow> rows = StatisticsUtil.executeQuery(FETCH_TABLE_STATISTICS, params);
        return rows == null ? Collections.emptyList() : rows;
    }

    private static ResultRow queryColumnStatisticById(
            long ctlId, long dbId, long tblId, long indexId, String colName) {
        return queryColumnStatisticById(ctlId, dbId, tblId, indexId, colName, false);
    }

    private static ResultRow queryColumnHistogramById(
            long ctlId, long dbId, long tblId, long indexId, String colName) {
        return queryColumnStatisticById(ctlId, dbId, tblId, indexId, colName, true);
    }

    private static ResultRow queryColumnStatisticById(long ctlId, long dbId, long tblId, long indexId, String colName,
            boolean isHistogram) {
        Map<String, String> map = new HashMap<>();
        String id = constructId(tblId, indexId, colName);
        map.put("id", StatisticsUtil.escapeSQL(id));
        generateCtlDbIdParams(ctlId, dbId, map);
        List<ResultRow> rows = isHistogram ? StatisticsUtil.executeQuery(FETCH_COLUMN_HISTOGRAM_TEMPLATE, map) :
                StatisticsUtil.executeQuery(FETCH_COLUMN_STATISTIC_TEMPLATE, map);
        int size = rows.size();
        if (size > 1) {
            throw new IllegalStateException(String.format("id: %s should be unique, but return more than one row", id));
        }
        return size == 0 ? null : rows.get(0);
    }

    private static Histogram queryColumnHistogramByName(
            long ctlId, long dbId, long tableId, long indexId, String colName) {
        ResultRow resultRow = queryColumnHistogramById(ctlId, dbId, tableId, indexId, colName);
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

    public static void dropStatistics(
            long ctlId, long dbId, long tblId, Set<String> colNames, Set<String> partNames) throws DdlException {
        if (colNames == null && partNames == null) {
            executeDropStatisticsAllColumnSql(ctlId, dbId, tblId);
        } else {
            dropStatisticsByColAndPartitionName(ctlId, dbId, tblId, colNames, partNames);
        }
    }

    public static void dropPartitionsColumnStatistics(long ctlId, long dbId, long tblId,
            String columnCondition, String partitionCondition) throws DdlException {
        Map<String, String> params = new HashMap<>();
        generateCtlDbIdParams(ctlId, dbId, params);
        params.put("tblId", String.valueOf(tblId));
        params.put("columnCondition", columnCondition);
        params.put("partitionCondition", partitionCondition);
        try {
            StatisticsUtil.execUpdate(
                new StringSubstitutor(params).replace(DELETE_PARTITION_STATISTICS_TEMPLATE));
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private static void dropStatisticsByColAndPartitionName(long ctlId, long dbId, long tblId,
                                                Set<String> colNames, Set<String> partNames)
            throws DdlException {
        Map<String, String> params = new HashMap<>();
        String columnCondition = "";
        String partitionCondition = "";
        if (colNames != null) {
            Iterator<String> iterator = colNames.iterator();
            StringBuilder inPredicate = new StringBuilder();
            while (iterator.hasNext()) {
                inPredicate.append("'");
                inPredicate.append(iterator.next());
                inPredicate.append("'");
                inPredicate.append(",");
            }
            if (inPredicate.length() > 0) {
                inPredicate.delete(inPredicate.length() - 1, inPredicate.length());
            }
            columnCondition = String.format("AND %s IN (%s)", "col_id", inPredicate);
        }
        if (partNames != null) {
            Iterator<String> iterator = partNames.iterator();
            StringBuilder inPredicate = new StringBuilder();
            while (iterator.hasNext()) {
                inPredicate.append("'");
                inPredicate.append(iterator.next());
                inPredicate.append("'");
                inPredicate.append(",");
            }
            if (inPredicate.length() > 0) {
                inPredicate.delete(inPredicate.length() - 1, inPredicate.length());
            }
            partitionCondition = String.format("AND %s IN (%s)", "part_name", inPredicate);
        }
        executeDropStatisticsByColumnAndPartitionSql(
                columnCondition, partitionCondition, ctlId, dbId, tblId, params, partNames == null);

    }

    private static void executeDropStatisticsByColumnAndPartitionSql(String columnCondition, String partitionCondition,
            long ctlId, long dbId, long tblId, Map<String, String> params, boolean tableLevel) throws DdlException {
        generateCtlDbIdParams(ctlId, dbId, params);
        params.put("tblId", String.valueOf(tblId));
        params.put("columnCondition", columnCondition);
        params.put("partitionCondition", partitionCondition);
        try {
            if (tableLevel) {
                StatisticsUtil.execUpdate(
                    new StringSubstitutor(params).replace(DELETE_TABLE_STATISTICS_BY_COLUMN_TEMPLATE));
            }
            StatisticsUtil.execUpdate(
                new StringSubstitutor(params).replace(DELETE_PARTITION_STATISTICS_TEMPLATE));
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private static void executeDropStatisticsAllColumnSql(long ctlId, long dbId, long tblId) throws DdlException {
        Map<String, String> params = new HashMap<>();
        generateCtlDbIdParams(ctlId, dbId, params);
        params.put("tblId", String.valueOf(tblId));
        params.put("columnCondition", "");
        params.put("partitionCondition", "");
        try {
            StatisticsUtil.execUpdate(
                new StringSubstitutor(params).replace(DELETE_TABLE_STATISTICS_ALL_COLUMN_TEMPLATE));
            StatisticsUtil.execUpdate(
                new StringSubstitutor(params).replace(DELETE_PARTITION_STATISTICS_TEMPLATE));
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
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
        long indexId = alterColumnStatsStmt.getIndexId();
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
            double size = Double.parseDouble(dataSize);
            double rows = Double.parseDouble(rowCount);
            if (size > 0) {
                builder.setDataSize(size);
                if (rows > 0) {
                    builder.setAvgSizeByte(size / rows);
                }
            }
        }

        ColumnStatistic columnStatistic = builder.build();
        Map<String, String> params = new HashMap<>();
        params.put("id", constructId(objects.table.getId(), indexId, colName));
        params.put("catalogId", String.valueOf(objects.catalog.getId()));
        params.put("dbId", String.valueOf(objects.db.getId()));
        params.put("idxId", String.valueOf(indexId));
        params.put("tblId", String.valueOf(objects.table.getId()));
        params.put("colId", String.valueOf(colName));
        params.put("count", String.valueOf(columnStatistic.count));
        params.put("ndv", String.valueOf(columnStatistic.ndv));
        params.put("nullCount", String.valueOf(columnStatistic.numNulls));
        params.put("min", min == null ? "NULL" : "'" + StatisticsUtil.escapeSQL(min) + "'");
        params.put("max", max == null ? "NULL" : "'" + StatisticsUtil.escapeSQL(max) + "'");
        params.put("dataSize", String.valueOf(columnStatistic.dataSize));

        if (partitionIds.isEmpty()) {
            // update table granularity statistics
            params.put("partId", "NULL");
            StatisticsUtil.execUpdate(INSERT_INTO_COLUMN_STATISTICS_FOR_ALTER, params);
            ColStatsData data = new ColStatsData(constructId(objects.table.getId(), indexId, colName),
                    objects.catalog.getId(), objects.db.getId(), objects.table.getId(), indexId, colName,
                    null, columnStatistic);
            Env.getCurrentEnv().getStatisticsCache().syncColStats(data);
            AnalysisInfo mockedJobInfo = new AnalysisInfoBuilder()
                    .setTblUpdateTime(System.currentTimeMillis())
                    .setColName("")
                    .setJobColumns(Sets.newHashSet())
                    .setUserInject(true)
                    .setJobType(AnalysisInfo.JobType.MANUAL)
                    .build();
            Env.getCurrentEnv().getAnalysisManager().updateTableStatsForAlterStats(mockedJobInfo, objects.table);
        } else {
            // update partition granularity statistics
            for (Long partitionId : partitionIds) {
                HashMap<String, String> partParams = Maps.newHashMap(params);
                partParams.put("partId", String.valueOf(partitionId));
                StatisticsUtil.execUpdate(INSERT_INTO_COLUMN_STATISTICS_FOR_ALTER, partParams);
                // TODO cache partition granular statistics
                // Env.getCurrentEnv().getStatisticsCache()
                //         .updateColStatsCache(partitionId, -1, colName, builder.build());
            }
        }
    }

    public static List<ResultRow> fetchRecentStatsUpdatedCol() {
        return StatisticsUtil.execStatisticQuery(FETCH_RECENT_STATS_UPDATED_COL);
    }

    public static List<ResultRow> fetchStatsFullName(long limit, long offset, boolean isTableStats) {
        Map<String, String> params = new HashMap<>();
        params.put("limit", String.valueOf(limit));
        params.put("offset", String.valueOf(offset));
        String template = isTableStats ? FETCH_TABLE_STATS_FULL_NAME : FETCH_PARTITION_STATS_FULL_NAME;
        return StatisticsUtil.execStatisticQuery(new StringSubstitutor(params).replace(template));
    }

    public static List<ResultRow> loadColStats(long ctlId, long dbId, long tableId, long idxId, String colName) {
        Map<String, String> params = new HashMap<>();
        String id = constructId(tableId, idxId, colName);
        params.put("id", StatisticsUtil.escapeSQL(id));
        generateCtlDbIdParams(ctlId, dbId, params);
        return StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                .replace(FETCH_COLUMN_STATISTIC_TEMPLATE));
    }

    public static List<ResultRow> loadPartitionColumnStats(long ctlId, long dbId, long tableId, long idxId,
                                                     String partName, String colName) {
        Map<String, String> params = new HashMap<>();
        generateCtlDbIdParams(ctlId, dbId, params);
        params.put("tableId", String.valueOf(tableId));
        params.put("indexId", String.valueOf(idxId));
        params.put("partName", partName);
        params.put("columnId", StatisticsUtil.escapeSQL(colName));
        return StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
            .replace(FETCH_PARTITION_STATISTIC_TEMPLATE));
    }

    private static void generateCtlDbIdParams(long ctdId, long dbId, Map<String, String> params) {
        params.put("catalogId", String.valueOf(ctdId));
        params.put("dbId", String.valueOf(dbId));
    }
}
