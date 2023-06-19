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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.InternalQueryResult;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveAnalysisTask extends HMSAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(HiveAnalysisTask.class);

    public static final String TOTAL_SIZE = "totalSize";
    public static final String NUM_ROWS = "numRows";
    public static final String NUM_FILES = "numFiles";
    public static final String TIMESTAMP = "transient_lastDdlTime";
    public static final String DELIMITER = "-";

    private static final String ANALYZE_SQL_TABLE_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "${idxId} AS idx_id, "
            + "'${colId}' AS col_id, "
            + "${partId} AS part_id, "
            + "COUNT(1) AS row_count, "
            + "NDV(`${colName}`) AS ndv, "
            + "SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
            + "MIN(`${colName}`) AS min, "
            + "MAX(`${colName}`) AS max, "
            + "${dataSizeFunction} AS data_size, "
            + "NOW() "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}`";

    private static final String ANALYZE_TABLE_COUNT_TEMPLATE = "SELECT COUNT(1) as rowCount "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}`";

    private final boolean isTableLevelTask;
    private final boolean isSamplingPartition;
    private final boolean isPartitionOnly;
    private final Set<String> partitionNames;

    public HiveAnalysisTask(AnalysisInfo info) {
        super(info);
        isTableLevelTask = info.externalTableLevelTask;
        isSamplingPartition = info.samplingPartition;
        isPartitionOnly = info.partitionOnly;
        partitionNames = info.partitionNames;
    }

    private static final String ANALYZE_META_TABLE_COLUMN_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " values ('${id}','${catalogId}', '${dbId}', '${tblId}', '-1', '${colId}', NULL, "
            + "${numRows}, ${ndv}, ${nulls}, '${min}', '${max}', ${dataSize}, '${update_time}')";

    private static final String ANALYZE_META_PARTITION_COLUMN_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " values ('${id}','${catalogId}', '${dbId}', '${tblId}', '-1', '${colId}', '${partId}', "
            + "${numRows}, ${ndv}, ${nulls}, '${min}', '${max}', ${dataSize}, '${update_time}')";

    private static final String ANALYZE_META_TABLE_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " values ('${id}','${catalogId}', '${dbId}', '${tblId}', '-1', '', NULL, "
            + "${numRows}, 0, 0, '', '', ${dataSize}, '${update_time}')";

    /**
     * Collect the stats for external table through sql.
     */
    @Override
    protected void getStatsBySql() throws Exception {
        if (isTableLevelTask) {
            getTableStatsBySql();
        } else {
            getTableColumnStatsBySql();
        }
    }

    /**
     * Get table row count and insert the result to __internal_schema.table_statistics
     */
    private void getTableStatsBySql() throws Exception {
        // Get table level information. An example sql for table stats:
        // INSERT INTO __internal_schema.table_statistics VALUES
        //   ('13055', 13002, 13038, 13055, -1, 'NULL', 5, 1686111064658, NOW())
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        if (isPartitionOnly) {
            for (String partId : partitionNames) {
                StringBuilder sb = new StringBuilder();
                sb.append(ANALYZE_TABLE_COUNT_TEMPLATE);
                sb.append(" where ");
                String[] splits = partId.split("/");
                for (int i = 0; i < splits.length; i++) {
                    String value = splits[i].split("=")[1];
                    splits[i] = splits[i].replace(value, "\'" + value + "\'");
                }
                sb.append(StringUtils.join(splits, " and "));
                Map<String, String> params = buildTableStatsParams(partId);
                setParameterData(parameters, params);
                List<InternalQueryResult.ResultRow> columnResult =
                        StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                        .replace(sb.toString()));
                String rowCount = columnResult.get(0).getColumnValue("rowCount");
                params.put("rowCount", rowCount);
                StatisticsRepository.persistTableStats(params);
            }
        } else {
            Map<String, String> params = buildTableStatsParams("NULL");
            List<InternalQueryResult.ResultRow> columnResult =
                    StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                    .replace(ANALYZE_TABLE_COUNT_TEMPLATE));
            String rowCount = columnResult.get(0).getColumnValue("rowCount");
            params.put("rowCount", rowCount);
            StatisticsRepository.persistTableStats(params);
        }
    }

    /**
     * Get column statistics and insert the result to __internal_schema.column_statistics
     */
    private void getTableColumnStatsBySql() throws Exception {
        // An example sql for a column stats:
        // INSERT INTO __internal_schema.column_statistics
        //   SELECT CONCAT(13055, '-', -1, '-', 'r_regionkey') AS id,
        //   13002 AS catalog_id,
        //   13038 AS db_id,
        //   13055 AS tbl_id,
        //   -1 AS idx_id,
        //   'r_regionkey' AS col_id,
        //   'NULL' AS part_id,
        //   COUNT(1) AS row_count,
        //   NDV(`r_regionkey`) AS ndv,
        //   SUM(CASE WHEN `r_regionkey` IS NULL THEN 1 ELSE 0 END) AS null_count,
        //   MIN(`r_regionkey`) AS min,
        //   MAX(`r_regionkey`) AS max,
        //   0 AS data_size,
        //   NOW() FROM `hive`.`tpch100`.`region`
        if (isPartitionOnly) {
            for (String partId : partitionNames) {
                StringBuilder sb = new StringBuilder();
                sb.append(ANALYZE_SQL_TABLE_TEMPLATE);
                sb.append(" where ");
                String[] splits = partId.split("/");
                for (int i = 0; i < splits.length; i++) {
                    String value = splits[i].split("=")[1];
                    splits[i] = splits[i].replace(value, "\'" + value + "\'");
                }
                sb.append(StringUtils.join(splits, " and "));
                Map<String, String> params = buildTableStatsParams(partId);
                params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
                params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
                params.put("colName", col.getName());
                params.put("colId", info.colName);
                params.put("dataSizeFunction", getDataSizeFunction(col));
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                String sql = stringSubstitutor.replace(sb.toString());
                try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
                    r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
                    this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
                    this.stmtExecutor.execute();
                }
            }
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(ANALYZE_SQL_TABLE_TEMPLATE);
            if (isSamplingPartition) {
                sb.append(" where 1=1 ");
                String[] splitExample = partitionNames.stream().findFirst().get().split("/");
                int parts = splitExample.length;
                List<String> partNames = new ArrayList<>();
                for (String split : splitExample) {
                    partNames.add(split.split("=")[0]);
                }
                List<List<String>> valueLists = new ArrayList<>();
                for (int i = 0; i < parts; i++) {
                    valueLists.add(new ArrayList<>());
                }
                for (String partId : partitionNames) {
                    String[] partIds = partId.split("/");
                    for (int i = 0; i < partIds.length; i++) {
                        valueLists.get(i).add("\'" + partIds[i].split("=")[1] + "\'");
                    }
                }
                for (int i = 0; i < parts; i++) {
                    sb.append(" and ");
                    sb.append(partNames.get(i));
                    sb.append(" in (");
                    sb.append(StringUtils.join(valueLists.get(i), ","));
                    sb.append(") ");
                }
            }
            Map<String, String> params = buildTableStatsParams("NULL");
            params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
            params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
            params.put("colName", col.getName());
            params.put("colId", info.colName);
            params.put("dataSizeFunction", getDataSizeFunction(col));
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            String sql = stringSubstitutor.replace(sb.toString());
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
                r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
                this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
                this.stmtExecutor.execute();
            }
            Env.getCurrentEnv().getStatisticsCache().refreshColStatsSync(tbl.getId(), -1, col.getName());
        }
    }

    private Map<String, String> buildTableStatsParams(String partId) {
        Map<String, String> commonParams = new HashMap<>();
        commonParams.put("id", String.valueOf(tbl.getId()));
        commonParams.put("catalogId", String.valueOf(catalog.getId()));
        commonParams.put("dbId", String.valueOf(db.getId()));
        commonParams.put("tblId", String.valueOf(tbl.getId()));
        commonParams.put("indexId", "-1");
        commonParams.put("idxId", "-1");
        commonParams.put("partId", "\'" + partId + "\'");
        commonParams.put("catalogName", catalog.getName());
        commonParams.put("dbName", db.getFullName());
        commonParams.put("tblName", tbl.getName());
        if (col != null) {
            commonParams.put("type", col.getType().toString());
        }
        commonParams.put("lastAnalyzeTimeInMs", String.valueOf(System.currentTimeMillis()));
        return commonParams;
    }

    @Override
    protected void getStatsByMeta() throws Exception {
        if (isTableLevelTask) {
            getTableStatsByMeta();
        } else {
            getColumnStatsByMeta();
        }
    }

    protected void getTableStatsByMeta() throws Exception {
        // Get table level information.
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        if (isPartitionOnly) {
            for (String partId : partitionNames) {
                Map<String, String> params = buildTableStatsParams(partId);
                // Collect table level row count, null number and timestamp.
                setParameterData(parameters, params);
                StatisticsRepository.persistTableStats(params);
            }
        } else {
            Map<String, String> params = buildTableStatsParams("NULL");
            // Collect table level row count, null number and timestamp.
            setParameterData(parameters, params);
            StatisticsRepository.persistTableStats(params);
        }
    }

    protected void getColumnStatsByMeta() throws Exception {
        List<String> columns = new ArrayList<>();
        columns.add(col.getName());
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("colId", String.valueOf(col.getName()));

        // Get table level information.
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        long rowCount;
        StringSubstitutor stringSubstitutor;
        if (isPartitionOnly) {
            // Collect table level row count, null number and timestamp.
            setParameterData(parameters, params);
            params.put("id", genColumnStatId(tbl.getId(), -1, col.getName(), null));
            List<ColumnStatisticsObj> tableStats = table.getHiveTableColumnStats(columns);
            rowCount = parameters.containsKey(NUM_ROWS) ? Long.parseLong(parameters.get(NUM_ROWS)) : 0;
            // Collect table level ndv, nulls, min and max. tableStats contains at most 1 item;
            for (ColumnStatisticsObj tableStat : tableStats) {
                if (!tableStat.isSetStatsData()) {
                    continue;
                }
                ColumnStatisticsData data = tableStat.getStatsData();
                getStatData(data, params, rowCount);
            }
            stringSubstitutor = new StringSubstitutor(params);
            String sql = stringSubstitutor.replace(ANALYZE_META_TABLE_COLUMN_TEMPLATE);
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
                r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
                this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
                this.stmtExecutor.execute();
            }
        }

        // Get partition level information.
        Map<String, List<ColumnStatisticsObj>> columnStats
                = table.getHivePartitionColumnStats(Lists.newArrayList(partitionNames), columns);
        List<String> partitionAnalysisSQLs = new ArrayList<>();
        for (Map.Entry<String, List<ColumnStatisticsObj>> entry : columnStats.entrySet()) {
            String partName = entry.getKey();
            List<String> partitionValues = new ArrayList<>();
            for (String p : partName.split("/")) {
                partitionValues.add(p.split("=")[1]);
            }
            Partition partition = table.getPartition(partitionValues);
            parameters = partition.getParameters();
            // Collect row count, null number and timestamp.
            setParameterData(parameters, params);
            params.put("id", genColumnStatId(tbl.getId(), -1, col.getName(), partName));
            params.put("partId", partName);
            List<ColumnStatisticsObj> value = entry.getValue();
            Preconditions.checkState(value.size() == 1);
            ColumnStatisticsObj stat = value.get(0);
            if (!stat.isSetStatsData()) {
                continue;
            }
            rowCount = parameters.containsKey(NUM_ROWS) ? Long.parseLong(parameters.get(NUM_ROWS)) : 0;
            // Collect ndv, nulls, min and max for different data type.
            ColumnStatisticsData data = stat.getStatsData();
            getStatData(data, params, rowCount);
            stringSubstitutor = new StringSubstitutor(params);
            partitionAnalysisSQLs.add(stringSubstitutor.replace(ANALYZE_META_PARTITION_COLUMN_TEMPLATE));
        }
        // Update partition level stats for this column.
        for (String partitionSql : partitionAnalysisSQLs) {
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
                r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
                this.stmtExecutor = new StmtExecutor(r.connectContext, partitionSql);
                this.stmtExecutor.execute();
            }
        }
        Env.getCurrentEnv().getStatisticsCache().refreshColStatsSync(tbl.getId(), -1, col.getName());
    }

    private void getStatData(ColumnStatisticsData data, Map<String, String> params, long rowCount) {
        long ndv = 0;
        long nulls = 0;
        String min = "";
        String max = "";
        long colSize = 0;
        if (!data.isSetStringStats()) {
            colSize = rowCount * col.getType().getSlotSize();
        }
        // Collect ndv, nulls, min and max for different data type.
        if (data.isSetLongStats()) {
            LongColumnStatsData longStats = data.getLongStats();
            ndv = longStats.getNumDVs();
            nulls = longStats.getNumNulls();
            min = String.valueOf(longStats.getLowValue());
            max = String.valueOf(longStats.getHighValue());
        } else if (data.isSetStringStats()) {
            StringColumnStatsData stringStats = data.getStringStats();
            ndv = stringStats.getNumDVs();
            nulls = stringStats.getNumNulls();
            double avgColLen = stringStats.getAvgColLen();
            colSize = Math.round(avgColLen * rowCount);
        } else if (data.isSetDecimalStats()) {
            DecimalColumnStatsData decimalStats = data.getDecimalStats();
            ndv = decimalStats.getNumDVs();
            nulls = decimalStats.getNumNulls();
            if (decimalStats.isSetLowValue()) {
                Decimal lowValue = decimalStats.getLowValue();
                if (lowValue != null) {
                    BigDecimal lowDecimal = new BigDecimal(new BigInteger(lowValue.getUnscaled()), lowValue.getScale());
                    min = lowDecimal.toString();
                }
            }
            if (decimalStats.isSetHighValue()) {
                Decimal highValue = decimalStats.getHighValue();
                if (highValue != null) {
                    BigDecimal highDecimal = new BigDecimal(
                            new BigInteger(highValue.getUnscaled()), highValue.getScale());
                    max = highDecimal.toString();
                }
            }
        } else if (data.isSetDoubleStats()) {
            DoubleColumnStatsData doubleStats = data.getDoubleStats();
            ndv = doubleStats.getNumDVs();
            nulls = doubleStats.getNumNulls();
            min = String.valueOf(doubleStats.getLowValue());
            max = String.valueOf(doubleStats.getHighValue());
        } else if (data.isSetDateStats()) {
            DateColumnStatsData dateStats = data.getDateStats();
            ndv = dateStats.getNumDVs();
            nulls = dateStats.getNumNulls();
            if (dateStats.isSetLowValue()) {
                org.apache.hadoop.hive.metastore.api.Date lowValue = dateStats.getLowValue();
                if (lowValue != null) {
                    LocalDate lowDate = LocalDate.ofEpochDay(lowValue.getDaysSinceEpoch());
                    min = lowDate.toString();
                }
            }
            if (dateStats.isSetHighValue()) {
                org.apache.hadoop.hive.metastore.api.Date highValue = dateStats.getHighValue();
                if (highValue != null) {
                    LocalDate highDate = LocalDate.ofEpochDay(highValue.getDaysSinceEpoch());
                    max = highDate.toString();
                }
            }
        } else {
            throw new RuntimeException("Not supported data type.");
        }
        params.put("ndv", String.valueOf(ndv));
        params.put("nulls", String.valueOf(nulls));
        params.put("min", min);
        params.put("max", max);
        params.put("dataSize", String.valueOf(colSize));
    }

    private void setParameterData(Map<String, String> parameters, Map<String, String> params) {
        String numRows = "";
        String timestamp = "";
        if (parameters.containsKey(NUM_ROWS)) {
            numRows = parameters.get(NUM_ROWS);
        }
        if (parameters.containsKey(TIMESTAMP)) {
            timestamp = parameters.get(TIMESTAMP);
        }
        params.put("numRows", numRows);
        params.put("rowCount", numRows);
        params.put("update_time", TimeUtils.DATETIME_FORMAT.format(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(timestamp) * 1000),
                        ZoneId.systemDefault())));
    }

    private String genColumnStatId(long tableId, long indexId, String columnName, String partitionName) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableId);
        sb.append(DELIMITER);
        sb.append(indexId);
        sb.append(DELIMITER);
        sb.append(columnName);
        if (partitionName != null) {
            sb.append(DELIMITER);
            sb.append(partitionName);
        }
        return sb.toString();
    }
}
