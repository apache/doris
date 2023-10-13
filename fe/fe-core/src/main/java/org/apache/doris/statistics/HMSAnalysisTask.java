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
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.InternalQueryResult;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HMSAnalysisTask extends BaseAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(HMSAnalysisTask.class);

    public static final String TOTAL_SIZE = "totalSize";
    public static final String NUM_ROWS = "numRows";
    public static final String NUM_FILES = "numFiles";
    public static final String TIMESTAMP = "transient_lastDdlTime";

    private static final String ANALYZE_SQL_TABLE_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "${idxId} AS idx_id, "
            + "'${colId}' AS col_id, "
            + "NULL AS part_id, "
            + "COUNT(1) AS row_count, "
            + "NDV(`${colName}`) AS ndv, "
            + "SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
            + "MIN(`${colName}`) AS min, "
            + "MAX(`${colName}`) AS max, "
            + "${dataSizeFunction} AS data_size, "
            + "NOW() "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}`";

    private static final String ANALYZE_SQL_PARTITION_TEMPLATE = "INSERT INTO "
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
    private HMSExternalTable table;

    public HMSAnalysisTask(AnalysisInfo info) {
        super(info);
        isTableLevelTask = info.externalTableLevelTask;
        isSamplingPartition = info.samplingPartition;
        isPartitionOnly = info.partitionOnly;
        partitionNames = info.partitionNames;
        table = (HMSExternalTable) tbl;
    }

    public void doExecute() throws Exception {
        if (isTableLevelTask) {
            getTableStats();
        } else {
            getTableColumnStats();
        }
    }

    /**
     * Get table row count and insert the result to __internal_schema.table_statistics
     */
    private void getTableStats() throws Exception {
        // Get table level information. An example sql for table stats:
        // INSERT INTO __internal_schema.table_statistics VALUES
        //   ('13055', 13002, 13038, 13055, -1, 'NULL', 5, 1686111064658, NOW())
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        if (isPartitionOnly) {
            for (String partId : partitionNames) {
                StringBuilder sb = new StringBuilder();
                sb.append(ANALYZE_SQL_PARTITION_TEMPLATE);
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
            Map<String, String> params = buildTableStatsParams(null);
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
    private void getTableColumnStats() throws Exception {
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
                executeInsertSql(sql);
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
            executeInsertSql(sql);
        }
    }

    private void executeInsertSql(String sql) throws Exception {
        long startTime = System.currentTimeMillis();
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
            this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            this.stmtExecutor.execute();
            QueryState queryState = r.connectContext.getState();
            if (queryState.getStateType().equals(QueryState.MysqlStateType.ERR)) {
                LOG.warn(String.format("Failed to analyze %s.%s.%s, sql: [%s], error: [%s]",
                        info.catalogName, info.dbName, info.colName, sql, queryState.getErrorMessage()));
                throw new RuntimeException(queryState.getErrorMessage());
            }
            LOG.debug(String.format("Analyze %s.%s.%s done. SQL: [%s]. Cost %d ms.",
                    info.catalogName, info.dbName, info.colName, sql, (System.currentTimeMillis() - startTime)));
        }
    }

    private Map<String, String> buildTableStatsParams(String partId) {
        Map<String, String> commonParams = new HashMap<>();
        String id = StatisticsUtil.constructId(tbl.getId(), -1);
        if (partId == null) {
            commonParams.put("partId", "NULL");
        } else {
            id = StatisticsUtil.constructId(id, partId);
            commonParams.put("partId", "\'" + partId + "\'");
        }
        commonParams.put("id", id);
        commonParams.put("catalogId", String.valueOf(catalog.getId()));
        commonParams.put("dbId", String.valueOf(db.getId()));
        commonParams.put("tblId", String.valueOf(tbl.getId()));
        commonParams.put("indexId", "-1");
        commonParams.put("idxId", "-1");
        commonParams.put("catalogName", catalog.getName());
        commonParams.put("dbName", db.getFullName());
        commonParams.put("tblName", tbl.getName());
        if (col != null) {
            commonParams.put("type", col.getType().toString());
        }
        commonParams.put("lastAnalyzeTimeInMs", String.valueOf(System.currentTimeMillis()));
        return commonParams;
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

    @Override
    protected void afterExecution() {
        if (isTableLevelTask) {
            Env.getCurrentEnv().getStatisticsCache().refreshTableStatsSync(catalog.getId(), db.getId(), tbl.getId());
        } else {
            Env.getCurrentEnv().getStatisticsCache().syncLoadColStats(tbl.getId(), -1, col.getName());
        }
    }
}
