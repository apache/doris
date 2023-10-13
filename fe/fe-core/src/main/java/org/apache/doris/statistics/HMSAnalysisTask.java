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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class HMSAnalysisTask extends BaseAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(HMSAnalysisTask.class);

    // While doing sample analysis, the sampled ndv result will multiply a factor (total size/sample size)
    // if ndv(col)/count(col) is greater than this threshold.
    private static final String NDV_MULTIPLY_THRESHOLD = "0.3";

    private static final String ANALYZE_TABLE_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "${idxId} AS idx_id, "
            + "'${colId}' AS col_id, "
            + "NULL AS part_id, "
            + "ROUND(COUNT(1) * ${scaleFactor}) AS row_count, "
            + "case when NDV(`${colName}`)/count('${colName}') < "
            + NDV_MULTIPLY_THRESHOLD
            + " then NDV(`${colName}`) "
            + "else NDV(`${colName}`) * ${scaleFactor} end AS ndv, "
            + "ROUND(SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor}) AS null_count, "
            + "MIN(`${colName}`) AS min, "
            + "MAX(`${colName}`) AS max, "
            + "${dataSizeFunction} * ${scaleFactor} AS data_size, "
            + "NOW() "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}` ${sampleExpr}";

    private static final String ANALYZE_PARTITION_TEMPLATE = " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}', '-', ${partId}) AS id, "
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
            + "NOW() FROM `${catalogName}`.`${dbName}`.`${tblName}` where ";

    private static final String ANALYZE_TABLE_COUNT_TEMPLATE = "SELECT ROUND(COUNT(1) * ${scaleFactor}) as rowCount "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}` ${sampleExpr}";

    // cache stats for each partition, it would be inserted into column_statistics in a batch.
    private final List<List<ColStatsData>> buf = new ArrayList<>();

    private final boolean isTableLevelTask;
    private final boolean isPartitionOnly;
    private Set<String> partitionNames;
    private HMSExternalTable table;

    public HMSAnalysisTask(AnalysisInfo info) {
        super(info);
        isTableLevelTask = info.externalTableLevelTask;
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
     * Get table row count
     */
    private void getTableStats() throws Exception {
        Map<String, String> params = buildTableStatsParams(null);
        List<ResultRow> columnResult =
                StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                        .replace(ANALYZE_TABLE_COUNT_TEMPLATE));
        String rowCount = columnResult.get(0).get(0);
        Env.getCurrentEnv().getAnalysisManager()
                .updateTableStatsStatus(
                        new TableStatsMeta(table.getId(), Long.parseLong(rowCount), info));
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
            getPartitionNames();
            List<String> partitionAnalysisSQLs = new ArrayList<>();
            for (String partId : this.partitionNames) {
                partitionAnalysisSQLs.add(generateSqlForPartition(partId));
            }
            execSQLs(partitionAnalysisSQLs);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(ANALYZE_TABLE_TEMPLATE);
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

    private void getPartitionNames() {
        if (partitionNames == null) {
            if (info.isAllPartition) {
                partitionNames = table.getPartitionNames();
            } else if (info.partitionCount > 0) {
                partitionNames = table.getPartitionNames().stream()
                    .limit(info.partitionCount).collect(Collectors.toSet());
            }
            if (partitionNames == null || partitionNames.isEmpty()) {
                throw new RuntimeException("Not a partition table or no partition specified.");
            }
        }
    }

    private String generateSqlForPartition(String partId) {
        StringBuilder sb = new StringBuilder();
        sb.append(ANALYZE_PARTITION_TEMPLATE);
        String[] splits = partId.split("/");
        for (int i = 0; i < splits.length; i++) {
            String[] kv = splits[i].split("=");
            sb.append(kv[0]);
            sb.append("='");
            sb.append(kv[1]);
            sb.append("'");
            if (i < splits.length - 1) {
                sb.append(" and ");
            }
        }
        Map<String, String> params = buildTableStatsParams(partId);
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("colName", col.getName());
        params.put("colId", info.colName);
        params.put("dataSizeFunction", getDataSizeFunction(col));
        return new StringSubstitutor(params).replace(sb.toString());
    }

    public void execSQLs(List<String> partitionAnalysisSQLs) throws Exception {
        long startTime = System.currentTimeMillis();
        LOG.debug("analyze task {} start at {}", info.toString(), new Date());
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            List<List<String>> sqlGroups = Lists.partition(partitionAnalysisSQLs, StatisticConstants.UNION_ALL_LIMIT);
            for (List<String> group : sqlGroups) {
                if (killed) {
                    return;
                }
                StringJoiner partitionCollectSQL = new StringJoiner("UNION ALL");
                group.forEach(partitionCollectSQL::add);
                stmtExecutor = new StmtExecutor(r.connectContext, partitionCollectSQL.toString());
                buf.add(stmtExecutor.executeInternalQuery()
                        .stream().map(ColStatsData::new).collect(Collectors.toList()));
                QueryState queryState = r.connectContext.getState();
                if (queryState.getStateType().equals(QueryState.MysqlStateType.ERR)) {
                    throw new RuntimeException(String.format("Failed to analyze %s.%s.%s, error: %s sql: %s",
                        info.catalogName, info.dbName, info.colName, partitionCollectSQL,
                        queryState.getErrorMessage()));
                }
            }
            for (List<ColStatsData> colStatsDataList : buf) {
                StringBuilder batchInsertSQL =
                        new StringBuilder("INSERT INTO " + StatisticConstants.FULL_QUALIFIED_STATS_TBL_NAME
                        + " VALUES ");
                StringJoiner sj = new StringJoiner(",");
                colStatsDataList.forEach(c -> sj.add(c.toSQL(true)));
                batchInsertSQL.append(sj);
                stmtExecutor = new StmtExecutor(r.connectContext, batchInsertSQL.toString());
                executeWithExceptionOnFail(stmtExecutor);
            }
        } finally {
            LOG.debug("analyze task {} end. cost {}ms", info, System.currentTimeMillis() - startTime);
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
        commonParams.put("sampleExpr", getSampleExpression());
        commonParams.put("scaleFactor", getSampleScaleFactor());
        if (col != null) {
            commonParams.put("type", col.getType().toString());
        }
        commonParams.put("lastAnalyzeTimeInMs", String.valueOf(System.currentTimeMillis()));
        return commonParams;
    }

    protected String getSampleExpression() {
        if (tableSample == null) {
            return "";
        }
        if (tableSample.isPercent()) {
            return String.format("TABLESAMPLE(%d PERCENT)", tableSample.getSampleValue());
        } else {
            return String.format("TABLESAMPLE(%d ROWS)", tableSample.getSampleValue());
        }
    }

    // Get the sample scale factor. While analyzing, the result of count, null count and data size need to
    // multiply this factor to get more accurate result.
    protected String getSampleScaleFactor() {
        if (tableSample == null) {
            return "1";
        }
        long target = 0;
        // Get list of all files' size in this HMS table.
        List<Long> chunkSizes = table.getChunkSizes();
        Collections.shuffle(chunkSizes, new Random(tableSample.getSeek()));
        long total = 0;
        // Calculate the total size of this HMS table.
        for (long size : chunkSizes) {
            total += size;
        }
        // Calculate the sample target size for percent and rows sample.
        if (tableSample.isPercent()) {
            target = total * tableSample.getSampleValue() / 100;
        } else {
            int columnSize = 0;
            for (Column column : table.getFullSchema()) {
                columnSize += column.getDataType().getSlotSize();
            }
            target = columnSize * tableSample.getSampleValue();
        }
        // Calculate the actual sample size (cumulate).
        long cumulate = 0;
        for (long size : chunkSizes) {
            cumulate += size;
            if (cumulate >= target) {
                break;
            }
        }
        return Double.toString(Math.max(((double) total) / cumulate, 1));
    }

    @Override
    protected void afterExecution() {
        // Table level task doesn't need to sync any value to sync stats, it stores the value in metadata.
        // Partition only task doesn't need to refresh cached.
        if (isTableLevelTask || isPartitionOnly) {
            return;
        }
        Env.getCurrentEnv().getStatisticsCache().syncLoadColStats(tbl.getId(), -1, col.getName());
    }
}
