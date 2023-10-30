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
import org.apache.doris.catalog.external.JdbcExternalTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcAnalysisTask extends BaseAnalysisTask {

    private static final String ANALYZE_SQL_TABLE_TEMPLATE = " SELECT "
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
            + "to_base64(MIN(`${colName}`)) AS min, "
            + "to_base64(MAX(`${colName}`)) AS max, "
            + "${dataSizeFunction} AS data_size, "
            + "NOW() "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}`";

    private static final String ANALYZE_TABLE_COUNT_TEMPLATE = "SELECT COUNT(1) as rowCount "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}`";

    private final boolean isTableLevelTask;
    private JdbcExternalTable table;

    public JdbcAnalysisTask(AnalysisInfo info) {
        super(info);
        isTableLevelTask = info.externalTableLevelTask;
        table = (JdbcExternalTable) tbl;
    }

    public void doExecute() throws Exception {
        if (isTableLevelTask) {
            getTableStats();
        } else {
            getTableColumnStats();
        }
    }

    /**
     * Get table row count and store the result to metadata.
     */
    private void getTableStats() throws Exception {
        Map<String, String> params = buildTableStatsParams(null);
        List<ResultRow> columnResult =
                StatisticsUtil.execStatisticQuery(new StringSubstitutor(params).replace(ANALYZE_TABLE_COUNT_TEMPLATE));
        String rowCount = columnResult.get(0).get(0);
        Env.getCurrentEnv().getAnalysisManager()
            .updateTableStatsStatus(new TableStatsMeta(table.getId(), Long.parseLong(rowCount), info));
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
        StringBuilder sb = new StringBuilder();
        sb.append(ANALYZE_SQL_TABLE_TEMPLATE);
        Map<String, String> params = buildTableStatsParams("NULL");
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("colName", col.getName());
        params.put("colId", info.colName);
        params.put("dataSizeFunction", getDataSizeFunction(col));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(sb.toString());
        runQuery(sql);
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

    @Override
    protected void afterExecution() {
        // Table level task doesn't need to sync any value to sync stats, it stores the value in metadata.
        if (isTableLevelTask) {
            return;
        }
        Env.getCurrentEnv().getStatisticsCache().syncLoadColStats(tbl.getId(), -1, col.getName());
    }
}
