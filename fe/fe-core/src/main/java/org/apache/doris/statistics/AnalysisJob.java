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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.persist.AnalysisJobScheduler;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisJobInfo.JobState;

import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalysisJob {

    private final AnalysisJobScheduler analysisJobScheduler;

    private final AnalysisJobInfo info;

    private CatalogIf catalog;

    private Database db;

    private Table tbl;

    private Column col;

    private StmtExecutor stmtExecutor;

    public AnalysisJob(AnalysisJobScheduler analysisJobScheduler, AnalysisJobInfo info) {
        this.analysisJobScheduler = analysisJobScheduler;
        this.info = info;
        init(info);
    }

    private void init(AnalysisJobInfo info) {
        catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(info.catalogName);
        if (catalog == null) {
            analysisJobScheduler.updateJobStatus(info.jobId, JobState.FAILED,
                    String.format("Catalog with name: %s not exists", info.dbName), System.currentTimeMillis());
            return;
        }
        db = Env.getCurrentEnv().getInternalCatalog().getDb(info.dbName).orElse(null);
        if (db == null) {
            analysisJobScheduler.updateJobStatus(info.jobId, JobState.FAILED,
                    String.format("DB with name %s not exists", info.dbName), System.currentTimeMillis());
            return;
        }
        tbl = db.getTable(info.tblName).orElse(null);
        if (tbl == null) {
            analysisJobScheduler.updateJobStatus(
                    info.jobId, JobState.FAILED,
                    String.format("Table with name %s not exists", info.tblName), System.currentTimeMillis());
        }
        col = tbl.getColumn(info.colName);
        if (col == null) {
            analysisJobScheduler.updateJobStatus(
                    info.jobId, JobState.FAILED, String.format("Column with name %s not exists", info.tblName),
                    System.currentTimeMillis());
        }
    }

    private static final String ANALYZE_PARTITION_SQL_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " SELECT "
            + "CONCAT(${tblId}, '-', '${colId}', '-', ${partId}) AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "'${colId}' AS col_id, "
            + "${partId} AS part_id, "
            + "COUNT(1) AS row_count, "
            + "NDV(${colName}) AS ndv, "
            + "SUM(CASE WHEN ${colName} IS NULL THEN 1 ELSE 0 END) AS null_count, "
            + "MIN(${colName}) AS min, "
            + "MAX(${colName}) AS max, "
            + "${dataSizeFunction} AS data_size, "
            + "NOW()"
            + "FROM `${dbName}`.`${tblName}` "
            + "PARTITION ${partName}";

    private static final String ANALYZE_COLUMN_SQL_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + "    SELECT id, catalog_id, db_id, tbl_id, col_id, part_id, row_count, "
            + "        ndv, null_count, min, max, data_size, update_time\n"
            + "    FROM \n"
            + "     (SELECT CONCAT(${tblId}, '-', '${colId}') AS id, "
            + "         ${catalogId} AS catalog_id, "
            + "         ${dbId} AS db_id, "
            + "         ${tblId} AS tbl_id, "
            + "         '${colId}' AS col_id, "
            + "         NULL AS part_id, "
            + "         SUM(count) AS row_count, \n"
            + "         SUM(null_count) AS null_count, "
            + "         MIN(CAST(min AS ${type})) AS min, "
            + "         MAX(CAST(max AS ${type})) AS max, "
            + "         SUM(data_size_in_bytes) AS data_size, "
            + "         NOW() AS update_time\n"
            + "     FROM ${internalDB}.${columnStatTbl}"
            + "     WHERE ${internalDB}.${columnStatTbl}.db_id = '${dbId}' AND "
            + "     ${internalDB}.${columnStatTbl}.tbl_id='${tblId}' AND "
            + "      ${internalDB}.${columnStatTbl}.col_id='${colId}'"
            + "     ) t1, \n"
            + "     (SELECT NDV(${colName}) AS ndv FROM `${dbName}`.`${tblName}`) t2\n";

    private String getDataSizeFunction() {
        if (col.getType().isStringType()) {
            return "SUM(LENGTH(${colName}))";
        }
        return "COUNT(1) * " + col.getType().getSlotSize();
    }

    public void execute() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", StatisticConstants.STATISTIC_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("colId", String.valueOf(info.colName));
        params.put("dataSizeFunction", getDataSizeFunction());
        params.put("dbName", info.dbName);
        params.put("colName", String.valueOf(info.colName));
        params.put("tblName", String.valueOf(info.tblName));
        List<String> partitionAnalysisSQLs = new ArrayList<>();
        try {
            tbl.readLock();
            Set<String> partNames = tbl.getPartitionNames();
            for (String partName : partNames) {
                Partition part = tbl.getPartition(partName);
                if (part == null) {
                    continue;
                }
                params.put("partId", String.valueOf(tbl.getPartition(partName).getId()));
                params.put("partName", String.valueOf(partName));
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                partitionAnalysisSQLs.add(stringSubstitutor.replace(ANALYZE_PARTITION_SQL_TEMPLATE));
            }
        } finally {
            tbl.readUnlock();
        }
        for (String sql : partitionAnalysisSQLs) {
            ConnectContext connectContext = StatisticsUtil.buildConnectContext();
            this.stmtExecutor = new StmtExecutor(connectContext, sql);
            this.stmtExecutor.execute();
        }
        params.remove("partId");
        params.put("type", col.getType().toString());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(ANALYZE_COLUMN_SQL_TEMPLATE);
        ConnectContext connectContext = StatisticsUtil.buildConnectContext();
        this.stmtExecutor = new StmtExecutor(connectContext, sql);
        this.stmtExecutor.execute();
    }

    public int getLastExecTime() {
        return info.lastExecTimeInMs;
    }

    public void cancel() {
        if (stmtExecutor != null) {
            stmtExecutor.cancel();
        }
        analysisJobScheduler
                .updateJobStatus(info.jobId, JobState.FAILED,
                        String.format("Job has been cancelled: %s", info.toString()), -1);
    }

    public void updateState(JobState jobState) {
        info.updateState(jobState);
    }

    public long getJobId() {
        return info.jobId;
    }
}
