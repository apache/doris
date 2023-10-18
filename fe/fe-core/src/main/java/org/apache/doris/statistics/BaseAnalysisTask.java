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

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public abstract class BaseAnalysisTask {

    public static final Logger LOG = LogManager.getLogger(BaseAnalysisTask.class);

    protected static final String NDV_MULTIPLY_THRESHOLD = "0.3";

    protected static final String NDV_SAMPLE_TEMPLATE = "ROUND(COUNT(1) * ${scaleFactor}) AS row_count, "
            + "case when NDV(`${colName}`)/count('${colName}') < "
            + NDV_MULTIPLY_THRESHOLD
            + " then NDV(`${colName}`) "
            + "else NDV(`${colName}`) * ${scaleFactor} end AS ndv, "
            ;

    /**
     * Stats stored in the column_statistics table basically has two types, `part_id` is null which means it is
     * aggregate from partition level stats, `part_id` is not null which means it is partition level stats.
     * For latter, it's id field contains part id, for previous doesn't.
     */
    protected static final String INSERT_PART_STATISTICS = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " SELECT "
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
            + "NOW() ";

    protected static final String INSERT_COL_STATISTICS = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + "    SELECT id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id, row_count, "
            + "        ndv, null_count, CAST(min AS string), CAST(max AS string), data_size, update_time\n"
            + "    FROM \n"
            + "     (SELECT CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "         ${catalogId} AS catalog_id, "
            + "         ${dbId} AS db_id, "
            + "         ${tblId} AS tbl_id, "
            + "         ${idxId} AS idx_id, "
            + "         '${colId}' AS col_id, "
            + "         NULL AS part_id, "
            + "         SUM(count) AS row_count, \n"
            + "         SUM(null_count) AS null_count, "
            + "         MIN(CAST(min AS ${type})) AS min, "
            + "         MAX(CAST(max AS ${type})) AS max, "
            + "         SUM(data_size_in_bytes) AS data_size, "
            + "         NOW() AS update_time \n"
            + "     FROM ${internalDB}.${columnStatTbl}"
            + "     WHERE ${internalDB}.${columnStatTbl}.db_id = '${dbId}' AND "
            + "     ${internalDB}.${columnStatTbl}.tbl_id='${tblId}' AND "
            + "     ${internalDB}.${columnStatTbl}.col_id='${colId}' AND "
            + "     ${internalDB}.${columnStatTbl}.idx_id='${idxId}' AND "
            + "     ${internalDB}.${columnStatTbl}.part_id IS NOT NULL"
            + "     ) t1, \n";

    protected static final String ANALYZE_PARTITION_COLUMN_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "${idxId} AS idx_id, "
            + "'${colId}' AS col_id, "
            + "NULL AS part_id, "
            + "${row_count} AS row_count, "
            + "${ndv} AS ndv, "
            + "${null_count} AS null_count, "
            + "'${min}' AS min, "
            + "'${max}' AS max, "
            + "${data_size} AS data_size, "
            + "NOW() ";

    protected AnalysisInfo info;

    protected CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog;

    protected DatabaseIf<? extends TableIf> db;

    protected TableIf tbl;

    protected Column col;

    protected StmtExecutor stmtExecutor;

    protected volatile boolean killed;

    protected TableSample tableSample = null;

    @VisibleForTesting
    public BaseAnalysisTask() {

    }

    public BaseAnalysisTask(AnalysisInfo info) {
        this.info = info;
        init(info);
    }

    protected void init(AnalysisInfo info) {
        DBObjects dbObjects = StatisticsUtil.convertIdToObjects(info.catalogId, info.dbId, info.tblId);
        catalog = dbObjects.catalog;
        db = dbObjects.db;
        tbl = dbObjects.table;
        tableSample = getTableSample();
        // External Table level task doesn't contain a column. Don't need to do the column related analyze.
        if (info.externalTableLevelTask) {
            return;
        }
        if (info.analysisType != null && (info.analysisType.equals(AnalysisType.FUNDAMENTALS)
                || info.analysisType.equals(AnalysisType.HISTOGRAM))) {
            col = tbl.getColumn(info.colName);
            if (col == null) {
                throw new RuntimeException(String.format("Column with name %s not exists", tbl.getName()));
            }
            Preconditions.checkArgument(!StatisticsUtil.isUnsupportedType(col.getType()),
                    String.format("Column with type %s is not supported", col.getType().toString()));
        }
    }

    public void execute() {
        prepareExecution();
        executeWithRetry();
        afterExecution();
    }

    protected void prepareExecution() {
        setTaskStateToRunning();
    }

    protected void executeWithRetry() {
        int retriedTimes = 0;
        while (retriedTimes <= StatisticConstants.ANALYZE_TASK_RETRY_TIMES) {
            if (killed) {
                break;
            }
            try {
                doExecute();
                break;
            } catch (Throwable t) {
                if (killed) {
                    throw new RuntimeException(t);
                }
                LOG.warn("Failed to execute analysis task, retried times: {}", retriedTimes++, t);
                if (retriedTimes > StatisticConstants.ANALYZE_TASK_RETRY_TIMES) {
                    throw new RuntimeException(t);
                }
                StatisticsUtil.sleep(TimeUnit.SECONDS.toMillis(2 ^ retriedTimes) * 10);
            }
        }
    }

    public abstract void doExecute() throws Exception;

    protected void afterExecution() {
        if (killed) {
            return;
        }
        long tblId = tbl.getId();
        String colName = col.getName();
        if (!Env.getCurrentEnv().getStatisticsCache().syncLoadColStats(tblId, -1, colName)) {
            Env.getCurrentEnv().getAnalysisManager().removeColStatsStatus(tblId, colName);
        }
    }

    protected void setTaskStateToRunning() {
        Env.getCurrentEnv().getAnalysisManager()
                .updateTaskStatus(info, AnalysisState.RUNNING, "", System.currentTimeMillis());
    }

    public void cancel() {
        killed = true;
        if (stmtExecutor != null) {
            stmtExecutor.cancel();
        }
        Env.getCurrentEnv().getAnalysisManager()
                .updateTaskStatus(info, AnalysisState.FAILED,
                        String.format("Job has been cancelled: %s", info.message), System.currentTimeMillis());
    }

    public long getJobId() {
        return info.jobId;
    }

    // TODO : time cost is intolerable when column is string type, return 0 directly for now.
    protected String getDataSizeFunction(Column column) {
        if (column.getType().isStringType()) {
            return "SUM(LENGTH(`${colName}`))";
        }
        return "COUNT(1) * " + column.getType().getSlotSize();
    }

    protected TableSample getTableSample() {
        if (info.forceFull) {
            return null;
        }
        // If user specified sample percent or sample rows, use it.
        if (info.samplePercent > 0) {
            return new TableSample(true, (long) info.samplePercent);
        } else if (info.sampleRows > 0) {
            return new TableSample(false, info.sampleRows);
        } else if (info.analysisMethod == AnalysisMethod.FULL
                && Config.enable_auto_sample
                && tbl.getDataSize(true) > Config.huge_table_lower_bound_size_in_bytes) {
            // If user doesn't specify sample percent/rows, use auto sample and update sample rows in analysis info.
            return new TableSample(false, (long) Config.huge_table_default_sample_rows);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("Job id [%d], Task id [%d], catalog [%s], db [%s], table [%s], column [%s]",
                info.jobId, info.taskId, catalog.getName(), db.getFullName(), tbl.getName(),
                col == null ? "TableRowCount" : col.getName());
    }

    protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {
        if (killed) {
            return;
        }
        LOG.debug("execute internal sql: {}", stmtExecutor.getOriginStmt());
        try {
            stmtExecutor.execute();
            QueryState queryState = stmtExecutor.getContext().getState();
            if (queryState.getStateType().equals(MysqlStateType.ERR)) {
                throw new RuntimeException(String.format("Failed to analyze %s.%s.%s, error: %s sql: %s",
                        catalog.getName(), db.getFullName(), info.colName, stmtExecutor.getOriginStmt().toString(),
                        queryState.getErrorMessage()));
            }
        } finally {
            AuditLogHelper.logAuditLog(stmtExecutor.getContext(), stmtExecutor.getOriginStmt().toString(),
                    stmtExecutor.getParsedStmt(), stmtExecutor.getQueryStatisticsForAuditLog(),
                    true);
        }
    }
}
