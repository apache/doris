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
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public abstract class BaseAnalysisTask {

    public static final Logger LOG = LogManager.getLogger(BaseAnalysisTask.class);

    protected static final String NDV_MULTIPLY_THRESHOLD = "0.3";

    protected static final String NDV_SAMPLE_TEMPLATE = "case when NDV(`${colName}`)/count('${colName}') < "
            + NDV_MULTIPLY_THRESHOLD
            + " then NDV(`${colName}`) "
            + "else NDV(`${colName}`) * ${scaleFactor} end AS ndv, "
            ;

    protected static final String COLLECT_COL_STATISTICS =
            "SELECT CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "         ${catalogId} AS catalog_id, "
            + "         ${dbId} AS db_id, "
            + "         ${tblId} AS tbl_id, "
            + "         ${idxId} AS idx_id, "
            + "         '${colId}' AS col_id, "
            + "         NULL AS part_id, "
            + "         COUNT(1) AS row_count, "
            + "         NDV(`${colName}`) AS ndv, "
            + "         COUNT(1) - COUNT(${colName}) AS null_count, "
            + "         CAST(MIN(${colName}) AS STRING) AS min, "
            + "         CAST(MAX(${colName}) AS STRING) AS max, "
            + "         ${dataSizeFunction} AS data_size, "
            + "         NOW() AS update_time "
            + " FROM `${dbName}`.`${tblName}`";

    protected static final String ANALYZE_PARTITION_COLUMN_TEMPLATE =
            " SELECT "
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

    protected AnalysisJob job;

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
                    job.taskFailed(this, t.getMessage());
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

    // Min value is not accurate while sample, so set it to NULL to avoid optimizer generate bad plan.
    protected String getMinFunction() {
        if (tableSample == null) {
            return "CAST(MIN(`${colName}`) as ${type}) ";
        } else {
            return "NULL ";
        }
    }

    // Max value is not accurate while sample, so set it to NULL to avoid optimizer generate bad plan.
    protected String getMaxFunction() {
        if (tableSample == null) {
            return "CAST(MAX(`${colName}`) as ${type}) ";
        } else {
            return "NULL ";
        }
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
        } else if (info.jobType.equals(JobType.SYSTEM) && info.analysisMethod == AnalysisMethod.FULL
                && tbl.getDataSize(true) > Config.huge_table_lower_bound_size_in_bytes * 10) {
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

    public void setJob(AnalysisJob job) {
        this.job = job;
    }

    protected void runQuery(String sql) {
        long startTime = System.currentTimeMillis();
        try (AutoCloseConnectContext a  = StatisticsUtil.buildConnectContext()) {
            stmtExecutor = new StmtExecutor(a.connectContext, sql);
            stmtExecutor.executeInternalQuery();
            ColStatsData colStatsData = new ColStatsData(stmtExecutor.executeInternalQuery().get(0));
            job.appendBuf(this, Collections.singletonList(colStatsData));
        } finally {
            LOG.debug("End cost time in secs: " + (System.currentTimeMillis() - startTime) / 1000);
        }
    }

}
