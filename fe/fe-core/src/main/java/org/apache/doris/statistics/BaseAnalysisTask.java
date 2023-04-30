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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public abstract class BaseAnalysisTask {

    public static final Logger LOG = LogManager.getLogger(BaseAnalysisTask.class);

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
            + "        ndv, null_count, min, max, data_size, update_time\n"
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
            + "         NOW() AS update_time\n"
            + "     FROM ${internalDB}.${columnStatTbl}"
            + "     WHERE ${internalDB}.${columnStatTbl}.db_id = '${dbId}' AND "
            + "     ${internalDB}.${columnStatTbl}.tbl_id='${tblId}' AND "
            + "     ${internalDB}.${columnStatTbl}.col_id='${colId}' AND "
            + "     ${internalDB}.${columnStatTbl}.idx_id='${idxId}' AND "
            + "     ${internalDB}.${columnStatTbl}.part_id IS NOT NULL"
            + "     ) t1, \n";

    protected AnalysisTaskInfo info;

    protected CatalogIf catalog;

    protected DatabaseIf db;

    protected TableIf tbl;

    protected Column col;

    protected StmtExecutor stmtExecutor;

    protected Set<PrimitiveType> unsupportedType = new HashSet<>();

    protected volatile boolean killed;

    @VisibleForTesting
    public BaseAnalysisTask() {

    }

    public BaseAnalysisTask(AnalysisTaskInfo info) {
        this.info = info;
        init(info);
    }

    protected void initUnsupportedType() {
        unsupportedType.add(PrimitiveType.HLL);
        unsupportedType.add(PrimitiveType.BITMAP);
        unsupportedType.add(PrimitiveType.ARRAY);
        unsupportedType.add(PrimitiveType.MAP);
        unsupportedType.add(PrimitiveType.JSONB);
        unsupportedType.add(PrimitiveType.STRUCT);
    }

    private void init(AnalysisTaskInfo info) {
        initUnsupportedType();
        catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(info.catalogName);
        if (catalog == null) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(info, AnalysisState.FAILED,
                    String.format("Catalog with name: %s not exists", info.dbName), System.currentTimeMillis());
            return;
        }
        db = (DatabaseIf) catalog.getDb(info.dbName).orElse(null);
        if (db == null) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(info, AnalysisState.FAILED,
                    String.format("DB with name %s not exists", info.dbName), System.currentTimeMillis());
            return;
        }
        tbl = (TableIf) db.getTable(info.tblName).orElse(null);
        if (tbl == null) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(
                    info, AnalysisState.FAILED,
                    String.format("Table with name %s not exists", info.tblName), System.currentTimeMillis());
        }
        if (info.analysisType != null && (info.analysisType.equals(AnalysisType.COLUMN)
                || info.analysisType.equals(AnalysisType.HISTOGRAM))) {
            col = tbl.getColumn(info.colName);
            if (col == null) {
                throw new RuntimeException(String.format("Column with name %s not exists", info.tblName));
            }
            if (isUnsupportedType(col.getType().getPrimitiveType())) {
                throw new RuntimeException(String.format("Column with type %s is not supported",
                        col.getType().toString()));
            }
        }

    }

    public abstract void execute() throws Exception;

    public void cancel() {
        if (stmtExecutor != null) {
            stmtExecutor.cancel();
        }
        if (killed) {
            return;
        }
        Env.getCurrentEnv().getAnalysisManager()
                .updateTaskStatus(info, AnalysisState.FAILED,
                        String.format("Job has been cancelled: %s", info.toString()), -1);
    }

    public int getLastExecTime() {
        return info.lastExecTimeInMs;
    }

    public long getJobId() {
        return info.jobId;
    }

    protected String getDataSizeFunction(Column column) {
        if (column.getType().isStringType()) {
            return "SUM(LENGTH(`${colName}`))";
        }
        return "COUNT(1) * " + column.getType().getSlotSize();
    }

    private boolean isUnsupportedType(PrimitiveType type) {
        return unsupportedType.contains(type);
    }

    protected String getSampleExpression() {
        if (info.analysisMethod == AnalysisMethod.FULL) {
            return "";
        }
        // TODO Add sampling methods for external tables
        if (info.samplePercent > 0) {
            return String.format("TABLESAMPLE(%d PERCENT)", info.samplePercent);
        } else {
            return String.format("TABLESAMPLE(%d ROWS)", info.sampleRows);
        }
    }

    public void markAsKilled() {
        this.killed = true;
        cancel();
    }
}
