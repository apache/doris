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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * show analyze command.
 */
public class ShowAnalyzeCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowAnalyzeCommand.class);

    private static final String STATE_NAME = "state";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("job_id", ScalarType.createVarchar(128)))
                    .addColumn(new Column("catalog_name", ScalarType.createStringType()))
                    .addColumn(new Column("db_name", ScalarType.createStringType()))
                    .addColumn(new Column("tbl_name", ScalarType.createStringType()))
                    .addColumn(new Column("col_name", ScalarType.createStringType()))
                    .addColumn(new Column("job_type", ScalarType.createVarchar(64)))
                    .addColumn(new Column("analysis_type", ScalarType.createVarchar(64)))
                    .addColumn(new Column("message", ScalarType.createStringType()))
                    .addColumn(new Column("last_exec_time_in_ms", ScalarType.createStringType()))
                    .addColumn(new Column("state", ScalarType.createVarchar(32)))
                    .addColumn(new Column("progress", ScalarType.createStringType()))
                    .addColumn(new Column("schedule_type", ScalarType.createStringType()))
                    .addColumn(new Column("start_time", ScalarType.createStringType()))
                    .addColumn(new Column("end_time", ScalarType.createStringType()))
                    .addColumn(new Column("priority", ScalarType.createStringType()))
                    .addColumn(new Column("enable_partition", ScalarType.createVarchar(32)))
                    .build();

    private final TableNameInfo tableNameInfo;
    private final long jobId;
    private final String stateKey;
    private final String stateValue;
    private final boolean isAuto;
    private String ctl;
    private String db;
    private String table;

    /**
     * Constructor.
     * @param tableName catalog.db.table
     * @param jobId analyze job id.
     * @param stateKey Filter column name, Only support "state" for now."
     * @param stateValue Filter column value. Only support STATE="PENDING|RUNNING|FINISHED|FAILED"
     * @param isAuto show auto analyze or manual analyze.
     */
    public ShowAnalyzeCommand(List<String> tableName, long jobId, String stateKey, String stateValue, boolean isAuto) {
        super(PlanType.SHOW_ANALYZE_COMMAND);
        this.tableNameInfo = tableName == null ? null : new TableNameInfo(tableName);
        this.jobId = jobId;
        this.stateKey = stateKey;
        this.stateValue = stateValue;
        this.isAuto = isAuto;
        this.ctl = null;
        this.db = null;
        this.table = null;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        checkShowAnalyzePriv(ctx);
        if (tableNameInfo != null) {
            tableNameInfo.analyze(ctx);
            ctl = tableNameInfo.getCtl();
            db = tableNameInfo.getDb();
            table = tableNameInfo.getTbl();
        }
        if (stateKey == null && stateValue != null || stateKey != null && stateValue == null) {
            throw new AnalysisException("Invalid where clause, should be STATE = \"PENDING|RUNNING|FINISHED|FAILED\"");
        }
        if (stateKey != null) {
            if (!stateKey.equalsIgnoreCase(STATE_NAME)
                    || !stateValue.equalsIgnoreCase("PENDING")
                    && !stateValue.equalsIgnoreCase("RUNNING")
                    && !stateValue.equalsIgnoreCase("FINISHED")
                    && !stateValue.equalsIgnoreCase("FAILED")) {
                throw new AnalysisException("Where clause should be STATE = \"PENDING|RUNNING|FINISHED|FAILED\"");
            }
        }
    }

    private void checkShowAnalyzePriv(ConnectContext ctx) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_ACCESS_DENIED_ERROR,
                    "SHOW ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowAnalyzeCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowAnalyze();
    }

    private ShowResultSet handleShowAnalyze() {
        List<AnalysisInfo> results = Env.getCurrentEnv().getAnalysisManager()
                .findAnalysisJobs(stateValue, ctl, db, table, jobId, isAuto);
        List<List<String>> resultRows = Lists.newArrayList();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (AnalysisInfo analysisInfo : results) {
            try {
                List<String> row = new ArrayList<>();
                row.add(String.valueOf(analysisInfo.jobId));
                CatalogIf<? extends DatabaseIf<? extends TableIf>> c
                        = StatisticsUtil.findCatalog(analysisInfo.catalogId);
                row.add(c.getName());
                Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = c.getDb(analysisInfo.dbId);
                row.add(databaseIf.isPresent() ? databaseIf.get().getFullName() : "DB may get deleted");
                if (databaseIf.isPresent()) {
                    Optional<? extends TableIf> table = databaseIf.get().getTable(analysisInfo.tblId);
                    row.add(table.isPresent() ? Util.getTempTableDisplayName(table.get().getName())
                            : "Table may get deleted");
                } else {
                    row.add("DB may get deleted");
                }
                String colNames = analysisInfo.colName;
                List<String> displayNameList = new ArrayList<>();
                if (colNames != null && colNames.length() > 1) {
                    colNames = colNames.substring(1, colNames.length() - 1);
                    for (String colName : colNames.split(",")) {
                        displayNameList.add(Util.getTempTableDisplayName(colName));
                    }
                }
                row.add(displayNameList.toString());
                row.add(analysisInfo.jobType.toString());
                row.add(analysisInfo.analysisType.toString());
                row.add(analysisInfo.message);
                row.add(TimeUtils.getDatetimeFormatWithTimeZone().format(
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.lastExecTimeInMs),
                                ZoneId.systemDefault())));
                row.add(analysisInfo.state.toString());
                row.add(Env.getCurrentEnv().getAnalysisManager().getJobProgress(analysisInfo.jobId));
                row.add(analysisInfo.scheduleType.toString());
                LocalDateTime startTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.startTime),
                                java.time.ZoneId.systemDefault());
                LocalDateTime endTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.endTime),
                                java.time.ZoneId.systemDefault());
                row.add(startTime.format(formatter));
                row.add(endTime.format(formatter));
                row.add(analysisInfo.priority.name());
                row.add(String.valueOf(analysisInfo.enablePartition));
                resultRows.add(row);
            } catch (Exception e) {
                LOG.warn("Failed to get analyze info for table {}.{}.{}, reason: {}",
                        analysisInfo.catalogId, analysisInfo.dbId, analysisInfo.tblId, e.getMessage());
            }
        }
        return new ShowResultSet(getMetaData(), resultRows);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
