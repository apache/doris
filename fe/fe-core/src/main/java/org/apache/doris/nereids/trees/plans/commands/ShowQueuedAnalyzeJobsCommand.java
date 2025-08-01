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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AutoAnalysisPendingJob;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Used to show queued auto analysis jobs.
 * syntax:
 *    SHOW QUEUED ANALYZE JOBS
 *        [TABLE]
 *        [
 *            WHERE
 *            [PRIORITY = ["HIGH"|"MID"|"LOW"|"VERY_LOW"]]
 *        ]
 */
public class ShowQueuedAnalyzeJobsCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowQueuedAnalyzeJobsCommand.class);

    private static final String STATE_NAME = "priority";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("catalog_name", ScalarType.createStringType()))
                    .addColumn(new Column("db_name", ScalarType.createStringType()))
                    .addColumn(new Column("tbl_name", ScalarType.createStringType()))
                    .addColumn(new Column("col_list", ScalarType.createStringType()))
                    .addColumn(new Column("priority", ScalarType.createStringType()))
                    .build();

    private final TableNameInfo tableNameInfo;
    private final String stateKey;
    private final String stateValue;
    private String ctl;
    private String db;
    private String table;

    /**
     * Constructor.
     * @param tableName catalog.db.table
     * @param stateKey Filter column name, Only support "priority" for now.
     * @param stateValue Filter column value. Only support PRIORITY="HIGH"|"MID"|"LOW"|"VERY_LOW"
     */
    public ShowQueuedAnalyzeJobsCommand(List<String> tableName, String stateKey, String stateValue) {
        super(PlanType.SHOW_QUEUED_ANALYZE_JOBS_COMMAND);
        this.tableNameInfo = tableName == null ? null : new TableNameInfo(tableName);
        this.stateKey = stateKey;
        this.stateValue = stateValue;
        ctl = null;
        db = null;
        table = null;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        checkShowQueuedAnalyzeJobsPriv(ctx);
        if (tableNameInfo != null) {
            tableNameInfo.analyze(ctx);
            ctl = tableNameInfo.getCtl();
            db = tableNameInfo.getDb();
            table = tableNameInfo.getTbl();
        }
        if (stateKey == null && stateValue != null || stateKey != null && stateValue == null) {
            throw new AnalysisException("Invalid where clause, should be PRIORITY = \"HIGH|MID|LOW|VERY_LOW\"");
        }
        if (stateKey != null) {
            if (!stateKey.equalsIgnoreCase(STATE_NAME)
                    || !stateValue.equalsIgnoreCase("HIGH")
                    && !stateValue.equalsIgnoreCase("MID")
                    && !stateValue.equalsIgnoreCase("LOW")
                    && !stateValue.equalsIgnoreCase("VERY_LOW")) {
                throw new AnalysisException("Where clause should be PRIORITY = \"HIGH|MID|LOW|VERY_LOW\"");
            }
        }
    }

    private void checkShowQueuedAnalyzeJobsPriv(ConnectContext ctx) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_ACCESS_DENIED_ERROR,
                    "SHOW QUEUED ANALYZE JOBS",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowQueuedAnalyzeJobsCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowQueuedAnalyzeJobs();
    }

    private ShowResultSet handleShowQueuedAnalyzeJobs() {
        List<AutoAnalysisPendingJob> jobs = Env.getCurrentEnv().getAnalysisManager().showAutoPendingJobs(
                new TableName(ctl, db, table), stateValue);
        List<List<String>> resultRows = Lists.newArrayList();
        for (AutoAnalysisPendingJob job : jobs) {
            try {
                List<String> row = new ArrayList<>();
                CatalogIf<? extends DatabaseIf<? extends TableIf>> c = StatisticsUtil.findCatalog(job.catalogName);
                row.add(c.getName());
                Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = c.getDb(job.dbName);
                row.add(databaseIf.isPresent() ? databaseIf.get().getFullName() : "DB may get deleted");
                if (databaseIf.isPresent()) {
                    Optional<? extends TableIf> table = databaseIf.get().getTable(job.tableName);
                    row.add(table.isPresent() ? table.get().getName() : "Table may get deleted");
                } else {
                    row.add("DB may get deleted");
                }
                row.add(job.getColumnNames());
                row.add(String.valueOf(job.priority));
                resultRows.add(row);
            } catch (Exception e) {
                LOG.warn("Failed to get pending jobs for table {}.{}.{}, reason: {}",
                        job.catalogName, job.dbName, job.tableName, e.getMessage());
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
