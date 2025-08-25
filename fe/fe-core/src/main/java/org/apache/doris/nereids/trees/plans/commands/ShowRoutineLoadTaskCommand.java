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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * ShowRoutineLoadTaskCommand
 */
public class ShowRoutineLoadTaskCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowRoutineLoadTaskCommand.class);
    private static final List<String> supportColumn = Arrays.asList("jobname");
    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
            .add("TaskId")
            .add("TxnId")
            .add("TxnStatus")
            .add("JobId")
            .add("CreateTime")
            .add("ExecuteStartTime")
            .add("Timeout")
            .add("BeId")
            .add("DataSourceProperties")
            .build();

    private final String dbName;
    private final Expression whereClause;

    private String jobName;
    private String dbFullName;

    public ShowRoutineLoadTaskCommand(String dbName, Expression whereClause) {
        super(PlanType.SHOW_ROUTINE_LOAD_TASK_COMMAND);
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowRoutineLoadTask();
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            if (Strings.isNullOrEmpty(ctx.getDatabase())) {
                throw new AnalysisException("please designate a database in show stmt");
            }
            dbFullName = ctx.getDatabase();
        }

        if (!isWhereClauseValid(whereClause)) {
            throw new AnalysisException("show routine load job only support one equal expr "
                + "which is sames like JobName=\"ILoveDoris\"");
        }
    }

    private boolean isWhereClauseValid(Expression expr) throws AnalysisException {
        if (expr == null) {
            throw new AnalysisException("please designate a jobName in where expr such as JobName=\"ILoveDoris\"");
        }

        if (!(expr instanceof EqualTo)) {
            return false;
        }

        //check left child
        if (!(expr.child(0) instanceof UnboundSlot)) {
            return false;
        }
        UnboundSlot leftExpr = (UnboundSlot) expr.child(0);
        if (!supportColumn.stream().anyMatch(entity -> entity.equals(leftExpr.getName().toLowerCase()))) {
            return false;
        }

        //check right child
        if (!(expr.child(1) instanceof StringLikeLiteral)) {
            return false;
        }
        jobName = ((StringLikeLiteral) expr.child(1)).getValue();
        return true;
    }

    private ShowResultSet handleShowRoutineLoadTask() throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        RoutineLoadJob routineLoadJob;
        try {
            routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(dbFullName, jobName);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }
        if (routineLoadJob == null) {
            throw new AnalysisException("The job named " + jobName + "does not exists "
                + "or job state is stopped or cancelled");
        }

        // check auth
        String tableName;
        try {
            tableName = routineLoadJob.getTableName();
        } catch (MetaNotFoundException e) {
            throw new AnalysisException("The table metadata of job has been changed."
                + " The job will be cancelled automatically", e);
        }
        if (routineLoadJob.isMultiTable()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbFullName,
                    PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        dbFullName);
            }
            rows.addAll(routineLoadJob.getTasksShowInfo());
            return new ShowResultSet(getMetaData(), rows);
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbFullName, tableName,
                    PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    dbFullName + ": " + tableName);
        }

        // get routine load task info
        rows.addAll(routineLoadJob.getTasksShowInfo());
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowRoutineLoadTaskCommand(this, context);
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
