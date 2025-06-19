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
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
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

import java.util.Comparator;
import java.util.List;

/**
  Show routine load progress by routine load name

  syntax:
      SHOW [ALL] ROUTINE LOAD [FOR JobName] [LIKE pattern]

      without ALL: only show job which is not final
      with ALL: show all of job include history job

      without name: show all of routine load job in database with different name
      with name: show all of job named ${name} in database

      without on db: show all of job in connection db
         if user does not choose db before, return error
      with on db: show all of job in ${db}

      example:
        show routine load named test in database1
        use database1
        SHOW ROUTINE LOAD for test;

        show routine load in database1 include history
        use database1;
        SHOW ALL ROUTINE LOAD;

        show routine load in database1 whose name match pattern "%test%"
        use database1;
        SHOW ROUTINE LOAD LIKE "%test%";

        show routine load in all of database
        please use show proc
 */

public class ShowRoutineLoadCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowRoutineLoadCommand.class);
    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
            .add("Id")
            .add("Name")
            .add("CreateTime")
            .add("PauseTime")
            .add("EndTime")
            .add("DbName")
            .add("TableName")
            .add("IsMultiTable")
            .add("State")
            .add("DataSourceType")
            .add("CurrentTaskNum")
            .add("JobProperties")
            .add("DataSourceProperties")
            .add("CustomProperties")
            .add("Statistic")
            .add("Progress")
            .add("Lag")
            .add("ReasonOfStateChanged")
            .add("ErrorLogUrls")
            .add("OtherMsg")
            .add("User")
            .add("Comment")
            .build();

    private final LabelNameInfo labelNameInfo;
    private String dbFullName; // optional
    private String name; // optional
    private boolean isAll = false;
    private String pattern; // optional

    public ShowRoutineLoadCommand(LabelNameInfo labelNameInfo, String pattern, boolean isAll) {
        super(PlanType.SHOW_ROUTINE_LOAD_COMMAND);
        this.labelNameInfo = labelNameInfo;
        this.pattern = pattern;
        this.isAll = isAll;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowRoutineLoad();
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        dbFullName = labelNameInfo == null ? null : labelNameInfo.getDb();
        if (Strings.isNullOrEmpty(dbFullName)) {
            dbFullName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbFullName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        name = labelNameInfo == null ? null : labelNameInfo.getLabel();
    }

    private ShowResultSet handleShowRoutineLoad() throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        List<RoutineLoadJob> routineLoadJobList;
        try {
            PatternMatcher matcher = null;
            if (pattern != null) {
                matcher = PatternMatcherWrapper.createMysqlPattern(pattern,
                    CaseSensibility.ROUTINE_LOAD.getCaseSensibility());
            }
            routineLoadJobList = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(dbFullName, name, isAll, matcher);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }

        if (routineLoadJobList != null) {
            String tableName = null;
            for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
                // check auth
                try {
                    tableName = routineLoadJob.getTableName();
                } catch (MetaNotFoundException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("error_msg", "The table metadata of job has been changed. "
                            + "The job will be cancelled automatically")
                            .build(), e);
                }
                if (routineLoadJob.isMultiTable()) {
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbFullName,
                            PrivPredicate.LOAD)) {
                        LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("operator",
                                "show routine load job").add("user", ConnectContext.get().getQualifiedUser())
                                .add("remote_ip", ConnectContext.get().getRemoteIP()).add("db_full_name", dbFullName)
                                .add("table_name", tableName).add("error_msg", "The database access denied"));
                        continue;
                    }
                    rows.add(routineLoadJob.getShowInfo());
                    continue;
                }
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbFullName,
                        tableName, PrivPredicate.LOAD)) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("operator",
                            "show routine load job").add("user", ConnectContext.get().getQualifiedUser())
                            .add("remote_ip", ConnectContext.get().getRemoteIP()).add("db_full_name", dbFullName)
                            .add("table_name", tableName).add("error_msg", "The table access denied"));
                    continue;
                }
                // get routine load info
                rows.add(routineLoadJob.getShowInfo());
            }
        }

        if (!Strings.isNullOrEmpty(name) && rows.size() == 0) {
            // if the jobName has been specified
            throw new AnalysisException("There is no job named " + name
                + " in db " + dbFullName
                + ". Include history? " + isAll);
        }
        // sort by create time
        rows.sort(Comparator.comparing(x -> x.get(2)));
        return new ShowResultSet(getMetaData(), rows);
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowRoutineLoadCommand(this, context);
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
