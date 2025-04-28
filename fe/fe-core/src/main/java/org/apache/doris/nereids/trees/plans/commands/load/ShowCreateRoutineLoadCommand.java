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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ShowCommand;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * SHOW CREATE ROUTINE LOAD statement.
 */
public class ShowCreateRoutineLoadCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowCreateRoutineLoadCommand.class);
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
            .addColumn(new Column("JobId", ScalarType.createVarchar(128)))
            .addColumn(new Column("JobName", ScalarType.createVarchar(128)))
            .addColumn(new Column("CreateStmt", ScalarType.createVarchar(65535)))
            .build();

    private final LabelNameInfo labelNameInfo;
    private boolean isAll;

    public ShowCreateRoutineLoadCommand(LabelNameInfo labelNameInfo, boolean isAll) {
        super(PlanType.SHOW_CREATE_ROUTINE_LOAD_COMMAND);
        this.labelNameInfo = labelNameInfo;
        this.isAll = isAll;
    }

    public String getDb() {
        return labelNameInfo.getDb();
    }

    public String getLabel() {
        return labelNameInfo.getLabel();
    }

    public boolean isAll() {
        return isAll;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowCreateRoutineLoad();
    }

    public void validate(ConnectContext ctx) throws AnalysisException {
        labelNameInfo.validate(ctx);
    }

    private ShowResultSet handleShowCreateRoutineLoad() throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        String dbName = getDb();
        String labelName = getLabel();
        // if include history return all create load
        if (isAll) {
            List<RoutineLoadJob> routineLoadJobList = new ArrayList<>();
            try {
                routineLoadJobList = Env.getCurrentEnv().getRoutineLoadManager().getJob(dbName, labelName, true, null);
            } catch (MetaNotFoundException e) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, labelName)
                        .add("error_msg", "Routine load cannot be found by this name")
                        .build(), e);
            }
            if (routineLoadJobList == null) {
                return new ShowResultSet(getMetaData(), rows);
            }
            for (RoutineLoadJob job : routineLoadJobList) {
                String tableName = "";
                try {
                    tableName = job.getTableName();
                } catch (MetaNotFoundException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, job.getId())
                            .add("error_msg", "The table name for this routine load does not exist")
                            .build(), e);
                }
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, tableName,
                        PrivPredicate.LOAD)) {
                    continue;
                }
                rows.add(Lists.newArrayList(String.valueOf(job.getId()), getLabel(), job.getShowCreateInfo()));
            }
        } else {
            // if job exists
            RoutineLoadJob routineLoadJob;
            try {
                routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager().checkPrivAndGetJob(dbName, labelName);
                // get routine load info
                rows.add(Lists.newArrayList(String.valueOf(routineLoadJob.getId()),
                        getLabel(),
                        routineLoadJob.getShowCreateInfo()));
            } catch (MetaNotFoundException | DdlException e) {
                LOG.warn(e.getMessage(), e);
                throw new AnalysisException(e.getMessage());
            }
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateRoutineLoadCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SHOW;
    }

    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
