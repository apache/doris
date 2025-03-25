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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the command for SHOW SYNC JOB.
 */
public class ShowSyncJobCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("JobName").add("Type").add("State").add("Channel").add("Status")
            .add("JobConfig").add("CreateTime").add("LastStartTime").add("LastStopTime").add("FinishTime").add("Msg")
            .build();

    private String databaseName;

    public ShowSyncJobCommand(String databaseName) {
        super(PlanType.SHOW_SYNC_JOB_COMMAND);
        this.databaseName = databaseName;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(databaseName)) {
            databaseName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(databaseName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                    databaseName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), databaseName);
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env env = Env.getCurrentEnv();
        DatabaseIf db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(databaseName);

        List<List<Comparable>> syncInfos = env.getSyncJobManager().getSyncJobsInfoByDbId(db.getId());
        Collections.sort(syncInfos, new ListComparator<List<Comparable>>(0));

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> syncInfo : syncInfos) {
            List<String> row = new ArrayList<String>(syncInfo.size());

            for (Comparable element : syncInfo) {
                row.add(element.toString());
            }
            rows.add(row);
        }
        return new ShowResultSet(this.getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowSyncJobCommand(this, context);
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
