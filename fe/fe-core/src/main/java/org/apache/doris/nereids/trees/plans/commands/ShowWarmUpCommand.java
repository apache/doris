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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

/**
 * ShowWarmUpCommand
 */
public class ShowWarmUpCommand extends ShowCommand {
    private static final ImmutableList<String> WARM_UP_JOB_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId")
            .add("SrcComputeGroup")
            .add("DstComputeGroup")
            .add("Status")
            .add("Type")
            .add("SyncMode")
            .add("CreateTime")
            .add("StartTime")
            .add("FinishBatch")
            .add("AllBatch")
            .add("FinishTime")
            .add("ErrMsg")
            .add("Tables")
            .build();
    private Expression whereClause;
    private boolean showAllJobs = false;
    private long jobId = -1;

    public ShowWarmUpCommand(Expression whereClause) {
        super(PlanType.SHOW_WARM_UP_COMMAND);
        this.whereClause = whereClause;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        return handleShowCloudWarmUpJob();
    }

    private ShowResultSet handleShowCloudWarmUpJob() throws AnalysisException {
        if (showAllJobs) {
            int limit = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().MAX_SHOW_ENTRIES;
            return new ShowResultSet(getMetaData(),
                ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getAllJobInfos(limit));
        } else {
            return new ShowResultSet(getMetaData(),
                ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getSingleJobInfo(jobId));
        }
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (whereClause == null) {
            showAllJobs = true;
            return;
        }

        if (!isWhereClauseValid(whereClause)) {
            throw new AnalysisException("Where clause should looks like one of them: id = 123");
        }
    }

    private boolean isWhereClauseValid(Expression expr) {
        if (!(expr instanceof EqualTo)) {
            return false;
        }

        // left child
        if (!(expr.child(0) instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) expr.child(0)).getName();
        if (leftKey.equalsIgnoreCase("id") && (expr.child(1) instanceof IntegerLikeLiteral)) {
            jobId = ((IntegerLikeLiteral) expr.child(1)).getLongValue();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowWarmupCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : WARM_UP_JOB_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
