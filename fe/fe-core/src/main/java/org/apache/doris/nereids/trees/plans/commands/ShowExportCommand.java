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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * show export command
 */
public class ShowExportCommand extends ShowCommand {

    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("TaskInfo").add("Path")
            .add("CreateTime").add("StartTime").add("FinishTime")
            .add("Timeout").add("ErrorMsg").add("OutfileInfo")
            .build();
    private static final String ID = "id";
    private static final String JOBID = "jobid";
    private static final String STATE = "state";
    private static final String LABEL = "label";
    private static final Logger LOG = LogManager.getLogger(ShowExportCommand.class);
    private String ctl;
    private String dbName;
    private final Expression wildWhere;
    private final long limit;
    private final List<OrderKey> orderKeys;
    private ArrayList<OrderByPair> orderByPairs;
    private String stateValue = null;
    private boolean isLabelUseLike = false;
    private ExportJobState jobState;
    private long jobId = 0;
    private String label = null;

    /**
     * constructor for show export
     */
    public ShowExportCommand(String ctl, String dbName, Expression wildWhere, List<OrderKey> orderKeys, long limit) {
        super(PlanType.SHOW_EXPORT_COMMAND);
        this.ctl = ctl;
        this.dbName = dbName;
        this.wildWhere = wildWhere;
        this.orderKeys = orderKeys;
        this.limit = limit;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    private ExportJobState getJobState() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return jobState;
    }

    @VisibleForTesting
    protected void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(ctl)) {
            ctl = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(ctl)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_CATALOG);
            }
        }

        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // where
        if (wildWhere != null) {
            analyzeExpression(wildWhere);
        }
    }

    private void analyzeExpression(Expression expr) throws AnalysisException {
        if (expr == null) {
            return;
        }
        boolean valid = false;

        if (expr.child(0) instanceof UnboundSlot) {
            String leftKey = ((UnboundSlot) expr.child(0)).getName().toLowerCase();

            if (expr instanceof EqualTo) {
                if ((JOBID.equalsIgnoreCase(leftKey) || ID.equalsIgnoreCase(leftKey))
                        && expr.child(1) instanceof IntegerLikeLiteral) {
                    jobId = ((IntegerLikeLiteral) expr.child(1)).getLongValue();
                    valid = true;
                } else if (STATE.equalsIgnoreCase(leftKey) && expr.child(1) instanceof StringLikeLiteral) {
                    String value = ((StringLikeLiteral) expr.child(1)).getStringValue();
                    if (!Strings.isNullOrEmpty(value)) {
                        stateValue = value.toUpperCase();
                        try {
                            jobState = ExportJobState.valueOf(stateValue);
                            valid = true;
                        } catch (IllegalArgumentException e) {
                            LOG.warn("illegal state argument in export stmt. stateValue={}, error={}", stateValue, e);
                        }
                    }
                } else if (LABEL.equalsIgnoreCase(leftKey) && expr.child(1) instanceof StringLikeLiteral) {
                    label = ((StringLikeLiteral) expr.child(1)).getStringValue();
                    valid = true;
                }
            } else if (expr instanceof Like) {
                if (LABEL.equalsIgnoreCase(leftKey) && expr.child(1) instanceof StringLikeLiteral) {
                    label = ((StringLikeLiteral) expr.child(1)).getStringValue();
                    isLabelUseLike = true;
                    valid = true;
                }
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like below: "
                + " ID = $your_job_id, or STATE = \"PENDING|EXPORTING|FINISHED|CANCELLED\", "
                + "or LABEL = \"xxx\" or LABEL like \"xxx%\"");
        }
    }

    private ShowResultSet handleShowExport(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // first validate the where
        validate(ctx);

        // then process the order by
        orderByPairs = getOrderByPairs(orderKeys, TITLE_NAMES);

        // last get export info
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = env.getCatalogMgr().getCatalogOrAnalysisException(ctl);
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        long dbId = db.getId();

        ExportMgr exportMgr = env.getExportMgr();

        Set<ExportJobState> states = null;
        ExportJobState state = getJobState();
        if (state != null) {
            states = Sets.newHashSet(state);
        }
        List<List<String>> infos = exportMgr.getExportJobInfosByIdOrState(
                dbId, jobId, label, isLabelUseLike, states, orderByPairs, limit >= 0 ? limit : -1);
        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowExport(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowExportCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
