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

import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.BuildIndexProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * show build index command
 */
public class ShowBuildIndexCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName")
            .add("PartitionName").add("AlterInvertedIndexes")
            .add("CreateTime").add("FinishTime")
            .add("TransactionId").add("State")
            .add("Msg").add("Progress")
            .build();
    private static String TABLE_NAME = "tablename";
    private static String PARTITION_NAME = "partitionname";
    private static String STATE = "state";
    private static String CREATE_TIME = "createtime";
    private static String FINISH_TIME = "finishtime";
    private static final Logger LOG = LogManager.getLogger(ShowBuildIndexCommand.class);
    private String dbName;
    private final Expression wildWhere;
    private final long limit;
    private final long offset;
    private final List<OrderKey> orderKeys;
    private HashMap<String, Expression> filterMap;
    private ArrayList<OrderByPair> orderByPairs;
    private ProcNodeInterface node;

    /**
     * constructor for show build index
     */
    public ShowBuildIndexCommand(String dbName, Expression wildWhere,
                                     List<OrderKey> orderKeys, long limit, long offset) {
        super(PlanType.SHOW_BUILD_INDEX_COMMAND);
        this.dbName = dbName;
        this.wildWhere = wildWhere;
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
        this.filterMap = new HashMap<String, Expression>();
    }

    private void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        analyzeSubPredicate(wildWhere);

        // then process the order by
        orderByPairs = getOrderByPairs(orderKeys, TITLE_NAMES);
    }

    private void analyzeSubPredicate(Expression subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }
        if (subExpr instanceof CompoundPredicate) {
            if (!(subExpr instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            for (Expression child : subExpr.children()) {
                analyzeSubPredicate(child);
            }
            return;
        }

        boolean isNotExpr = false;
        if (subExpr instanceof Not) {
            isNotExpr = true;
            subExpr = subExpr.child(0);
            if (!(subExpr instanceof EqualTo)) {
                throw new AnalysisException("Only operator =|>=|<=|>|<|!=|like are supported.");
            }
        }

        getPredicateValue(subExpr, isNotExpr);
    }

    private void getPredicateValue(Expression subExpr, boolean isNotExpr) throws AnalysisException {
        if (!(subExpr instanceof ComparisonPredicate)) {
            throw new AnalysisException("The operator =|>=|<=|>|<|!= are supported.");
        }

        ComparisonPredicate binaryPredicate = (ComparisonPredicate) subExpr;
        if (!(subExpr.child(0) instanceof UnboundSlot)) {
            throw new AnalysisException("Only support column = xxx syntax.");
        }
        String leftKey = ((UnboundSlot) subExpr.child(0)).getName().toLowerCase();
        if (leftKey.equalsIgnoreCase(TABLE_NAME)
                || leftKey.equalsIgnoreCase(STATE)
                || leftKey.equalsIgnoreCase(PARTITION_NAME)) {
            if (!(subExpr.child(1) instanceof StringLikeLiteral) || !(binaryPredicate instanceof EqualTo)) {
                throw new AnalysisException("Where clause : TableName = \"table1\" or "
                        + "State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\"");
            }
        } else if (leftKey.equalsIgnoreCase(CREATE_TIME) || leftKey.equalsIgnoreCase(FINISH_TIME)) {
            if (!(subExpr.child(1) instanceof StringLikeLiteral)) {
                throw new AnalysisException("Where clause : CreateTime/FinishTime =|>=|<=|>|<|!= "
                        + "\"2019-12-02|2019-12-02 14:54:00\"");
            }
            Expression left = subExpr.child(0);
            Expression right = subExpr.child(1).castTo(Config.enable_date_conversion
                    ? DateTimeV2Type.MAX : DateTimeType.INSTANCE);
            subExpr = subExpr.withChildren(left, right);
        } else {
            throw new AnalysisException(
                    "The columns of TableName/PartitionName/CreateTime/FinishTime/State are supported.");
        }
        filterMap.put(leftKey.toLowerCase(), isNotExpr ? new Not(subExpr) : subExpr);
    }

    private void analyze(ConnectContext ctx) throws UserException {
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
        // build proc path
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        sb.append("/build_index");

        if (LOG.isDebugEnabled()) {
            LOG.debug("process SHOW PROC '{}';", sb.toString());
        }
        // create show proc stmt
        // '/jobs/db_name/build_index/
        node = ProcService.getInstance().open(sb.toString());
        if (node == null) {
            throw new AnalysisException("Failed to show build index");
        }
    }

    @VisibleForTesting
    protected ShowResultSet handleShowBuildIndex(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // first validate the where
        validate(ctx);

        // then analyze
        analyze(ctx);

        Preconditions.checkNotNull(node);
        LimitElement limitElement = null;
        if (limit > 0) {
            limitElement = new LimitElement(offset == -1L ? 0 : offset, limit);
        }

        List<List<String>> rows = ((BuildIndexProcDir) node).fetchResultByFilterExpression(
                filterMap, orderByPairs, limitElement).getRows();
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowBuildIndex(ctx, executor);
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
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowBuildIndexCommand(this, context);
    }
}
