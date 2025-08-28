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
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcDir;
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
 * show alter table command
 */
public class ShowAlterTableCommand extends ShowCommand {
    /**
     * enum of AlterType, including column, rollup, materialized view
     */
    public enum AlterType {
        COLUMN, ROLLUP, MV
    }

    private static final Logger LOG = LogManager.getLogger(ShowAlterTableCommand.class);

    private static final ImmutableList<String> ROLLUP_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
            .add("BaseIndexName").add("RollupIndexName").add("RollupId").add("TransactionId")
            .add("State").add("Msg").add("Progress").add("Timeout")
            .build();
    private static final ImmutableList<String> SCHEMA_CHANGE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
            .add("IndexName").add("IndexId").add("OriginIndexId").add("SchemaVersion")
            .add("TransactionId").add("State").add("Msg").add("Progress").add("Timeout")
            .build();
    private static String STATE = "state";
    private static String TABLE_NAME = "tablename";
    private static String INDEX_NAME = "indexname";
    private static String CREATE_TIME = "createtime";
    private static String FINISH_TIME = "finishtime";
    private static String BASE_INDEX_NAME = "baseindexname";
    private static String ROLLUP_INDEX_NAME = "rollupindexname";

    private AlterType type;
    private String dbName;
    private Expression whereClause;
    private List<OrderKey> orderKeys;
    private final long limit;
    private final long offset;
    private HashMap<String, Expression> filterMap;
    private ArrayList<OrderByPair> orderByPairs;
    private ProcNodeInterface node;

    /**
     * struct of ShowAlterTableCommand
     */
    public ShowAlterTableCommand(String dbName, Expression whereClause, List<OrderKey> orderKeys,
                                     long limit, long offset, AlterType alterType) {
        super(PlanType.SHOW_ALTER_TABLE_COMMAND);
        this.dbName = dbName;
        this.whereClause = whereClause;
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
        this.type = alterType;
        this.filterMap = new HashMap<String, Expression>();
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
        if (leftKey.equals(TABLE_NAME) || leftKey.equals(STATE) || leftKey.equals(INDEX_NAME)
                || leftKey.equals(BASE_INDEX_NAME) || leftKey.equals(ROLLUP_INDEX_NAME)) {
            if (!(subExpr.child(1) instanceof StringLikeLiteral) || !(binaryPredicate instanceof EqualTo)) {
                throw new AnalysisException("Where clause : TableName = \"table1\" or "
                    + "State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\"");
            }
        } else if (leftKey.equals(CREATE_TIME) || leftKey.equals(FINISH_TIME)) {
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
                "The columns of TableName/IndexName/CreateTime/FinishTime/State/BaseIndexName/RollupIndexName "
                        + "are supported.");
        }
        filterMap.put(leftKey.toLowerCase(), isNotExpr ? new Not(subExpr) : subExpr);
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

    @VisibleForTesting
    protected void validate(ConnectContext ctx) throws AnalysisException, UserException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        Preconditions.checkNotNull(type);

        // analyze where clause if not null
        if (whereClause != null) {
            analyzeSubPredicate(whereClause);
        }

        // order by
        orderByPairs = getOrderByPairs(orderKeys, SCHEMA_CHANGE_TITLE_NAMES);
    }

    private void analyze(ConnectContext ctx) throws UserException {
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);

        // build proc path
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        if (type == AlterType.COLUMN) {
            sb.append("/schema_change");
        } else if (type == AlterType.ROLLUP || type == AlterType.MV) {
            sb.append("/rollup");
        } else {
            throw new UserException("SHOW " + type.name() + " does not implement yet");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("process SHOW PROC '{}';", sb.toString());
        }
        // create show proc stmt
        // '/jobs/db_name/rollup|schema_change/
        node = ProcService.getInstance().open(sb.toString());
        if (node == null) {
            throw new AnalysisException("Failed to show alter table");
        }
    }

    @VisibleForTesting
    protected ShowResultSet handleShowAlterTable(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // validate the where clause
        validate(ctx);

        // analyze
        analyze(ctx);

        Preconditions.checkNotNull(node);
        List<List<String>> rows;
        LimitElement limitElement = null;
        if (limit > 0) {
            limitElement = new LimitElement(offset == -1L ? 0 : offset, limit);
        }

        // Only SchemaChangeProc support where/order by/limit syntax
        if (node instanceof SchemaChangeProcDir) {
            rows = ((SchemaChangeProcDir) node).fetchResultByFilterExpression(
                    filterMap, orderByPairs, limitElement).getRows();
        } else if (node instanceof RollupProcDir) {
            rows = ((RollupProcDir) node).fetchResultByFilterExpression(
                    filterMap, orderByPairs, limitElement).getRows();
        } else {
            rows = node.fetchResult().getRows();
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowAlterTable(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowAlterTableCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ImmutableList<String> titleNames = null;
        if (type == AlterType.ROLLUP || type == AlterType.MV) {
            titleNames = ROLLUP_TITLE_NAMES;
        } else if (type == AlterType.COLUMN) {
            titleNames = SCHEMA_CHANGE_TITLE_NAMES;
        }

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }

        return builder.build();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
