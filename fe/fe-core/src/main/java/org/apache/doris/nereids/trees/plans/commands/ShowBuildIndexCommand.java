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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * ShowBuildIndexCommand
 */
public class ShowBuildIndexCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowBuildIndexCommand.class);
    private String catalog;
    private String dbName;
    private final List<OrderKey> orderByElements;
    private ArrayList<OrderByPair> orderByPairs;
    private final Expression whereClause;
    private final long limit;
    private final long offset;
    private final LimitElement limitElement;
    private ProcNodeInterface node;
    private final HashMap<String, Expression> filterMap = new HashMap<>();

    /**
     * ShowBuildIndexCommand
     */
    public ShowBuildIndexCommand(String catalog, String dbName, List<OrderKey> orderByElements,
                                 Expression where, long limit, long offset) {
        super(PlanType.SHOW_BUILD_INDEX_COMMAND);
        this.catalog = catalog;
        this.dbName = dbName;
        this.orderByElements = orderByElements;
        this.whereClause = where;
        this.limit = limit;
        this.offset = offset;
        limitElement = new LimitElement(offset, limit);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowBuildIndex();
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
            }
        }

        analyzeSubExpr(whereClause);

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderKey orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof UnboundSlot)) {
                    throw new AnalysisException("Should order by column");
                }
                UnboundSlot slot = (UnboundSlot) orderByElement.getExpr();
                int index = BuildIndexProcDir.analyzeColumn(slot.getName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.isAsc());
                orderByPairs.add(orderByPair);
            }
        }

        DatabaseIf db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);
        // build proc path
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        sb.append("/build_index");

        if (LOG.isDebugEnabled()) {
            LOG.debug("process SHOW PROC '{}';", sb.toString());
        }
        // create show proc command
        // '/jobs/db_name/build_index/
        node = ProcService.getInstance().open(sb.toString());
        if (node == null) {
            throw new AnalysisException("Failed to show build index");
        }
    }

    private void checkAndExpression(Expression left, Expression right) throws AnalysisException {
        if (left.child(0).equals(right.child(0))) {
            throw new AnalysisException("names on both sides of operator AND should be diffrent");
        }
    }

    private void analyzeSubExpr(Expression expr) throws AnalysisException {
        if (expr == null) {
            return;
        }

        if (expr instanceof CompoundPredicate) {
            CompoundPredicate predicate = (CompoundPredicate) expr;
            if (!(expr instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            Expression left = predicate.child(0);
            Expression right = predicate.child(1);
            checkAndExpression(left, right);
            analyzeSubExpr(left);
            analyzeSubExpr(right);
        } else {
            getExprValue(expr);
        }
    }

    private void getExprValue(Expression expr) throws AnalysisException {
        if (!(expr instanceof ComparisonPredicate || (expr instanceof Not && expr.child(0) instanceof EqualTo))) {
            throw new AnalysisException("The operator =|>=|<=|>|<|!= are supported.");
        }

        String leftKey = "";
        if (expr instanceof ComparisonPredicate) {
            if (!(expr.child(0) instanceof UnboundSlot)) {
                throw new AnalysisException("Only support column = xxx syntax.");
            }
            leftKey = ((UnboundSlot) expr.child(0)).getName();
        } else if (expr instanceof Not && expr.child(0) instanceof EqualTo) {
            if (!(expr.child(0).child(0) instanceof UnboundSlot)) {
                throw new AnalysisException("Only support column = xxx syntax.");
            }
            leftKey = ((UnboundSlot) expr.child(0).child(0)).getName();
        }

        if (leftKey.equalsIgnoreCase("tablename")
                || leftKey.equalsIgnoreCase("state")
                || leftKey.equalsIgnoreCase("partitionname")) {
            if (!(expr.child(1) instanceof StringLikeLiteral) || !(expr instanceof EqualTo)) {
                throw new AnalysisException("Where clause : TableName = \"table1\" or "
                    + "State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\"");
            }
        } else if (leftKey.equalsIgnoreCase("createtime") || leftKey.equalsIgnoreCase("finishtime")) {
            if (expr instanceof ComparisonPredicate && !(expr.child(1) instanceof StringLikeLiteral)) {
                throw new AnalysisException("Where clause : CreateTime/FinishTime =|>=|<=|>|<|!= "
                    + "\"2019-12-02|2019-12-02 14:54:00\"");
            }

            if (expr instanceof Not && expr.child(0) instanceof EqualTo) {
                if (!(expr.child(0).child(1) instanceof StringLikeLiteral)) {
                    throw new AnalysisException("Where clause : CreateTime/FinishTime =|>=|<=|>|<|!= "
                        + "\"2019-12-02|2019-12-02 14:54:00\"");
                }
                expr = rebuildExpr(expr, expr.child(0).child(1).castTo(DateTimeType.INSTANCE.conversion()));
            } else {
                expr = rebuildExpr(expr, expr.child(1).castTo(DateTimeType.INSTANCE.conversion()));
            }
        } else {
            throw new AnalysisException(
                "The columns of TableName/PartitionName/CreateTime/FinishTime/State are supported.");
        }

        filterMap.put(leftKey, expr);
    }

    private Expression rebuildExpr(Expression expr, Expression right) {
        if (expr instanceof EqualTo) {
            expr = new EqualTo(expr.child(0), right);
        } else if (expr instanceof GreaterThan) {
            expr = new GreaterThan(expr.child(0), right);
        } else if (expr instanceof GreaterThanEqual) {
            expr = new GreaterThanEqual(expr.child(0), right);
        } else if (expr instanceof LessThan) {
            expr = new LessThan(expr.child(0), right);
        } else if (expr instanceof LessThanEqual) {
            expr = new LessThanEqual(expr.child(0), right);
        } else if (expr instanceof NullSafeEqual) {
            expr = new NullSafeEqual(expr.child(0), right);
        } else if (expr instanceof Not && expr.child(0) instanceof EqualTo) {
            expr = new Not(new EqualTo(expr.child(0).child(0), right));
        }
        return expr;
    }

    private ShowResultSet handleShowBuildIndex() throws AnalysisException {
        ProcNodeInterface procNodeI = node;
        Preconditions.checkNotNull(procNodeI);
        // List<List<String>> rows = ((BuildIndexProcDir) procNodeI).fetchResult().getRows();
        List<List<String>> rows = ((BuildIndexProcDir) procNodeI).fetchResultByFilter(this.filterMap,
                this.orderByPairs, this.limitElement).getRows();
        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW BUILD INDEX ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }
        // Order By clause
        if (orderByElements != null) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                sb.append(orderByElements.get(i).toSql());
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (limitElement != null) {
            sb.append(limitElement.toSql());
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowBuildIndexCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        ImmutableList<String> titleNames = BuildIndexProcDir.TITLE_NAMES;

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
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
}
