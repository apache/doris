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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcDir;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 * ShowAlterStmt: used to show process state of alter statement.
 * Syntax:
 *      SHOW ALTER TABLE [COLUMN | ROLLUP] [FROM dbName] [WHERE TableName="xxx"]
 *      [ORDER BY CreateTime DESC] [LIMIT [offset,]rows]
 */
public class ShowAlterStmt extends ShowStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowAlterStmt.class);

    public static enum AlterType {
        COLUMN, ROLLUP, INDEX
    }

    private AlterType type;
    private String dbName;
    private Expr whereClause;
    private HashMap<String, Expr> filterMap;
    private List<OrderByElement> orderByElements;
    private ArrayList<OrderByPair> orderByPairs;
    private LimitElement limitElement;

    private ProcNodeInterface node;

    public AlterType getType() {
        return type;
    }

    public String getDbName() {
        return dbName;
    }

    public HashMap<String, Expr> getFilterMap() {
        return filterMap;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    public ArrayList<OrderByPair> getOrderPairs() {
        return orderByPairs;
    }

    public ProcNodeInterface getNode() {
        return this.node;
    }

    public ShowAlterStmt(AlterType type, String dbName, Expr whereClause, List<OrderByElement> orderByElements,
                         LimitElement limitElement) {
        this.type = type;
        this.dbName = dbName;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
        this.filterMap = new HashMap<String, Expr>();
    }

    private void getPredicateValue(Expr subExpr) throws AnalysisException {
        if (!(subExpr instanceof BinaryPredicate)) {
            throw new AnalysisException("The operator =|>=|<=|>|<|!= are supported.");
        }

        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if (!(subExpr.getChild(0) instanceof SlotRef)) {
            throw new AnalysisException("Only support column = xxx syntax.");
        }
        String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName().toLowerCase();
        if (leftKey.equals("tablename") || leftKey.equals("state") || leftKey.equals("indexname")) {
            if (!(subExpr.getChild(1) instanceof StringLiteral)
                    || binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Where clause : TableName = \"table1\" or "
                    + "State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\"");
            }
        } else if (leftKey.equals("createtime") || leftKey.equals("finishtime")) {
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                throw new AnalysisException("Where clause : CreateTime/FinishTime =|>=|<=|>|<|!= "
                    + "\"2019-12-02|2019-12-02 14:54:00\"");
            }
            subExpr.setChild(1, (subExpr.getChild(1)).castTo(
                    ScalarType.getDefaultDateType(Type.DATETIME)));
        } else {
            throw new AnalysisException(
                    "The columns of TableName/IndexName/CreateTime/FinishTime/State are supported.");
        }
        filterMap.put(leftKey, subExpr);
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }
        if (subExpr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) subExpr;
            if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            analyzeSubPredicate(cp.getChild(0));
            analyzeSubPredicate(cp.getChild(1));
            return;
        }
        getPredicateValue(subExpr);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        //first analyze
        analyzeSyntax(analyzer);

        // check auth when get job info
        handleShowAlterTable(analyzer);
    }

    public void analyzeSyntax(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
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
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<OrderByPair>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = SchemaChangeProcDir.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }

        if (limitElement != null) {
            limitElement.analyze(analyzer);
        }
    }

    public void handleShowAlterTable(Analyzer analyzer) throws UserException {
        DatabaseIf db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);

        // build proc path
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        if (type == AlterType.COLUMN) {
            sb.append("/schema_change");
        } else if (type == AlterType.ROLLUP) {
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

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ALTER TABLE ").append(type.name()).append(" ");
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
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ImmutableList<String> titleNames = null;
        if (type == AlterType.ROLLUP) {
            titleNames = RollupProcDir.TITLE_NAMES;
        } else if (type == AlterType.COLUMN) {
            titleNames = SchemaChangeProcDir.TITLE_NAMES;
        }

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }

        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
