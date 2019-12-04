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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcNode;
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
 *      SHOW ALTER TABLE [COLUMN | ROLLUP] [FROM dbName] [WHERE TableName="xxx"] [ORDER BY CreateTime DESC] [LIMIT [offset,]rows]
 */
public class ShowAlterStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowAlterStmt.class);

    public static enum AlterType {
        COLUMN, ROLLUP
    }

    private AlterType type;
    private String dbName;
    private Expr whereClause;
    private HashMap<String, Expr> filterMap;
    private List<OrderByElement> orderByElements;
    private ArrayList<OrderByPair> orderByPairs;
    private LimitElement limitElement;

    private ProcNodeInterface node;

    public AlterType getType() { return type; }
    public String getDbName() { return dbName; }
    public HashMap<String, Expr> getFilterMap() { return filterMap; }
    public LimitElement getLimitElement(){ return limitElement; }
    public ArrayList<OrderByPair> getOrderPairs(){ return orderByPairs; }

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

    private boolean getPredicateValue(Expr subExpr) throws AnalysisException {
        if (!(subExpr instanceof BinaryPredicate)) {
            return false;
        }
        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if (!(subExpr.getChild(0) instanceof SlotRef)) {
            return false;
        }
        String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName().toLowerCase();
        if (leftKey.equals("tablename") || leftKey.equals("state")) {
            if (!(subExpr.getChild(1) instanceof StringLiteral) ||
                    binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                return false;
            }
        } else if (leftKey.equals("createtime") || leftKey.equals("finishtime")) {
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                return false;
            }
            subExpr.setChild(1,((StringLiteral) subExpr.getChild(1)).castTo(Type.DATETIME));
        } else {
            return false;
        }
        filterMap.put(leftKey, subExpr);
        return true;
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
        boolean valid = getPredicateValue(subExpr);
        if (!valid) {
            filterMap.clear();
            throw new AnalysisException("Where clause should looks like: TableName = \"tablename\","
                    + " or State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\", or CreateTime =/>=/<=/>/< \"2019-12-02\","
                    + " FinishTime =/>=/<=/>/< \"2019-12-02 14:54:00\" or compound predicate with operator AND");
        }
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
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        Preconditions.checkNotNull(type);

        // analyze where clause if not null
        if (whereClause != null) {
            if (whereClause instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) whereClause;
                if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }
                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
            } else {
                analyzeSubPredicate(whereClause);
            }
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<OrderByPair>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = SchemaChangeProcNode.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }

        if (limitElement != null) {
            limitElement.analyze(analyzer);
        }
    }
    
    
    public void handleShowAlterTable(Analyzer analyzer) throws AnalysisException, UserException {
        final String dbNameWithoutPrefix = ClusterNamespace.getNameFromFullName(dbName);
        Database db = analyzer.getCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbNameWithoutPrefix);
        }

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

        LOG.debug("process SHOW PROC '{}';", sb.toString());
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
                sb.append(orderByElements.get(i).getExpr().toSql());
                sb.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (limitElement != null && limitElement.hasLimit()) {
            sb.append(" LIMIT ").append(limitElement.getLimit());
            if (limitElement.hasOffset()) {
                sb.append(" OFFSET ").append(limitElement.getOffset());
            }
        }
        LOG.info("AlterSQL '{}'", sb.toString());
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
            titleNames = SchemaChangeProcNode.TITLE_NAMES;
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
