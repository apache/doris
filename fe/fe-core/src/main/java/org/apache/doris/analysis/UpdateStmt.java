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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
import org.apache.doris.rewrite.ExprRewriter;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * UPDATE is a DML statement that modifies rows in a table.
 * The current update syntax only supports updating the filtered data of a single table.
 *
 * UPDATE table_reference
 *     SET assignment_list
 *     [WHERE where_condition]
 *
 * value:
 *     {expr}
 *
 * assignment:
 *     col_name = value
 *
 * assignment_list:
 *     assignment [, assignment] ...
 */
public class UpdateStmt extends DdlStmt {

    private TableName tableName;
    private List<Expr> setExprs;
    private Expr whereExpr;

    // After analyzed
    private Table targetTable;
    private TupleDescriptor srcTupleDesc;

    public UpdateStmt(TableName tableName, List<Expr> setExprs, Expr whereExpr) {
        this.tableName = tableName;
        this.setExprs = setExprs;
        this.whereExpr = whereExpr;
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<Expr> getSetExprs() {
        return setExprs;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public TupleDescriptor getSrcTupleDesc() {
        return srcTupleDesc;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        analyzeTargetTable(analyzer);
        analyzeSetExprs(analyzer);
        analyzeWhereExpr(analyzer);
    }

    private void analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
        // step1: analyze table name
        tableName.analyze(analyzer);
        // step2: resolve table name with catalog, only unique olap table could be update
        String dbName = tableName.getDb();
        String targetTableName = tableName.getTbl();
        Preconditions.checkNotNull(dbName);
        Preconditions.checkNotNull(targetTableName);
        Database database = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbName);
        targetTable = database.getTableOrAnalysisException(tableName.getTbl());
        if (targetTable.getType() != Table.TableType.OLAP
                || ((OlapTable) targetTable).getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("Only unique olap table could be updated.");
        }
        // step3: register tuple desc
        targetTable.readLock();
        try {
            srcTupleDesc = analyzer.registerOlapTable(targetTable, tableName, null);
        } finally {
            targetTable.readUnlock();
        }
    }

    private void analyzeSetExprs(Analyzer analyzer) throws AnalysisException {
        // step1: analyze set exprs
        Set<String> columnMappingNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // the column expr only support binary predicate which's child(0) must be a SloRef.
        // the duplicate column name of SloRef is forbidden.
        for (Expr setExpr : setExprs) {
            if (!(setExpr instanceof BinaryPredicate)) {
                throw new AnalysisException("Set function expr only support eq binary predicate. "
                        + "Expr: " + setExpr.toSql());
            }
            BinaryPredicate predicate = (BinaryPredicate) setExpr;
            if (predicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Set function expr only support eq binary predicate. "
                        + "The predicate operator error, op: " + predicate.getOp());
            }
            Expr lhs = predicate.getChild(0);
            if (!(lhs instanceof SlotRef)) {
                throw new AnalysisException("Set function expr only support eq binary predicate "
                        + "which's child(0) must be a column name. "
                        + "The child(0) expr error. expr: " + lhs.toSql());
            }
            String column = ((SlotRef) lhs).getColumnName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column setting: " + column);
            }
        }
        // step2: resolve target columns with catalog,
        //        only value columns which belong to target table could be updated.
        for (Expr setExpr : setExprs) {
            Preconditions.checkState(setExpr instanceof BinaryPredicate);
            // check target column
            // 1. columns must belong to target table
            // 2. only value columns could be updated
            Expr lhs = setExpr.getChild(0);
            if (!(lhs instanceof SlotRef)) {
                throw new AnalysisException("The left side of the set expr must be the column name");
            }
            lhs.analyze(analyzer);
            if (((SlotRef) lhs).getColumn().getAggregationType() != AggregateType.REPLACE) {
                throw new AnalysisException("Only value columns of unique table could be updated.");
            }
            // check set expr of target column
            Expr rhs = setExpr.getChild(1);
            checkLargeIntOverflow(rhs);
            rhs.analyze(analyzer);
            if (lhs.getType() != rhs.getType()) {
                setExpr.setChild(1, rhs.checkTypeCompatibility(lhs.getType()));
            }
        }
    }

    /*
   The overflow detection of LargeInt needs to be verified again here.
   The reason is: the first overflow detection(in constructor) cannot filter 2^127.
   Therefore, a second verification is required here.
    */
    private void checkLargeIntOverflow(Expr expr) throws AnalysisException {
        if (expr instanceof LargeIntLiteral) {
            expr.analyzeImpl(analyzer);
        }
    }

    private void analyzeWhereExpr(Analyzer analyzer) throws AnalysisException {
        if (whereExpr == null) {
            throw new AnalysisException("Where clause is required");
        }
        whereExpr = analyzer.getExprRewriter().rewrite(whereExpr, analyzer, ExprRewriter.ClauseType.WHERE_CLAUSE);
        whereExpr.analyze(analyzer);
        if (!whereExpr.getType().equals(Type.BOOLEAN)) {
            throw new AnalysisException("Where clause is not a valid statement return bool");
        }
        analyzer.registerConjunct(whereExpr, srcTupleDesc.getId());
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(tableName.toSql()).append("\n");
        sb.append("  ").append("SET ");
        for (Expr setExpr : setExprs) {
            sb.append(setExpr.toSql()).append(", ");
        }
        sb.append("\n");
        if (whereExpr != null) {
            sb.append("  ").append("WHERE ").append(whereExpr.toSql());
        }
        return sb.toString();
    }
}
