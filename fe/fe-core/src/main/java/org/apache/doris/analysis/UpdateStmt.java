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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * UPDATE is a DML statement that modifies rows in a unique key olap table.
 * The current update syntax only supports updating the filtered data of a single table.
 * <p>
 * UPDATE table_reference
 *     SET assignment_list
 *     [from_clause]
 *     [WHERE where_condition]
 * <p>
 * assignment_list:
 *     assignment [, assignment] ...
 * <p>
 * assignment:
 *     col_name = value
 * <p>
 * value:
 *     {expr}
 */
@Deprecated
public class UpdateStmt extends DdlStmt implements NotFallbackInParser {
    private TableRef targetTableRef;
    private TableName tableName;
    private final List<BinaryPredicate> setExprs;
    private final Expr whereExpr;
    private final FromClause fromClause;
    private InsertStmt insertStmt;
    private TableIf targetTable;
    List<SelectListItem> selectListItems = Lists.newArrayList();
    List<String> cols = Lists.newArrayList();
    private boolean isPartialUpdate = false;

    public UpdateStmt(TableRef targetTableRef, List<BinaryPredicate> setExprs, FromClause fromClause, Expr whereExpr) {
        this.targetTableRef = targetTableRef;
        this.tableName = targetTableRef.getName();
        this.setExprs = setExprs;
        this.fromClause = fromClause;
        this.whereExpr = whereExpr;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isInDebugMode()) {
            throw new AnalysisException("Update is forbidden since current session is in debug mode."
                    + " Please check the following session variables: "
                    + ConnectContext.get().getSessionVariable().printDebugModeVariables());
        }
        analyzeTargetTable(analyzer);
        analyzeSetExprs(analyzer);
        constructInsertStmt();
    }

    private void constructInsertStmt() {
        // not use origin from clause, because we need to mod it, and this action will affect toSql().
        FromClause fromUsedInInsert;
        if (fromClause == null) {
            fromUsedInInsert = new FromClause(Lists.newArrayList(targetTableRef));
        } else {
            fromUsedInInsert = fromClause.clone();
            fromUsedInInsert.getTableRefs().add(0, targetTableRef);
        }
        SelectStmt selectStmt = new SelectStmt(
                // select list
                new SelectList(selectListItems, false),
                // from clause
                fromUsedInInsert,
                // where expr
                whereExpr,
                // group by
                null,
                // having
                null,
                // order by
                null,
                // limit
                LimitElement.NO_LIMIT
        );

        insertStmt = new NativeInsertStmt(
                new InsertTarget(tableName, null),
                null,
                cols,
                new InsertSource(selectStmt),
                null,
                isPartialUpdate, NativeInsertStmt.InsertType.UPDATE);
        ((NativeInsertStmt) insertStmt).setIsFromDeleteOrUpdateStmt(true);
    }

    private void analyzeTargetTable(Analyzer analyzer) throws UserException {
        // step1: analyze table name and origin table alias
        targetTableRef = analyzer.resolveTableRef(targetTableRef);
        targetTableRef.analyze(analyzer);
        tableName = targetTableRef.getName();
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
        // check load privilege, select privilege will check when analyze insert stmt
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }

        // step2: resolve table name with catalog, only unique olap table could be updated
        targetTable = targetTableRef.getTable();
        if (targetTable.getType() != Table.TableType.OLAP
                || ((OlapTable) targetTable).getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("Only unique table could be updated.");
        }
    }

    private void analyzeSetExprs(Analyzer analyzer) throws AnalysisException {
        // step1: analyze set exprs
        Set<String> columnMappingNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // the column expr only support binary predicate which child(0) must be a SloRef.
        // the duplicate column name of SloRef is forbidden.
        for (BinaryPredicate predicate : setExprs) {
            if (predicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Set function expr only support eq binary predicate. "
                        + "The predicate operator error, op: " + predicate.getOp());
            }
            Expr lhs = predicate.getChild(0);
            if (!(lhs instanceof SlotRef)) {
                throw new AnalysisException("Set function expr only support eq binary predicate "
                        + "which child(0) must be a column name. "
                        + "The child(0) expr error. expr: " + lhs.toSql());
            }
            String column = ((SlotRef) lhs).getColumnName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column setting: " + column);
            }
        }
        // step2: resolve target columns with catalog,
        //        only value columns which belong to target table could be updated.
        for (BinaryPredicate setExpr : setExprs) {
            // check target column
            // 1. columns must belong to target table
            // 2. only value columns could be updated
            Expr lhs = setExpr.getChild(0);
            if (!(lhs instanceof SlotRef)) {
                throw new AnalysisException("The left side of the set expr must be the column name");
            }
            lhs.analyze(analyzer);
            if (((SlotRef) lhs).getColumn().isKey()) {
                throw new AnalysisException("Only value columns of unique table could be updated");
            }
        }

        // step3: generate select list and insert column name list in insert stmt
        boolean isMow = ((OlapTable) targetTable).getEnableUniqueKeyMergeOnWrite();
        int setExprCnt = 0;
        for (Column column : targetTable.getColumns()) {
            for (BinaryPredicate setExpr : setExprs) {
                Expr lhs = setExpr.getChild(0);
                if (((SlotRef) lhs).getColumn().equals(column)) {
                    setExprCnt++;
                }
            }
        }
        // table with sequence col cannot use partial update cause in MOW, we encode pk
        // with seq column but we don't know which column is sequence in update
        if (isMow && ((OlapTable) targetTable).getSequenceCol() == null
                && setExprCnt <= targetTable.getColumns().size() * 3 / 10) {
            isPartialUpdate = true;
        }
        Optional<Column> sequenceMapCol = Optional.empty();
        OlapTable olapTable = (OlapTable) targetTable;
        if (olapTable.hasSequenceCol() && olapTable.getSequenceMapCol() != null) {
            sequenceMapCol = olapTable.getFullSchema().stream()
                    .filter(col -> col.getName().equalsIgnoreCase(olapTable.getSequenceMapCol())).findFirst();
        }
        for (Column column : targetTable.getColumns()) {
            Expr expr = new SlotRef(targetTableRef.getAliasAsName(), column.getName());
            boolean existInExpr = false;
            for (BinaryPredicate setExpr : setExprs) {
                Expr lhs = setExpr.getChild(0);
                Column exprColumn = ((SlotRef) lhs).getColumn();
                // when updating the sequence map column, the real sequence column need to set with the same value.
                boolean isSequenceMapColumn = sequenceMapCol.isPresent()
                        && exprColumn.equals(sequenceMapCol.get());
                if (exprColumn.equals(column) || (olapTable.hasSequenceCol()
                        && column.equals(olapTable.getSequenceCol()) && isSequenceMapColumn)) {
                    expr = setExpr.getChild(1);
                    existInExpr = true;
                }
            }
            if (column.isKey() || existInExpr || !isPartialUpdate) {
                selectListItems.add(new SelectListItem(expr, null));
                cols.add(column.getName());
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(targetTableRef.toSql()).append("\n");
        sb.append("  ").append("SET ");
        for (Expr setExpr : setExprs) {
            sb.append(setExpr.toSql()).append(", ");
        }
        if (fromClause != null) {
            sb.append("\n").append(fromClause.toSql());
        }
        sb.append("\n");
        if (whereExpr != null) {
            sb.append("  ").append("WHERE ").append(whereExpr.toSql());
        }
        return sb.toString();
    }
}
