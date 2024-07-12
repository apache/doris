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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.BetweenToCompoundRule;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.FoldConstantsRule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Deprecated
public class DeleteStmt extends DdlStmt implements NotFallbackInParser {

    private static final List<ExprRewriteRule> EXPR_NORMALIZE_RULES = ImmutableList.of(
            BetweenToCompoundRule.INSTANCE
    );

    private TableRef targetTableRef;
    private TableName tableName;
    private final PartitionNames partitionNames;
    private final FromClause fromClause;
    private Expr wherePredicate;

    private final List<Predicate> deleteConditions = new LinkedList<>();

    private InsertStmt insertStmt;
    private TableIf targetTable;
    private final List<SelectListItem> selectListItems = Lists.newArrayList();
    private final List<String> cols = Lists.newArrayList();

    public DeleteStmt(TableName tableName, PartitionNames partitionNames, Expr wherePredicate) {
        this(new TableRef(tableName, null), partitionNames, null, wherePredicate);
    }

    public DeleteStmt(TableRef targetTableRef, PartitionNames partitionNames,
            FromClause fromClause, Expr wherePredicate) {
        this.targetTableRef = targetTableRef;
        this.tableName = targetTableRef.getName();
        this.partitionNames = partitionNames;
        this.fromClause = fromClause;
        this.wherePredicate = wherePredicate;
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public List<String> getPartitionNames() {
        return partitionNames == null ? Lists.newArrayList() : partitionNames.getPartitionNames();
    }

    public FromClause getFromClause() {
        return fromClause;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public List<Predicate> getDeleteConditions() {
        return deleteConditions;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        analyzeTargetTable(analyzer);

        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support deleting temp partitions");
            }
        }

        // analyze predicate
        if ((fromClause == null && !((OlapTable) targetTable).getEnableUniqueKeyMergeOnWrite())
                || (fromClause == null && ((OlapTable) targetTable).getEnableMowLightDelete())) {
            if (wherePredicate == null) {
                throw new AnalysisException("Where clause is not set");
            }
            ExprRewriter exprRewriter = new ExprRewriter(EXPR_NORMALIZE_RULES);
            wherePredicate = exprRewriter.rewrite(wherePredicate, analyzer);
            try {
                analyzePredicate(wherePredicate, analyzer);
                checkDeleteConditions();
            } catch (AnalysisException e) {
                if (!(((OlapTable) targetTable).getKeysType() == KeysType.UNIQUE_KEYS)) {
                    throw new AnalysisException(e.getMessage(), e.getCause());
                }
                wherePredicate.reset();
                constructInsertStmt();
            }
        } else {
            constructInsertStmt();
        }
    }

    private void constructInsertStmt() throws AnalysisException {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isInDebugMode()) {
            throw new AnalysisException("Delete is forbidden since current session is in debug mode."
                    + " Please check the following session variables: "
                    + ConnectContext.get().getSessionVariable().printDebugModeVariables());
        }
        boolean isMow = ((OlapTable) targetTable).getEnableUniqueKeyMergeOnWrite();
        for (Column column : targetTable.getColumns()) {
            Expr expr;
            // in mow, we can use partial update so we only need key column and delete sign
            if (!column.isVisible() && column.getName().equalsIgnoreCase(Column.DELETE_SIGN)) {
                expr = new BoolLiteral(true);
            } else if (column.isKey()) {
                expr = new SlotRef(targetTableRef.getAliasAsName(), column.getName());
            } else if (!isMow && (!column.isVisible() || (!column.isAllowNull() && !column.hasDefaultValue()))) {
                expr = new SlotRef(targetTableRef.getAliasAsName(), column.getName());
            } else {
                continue;
            }
            selectListItems.add(new SelectListItem(expr, null));
            cols.add(column.getName());
        }

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
                wherePredicate,
                // group by
                null,
                // having
                null,
                // order by
                null,
                // limit
                LimitElement.NO_LIMIT
        );
        boolean isPartialUpdate = false;
        if (((OlapTable) targetTable).getEnableUniqueKeyMergeOnWrite()
                && cols.size() < targetTable.getColumns().size()) {
            isPartialUpdate = true;
        }

        insertStmt = new NativeInsertStmt(
                new InsertTarget(tableName, null),
                null,
                cols,
                new InsertSource(selectStmt),
                null,
                isPartialUpdate,
                NativeInsertStmt.InsertType.DELETE);
        ((NativeInsertStmt) insertStmt).setIsFromDeleteOrUpdateStmt(true);
    }

    private void analyzeTargetTable(Analyzer analyzer) throws UserException {
        // step1: analyze table name and origin table alias
        if (tableName == null) {
            throw new AnalysisException("Table is not set");
        }
        targetTableRef = analyzer.resolveTableRef(targetTableRef);
        targetTableRef.analyze(analyzer);
        tableName = targetTableRef.getName();
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
        // check load privilege, select privilege will check when analyze insert stmt
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), tableName.getDb() + ": " + tableName.getTbl());
        }

        // step2: resolve table name with catalog, only unique olap table could be updated with using
        targetTable = targetTableRef.getTable();
        if (fromClause != null && (targetTable.getType() != Table.TableType.OLAP
                || ((OlapTable) targetTable).getKeysType() != KeysType.UNIQUE_KEYS)) {
            throw new AnalysisException("Only unique table could use delete with using.");
        }
    }

    @VisibleForTesting
    void analyzePredicate(Expr predicate, Analyzer analyzer) throws AnalysisException {
        if (predicate == null) {
            throw new AnalysisException("Where clause is not set");
        }
        if (predicate instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
            binaryPredicate.getChild(0).analyze(analyzer);
            binaryPredicate.getChild(1).analyze(analyzer);

            binaryPredicate.setChild(1, binaryPredicate.getChild(1).castTo(binaryPredicate.getChild(0).getType()));
            binaryPredicate.analyze(analyzer);

            Expr rightChild = binaryPredicate.getChild(1);
            Expr rewrittenExpr = FoldConstantsRule.INSTANCE.apply(rightChild, analyzer, null);
            if (rightChild != rewrittenExpr) {
                binaryPredicate.setChild(1, rewrittenExpr);
            }

            Expr leftExpr = binaryPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException(
                        "Left expr of binary predicate should be column name, predicate: " + binaryPredicate.toSql()
                                + ", left expr type:" + leftExpr.getType());
            }
            Expr rightExpr = binaryPredicate.getChild(1);
            if (!(rightExpr instanceof LiteralExpr)) {
                throw new AnalysisException(
                        "Right expr of binary predicate should be value, predicate: " + binaryPredicate.toSql()
                                + ", right expr type:" + rightExpr.getType());
            }
            deleteConditions.add(binaryPredicate);
        } else if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            if (compoundPredicate.getOp() != Operator.AND) {
                throw new AnalysisException("Compound predicate's op should be AND");
            }

            analyzePredicate(compoundPredicate.getChild(0), analyzer);
            analyzePredicate(compoundPredicate.getChild(1), analyzer);
        } else if (predicate instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) predicate;
            Expr leftExpr = isNullPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of is_null predicate should be column name");
            }
            deleteConditions.add(isNullPredicate);
        } else if (predicate instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) predicate;
            Expr leftExpr = inPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of in predicate should be column name");
            }
            int inElementNum = inPredicate.getInElementNum();
            int maxAllowedInElementNumOfDelete = Config.max_allowed_in_element_num_of_delete;
            if (inElementNum > maxAllowedInElementNumOfDelete) {
                throw new AnalysisException("Element num of in predicate should not be more than "
                        + maxAllowedInElementNumOfDelete);
            }
            for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                Expr expr = inPredicate.getChild(i);
                if (!(expr instanceof LiteralExpr)) {
                    throw new AnalysisException("Child of in predicate should be value");
                }
            }
            deleteConditions.add(inPredicate);
        } else {
            throw new AnalysisException("Where clause only supports compound predicate,"
                    + " binary predicate, is_null predicate or in predicate");
        }
    }

    private void checkDeleteConditions() throws AnalysisException {
        // check condition column is key column and condition value
        // Here we use "getFullSchema()" to get all columns including VISIBLE and SHADOW columns
        CatalogIf catalog = getCatalog();
        // we ensure the db and table exists.
        Database db = (Database) catalog.getDb(getDbName()).get();
        OlapTable table = ((OlapTable) db.getTable(getTableName()).get());

        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getFullSchema()) {
            nameToColumn.put(column.getName(), column);
        }

        for (Predicate condition : deleteConditions) {
            SlotRef slotRef = getSlotRef(condition);
            String columnName = slotRef.getColumnName();
            if (!nameToColumn.containsKey(columnName)) {
                throw new AnalysisException(String.format("Unknown column '%s' in '%s'", columnName, table.getName()));
            }

            if (Column.isShadowColumn(columnName)) {
                throw new AnalysisException("Can not apply delete condition to shadow column");
            }

            // Check if this column is under schema change, if yes, there will be a shadow column related to it.
            // And we don't allow doing delete operation when a condition column is under schema change.
            String shadowColName = Column.getShadowName(columnName);
            if (nameToColumn.containsKey(shadowColName)) {
                throw new AnalysisException(String.format("Column '%s' is under"
                        + " schema change operation. Do not allow delete operation", columnName));
            }

            Column column = nameToColumn.get(columnName);
            // TODO(Now we can not push down non-scala type like array/map/struct to storage layer because of
            //  predict_column in be not support non-scala type, so we just should ban this type in delete predict, when
            //  we delete predict_column in be we should delete this ban)
            if (!column.getType().isScalarType()
                    || (column.getType().isOnlyMetricType() && !column.getType().isJsonbType())) {
                throw new AnalysisException(String.format("Can not apply delete condition to column type: %s ",
                        column.getType()));

            }
            // Due to rounding errors, most floating-point numbers end up being slightly imprecise,
            // it also means that numbers expected to be equal often differ slightly, so we do not allow compare with
            // floating-point numbers, floating-point number not allowed in where clause
            if (column.getDataType().isFloatingPointType()) {
                throw new AnalysisException("Column[" + columnName + "] type is float or double.");
            }
            if (!column.isKey()) {
                if (table.getKeysType() == KeysType.AGG_KEYS) {
                    throw new AnalysisException("delete predicate on value column only supports Unique table with"
                            + " merge-on-write enabled and Duplicate table, but " + "Table[" + table.getName()
                                    + "] is an Aggregate table.");
                } else if (table.getKeysType() == KeysType.UNIQUE_KEYS && !table.getEnableUniqueKeyMergeOnWrite()) {
                    throw new AnalysisException("delete predicate on value column only supports Unique table with"
                            + " merge-on-write enabled and Duplicate table, but " + "Table[" + table.getName()
                                    + "] is an Aggregate table.");
                }
            }

            if (condition instanceof BinaryPredicate) {
                String value = null;
                try {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    // if a bool cond passed to be, be's zone_map cannot handle bool correctly,
                    // change it to a tinyint type here;
                    value = binaryPredicate.getChild(1).getStringValue();
                    if (column.getDataType() == PrimitiveType.BOOLEAN) {
                        if (value.equalsIgnoreCase("true")) {
                            binaryPredicate.setChild(1, LiteralExpr.create("1", Type.TINYINT));
                        } else if (value.equalsIgnoreCase("false")) {
                            binaryPredicate.setChild(1, LiteralExpr.create("0", Type.TINYINT));
                        }
                    } else if (column.getDataType() == PrimitiveType.DATE
                            || column.getDataType() == PrimitiveType.DATETIME
                            || column.getDataType() == PrimitiveType.DATEV2) {
                        DateLiteral dateLiteral = new DateLiteral(value, Type.fromPrimitiveType(column.getDataType()));
                        value = dateLiteral.getStringValue();
                        binaryPredicate.setChild(1, LiteralExpr.create(value,
                                Type.fromPrimitiveType(column.getDataType())));
                    } else if (column.getDataType() == PrimitiveType.DATETIMEV2) {
                        DateLiteral dateLiteral = new DateLiteral(value,
                                ScalarType.createDatetimeV2Type(ScalarType.MAX_DATETIMEV2_SCALE));
                        value = dateLiteral.getStringValue();
                        binaryPredicate.setChild(1, LiteralExpr.create(value,
                                ScalarType.createDatetimeV2Type(ScalarType.MAX_DATETIMEV2_SCALE)));
                    }
                    LiteralExpr.create(value, column.getType());
                } catch (AnalysisException e) {
                    throw new AnalysisException("Invalid column value[" + value + "] for column " + columnName);
                }
            } else if (condition instanceof InPredicate) {
                String value = null;
                try {
                    InPredicate inPredicate = (InPredicate) condition;
                    for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                        value = inPredicate.getChild(i).getStringValue();
                        if (column.getDataType() == PrimitiveType.DATE
                                || column.getDataType() == PrimitiveType.DATETIME
                                || column.getDataType() == PrimitiveType.DATEV2
                                || column.getDataType() == PrimitiveType.DATETIMEV2) {
                            DateLiteral dateLiteral = new DateLiteral(value,
                                    column.getType());
                            value = dateLiteral.getStringValue();
                            inPredicate.setChild(i, LiteralExpr.create(value,
                                    column.getType()));
                        } else {
                            LiteralExpr.create(value, Type.fromPrimitiveType(column.getDataType()));
                        }
                    }
                } catch (AnalysisException e) {
                    throw new AnalysisException("Invalid column value[" + value + "] for column " + columnName);
                }
            }

            // set schema column name
            slotRef.setCol(column.getName());
        }
    }

    private SlotRef getSlotRef(Predicate condition) {
        SlotRef slotRef = null;
        if (condition instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
            slotRef = (SlotRef) binaryPredicate.getChild(0);
        } else if (condition instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
            slotRef = (SlotRef) isNullPredicate.getChild(0);
        } else if (condition instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) condition;
            slotRef = (SlotRef) inPredicate.getChild(0);
        }
        return slotRef;
    }

    private CatalogIf getCatalog() {
        Env env = Env.getCurrentEnv();
        if (null == tableName.getCtl()) {
            return env.getCurrentCatalog();
        } else {
            return env.getCatalogMgr().getCatalog(tableName.getCtl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tableName.toSql());
        if (partitionNames != null) {
            sb.append(" PARTITION (");
            sb.append(Joiner.on(", ").join(partitionNames.getPartitionNames()));
            sb.append(")");
        }
        sb.append(" WHERE ").append(wherePredicate.toSql());
        return sb.toString();
    }
}
