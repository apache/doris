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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundHiveTableSink;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The helper class for insert operation.
 */
public class InsertUtils {

    /**
     * execute insert values in transaction.
     */
    public static void executeBatchInsertTransaction(ConnectContext ctx, String dbName, String tableName,
            List<Column> columns, List<List<NamedExpression>> constantExprsList) {
        if (ctx.isInsertValuesTxnIniting()) { // first time, begin txn
            beginBatchInsertTransaction(ctx, dbName, tableName, columns);
        }
        if (!ctx.getTxnEntry().getTxnConf().getDb().equals(dbName)
                || !ctx.getTxnEntry().getTxnConf().getTbl().equals(tableName)) {
            throw new AnalysisException("Only one table can be inserted in one transaction.");
        }

        TransactionEntry txnEntry = ctx.getTxnEntry();
        int effectRows = 0;
        for (List<NamedExpression> row : constantExprsList) {
            ++effectRows;
            InternalService.PDataRow data = getRowStringValue(row);
            if (data == null) {
                continue;
            }
            List<InternalService.PDataRow> dataToSend = txnEntry.getDataToSend();
            dataToSend.add(data);
            if (dataToSend.size() >= StmtExecutor.MAX_DATA_TO_SEND_FOR_TXN) {
                // send data
                InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
                try {
                    executor.sendData();
                } catch (Exception e) {
                    throw new AnalysisException("send data to be failed, because " + e.getMessage(), e);
                }
            }
        }
        txnEntry.setRowsInTransaction(txnEntry.getRowsInTransaction() + effectRows);

        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        String sb = "{'label':'" + ctx.getTxnEntry().getLabel()
                + "', 'status':'" + TransactionStatus.PREPARE.name()
                + "', 'txnId':'" + ctx.getTxnEntry().getTxnConf().getTxnId() + "'"
                + "}";

        ctx.getState().setOk(effectRows, 0, sb);
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(
                ctx.getTxnEntry().getTxnConf().getTxnId(),
                ctx.getTxnEntry().getLabel(),
                dbName,
                tableName,
                TransactionStatus.PREPARE,
                effectRows,
                0);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows(effectRows);
    }

    /**
     * literal expr in insert operation
     */
    public static InternalService.PDataRow getRowStringValue(List<NamedExpression> cols) {
        if (cols.isEmpty()) {
            return null;
        }
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (Expression expr : cols) {
            while (expr instanceof Alias || expr instanceof Cast) {
                expr = expr.child(0);
            }
            if (!(expr instanceof Literal)) {
                throw new AnalysisException(
                        "do not support non-literal expr in transactional insert operation: " + expr.toSql());
            }
            if (expr instanceof NullLiteral) {
                row.addColBuilder().setValue(StmtExecutor.NULL_VALUE_FOR_LOAD);
            } else if (expr instanceof ArrayLiteral) {
                row.addColBuilder().setValue(String.format("\"%s\"",
                        ((ArrayLiteral) expr).toLegacyLiteral().getStringValueForArray()));
            } else {
                row.addColBuilder().setValue(String.format("\"%s\"",
                        ((Literal) expr).toLegacyLiteral().getStringValue()));
            }
        }
        return row.build();
    }

    private static void beginBatchInsertTransaction(ConnectContext ctx,
            String dbName, String tblName, List<Column> columns) {
        TransactionEntry txnEntry = ctx.getTxnEntry();
        if (txnEntry.isTransactionBegan()) {
            // FIXME: support mix usage of `insert into values` and `insert into select`
            throw new AnalysisException(
                    "Transaction insert can not insert into values and insert into select at the same time");
        }
        TTxnParams txnConf = txnEntry.getTxnConf();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        long timeoutSecond = ctx.getExecTimeout();
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(dbName, s -> new AnalysisException("database is invalid for dbName: " + s));
        Table tblObj = dbObj.getTableOrException(tblName, s -> new AnalysisException("table is invalid: " + s));
        txnConf.setDbId(dbObj.getId()).setTbl(tblName).setDb(dbName);
        txnEntry.setTable(tblObj);
        txnEntry.setDb(dbObj);
        String label = txnEntry.getLabel();
        try {
            long txnId;
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            if (Config.isCloudMode() || Env.getCurrentEnv().isMaster()) {
                txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                        txnConf.getDbId(), Lists.newArrayList(tblObj.getId()), label,
                        new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0,
                                FrontendOptions.getLocalHostAddress(),
                                ExecuteEnv.getInstance().getStartupTime()),
                        sourceType, timeoutSecond);
            } else {
                MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(ctx);
                TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
                request.setDb(txnConf.getDb()).setTbl(txnConf.getTbl()).setToken(token)
                        .setLabel(label).setUser("").setUserIp("").setPasswd("");
                TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
                txnId = result.getTxnId();
            }
            txnConf.setTxnId(txnId);
            txnConf.setToken(token);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        long maxExecMemByte = sessionVariable.getMaxExecMemByte();
        String timeZone = sessionVariable.getTimeZone();
        int sendBatchParallelism = sessionVariable.getSendBatchParallelism();
        request.setTxnId(txnConf.getTxnId())
                .setDb(txnConf.getDb())
                .setTbl(txnConf.getTbl())
                .setColumns(columns.stream()
                        .map(Column::getName)
                        .map(n -> n.replace("`", "``"))
                        .map(n -> "`" + n + "`")
                        .collect(Collectors.joining(",")))
                .setFileType(TFileType.FILE_STREAM)
                .setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND)
                .setThriftRpcTimeoutMs(5000)
                .setLoadId(ctx.queryId())
                .setExecMemLimit(maxExecMemByte)
                .setTimeout((int) timeoutSecond)
                .setTimezone(timeZone)
                .setSendBatchParallelism(sendBatchParallelism)
                .setTrimDoubleQuotes(true)
                .setSequenceCol(columns.stream()
                        .filter(c -> Column.SEQUENCE_COL.equalsIgnoreCase(c.getName()))
                        .map(Column::getName)
                        .findFirst()
                        .orElse(null));

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        try {
            executor.beginTransaction(request);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    /**
     * normalize plan to let it could be process correctly by nereids
     */
    public static Plan normalizePlan(Plan plan, TableIf table) {
        UnboundLogicalSink<? extends Plan> unboundLogicalSink = (UnboundLogicalSink<? extends Plan>) plan;
        if (table instanceof HMSExternalTable) {
            HMSExternalTable hiveTable = (HMSExternalTable) table;
            if (hiveTable.isView()) {
                throw new AnalysisException("View is not support in hive external table.");
            }
        }
        if (table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.UNIQUE_KEYS) {
            if (unboundLogicalSink instanceof UnboundTableSink
                    && ((UnboundTableSink<? extends Plan>) unboundLogicalSink).isPartialUpdate()) {
                // check the necessary conditions for partial updates
                OlapTable olapTable = (OlapTable) table;

                if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                    // when enable_unique_key_partial_update = true,
                    // only unique table with MOW insert with target columns can consider be a partial update,
                    // and unique table without MOW, insert will be like a normal insert.
                    ((UnboundTableSink<? extends Plan>) unboundLogicalSink).setPartialUpdate(false);
                } else {
                    if (unboundLogicalSink.getDMLCommandType() == DMLCommandType.INSERT) {
                        if (unboundLogicalSink.getColNames().isEmpty()) {
                            throw new AnalysisException("You must explicitly specify the columns to be updated when "
                                    + "updating partial columns using the INSERT statement.");
                        }
                        for (Column col : olapTable.getFullSchema()) {
                            Optional<String> insertCol = unboundLogicalSink.getColNames().stream()
                                    .filter(c -> c.equalsIgnoreCase(col.getName())).findFirst();
                            if (col.isKey() && !insertCol.isPresent()) {
                                throw new AnalysisException("Partial update should include all key columns, missing: "
                                        + col.getName());
                            }
                        }
                    }
                }
            }
        }
        Plan query = unboundLogicalSink.child();
        if (!(query instanceof LogicalInlineTable)) {
            return plan;
        }
        LogicalInlineTable logicalInlineTable = (LogicalInlineTable) query;
        ImmutableList.Builder<LogicalPlan> oneRowRelationBuilder = ImmutableList.builder();
        List<Column> columns = table.getBaseSchema(false);

        for (List<NamedExpression> values : logicalInlineTable.getConstantExprsList()) {
            ImmutableList.Builder<NamedExpression> constantExprs = ImmutableList.builder();
            if (values.isEmpty()) {
                if (CollectionUtils.isNotEmpty(unboundLogicalSink.getColNames())) {
                    throw new AnalysisException("value list should not be empty if columns are specified");
                }
                for (Column column : columns) {
                    constantExprs.add(generateDefaultExpression(column));
                }
            } else {
                if (CollectionUtils.isNotEmpty(unboundLogicalSink.getColNames())) {
                    if (values.size() != unboundLogicalSink.getColNames().size()) {
                        throw new AnalysisException("Column count doesn't match value count");
                    }
                    for (int i = 0; i < values.size(); i++) {
                        Column sameNameColumn = null;
                        for (Column column : table.getBaseSchema(true)) {
                            if (unboundLogicalSink.getColNames().get(i).equalsIgnoreCase(column.getName())) {
                                sameNameColumn = column;
                                break;
                            }
                        }
                        if (sameNameColumn == null) {
                            throw new AnalysisException("Unknown column '"
                                    + unboundLogicalSink.getColNames().get(i) + "' in target table.");
                        }
                        if (values.get(i) instanceof DefaultValueSlot) {
                            constantExprs.add(generateDefaultExpression(sameNameColumn));
                        } else {
                            DataType targetType = DataType.fromCatalogType(sameNameColumn.getType());
                            constantExprs.add((NamedExpression) castValue(values.get(i), targetType));
                        }
                    }
                } else {
                    if (values.size() != columns.size()) {
                        throw new AnalysisException("Column count doesn't match value count");
                    }
                    for (int i = 0; i < columns.size(); i++) {
                        if (values.get(i) instanceof DefaultValueSlot) {
                            constantExprs.add(generateDefaultExpression(columns.get(i)));
                        } else {
                            DataType targetType = DataType.fromCatalogType(columns.get(i).getType());
                            constantExprs.add((NamedExpression) castValue(values.get(i), targetType));
                        }
                    }
                }
            }
            oneRowRelationBuilder.add(new UnboundOneRowRelation(
                    StatementScopeIdGenerator.newRelationId(), constantExprs.build()));
        }
        List<LogicalPlan> oneRowRelations = oneRowRelationBuilder.build();
        if (oneRowRelations.size() == 1) {
            return plan.withChildren(oneRowRelations.get(0));
        } else {
            return plan.withChildren(
                    LogicalPlanBuilder.reduceToLogicalPlanTree(0, oneRowRelations.size() - 1,
                            oneRowRelations, Qualifier.ALL));
        }
    }

    private static Expression castValue(Expression value, DataType targetType) {
        if (value instanceof UnboundAlias) {
            return value.withChildren(TypeCoercionUtils.castUnbound(((UnboundAlias) value).child(), targetType));
        } else {
            return TypeCoercionUtils.castUnbound(value, targetType);
        }
    }

    /**
     * get target table from names.
     */
    public static TableIf getTargetTable(Plan plan, ConnectContext ctx) {
        UnboundLogicalSink<? extends Plan> unboundTableSink;
        if (plan instanceof UnboundTableSink) {
            unboundTableSink = (UnboundTableSink<? extends Plan>) plan;
        } else if (plan instanceof UnboundHiveTableSink) {
            unboundTableSink = (UnboundHiveTableSink<? extends Plan>) plan;
        } else if (plan instanceof UnboundIcebergTableSink) {
            unboundTableSink = (UnboundIcebergTableSink<? extends Plan>) plan;
        } else {
            throw new AnalysisException("the root of plan should be"
                    + " [UnboundTableSink, UnboundHiveTableSink, UnboundIcebergTableSink],"
                    + " but it is " + plan.getType());
        }
        List<String> tableQualifier = RelationUtil.getQualifierName(ctx, unboundTableSink.getNameParts());
        return RelationUtil.getDbAndTable(tableQualifier, ctx.getEnv()).second;
    }

    private static NamedExpression generateDefaultExpression(Column column) {
        try {
            if (column.getDefaultValue() == null) {
                throw new AnalysisException("Column has no default value, column=" + column.getName());
            }
            if (column.getDefaultValueExpr() != null) {
                Expression defualtValueExpression = new NereidsParser().parseExpression(
                        column.getDefaultValueExpr().toSqlWithoutTbl());
                if (!(defualtValueExpression instanceof UnboundAlias)) {
                    defualtValueExpression = new UnboundAlias(defualtValueExpression);
                }
                return (NamedExpression) defualtValueExpression;
            } else {
                return new Alias(Literal.of(column.getDefaultValue())
                        .checkedCastTo(DataType.fromCatalogType(column.getType())),
                        column.getName());
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    /**
     * get plan for explain.
     */
    public static Plan getPlanForExplain(ConnectContext ctx, LogicalPlan logicalQuery) {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        return InsertUtils.normalizePlan(logicalQuery, InsertUtils.getTargetTable(logicalQuery, ctx));
    }
}
